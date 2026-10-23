package secretproviders

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type providerFunc func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error)

func (f providerFunc) ResolveSecrets(ctx context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	return f(ctx, r)
}
func values(v map[string]jobdb.SecretValue) jobdb.SecretResolveResult {
	return jobdb.SecretResolveResult{Secrets: v}
}

func TestFirstProviderWinsAfterEveryResponse(t *testing.T) {
	laterReturned := make(chan struct{})
	principal := "verified"
	expiry := time.Now().Add(time.Minute)
	req := jobdb.SecretResolveRequest{Names: []string{"overlap", "only_later"}, Principal: &principal, AppMetadata: []byte(`{"x":1}`), Lease: jobdb.SecretLeaseContext{LeaseID: "lease", WorkerID: "worker", LeaseExpiresAt: expiry, AuthorizedNotAfter: expiry}}
	var observations []Selection
	first := providerFunc(func(_ context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		<-laterReturned
		r.Names[0] = "mutated"
		*r.Principal = "mutated"
		r.AppMetadata[0] = '!'
		return values(map[string]jobdb.SecretValue{"overlap": {Value: "", Version: "local-version", ProviderID: "forged"}}), nil
	})
	second := providerFunc(func(_ context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		require.Equal(t, req, r)
		defer close(laterReturned)
		return values(map[string]jobdb.SecretValue{"overlap": {Value: "external", Version: "external-version", ExpiresAt: &expiry, LeaseBound: true}, "only_later": {Value: "later"}}), nil
	})
	c, err := New([]Member{{"local", first}, {"external", second}}, Options{Observe: func(s Selection) { observations = append(observations, s) }})
	require.NoError(t, err)
	r, err := c.ResolveSecrets(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, jobdb.SecretValue{Value: "", Version: "local-version", ProviderID: "local"}, r.Secrets["overlap"])
	require.Equal(t, "external", r.Secrets["only_later"].ProviderID)
	require.Equal(t, "verified", principal)
	require.JSONEq(t, `{"x":1}`, string(req.AppMetadata))
	require.Equal(t, "overlap", req.Names[0])
	require.Len(t, observations, 2)
}

func TestInvalidShadowedResponsesFailEntireCollection(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	tooLate := deadline.Add(time.Minute)
	expired := time.Now().Add(-time.Minute)
	for _, tc := range []struct {
		name   string
		result jobdb.SecretResolveResult
		err    error
		want   error
	}{
		{"error", values(map[string]jobdb.SecretValue{"token": {Value: "sensitive"}}), errors.New("sensitive"), jobdb.ErrSecretResolutionFailed},
		{"unavailable", jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable, jobdb.ErrSecretsUnavailable},
		{"extra", values(map[string]jobdb.SecretValue{"extra": {Value: "sensitive"}}), nil, jobdb.ErrSecretResolutionFailed},
		{"overlong", values(map[string]jobdb.SecretValue{"token": {Value: "sensitive", LeaseBound: true, ExpiresAt: &tooLate}}), nil, jobdb.ErrSecretResolutionFailed},
		{"missing_expiry", values(map[string]jobdb.SecretValue{"token": {Value: "sensitive", LeaseBound: true}}), nil, jobdb.ErrSecretResolutionFailed},
		{"expired", values(map[string]jobdb.SecretValue{"token": {Value: "sensitive", ExpiresAt: &expired}}), nil, jobdb.ErrSecretsUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			c, err := New([]Member{
				{"local", providerFunc(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
					calls.Add(1)
					return values(map[string]jobdb.SecretValue{"token": {Value: "safe"}}), nil
				})},
				{"bad", providerFunc(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
					calls.Add(1)
					return tc.result, tc.err
				})},
				{"last", providerFunc(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
					calls.Add(1)
					return jobdb.SecretResolveResult{}, nil
				})},
			}, Options{MaxParallel: 1})
			require.NoError(t, err)
			r, err := c.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"token"}, Lease: jobdb.SecretLeaseContext{LeaseExpiresAt: deadline, AuthorizedNotAfter: deadline}})
			require.ErrorIs(t, err, tc.want)
			require.Empty(t, r.Secrets)
			require.NotContains(t, err.Error(), "sensitive")
			require.EqualValues(t, 3, calls.Load())
		})
	}
}

func TestCollectionBoundsAndCancellation(t *testing.T) {
	makeProvider := func(name string) providerFunc {
		return func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
			return values(map[string]jobdb.SecretValue{name: {Value: strings.Repeat("a", 50)}}), nil
		}
	}
	for _, same := range []bool{false, true} {
		name := "b"
		if same {
			name = "a"
		}
		c, err := New([]Member{{"first", makeProvider("a")}, {"second", makeProvider(name)}}, Options{Limits: jobdb.SecretLimits{MaxResponseBytes: 140}})
		require.NoError(t, err)
		r, err := c.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"a", "b"}})
		if same {
			require.NoError(t, err)
			require.Len(t, r.Secrets, 1)
		} else {
			require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
			require.Empty(t, r.Secrets)
		}
	}
	started := make(chan struct{})
	stopped := make(chan struct{})
	c, err := New([]Member{{"slow", providerFunc(func(ctx context.Context, _ jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return jobdb.SecretResolveResult{}, ctx.Err()
	})}}, Options{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.ResolveSecrets(ctx, jobdb.SecretResolveRequest{Names: []string{"token"}})
		done <- err
	}()
	<-started
	cancel()
	require.ErrorIs(t, <-done, jobdb.ErrSecretsUnavailable)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("provider not canceled")
	}
}

func TestCollectionRejectsInvalidConfiguration(t *testing.T) {
	var nilFunc providerFunc
	for _, members := range [][]Member{nil, {{"x", nil}}, {{"x", nilFunc}}, {{"bad id", providerFunc(nil)}}, {{"x", providerFunc(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		return jobdb.SecretResolveResult{}, nil
	})}, {"x", nil}}} {
		_, err := New(members, Options{})
		require.Error(t, err)
	}
}
