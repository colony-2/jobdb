package workflow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestFreshSecretsReplaceOnlyRequestedCacheEntries(t *testing.T) {
	ctx := context.Background()
	var requests [][]string
	omit := map[string]bool{"denied": true}
	failure := false
	lease := &cacheTestLease{secrets: func(_ context.Context, names []string) (SecretResolveResult, error) {
		requests = append(requests, append([]string(nil), names...))
		if failure {
			return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
		}
		result := SecretResolveResult{Secrets: map[string]SecretValue{}}
		for _, name := range names {
			if !omit[name] {
				result.Secrets[name] = SecretValue{Value: fmt.Sprint(len(requests))}
			}
		}
		return result, nil
	}}
	runner := &workerRunner{lease: lease}
	_, err := runner.ResolveSecrets(ctx, []string{"token", "untouched", "denied"})
	require.NoError(t, err)
	// Fresh reads recover both expired values and cached omissions.
	expired := time.Now().Add(-time.Second)
	runner.secretCache.value["token"].ExpiresAt = &expired
	omit["denied"] = false
	fresh, err := runner.ResolveSecretsFresh(ctx, []string{"token", "denied"})
	require.NoError(t, err)
	require.Equal(t, "2", fresh.Secrets["token"].Value)
	cached, err := runner.ResolveSecrets(ctx, []string{"token", "denied", "untouched"})
	require.NoError(t, err)
	require.Equal(t, "2", cached.Secrets["denied"].Value)
	require.Equal(t, "1", cached.Secrets["untouched"].Value)
	require.Equal(t, [][]string{{"denied", "token", "untouched"}, {"denied", "token"}}, requests)
	// Another fresh call must invoke resolution even though the cache is valid.
	omit["token"] = true
	_, err = runner.ResolveSecretsFresh(ctx, []string{"token"})
	require.NoError(t, err)
	cached, err = runner.ResolveSecrets(ctx, []string{"token", "untouched"})
	require.NoError(t, err)
	require.NotContains(t, cached.Secrets, "token")
	require.Equal(t, "1", cached.Secrets["untouched"].Value)
	failure = true
	_, err = runner.ResolveSecretsFresh(ctx, []string{"denied"})
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
	cached, err = runner.ResolveSecrets(ctx, []string{"denied"})
	require.NoError(t, err)
	require.Equal(t, "2", cached.Secrets["denied"].Value)
	require.Len(t, requests, 4)
	// The original caller-owned result and other executions' caches are independent.
	require.Equal(t, "2", fresh.Secrets["token"].Value)
	failure = false
	other := &workerRunner{lease: lease}
	_, err = other.ResolveSecrets(ctx, []string{"denied"})
	require.NoError(t, err)
	require.Len(t, requests, 5)
}

func TestFreshSecretsRejectLateOrInvalidResultsWithoutUpdatingCache(t *testing.T) {
	for _, kind := range []string{"cancel", "handoff", "expired", "replay", "closed"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runner := &workerRunner{ctx: ctx}
			calls := 0
			runner.lease = &cacheTestLease{secrets: func(ctx context.Context, names []string) (SecretResolveResult, error) {
				calls++
				value := SecretValue{Value: fmt.Sprint(calls)}
				if calls > 1 {
					switch kind {
					case "cancel":
						cancel()
						<-ctx.Done()
					case "handoff":
						cancel() // Suspension cancels the execution context.
					case "expired":
						expiry := time.Now().Add(-time.Second)
						value.ExpiresAt = &expiry
					}
				}
				return SecretResolveResult{Secrets: map[string]SecretValue{"token": value}}, nil
			}}
			_, err := runner.ResolveSecrets(context.Background(), []string{"token"})
			require.NoError(t, err)
			if kind == "replay" {
				runner.replay = true
			}
			if kind == "closed" {
				cancel()
			}
			result, err := runner.ResolveSecretsFresh(context.Background(), []string{"token"})
			require.Error(t, err)
			require.Empty(t, result.Secrets)
			require.Equal(t, "1", runner.secretCache.value["token"].Value)
			if kind == "replay" || kind == "closed" {
				require.Equal(t, 1, calls)
			}
		})
	}
	_, err := (TaskContext{}).ResolveSecretsFresh(context.Background(), []string{"token"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	_, ok := SecretResolverFromContext(context.Background())
	require.False(t, ok)
}

func TestConcurrentFreshSecretReadsAreNotCoalesced(t *testing.T) {
	var calls atomic.Int32
	runner := &workerRunner{lease: &cacheTestLease{secrets: func(context.Context, []string) (SecretResolveResult, error) {
		return SecretResolveResult{Secrets: map[string]SecretValue{"token": {Value: fmt.Sprint(calls.Add(1))}}}, nil
	}}}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := runner.ResolveSecretsFresh(context.Background(), []string{"token"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(8), calls.Load())
	result, err := runner.ResolveSecrets(context.Background(), []string{"token"})
	require.NoError(t, err)
	require.Equal(t, "8", result.Secrets["token"].Value)
}
