package secretsbroker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type providerFunc func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error)

func (f providerFunc) ResolveSecrets(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	return f(ctx, req)
}

func TestLeaseBoundResultDeadlines(t *testing.T) {
	now := time.Now()
	capExpiry, backendExpiry := now.Add(time.Minute), now.Add(2*time.Minute)
	for _, tc := range []struct {
		name                              string
		bound                             bool
		expiry                            *time.Time
		backend, capability, after, final time.Time
		want                              error
	}{
		{name: "dynamic_at_capability", bound: true, expiry: &capExpiry},
		{name: "dynamic_over_capability", bound: true, expiry: &backendExpiry, want: jobdb.ErrSecretResolutionFailed},
		{name: "dynamic_missing_expiry", bound: true, want: jobdb.ErrSecretResolutionFailed},
		{name: "static_unbounded"},
		{name: "static_expiry_beyond_lease", expiry: &backendExpiry},
		{name: "backend_is_shorter", bound: true, expiry: &capExpiry, backend: capExpiry, capability: backendExpiry},
		{name: "dynamic_over_backend", bound: true, expiry: &backendExpiry, backend: capExpiry, capability: backendExpiry, want: jobdb.ErrSecretResolutionFailed},
		{name: "deadline_shrinks", bound: true, expiry: &capExpiry, after: now.Add(30 * time.Second), want: jobdb.ErrSecretResolutionFailed},
		{name: "deadline_shrinks_at_final_check", bound: true, expiry: &capExpiry, final: now.Add(30 * time.Second), want: jobdb.ErrSecretResolutionFailed},
		{name: "renewal_preserves_old_bound", bound: true, expiry: &capExpiry, after: now.Add(3 * time.Minute)},
		{name: "renewal_cannot_relax_initial_bound", bound: true, expiry: &backendExpiry, backend: capExpiry, capability: backendExpiry, after: backendExpiry, want: jobdb.ErrSecretResolutionFailed},
		{name: "expired_dynamic", bound: true, expiry: &now, want: jobdb.ErrSecretsUnavailable},
		{name: "expired_static", expiry: &now, want: jobdb.ErrSecretsUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, capability := tc.backend, tc.capability
			if backend.IsZero() {
				backend = backendExpiry
			}
			if capability.IsZero() {
				capability = capExpiry
			}
			access := Access{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, LeaseID: "lease", WorkerID: "worker", TokenExpiry: capability}
			checks := 0
			validate := func(context.Context) (Snapshot, error) {
				checks++
				expiry := backend
				if checks > 1 && !tc.after.IsZero() {
					expiry = tc.after
				}
				if checks > 2 && !tc.final.IsZero() {
					expiry = tc.final
				}
				return Snapshot{AppMetadata: []byte(`{"leaseExpiry":"forged"}`), LeaseExpiresAt: expiry}, nil
			}
			provider := providerFunc(func(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
				require.Equal(t, "tenant", req.TenantID)
				require.Equal(t, "job", req.JobID)
				require.Equal(t, "lease", req.Lease.LeaseID)
				require.Equal(t, "worker", req.Lease.WorkerID)
				require.Equal(t, backend, req.Lease.LeaseExpiresAt)
				want := backend
				if capability.Before(want) {
					want = capability
				}
				require.Equal(t, want, req.Lease.AuthorizedNotAfter)
				lookupDeadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.True(t, lookupDeadline.Before(want), "lookup timeout is separate from credential lifetime")
				// Providers cannot mutate the broker's trusted bound.
				req.Lease.AuthorizedNotAfter = now.Add(time.Hour)
				return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{
					"token":    {Value: "private", LeaseBound: tc.bound, ExpiresAt: tc.expiry},
					"password": {Value: "static"},
				}}, nil
			})
			result, err := Resolve(context.Background(), provider, jobdb.SecretLimits{}, access, []string{"token", "password"}, validate)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Empty(t, result.Secrets, "invalid dynamic values discard static values too")
			} else {
				require.NoError(t, err)
				require.Len(t, result.Secrets, 2)
				require.Equal(t, tc.bound, result.Secrets["token"].LeaseBound)
			}
		})
	}
}

func TestMissingLeaseDeadlineAndLostAuthority(t *testing.T) {
	for _, missing := range []string{"backend", "capability"} {
		for _, bound := range []bool{false, true} {
			t.Run(missing+"/"+map[bool]string{false: "static", true: "dynamic"}[bound], func(t *testing.T) {
				expiry := time.Now().Add(time.Minute)
				access := Access{JobKey: jobdb.JobKey{TenantId: "t", JobId: "j"}, LeaseID: "l", WorkerID: "w", TokenExpiry: expiry}
				snapshot := Snapshot{LeaseExpiresAt: expiry}
				if missing == "backend" {
					snapshot.LeaseExpiresAt = time.Time{}
				} else {
					access.TokenExpiry = time.Time{}
				}
				provider := providerFunc(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
					require.True(t, req.Lease.AuthorizedNotAfter.IsZero())
					return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "value", ExpiresAt: &expiry, LeaseBound: bound}}}, nil
				})
				result, err := Resolve(context.Background(), provider, jobdb.SecretLimits{}, access, []string{"token"}, func(context.Context) (Snapshot, error) { return snapshot, nil })
				if bound {
					require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
					require.Empty(t, result.Secrets)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
	for _, lostAt := range []int{2, 3} {
		expiry := time.Now().Add(time.Minute)
		checks := 0
		result, err := Resolve(context.Background(), providerFunc(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
			return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "private", ExpiresAt: &expiry, LeaseBound: true}}}, nil
		}), jobdb.SecretLimits{}, Access{JobKey: jobdb.JobKey{TenantId: "t", JobId: "j"}, LeaseID: "l", WorkerID: "w", TokenExpiry: expiry}, []string{"token"}, func(context.Context) (Snapshot, error) {
			checks++
			if checks == lostAt {
				return Snapshot{}, jobdb.ErrExecutionLeaseLost
			}
			return Snapshot{LeaseExpiresAt: expiry}, nil
		})
		require.True(t, errors.Is(err, jobdb.ErrExecutionLeaseLost))
		require.Empty(t, result.Secrets)
	}
}
