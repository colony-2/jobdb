package runtimecore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type secretScheduler struct {
	runtimecore.Scheduler
	row runtimecore.StoredJob
}

func (s *secretScheduler) GetJob(context.Context, jobdb.JobKey) (runtimecore.StoredJob, error) {
	return s.row, nil
}
func (s *secretScheduler) ValidateLease(_ context.Context, identity runtimecore.LeaseIdentity) (runtimecore.LeaseSnapshot, error) {
	if s.row.Lease == nil || identity.JobKey != s.row.Lease.JobKey || identity.LeaseID != s.row.Lease.LeaseID || identity.WorkerID != s.row.Lease.WorkerID || !time.Now().Before(s.row.Lease.ExpiresAt) {
		return runtimecore.LeaseSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	return runtimecore.LeaseSnapshot{Identity: identity}, nil
}
func (s *secretScheduler) AcquireJobLease(context.Context, runtimecore.JobLeaseRequest) (*runtimecore.LeaseSnapshot, error) {
	return &runtimecore.LeaseSnapshot{Identity: *s.row.Lease, RouteJobType: "job"}, nil
}

func (s *secretScheduler) KeepAliveLease(ctx context.Context, req runtimecore.LeaseMutation) (runtimecore.LeaseSnapshot, error) {
	if _, err := s.ValidateLease(ctx, req.Identity); err != nil {
		return runtimecore.LeaseSnapshot{}, err
	}
	s.row.Lease.ExpiresAt = req.Now.Add(time.Minute)
	return runtimecore.LeaseSnapshot{Identity: *s.row.Lease, RouteJobType: "job", Duration: time.Minute}, nil
}

type coreSecretProvider func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error)

func (f coreSecretProvider) ResolveSecrets(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	return f(ctx, req)
}

func TestCoreSecretsUseIndependentProvider(t *testing.T) {
	ctx := context.Background()
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	scheduler := &secretScheduler{row: runtimecore.StoredJob{JobKey: key, Store: jobdb.JobStoreActive, AppMetadata: json.RawMessage(`{"app":"test"}`), Lease: &runtimecore.LeaseIdentity{JobKey: key, LeaseID: "lease", WorkerID: "worker", ExpiresAt: time.Now().Add(time.Minute)}}}
	var calls int
	var last jobdb.SecretResolveRequest
	provider := coreSecretProvider(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		calls++
		last = req
		req.AppMetadata[0] = 'x'
		if calls == 4 {
			scheduler.row.CancelRequested = true
		}
		expiry := req.Lease.AuthorizedNotAfter
		return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: fmt.Sprint(calls), LeaseBound: true, ExpiresAt: &expiry}}}, nil
	})
	runtime, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: scheduler, Chapters: &readTestChapters{}, Schemas: readTestSchemas{}, SecretProvider: provider})
	require.NoError(t, err)
	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	for i := 1; i <= 2; i++ {
		result, err := lease.ResolveSecrets(ctx, []string{"token"})
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(i), result.Secrets["token"].Value)
		require.Nil(t, last.Principal)
		require.Equal(t, key.TenantId, last.TenantID)
		require.Equal(t, key.JobId, last.JobID)
		require.Equal(t, "lease", last.Lease.LeaseID)
		require.Equal(t, "worker", last.Lease.WorkerID)
		require.Equal(t, scheduler.row.Lease.ExpiresAt, last.Lease.LeaseExpiresAt)
		require.Equal(t, lease.(jobdb.RenewableExecutionLease).LeaseExpiry(), last.Lease.AuthorizedNotAfter)
		require.True(t, result.Secrets["token"].LeaseBound)
		require.JSONEq(t, `{"app":"test"}`, string(scheduler.row.AppMetadata))
	}
	request := jobdb.ResolveSecretsRequest{JobKey: key, LeaseID: "lease", Names: []string{"token"}}
	_, err = runtime.ResolveSecrets(ctx, request)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Equal(t, 2, calls)
	principal := "verified"
	authorized := leaseauth.WithClaims(ctx, leaseauth.Claims{TenantID: key.TenantId, JobID: key.JobId, LeaseID: "lease", WorkerID: "worker", ExpiresAt: time.Now().Add(time.Minute), Principal: &principal})
	_, err = runtime.ResolveSecrets(authorized, request)
	require.NoError(t, err)
	require.Equal(t, &principal, last.Principal)
	result, err := lease.ResolveSecrets(ctx, []string{"token"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Empty(t, result.Secrets)
}

func TestCoreLeaseHandleExpiryAfterIndependentRenewal(t *testing.T) {
	for _, duringLookup := range []bool{false, true} {
		t.Run(fmt.Sprintf("during_lookup=%t", duringLookup), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
			scheduler := &secretScheduler{row: runtimecore.StoredJob{JobKey: key, Store: jobdb.JobStoreActive, Lease: &runtimecore.LeaseIdentity{JobKey: key, LeaseID: "lease", WorkerID: "worker", ExpiresAt: time.Now().Add(500 * time.Millisecond)}}}
			calls := 0
			provider := coreSecretProvider(func(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
				calls++
				if req.Names[0] == "slow" {
					<-ctx.Done()
				}
				return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{req.Names[0]: {Value: "value"}}}, nil
			})
			runtime, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: scheduler, Chapters: &readTestChapters{}, Schemas: readTestSchemas{}, SecretProvider: provider})
			require.NoError(t, err)
			issued, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
			require.NoError(t, err)
			old := issued.(jobdb.RenewableExecutionLease)
			t.Cleanup(old.StopKeepAlive)
			current, err := old.Renew(ctx)
			require.NoError(t, err)
			require.True(t, current.LeaseExpiry().After(old.LeaseExpiry()))
			if duringLookup {
				result, err := old.ResolveSecrets(ctx, []string{"slow"})
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				require.Empty(t, result.Secrets)
				require.Equal(t, 1, calls)
			} else {
				time.Sleep(time.Until(old.LeaseExpiry()) + time.Millisecond)
			}
			before := calls
			result, err := old.ResolveSecrets(ctx, []string{"token"})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.Empty(t, result.Secrets)
			require.Equal(t, before, calls)
			_, err = old.GetConfiguration(ctx)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			_, err = old.Renew(ctx)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.ErrorIs(t, old.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
			require.ErrorIs(t, old.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}), jobdb.ErrExecutionLeaseLost)
			require.ErrorIs(t, old.Complete(ctx, jobdb.CompleteExecutionRequest{}), jobdb.ErrExecutionLeaseLost)
			_, err = old.SubmitJob(ctx, jobdb.SubmitJobRequest{})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			_, err = old.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			result, err = current.ResolveSecrets(ctx, []string{"token"})
			require.NoError(t, err)
			require.Equal(t, "value", result.Secrets["token"].Value)
			_, err = current.Renew(ctx)
			require.NoError(t, err)
		})
	}
}
