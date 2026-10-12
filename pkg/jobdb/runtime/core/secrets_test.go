package runtimecore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
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
	if s.row.Lease == nil || identity != *s.row.Lease || !time.Now().Before(identity.ExpiresAt) {
		return runtimecore.LeaseSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	return runtimecore.LeaseSnapshot{Identity: identity}, nil
}
func (s *secretScheduler) AcquireJobLease(context.Context, runtimecore.JobLeaseRequest) (*runtimecore.LeaseSnapshot, error) {
	return &runtimecore.LeaseSnapshot{Identity: *s.row.Lease, RouteJobType: "job"}, nil
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
		return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: fmt.Sprint(calls)}}}, nil
	})
	runtime, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: scheduler, Chapters: &readTestChapters{}, Schemas: readTestSchemas{}, SecretProvider: provider})
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
