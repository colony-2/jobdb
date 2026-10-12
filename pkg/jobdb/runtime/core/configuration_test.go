package runtimecore_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type interceptedConfigurationStore struct {
	configuration.Store
	read func(context.Context, string) (jobdb.ConfigurationSnapshot, error)
}

func (s interceptedConfigurationStore) Read(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
	return s.read(ctx, tenant)
}
func TestCoreConfigurationRequiresStoreAndLiveLease(t *testing.T) {
	ctx := context.Background()
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	scheduler := &secretScheduler{row: runtimecore.StoredJob{JobKey: key, Store: jobdb.JobStoreActive, Lease: &runtimecore.LeaseIdentity{JobKey: key, LeaseID: "lease", WorkerID: "worker", ExpiresAt: time.Now().Add(time.Minute)}}}
	cfg := runtimecore.Config{Scheduler: scheduler, Chapters: &readTestChapters{}, Schemas: readTestSchemas{}}
	_, err := runtimecore.NewRuntime(cfg)
	require.ErrorContains(t, err, "configuration store is required")
	backing := &configuration.MemoryStore{}
	calls := 0
	store := interceptedConfigurationStore{Store: backing}
	store.read = func(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
		calls++
		if calls == 3 {
			scheduler.row.CancelRequested = true
		}
		return backing.Read(ctx, tenant)
	}
	cfg.Configurations = store
	rt, err := runtimecore.NewRuntime(cfg)
	require.NoError(t, err)
	_, err = rt.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "key", Value: json.RawMessage(`1`)})
	require.NoError(t, err)
	req := jobdb.GetConfigurationRequest{JobKey: key, LeaseID: "lease", LeaseToken: "untrusted"}
	_, err = rt.GetConfiguration(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Zero(t, calls)
	claims := leaseauth.Claims{TenantID: key.TenantId, JobID: key.JobId, LeaseID: "lease", WorkerID: "worker", ExpiresAt: time.Now().Add(time.Minute)}
	authorized := leaseauth.WithClaims(ctx, claims)
	first, err := rt.GetConfiguration(authorized, req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	second, err := lease.GetConfiguration(ctx)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 2, calls)
	discarded, err := lease.GetConfiguration(ctx)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Empty(t, discarded.Values)
	require.Equal(t, 3, calls)
	_, err = lease.GetConfiguration(ctx)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Equal(t, 3, calls)
}
func TestCoreConfigurationTokenExpiryAndDeadlineDiscardResults(t *testing.T) {
	ctx := context.Background()
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	scheduler := &secretScheduler{row: runtimecore.StoredJob{JobKey: key, Store: jobdb.JobStoreActive, Lease: &runtimecore.LeaseIdentity{JobKey: key, LeaseID: "lease", WorkerID: "worker", ExpiresAt: time.Now().Add(time.Minute)}}}
	store := interceptedConfigurationStore{Store: &configuration.MemoryStore{}, read: func(ctx context.Context, _ string) (jobdb.ConfigurationSnapshot, error) {
		<-ctx.Done()
		return configuration.Empty(), nil
	}}
	rt, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: scheduler, Chapters: &readTestChapters{}, Schemas: readTestSchemas{}, Configurations: store, ConfigurationLimits: jobdb.ConfigurationLimits{ReadTimeout: 20 * time.Millisecond}})
	require.NoError(t, err)
	req := jobdb.GetConfigurationRequest{JobKey: key, LeaseID: "lease"}
	claims := leaseauth.Claims{TenantID: "tenant", JobID: "job", LeaseID: "lease", WorkerID: "worker", ExpiresAt: time.Now().Add(time.Minute)}
	result, err := rt.GetConfiguration(leaseauth.WithClaims(ctx, claims), req)
	require.ErrorIs(t, err, jobdb.ErrConfigurationUnavailable)
	require.Empty(t, result.Revision)
	claims.ExpiresAt = time.Now().Add(5 * time.Millisecond)
	result, err = rt.GetConfiguration(leaseauth.WithClaims(ctx, claims), req)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Empty(t, result.Revision)
}
