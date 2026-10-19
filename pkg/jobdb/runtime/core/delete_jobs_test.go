package runtimecore_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type deleteTestScheduler struct {
	runtimecore.Scheduler
	request runtimecore.DeleteJobsRequest
	calls   int
}

func (s *deleteTestScheduler) DeleteJobs(_ context.Context, req runtimecore.DeleteJobsRequest) (jobdb.DeleteJobsResponse, error) {
	s.request = req
	s.calls++
	return jobdb.DeleteJobsResponse{MatchedCount: 3}, nil
}

func TestRuntimeJobDeletionPort(t *testing.T) {
	ctx := context.Background()
	store := &deleteTestScheduler{}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	cfg := runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: store, Chapters: noSummaryHistory{}, Schemas: readTestSchemas{}, Now: func() time.Time { return now }}
	rt, err := runtimecore.NewRuntime(cfg)
	require.NoError(t, err)
	for _, req := range []jobdb.DeleteJobsRequest{{All: true}, {TenantID: "tenant"}, {TenantID: "tenant", All: true, JobIDs: []string{"job"}}, {TenantID: "tenant", JobIDs: []string{""}}} {
		_, err = rt.DeleteJobs(ctx, req)
		require.ErrorIs(t, err, jobdb.ErrInvalidJobDeletion)
	}
	require.Zero(t, store.calls)
	filter, err := jobdb.Metadata().EqualFilter("recipe", "build")
	require.NoError(t, err)
	cutoff := now.Add(-24 * time.Hour)
	result, err := rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{"job"}, MetadataFilter: filter, CreatedBefore: &cutoff, DryRun: true})
	require.NoError(t, err)
	require.Equal(t, jobdb.DeleteJobsResponse{MatchedCount: 3}, result)
	require.Equal(t, runtimecore.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{"job"}, CreatedBefore: &cutoff, MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"recipe"}, Values: []any{"build"}}}, DryRun: true, Now: now}, store.request)
	cfg.Scheduler = listTestScheduler{}
	rt, err = runtimecore.NewRuntime(cfg)
	require.NoError(t, err)
	_, err = rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", All: true})
	require.ErrorIs(t, err, jobdb.ErrJobDeletionUnsupported)
}
