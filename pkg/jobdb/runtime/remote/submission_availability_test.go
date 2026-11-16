package remote

import (
	"context"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestSubmissionAvailabilityConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunSubmissionAvailabilityConformance)
}

func assertRemoteAvailabilityAbsent(t *testing.T, rt jobdb.WorkflowRuntime, id string) {
	t.Helper()
	ctx := celltest.Context(context.Background())
	key := jobdb.JobKey{TenantId: "tenant", JobId: id}
	_, err := rt.GetJob(ctx, key)
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	rows, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{key}})
	require.NoError(t, err)
	require.Empty(t, rows.Jobs)
	_, err = rt.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: key, WaitID: "submission"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
}

func TestHTTPSubmissionAvailabilityRangeAndRetries(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	backend, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close(ctx)) })
	application := NewServer(backend)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
	}))
	t.Cleanup(server.Close)
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	p := goalSubmit(t, rt, "producer")
	parent, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: p.JobKey, WorkerID: "parent", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, parent)
	min := time.Date(1677, 9, 21, 0, 12, 43, 145224192, time.UTC)
	max := time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC)
	for _, selected := range []bool{false, true} {
		for i, at := range []time.Time{min, time.Unix(0, 0).UTC(), max, min.Add(-time.Nanosecond), max.Add(time.Nanosecond), time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC), {}} {
			t.Run(fmt.Sprintf("selected=%v/case=%d", selected, i), func(t *testing.T) {
				req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: fmt.Sprintf("consumer-%v-%d", selected, i), JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), AvailableAt: &at, Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqComplete}}}}
				if selected {
					req.Job.GoalID = p.GoalID
				}
				before, err := rt.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant"})
				require.NoError(t, err)
				h, err := rt.SubmitJob(ctx, req)
				if i >= 3 {
					require.ErrorContains(t, err, "availableAt")
					assertRemoteAvailabilityAbsent(t, rt, req.Job.JobID)
					after, err := rt.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant"})
					require.NoError(t, err)
					require.Equal(t, before, after)
					req.Job.JobID += "-lease"
					_, err = parent.SubmitJob(ctx, req)
					if selected {
						require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
					} else {
						require.ErrorContains(t, err, "availableAt")
					}
					assertRemoteAvailabilityAbsent(t, rt, req.Job.JobID)
					return
				}
				require.NoError(t, err)
				equiv := at.In(time.FixedZone("offset", 3600))
				req.Job.AvailableAt = &equiv
				again, err := rt.SubmitJob(ctx, req)
				require.NoError(t, err)
				require.Equal(t, h, again)
				rows, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{h.JobKey}})
				require.NoError(t, err)
				require.Len(t, rows.Jobs, 1)
				require.True(t, at.Equal(rows.Jobs[0].AvailableAt))
				bad := time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
				req.Job.AvailableAt = &bad
				_, err = rt.SubmitJob(ctx, req)
				require.ErrorContains(t, err, "availableAt")
			})
		}
	}
}
