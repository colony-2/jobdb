package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestDeletionPersistsAndRollsBackAtomically(t *testing.T) {
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
	rt, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	for _, id := range []string{"a", "b"} {
		_, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
	}
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "a"}, Routes: []jobdb.Route{{JobType: "job"}}, WorkerID: "holder", LeaseDuration: time.Hour})
	require.NoError(t, err)
	require.NotNil(t, lease)
	_, err = rt.db.ExecContext(ctx, `CREATE TRIGGER reject_delete BEFORE UPDATE OF hidden_at_ns ON jobdb_jobs WHEN NEW.job_id='b' BEGIN SELECT RAISE(ABORT, 'injected failure'); END`)
	require.NoError(t, err)
	result, err := rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", All: true, DryRun: true})
	require.NoError(t, err)
	require.EqualValues(t, 2, result.MatchedCount)
	result, err = rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", All: true})
	require.ErrorContains(t, err, "injected failure")
	require.Zero(t, result.DeletedCount)
	listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
	require.NoError(t, err)
	require.Len(t, listed.Jobs, 2)
	require.NoError(t, lease.KeepAlive(ctx), "failed deletion must roll back lease revocation too")
	_, err = rt.db.ExecContext(ctx, `DROP TRIGGER reject_delete`)
	require.NoError(t, err)
	result, err = rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", All: true})
	require.NoError(t, err)
	require.EqualValues(t, 2, result.DeletedCount)
	require.NoError(t, rt.Close(ctx))
	rt, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer rt.Close(ctx)
	listed, err = rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
	require.NoError(t, err)
	require.Empty(t, listed.Jobs)
	listed, err = rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true})
	require.NoError(t, err)
	require.Len(t, listed.Jobs, 2)
	for _, j := range listed.Jobs {
		require.Equal(t, jobdb.JobStatusCancelled, j.Status)
		_, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: j.JobKey, Ordinal: 0})
		require.NoError(t, err)
	}
}
