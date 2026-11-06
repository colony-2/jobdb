package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/crashconcern"
	"github.com/stretchr/testify/require"
)

func TestCrashConcernPersistenceAndReset(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "job.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(ctx) })
	h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobID: "job", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	claim := func() jobdb.ExecutionLease {
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
		require.NoError(t, err)
		require.NotNil(t, lease)
		return lease
	}
	for n := 0; n <= crashconcern.DefaultThreshold; n++ {
		lease := claim()
		_, err := lease.(jobdb.RenewableExecutionLease).Renew(ctx)
		require.NoError(t, err)
		row, err := r.loadJobRow(ctx, h.JobKey)
		require.NoError(t, err)
		require.EqualValues(t, n, row.consecutiveExpirations, "renewal preserves the count")
		_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET lease_expires_at_ns=0`)
		require.NoError(t, err)
		// A daemon restart must neither lose nor double-count an expiry.
		require.NoError(t, r.Close(ctx))
		r, err = NewFromConfig(ctx, cfg)
		require.NoError(t, err)
	}
	info, err := r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusCrashConcern, info.Status)

	before, err := r.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	require.NoError(t, r.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: h.JobKey}))
	after, err := r.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	before.consecutiveExpirations = 0
	require.Equal(t, before, after, "clear changes only the consecutive count")
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	lease := claim()
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}))
	row, err := r.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	require.Zero(t, row.consecutiveExpirations)
	claim()
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET lease_expires_at_ns=0`)
	require.NoError(t, err)
	info, err = r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusReady, info.Status)
}

func TestCrashConcernBlocksTaskAndAlternateClaims(t *testing.T) {
	ctx := celltest.Context(context.Background())
	r, _, req := waitingTaskFixture(t)
	_, err := r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET consecutive_expirations=?,lease_id='expired',lease_expires_at_ns=0,
alternate_job_type='job',alternate_task_type='fallback',alternate_at_ns=?`, crashconcern.DefaultThreshold, time.Now().Add(-time.Second).UnixNano())
	require.NoError(t, err)
	before, err := r.loadJobRow(ctx, req.JobKey)
	require.NoError(t, err)
	for _, route := range []jobdb.Route{req.Route, {JobType: "job", TaskType: "fallback"}} {
		leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: req.JobKey.TenantId, WorkerID: "worker", Routes: []jobdb.Route{route}})
		require.NoError(t, err)
		require.Empty(t, leases)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: req.JobKey, WorkerID: "worker", Routes: []jobdb.Route{route}})
		require.NoError(t, err)
		require.Nil(t, lease)
	}
	require.ErrorIs(t, r.CompleteTaskIfWaiting(ctx, req), jobdb.ErrConflict)
	_, err = r.GetChapter(ctx, jobdb.ChapterRef{JobKey: req.JobKey, Ordinal: 1})
	require.ErrorIs(t, err, jobdb.ErrChapterNotFound)
	after, err := r.loadJobRow(ctx, req.JobKey)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, r.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: req.JobKey}))
	leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: req.JobKey.TenantId, WorkerID: "recovered", Routes: []jobdb.Route{{JobType: "job", TaskType: "fallback"}}})
	require.NoError(t, err)
	require.Len(t, leases, 1, "clearing also recovers task work")
}

func TestClearCrashConcernPreservesSchedulingAndRejectsFinalJobs(t *testing.T) {
	ctx := celltest.Context(context.Background())
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "job.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(ctx) })
	h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	req := jobdb.ClearCrashConcernRequest{JobKey: h.JobKey}
	waitFor, err := encodeWaitFor([]string{"dependency"})
	require.NoError(t, err)
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET consecutive_expirations=5, lease_id='old', lease_worker_id='old', lease_expires_at_ns=0,
available_at_ns=?, wait_for=?`, time.Now().Add(time.Hour).UnixNano(), waitFor)
	require.NoError(t, err)
	before, err := r.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	require.NoError(t, r.ClearCrashConcern(ctx, req))
	after, err := r.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	before.consecutiveExpirations = 0
	require.Equal(t, before, after)
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	require.Nil(t, lease, "clearing does not bypass other scheduling constraints")
	for _, column := range []string{"lease_expires_at_ns", "archived_at_ns", "cancel_requested", "hidden_at_ns"} {
		t.Run(column, func(t *testing.T) {
			_, err := r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET consecutive_expirations=5, lease_expires_at_ns=0, archived_at_ns=NULL, cancel_requested=0, hidden_at_ns=NULL`)
			require.NoError(t, err)
			value := time.Now().Add(time.Hour).UnixNano()
			if column == "cancel_requested" {
				value = 1
			}
			_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET `+column+`=?`, value)
			require.NoError(t, err)
			require.ErrorIs(t, r.ClearCrashConcern(ctx, req), jobdb.ErrConflict)
			var count int
			require.NoError(t, r.db.QueryRowContext(ctx, `SELECT consecutive_expirations FROM jobdb_jobs`).Scan(&count))
			require.Equal(t, 5, count)
		})
	}
}
