package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/crashconcern"
	"github.com/stretchr/testify/require"
)

func TestCrashConcernPersistenceAndReset(t *testing.T) {
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "job.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(ctx) })
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
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

	// Test a live lease with prior crashes successfully rescheduling. Resetting
	// the counter here models explicit operator recovery for the first claim.
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET consecutive_expirations=0`)
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
	ctx := context.Background()
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
}

func TestVersionThreeUpgradePreservesJobs(t *testing.T) {
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "job.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "existing", JobType: "job", Data: jobdb.NewTaskDataOrPanic(42)}})
	require.NoError(t, err)
	_, err = r.db.ExecContext(ctx, `ALTER TABLE jobdb_jobs DROP COLUMN consecutive_expirations; PRAGMA user_version=3;`)
	require.NoError(t, err)
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	row, err := r.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	require.Zero(t, row.consecutiveExpirations)
	chapter, err := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 0})
	require.NoError(t, err)
	require.NotNil(t, chapter.Body)
	var version int
	require.NoError(t, r.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version))
	require.Equal(t, 4, version)
}
