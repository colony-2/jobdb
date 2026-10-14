package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestJoinDeadlineSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
	rt, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	submit := func(id string) jobdb.JobKey {
		h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, JobType: "job", Data: jobdb.NewTaskDataOrPanic(id)}})
		require.NoError(t, err)
		return h.JobKey
	}
	child := submit("child")
	parent := submit("parent")
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent, WorkerID: "first", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	deadline := time.Now().UTC().Add(time.Hour)
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}, WaitForJobIDs: []string{child.JobId}, WaitUntil: &deadline}))
	require.NoError(t, rt.Close(ctx))
	rt, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer rt.Close(ctx)
	row, err := rt.loadJobRow(ctx, parent)
	require.NoError(t, err)
	require.Equal(t, deadline.UnixNano(), row.waitDeadlineNS.Int64)
	check := func(now time.Time, want jobdb.JobStatus) {
		t.Helper()
		status, err := statusFromRow(ctx, rt.db, row, now)
		require.NoError(t, err)
		require.Equal(t, want, status)
	}
	check(deadline.Add(-time.Nanosecond), jobdb.JobStatusPendingJobs)
	check(deadline, jobdb.JobStatusReady)
	check(deadline.Add(time.Nanosecond), jobdb.JobStatusReady)
	require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: child}))
	check(deadline.Add(-time.Minute), jobdb.JobStatusReady)
	// Submission's AvailableAt remains a not-before restriction, even with dependencies.
	row.waitDeadlineNS.Valid = false
	row.availableAtNS = deadline.UnixNano()
	check(deadline.Add(-time.Nanosecond), jobdb.JobStatusAwaitingFuture)
	check(deadline, jobdb.JobStatusReady)
}

func TestVersionFourJoinDeadlineMigration(t *testing.T) {
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
	rt, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "job", Data: jobdb.NewTaskDataOrPanic("existing")}})
	require.NoError(t, err)
	_, err = rt.db.ExecContext(ctx, `ALTER TABLE jobdb_jobs DROP COLUMN wait_deadline_ns; PRAGMA user_version=4;`)
	require.NoError(t, err)
	require.NoError(t, rt.Close(ctx))
	rt, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer rt.Close(ctx)
	row, err := rt.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	require.False(t, row.waitDeadlineNS.Valid)
	info, err := rt.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusReady, info.Status)
}
