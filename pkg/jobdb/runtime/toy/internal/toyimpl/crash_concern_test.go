package toyimpl

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/crashconcern"
	"github.com/stretchr/testify/require"
)

func TestCrashConcernBlocksTaskAndAlternateClaims(t *testing.T) {
	ctx := context.Background()
	r := New()
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	primary := jobdb.Route{JobType: "job", TaskType: "external"}
	fallback := jobdb.Route{JobType: "job", TaskType: "fallback"}
	zero := time.Duration(0)
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
		NextRoute: primary, TaskWait: &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "input", ResumeJobType: "job"},
		AlternateRoute: &fallback, AlternateAfter: &zero,
	}))
	record := r.engine.getJobRecord(h.JobKey)
	record.leased, record.leaseID, record.status = true, "expired", jobdb.JobStatusActive
	record.leaseExpiresAt = time.Now().Add(-time.Second)
	record.consecutiveExpirations = crashconcern.DefaultThreshold
	info, err := r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusCrashConcern, info.Status)
	require.ErrorIs(t, r.CompleteTaskIfWaiting(ctx, jobdb.CompleteTaskIfWaitingRequest{
		JobKey: h.JobKey, Route: primary, OutputOrdinal: 1, InputHash: "input", Data: jobdb.NewTaskDataOrPanic(2),
	}), jobdb.ErrConflict)
	_, err = r.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 1})
	require.ErrorIs(t, err, jobdb.ErrChapterNotFound)
	for _, route := range []jobdb.Route{primary, fallback} {
		leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "worker", Routes: []jobdb.Route{route}})
		require.NoError(t, err)
		require.Empty(t, leases)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{route}})
		require.NoError(t, err)
		require.Nil(t, lease)
	}
	require.Equal(t, primary, record.route)
	require.Equal(t, &fallback, record.alternateRoute)
	require.Equal(t, "expired", record.leaseID)
	require.EqualValues(t, crashconcern.DefaultThreshold, record.consecutiveExpirations)
}

func TestCrashConcernRenewalAndReschedule(t *testing.T) {
	ctx := context.Background()
	r := New()
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	get := func() jobdb.ExecutionLease {
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
		require.NoError(t, err)
		require.NotNil(t, lease)
		return lease
	}
	lease := get()
	record := r.engine.getJobRecord(h.JobKey)
	record.consecutiveExpirations = crashconcern.DefaultThreshold
	_, err = lease.(jobdb.RenewableExecutionLease).Renew(ctx)
	require.NoError(t, err)
	require.EqualValues(t, crashconcern.DefaultThreshold, record.consecutiveExpirations)
	info, err := r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusActive, info.Status)
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}))
	require.Zero(t, record.consecutiveExpirations)
	get()
	record.leaseExpiresAt = time.Now().Add(-time.Second)
	info, err = r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusReady, info.Status)
}
