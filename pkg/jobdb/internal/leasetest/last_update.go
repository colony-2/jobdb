package leasetest

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

type Runtime interface {
	jobdb.ConsumerRuntime
	KeepAliveLeaseByIDWithExpiry(context.Context, jobdb.JobKey, string, string, time.Duration) (time.Time, error)
	RenewExecutionLeaseByID(context.Context, jobdb.JobKey, string, string, time.Duration) (jobdb.RenewableExecutionLease, error)
}

func Run(t *testing.T, runtime Runtime, bindClock func(func() time.Time), listsExpiry bool) {
	t.Helper()
	ctx := celltest.Context(context.Background())
	now := time.Now().UTC().Add(time.Second)
	tick := false
	bindClock(func() time.Time {
		if tick {
			now = now.Add(time.Millisecond)
		}
		return now
	})
	step := func() time.Time { now = now.Add(time.Second); return now }
	route := jobdb.Route{JobType: "work"}
	submit := func(id string) jobdb.JobKey {
		handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: route.JobType, Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, err)
		return handle.JobKey
	}
	check := func(key jobdb.JobKey, updated, expiry *time.Time) jobdb.JobSummary {
		t.Helper()
		page, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, JobKeys: []jobdb.JobKey{key}})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 1)
		row := page.Jobs[0]
		require.Equal(t, updated, row.LastLeaseUpdatedAt)
		if !listsExpiry {
			expiry = nil
		}
		require.Equal(t, expiry, row.LeaseExpiresAt)
		return row
	}
	claim := func(key jobdb.JobKey) jobdb.ExecutionLease {
		t.Helper()
		lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{route}, LeaseDuration: time.Hour})
		require.NoError(t, err)
		require.NotNil(t, lease)
		return lease
	}
	key := submit("lifecycle")
	check(key, nil, nil)
	polledAt := step()
	leases, err := runtime.PollWork(ctx, jobdb.PollWorkRequest{TenantId: key.TenantId, WorkerID: "worker", Routes: []jobdb.Route{route}, LeaseDuration: time.Hour, Limit: 1})
	require.NoError(t, err)
	require.Len(t, leases, 1)
	lease := leases[0]
	firstExpiry := polledAt.Add(time.Hour)
	check(key, &polledAt, &firstExpiry)
	legacyAt := step()
	legacyExpiry, err := runtime.KeepAliveLeaseByIDWithExpiry(ctx, key, lease.LeaseID(), "worker", 2*time.Hour)
	require.NoError(t, err)
	require.Equal(t, legacyAt.Add(2*time.Hour), legacyExpiry)
	check(key, &legacyAt, &legacyExpiry)
	renewedAt := step()
	renewed, err := runtime.RenewExecutionLeaseByID(ctx, key, lease.LeaseID(), "worker", time.Minute)
	require.NoError(t, err)
	require.Equal(t, lease.LeaseID(), renewed.LeaseID())
	expiry := renewedAt.Add(time.Minute)
	require.Equal(t, expiry, renewed.LeaseExpiry())
	check(key, &renewedAt, &expiry)
	step()
	_, err = runtime.RenewExecutionLeaseByID(ctx, key, lease.LeaseID(), "wrong", time.Hour)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	_, err = runtime.KeepAliveLeaseByIDWithExpiry(ctx, key, "stale", "worker", time.Hour)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	check(key, &renewedAt, &expiry)
	now = expiry.Add(time.Second)
	_, err = runtime.RenewExecutionLeaseByID(ctx, key, lease.LeaseID(), "worker", time.Hour)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	check(key, &renewedAt, &expiry)
	claimedAt := step()
	lease = claim(key)
	claimedExpiry := claimedAt.Add(time.Hour)
	check(key, &claimedAt, &claimedExpiry)
	step()
	waitRoute := jobdb.Route{JobType: route.JobType, TaskType: "external"}
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: waitRoute, TaskWait: &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, ResumeJobType: route.JobType, InputHash: "input"}}))
	check(key, &claimedAt, nil)
	handoffAt := step()
	require.NoError(t, runtime.CompleteTaskIfWaiting(ctx, jobdb.CompleteTaskIfWaitingRequest{JobKey: key, Route: waitRoute, OutputOrdinal: 1, InputHash: "input", Data: jobdb.NewTaskDataOrPanic(2)}))
	check(key, &handoffAt, nil)
	finalClaimAt := step()
	lease = claim(key)
	step()
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 2, TaskType: route.JobType, CreatedAt: now, Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`3`)}}}}}))
	require.NotNil(t, check(key, &finalClaimAt, nil).ArchivedAt)

	appendReq := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "updates", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: route.JobType, Data: jobdb.NewTaskDataOrPanic(1)}, Append: []byte(`2`)}}
	handle, err := runtime.StartOrAppendJob(ctx, appendReq)
	require.NoError(t, err)
	prior := step()
	lease = claim(handle.JobKey)
	step()
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route}))
	check(handle.JobKey, &prior, nil)
	now = prior.Add(time.Hour + time.Second)
	for _, fail := range []bool{false, true} {
		step()
		if fail {
			appendReq.Job.Append = []byte(`"` + string(bytes.Repeat([]byte{'x'}, 1<<20)) + `"`)
		}
		tick = true
		appended, err := runtime.StartOrAppendJob(ctx, appendReq)
		tick = false
		if fail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, handle.JobKey, appended.JobKey)
		}
		lastRead := now
		step()
		events, err := runtime.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
		require.NoError(t, err)
		require.NotEmpty(t, events.Events)
		event := events.Events[0]
		require.Equal(t, jobdb.LeaseAcquisitionAppend, event.AcquisitionSource)
		require.Equal(t, handle.JobKey, event.JobKey)
		require.True(t, event.OccurredAt.After(prior) && event.OccurredAt.Before(lastRead))
		check(handle.JobKey, &event.OccurredAt, nil)
		prior = event.OccurredAt
	}
}
