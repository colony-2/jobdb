package runtimetest

import (
	"context"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

// renewableLease requires the synchronous renewal contract even for rejected renewals.
func renewableLease(t *testing.T, lease jobdb.ExecutionLease) jobdb.RenewableExecutionLease {
	t.Helper()
	renewable, ok := lease.(jobdb.RenewableExecutionLease)
	require.True(t, ok, "lease must implement RenewableExecutionLease")
	require.NotNil(t, renewable)
	return renewable
}

func renewLease(t *testing.T, ctx context.Context, lease jobdb.ExecutionLease) jobdb.RenewableExecutionLease {
	t.Helper()
	previous := renewableLease(t, lease)
	next, err := previous.Renew(ctx)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Equal(t, previous.Job().JobKey, next.Job().JobKey)
	require.Equal(t, previous.LeaseID(), next.LeaseID())
	require.Equal(t, previous.LeaseWorkerID(), next.LeaseWorkerID())
	return next
}

// leaseEventExpiry checks an event against an independent backend read, never
// using the event itself as the validity oracle.
func leaseEventExpiry(ctx context.Context, f Fixture, key jobdb.JobKey, leaseID, workerID string, safe, event time.Time) error {
	validate := f.LeaseEventLeaseValidity
	native := validate == nil
	if native {
		v, ok := f.Runtime.(interface {
			ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
		})
		if !ok {
			return fmt.Errorf("lease-event backend validator required: supply Fixture.LeaseEventLeaseValidity")
		}
		validate = v.ValidateExecutionLeaseByID
	}
	validity, err := validate(ctx, key, leaseID, workerID)
	if err != nil {
		return fmt.Errorf("lease-event backend validation: %w", err)
	}
	if validity.ExpiresAt.IsZero() {
		return fmt.Errorf("lease-event backend expiry is zero")
	}
	if !event.Equal(validity.ExpiresAt) {
		return fmt.Errorf("lease-event expiry differs from backend expiry")
	}
	if safe.IsZero() || safe.After(validity.ExpiresAt) {
		return fmt.Errorf("lease-event safe deadline is zero or after backend expiry")
	}
	if native && !safe.Equal(validity.ExpiresAt) {
		return fmt.Errorf("lease-event native deadline differs from backend expiry")
	}
	return nil
}

// RunLeaseEventConformance exercises canonical history through the mandatory public reader.
// Backend owners additionally test insertion rollback, installation, query budgets and plans.
func RunLeaseEventConformance(t *testing.T, harnesses ...Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := h.New(t)
			if f.Cleanup != nil {
				t.Cleanup(func() { require.NoError(t, f.Cleanup(context.Background())) })
			}
			r := f.Runtime
			ctx := celltest.Context(context.Background())
			tenant := "lease-events-" + h.Name
			route := jobdb.Route{JobType: "lease-events"}
			submit := func(id string) jobdb.JobKey {
				v, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: route.JobType, Data: jobdb.NewTaskDataOrPanic(1)}})
				require.NoError(t, e)
				return v.JobKey
			}
			list := func(q jobdb.ListLeaseEventsRequest) jobdb.ListLeaseEventsResponse {
				q.TenantID = tenant
				v, e := jobdb.ListLeaseEvents(ctx, r, q)
				require.NoError(t, e)
				require.NotNil(t, v.Events)
				return v
			}
			key := submit("first")
			require.Empty(t, list(jobdb.ListLeaseEventsRequest{}).Events)
			lease, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker-1", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
			require.NoError(t, e)
			require.NotNil(t, lease)
			acquired := list(jobdb.ListLeaseEventsRequest{}).Events
			require.Len(t, acquired, 1)
			a := acquired[0]
			require.Equal(t, jobdb.LeaseEventAcquired, a.EventType)
			require.Equal(t, jobdb.LeaseAcquisitionTargeted, a.AcquisitionSource)
			require.Equal(t, key, a.JobKey)
			require.Equal(t, route, a.Route)
			require.Equal(t, route.JobType, a.JobType)
			require.Equal(t, lease.LeaseID(), a.LeaseID)
			require.Equal(t, "worker-1", a.WorkerID)
			require.NotEmpty(t, a.CellID)
			require.NotEmpty(t, a.GoalID)
			require.Equal(t, "test", a.Cell)
			require.Equal(t, "example.test/test", a.CellRepo)
			require.Empty(t, a.PreviousLeaseID)
			require.Nil(t, a.PreviousLeaseExpiresAt)
			require.NoError(t, leaseEventExpiry(ctx, f, key, lease.LeaseID(), renewableLease(t, lease).LeaseWorkerID(), renewableLease(t, lease).LeaseExpiry(), a.LeaseExpiresAt))
			lease = renewLease(t, ctx, lease)
			renewed := list(jobdb.ListLeaseEventsRequest{}).Events
			require.Len(t, renewed, 2)
			b := renewed[0]
			require.Equal(t, jobdb.LeaseEventRenewed, b.EventType)
			require.Equal(t, a.LeaseID, b.LeaseID)
			require.NotNil(t, b.PreviousLeaseExpiresAt)
			require.Equal(t, a.LeaseExpiresAt, *b.PreviousLeaseExpiresAt)
			require.NoError(t, leaseEventExpiry(ctx, f, key, lease.LeaseID(), renewableLease(t, lease).LeaseWorkerID(), renewableLease(t, lease).LeaseExpiry(), b.LeaseExpiresAt))
			require.Equal(t, renewed, list(jobdb.ListLeaseEventsRequest{}).Events, "backend oracle must not mutate history")
			require.Empty(t, b.AcquisitionSource)
			require.Empty(t, b.PreviousLeaseID)
			// Validation/read actions are never renewal mutations.
			if v, ok := r.(interface {
				ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
			}); ok {
				_, e = v.ValidateExecutionLeaseByID(ctx, key, lease.LeaseID(), "worker-1")
				require.NoError(t, e)
			}
			losing, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "loser", Routes: []jobdb.Route{route}})
			require.NoError(t, e)
			require.Nil(t, losing)
			require.Len(t, list(jobdb.ListLeaseEventsRequest{}).Events, 2)
			// Every filter dimension and the half-open interval agree with stored snapshots.
			after, before := a.OccurredAt, a.OccurredAt.Add(time.Nanosecond)
			for _, q := range []jobdb.ListLeaseEventsRequest{
				{JobIDs: []string{key.JobId}}, {GoalIDs: []string{a.GoalID}}, {WorkerIDs: []string{a.WorkerID}}, {LeaseIDs: []string{a.LeaseID}}, {JobTypes: []string{a.JobType}}, {Routes: []jobdb.Route{a.Route}}, {CellIDs: []string{a.CellID}}, {CellNames: []string{a.Cell}},
			} {
				require.Len(t, list(q).Events, 2)
			}
			require.Len(t, list(jobdb.ListLeaseEventsRequest{OccurredAfter: &after, OccurredBefore: &before}).Events, 1)
			require.Len(t, list(jobdb.ListLeaseEventsRequest{EventTypes: []jobdb.LeaseEventType{jobdb.LeaseEventAcquired}}).Events, 1)
			for _, q := range []jobdb.ListLeaseEventsRequest{{JobIDs: []string{"unknown"}}, {GoalIDs: []string{"unknown"}}, {WorkerIDs: []string{"unknown"}}, {LeaseIDs: []string{"unknown"}}, {Routes: []jobdb.Route{{JobType: a.JobType, TaskType: "other"}}}} {
				require.Empty(t, list(q).Events)
			}
			// Returned pointers cannot rewrite retained history.
			renewed[0].Cell = "changed"
			*renewed[0].PreviousLeaseExpiresAt = time.Time{}
			require.Equal(t, b.Cell, list(jobdb.ListLeaseEventsRequest{}).Events[0].Cell)
			require.False(t, list(jobdb.ListLeaseEventsRequest{}).Events[0].PreviousLeaseExpiresAt.IsZero())
			for i := 0; i < 8; i++ {
				lease = renewLease(t, ctx, lease)
			}
			original := list(jobdb.ListLeaseEventsRequest{}).Events
			require.Len(t, original, 10)
			for _, size := range []int{1, 100, 200} {
				q := jobdb.ListLeaseEventsRequest{PageSize: size}
				seen := []jobdb.LeaseEvent{}
				for {
					page := list(q)
					seen = append(seen, page.Events...)
					if page.NextPageToken == "" {
						break
					}
					q.PageToken = page.NextPageToken
				}
				require.Equal(t, original, seen)
			}
			page := list(jobdb.ListLeaseEventsRequest{PageSize: 1})
			require.NotEmpty(t, page.NextPageToken)
			for _, q := range []jobdb.ListLeaseEventsRequest{
				{TenantID: tenant, PageSize: 1, PageToken: page.NextPageToken + "x"},
				{TenantID: tenant, PageSize: 2, PageToken: page.NextPageToken},
				{TenantID: tenant, PageSize: 1, PageToken: page.NextPageToken, IncludeHidden: true},
				{TenantID: tenant, PageSize: 1, PageToken: page.NextPageToken, WorkerIDs: []string{"worker-1"}},
				{TenantID: "other", PageSize: 1, PageToken: page.NextPageToken},
			} {
				_, e := jobdb.ListLeaseEvents(ctx, r, q)
				require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventPageToken)
			}
			// Cancellation retains history and renewal rejection appends nothing.
			e = r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key})
			require.NoError(t, e)
			_, e = renewableLease(t, lease).Renew(ctx)
			require.ErrorIs(t, e, jobdb.ErrExecutionLeaseLost)
			require.Equal(t, original, list(jobdb.ListLeaseEventsRequest{}).Events)
			// Poll attribution and competing targeted claims.
			second := submit("second")
			var wg sync.WaitGroup
			var mu sync.Mutex
			wins := 0
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: second, WorkerID: fmt.Sprint("competitor-", i), Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					if l != nil {
						mu.Lock()
						wins++
						mu.Unlock()
					}
				}(i)
			}
			wg.Wait()
			require.Equal(t, 1, wins)
			require.Len(t, list(jobdb.ListLeaseEventsRequest{JobIDs: []string{second.JobId}}).Events, 1)
			third := submit("third")
			ls, e := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: tenant, WorkerID: "poller", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
			require.NoError(t, e)
			require.Len(t, ls, 1)
			polled := list(jobdb.ListLeaseEventsRequest{JobIDs: []string{third.JobId}}).Events
			require.Len(t, polled, 1)
			require.Equal(t, jobdb.LeaseAcquisitionPoll, polled[0].AcquisitionSource) // Renames and aliases resolve current identity while events keep historical facts.
			c, e := r.GetCell(ctx, jobdb.CellKey{TenantID: tenant, CellID: a.CellID})
			require.NoError(t, e)
			renamed := "renamed"
			c, e = r.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &renamed})
			require.NoError(t, e)
			byName := list(jobdb.ListLeaseEventsRequest{CellNames: []string{renamed}})
			byAlias := list(jobdb.ListLeaseEventsRequest{CellNames: []string{"test"}})
			require.Equal(t, byName, byAlias)
			require.Equal(t, "test", byName.Events[0].Cell)
			// Additional membership added later matches without changing the immutable primary goal.
			target := submit("goal-target")
			info, e := r.GetJob(ctx, target)
			require.NoError(t, e)
			_, e = r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: tenant, GoalID: info.GoalID}, JobID: key.JobId})
			require.NoError(t, e)
			matched := list(jobdb.ListLeaseEventsRequest{GoalIDs: []string{info.GoalID, info.GoalID}, JobIDs: []string{key.JobId}})
			require.Len(t, matched.Events, len(original))
			for _, v := range matched.Events {
				require.Equal(t, a.GoalID, v.GoalID)
			}

			// Live traversal: old continuation excludes a new renewal; fresh first page sees it.
			first := list(jobdb.ListLeaseEventsRequest{PageSize: 1})
			beforeRenewal := list(jobdb.ListLeaseEventsRequest{}).Events
			ls[0] = renewLease(t, ctx, ls[0])
			require.Len(t, list(jobdb.ListLeaseEventsRequest{}).Events, len(beforeRenewal)+1)
			fresh := list(jobdb.ListLeaseEventsRequest{PageSize: 1})
			require.NotEqual(t, first.Events[0].EventID, fresh.Events[0].EventID)
			continuation := list(jobdb.ListLeaseEventsRequest{PageSize: 1, PageToken: first.NextPageToken})
			require.NotEqual(t, fresh.Events[0].EventID, continuation.Events[0].EventID)
			taskRoute := jobdb.Route{JobType: "resumed", TaskType: "external"}
			wait := &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "input", ResumeJobType: "resume"}
			require.NoError(t, ls[0].Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: taskRoute, TaskWait: wait}))
			taskLease, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: third, WorkerID: "task-worker", Routes: []jobdb.Route{taskRoute}, LeaseDuration: time.Minute})
			require.NoError(t, e)
			require.NotNil(t, taskLease)
			taskEvent := list(jobdb.ListLeaseEventsRequest{JobIDs: []string{third.JobId}, PageSize: 1}).Events[0]
			require.Equal(t, taskRoute, taskEvent.Route)
			require.Equal(t, route.JobType, taskEvent.JobType)
			require.Empty(t, taskEvent.PreviousLeaseID)
			require.Equal(t, "renamed", taskEvent.Cell)
			require.NoError(t, taskLease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: taskRoute, TaskWait: wait}))
			require.NoError(t, r.CompleteTaskIfWaiting(ctx, jobdb.CompleteTaskIfWaitingRequest{JobKey: third, Route: taskRoute, InputOrdinal: 0, OutputOrdinal: 1, InputHash: "input", Data: jobdb.NewTaskDataOrPanic(2)}))
			internal := list(jobdb.ListLeaseEventsRequest{JobIDs: []string{third.JobId}, PageSize: 1}).Events[0]
			require.Equal(t, jobdb.LeaseAcquisitionTaskCompletion, internal.AcquisitionSource)
			require.Equal(t, jobdb.Route{JobType: "resume"}, internal.Route)
			resume, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: third, WorkerID: "resume-worker", Routes: []jobdb.Route{{JobType: "resume"}}, LeaseDuration: time.Minute})
			require.NoError(t, e)
			require.NotNil(t, resume)
			zero := time.Duration(0)
			promoted := jobdb.Route{JobType: "promoted"}
			require.NoError(t, resume.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "ordinary"}, AlternateRoute: &promoted, AlternateAfter: &zero}))
			promotedLeases, e := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: tenant, WorkerID: "promotion-worker", Routes: []jobdb.Route{promoted}, LeaseDuration: time.Minute})
			require.NoError(t, e)
			require.Len(t, promotedLeases, 1)
			promotion := list(jobdb.ListLeaseEventsRequest{JobIDs: []string{third.JobId}, PageSize: 1}).Events[0]
			require.Equal(t, promoted, promotion.Route)
			require.Equal(t, route.JobType, promotion.JobType)
			require.Equal(t, jobdb.LeaseAcquisitionPoll, promotion.AcquisitionSource)
			beforeComplete := list(jobdb.ListLeaseEventsRequest{JobIDs: []string{third.JobId}})
			final := jobdb.Chapter{Ordinal: 2, TaskType: "promoted", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}
			require.NoError(t, promotedLeases[0].Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
			require.Equal(t, beforeComplete, list(jobdb.ListLeaseEventsRequest{JobIDs: []string{third.JobId}}))
			for _, k := range []jobdb.JobKey{second, target} {
				require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: k}))
			}

		})
	}
}
