package runtimetest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

// RunGoalAdmissionConformance exercises publication through the public runtime,
// including actual HTTP adapters. Retention-only states and independent SQLite
// connections have additional backend-specific tests.
func RunGoalAdmissionConformance(t *testing.T, r jobdb.WorkflowRuntime) {
	t.Helper()
	ctx := PrimaryGoalContext(context.Background())
	request := func(tenant, id, goal string) jobdb.SubmitJobRequest {
		return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: id, GoalID: goal,
			Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	}
	for _, state := range []string{"ready", "delayed", "dependency-blocked", "leased", "expired", "task-routed"} {
		t.Run(state, func(t *testing.T) {
			tenant := "admission-" + state
			req := request(tenant, "origin", "")
			if state == "delayed" {
				at := time.Now().Add(time.Hour)
				req.Job.AvailableAt = &at
			}
			if state == "dependency-blocked" {
				prerequisite, err := celltest.Submit(r, ctx, request(tenant, "prerequisite", ""))
				require.NoError(t, err)
				req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: prerequisite.GoalID}, Condition: jobdb.JobPrereqSuccess}}
			}
			h, err := celltest.Submit(r, ctx, req)
			require.NoError(t, err)
			if state == "leased" || state == "expired" || state == "task-routed" {
				duration := time.Minute
				if state == "expired" {
					duration = 20 * time.Millisecond
				}
				lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: duration})
				require.NoError(t, err)
				require.NotNil(t, lease)
				if state == "expired" {
					time.Sleep(30 * time.Millisecond)
				}
				if state == "task-routed" {
					require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work", TaskType: "external"}, TaskWait: &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "admission-input", ResumeJobType: "work"}}))
				}
			}
			fresh := request(tenant, "recovery", h.GoalID)
			_, err = r.SubmitJob(ctx, fresh)
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: tenant, JobId: "recovery"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
			accepted, err := r.SubmitJob(ctx, fresh)
			require.NoError(t, err)
			require.Equal(t, h.GoalID, accepted.GoalID)
			g, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: tenant, GoalID: h.GoalID}, Message: "permanent"})
			require.NoError(t, err)
			require.Equal(t, []string{accepted.JobKey.JobId}, g.Failure.NonTerminalJobIDs)
			replay, err := r.SubmitJob(ctx, fresh)
			require.NoError(t, err)
			require.Equal(t, accepted, replay)
			fresh.Job.JobID = "after-closure"
			_, err = r.SubmitJob(ctx, fresh)
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			original, err := r.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			require.Equal(t, h.GoalID, original.GoalID)
			require.Empty(t, original.OtherGoalIDs)
		})
	}
	t.Run("distinct_publishers", func(t *testing.T) {
		h, err := celltest.Submit(r, ctx, request("admission-contention", "origin", ""))
		require.NoError(t, err)
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
		start := make(chan struct{})
		results := make(chan error, 8)
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, err := r.SubmitJob(ctx, request(h.JobKey.TenantId, fmt.Sprintf("recovery-%d", i), h.GoalID))
				results <- err
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		wins := 0
		for err := range results {
			if err == nil {
				wins++
			} else {
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			}
		}
		require.Equal(t, 1, wins)
		rows, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{h.JobKey.TenantId}, GoalIDs: []string{h.GoalID}, IncludeHidden: true})
		require.NoError(t, err)
		require.Len(t, rows.Jobs, 2)
	})
	t.Run("ordinary_failure_shared_restart_and_replay", func(t *testing.T) {
		h, err := celltest.Submit(r, ctx, request("admission-restart", "origin", ""))
		require.NoError(t, err)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "ordinary failure"}}}}
		require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
		req := jobdb.SubmitRestartJobRequest{RequestID: "first", Job: jobdb.SubmitRestartJob{PriorJobKey: h.JobKey, JobID: "restart", Cell: h.Cell, ShareGoal: true}}
		accepted, err := r.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, h.GoalID, accepted.GoalID)
		other := req
		other.RequestID = "other"
		other.Job.JobID = "competitor"
		_, err = r.SubmitRestartJob(ctx, other)
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}, Message: "closed"})
		require.NoError(t, err)
		replay, err := r.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, accepted, replay)
		_, err = r.SubmitRestartJob(ctx, other)
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	})
	t.Run("shared_restart_through_parent_lease", func(t *testing.T) {
		tenant := "admission-lease"
		parent, err := celltest.Submit(r, ctx, request(tenant, "parent", ""))
		require.NoError(t, err)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		source, err := celltest.Submit(r, ctx, request(tenant, "source", ""))
		require.NoError(t, err)
		sourceLease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: source.JobKey, WorkerID: "source-worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, sourceLease)
		chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "ordinary failure"}}}}
		require.NoError(t, sourceLease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
		active, err := r.SubmitJob(ctx, request(tenant, "active", source.GoalID))
		require.NoError(t, err)
		req := jobdb.SubmitRestartJobRequest{RequestID: "lease-restart", Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, JobID: "restart", Cell: source.Cell, ShareGoal: true}}
		_, err = lease.SubmitRestartJob(ctx, req)
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: active.JobKey}))
		accepted, err := lease.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, source.GoalID, accepted.GoalID)
		_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: tenant, GoalID: source.GoalID}, Message: "closed"})
		require.NoError(t, err)
		replay, err := lease.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, accepted, replay)
		fresh := req
		fresh.RequestID = "fresh"
		fresh.Job.JobID = "fresh"
		_, err = lease.SubmitRestartJob(ctx, fresh)
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		independent := req
		independent.RequestID = "independent"
		independent.Job.JobID = "independent"
		independent.Job.ShareGoal = false
		child, err := lease.SubmitRestartJob(ctx, independent)
		require.NoError(t, err)
		require.NotEqual(t, source.GoalID, child.GoalID)
		require.NotEqual(t, parent.GoalID, child.GoalID)
	})

	t.Run("succeeded_goal_rejects_fresh_selection_and_shared_restart", func(t *testing.T) {
		h, err := celltest.Submit(r, ctx, request("admission-success", "origin", ""))
		require.NoError(t, err)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
		require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter}))
		_, err = r.SubmitJob(ctx, request(h.JobKey.TenantId, "selected", h.GoalID))
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		_, err = r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{RequestID: "after-success", Job: jobdb.SubmitRestartJob{PriorJobKey: h.JobKey, JobID: "restart", Cell: h.Cell, ShareGoal: true}})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		rows, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{h.JobKey.TenantId}, GoalIDs: []string{h.GoalID}, IncludeHidden: true})
		require.NoError(t, err)
		require.Len(t, rows.Jobs, 1)
	})

}
