package remote

import (
	"context"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func goalRuntimes(t *testing.T, run func(*testing.T, jobdb.WorkflowRuntime)) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, transport := range []string{"local", "http"} {
			t.Run(backend+"/"+transport, func(t *testing.T) {
				var runtime jobdb.WorkflowRuntime
				if backend == "toy" {
					runtime = toyruntime.New()
				} else {
					embedded, err := sqliteruntime.StartEmbeddedRuntime(context.Background())
					require.NoError(t, err)
					t.Cleanup(embedded.Shutdown)
					runtime = embedded.Runtime
				}
				if transport == "http" {
					server := httptest.NewServer(NewServer(runtime))
					t.Cleanup(server.Close)
					remote, err := New(server.URL, server.Client())
					require.NoError(t, err)
					runtime = remote
				}
				run(t, runtime)
			})
		}
	}
}
func goalSubmit(t *testing.T, r jobdb.WorkflowRuntime, id string, deps ...jobdb.Dependency) jobdb.JobHandle {
	t.Helper()
	handle, err := r.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, JobType: "work", Data: jobdb.NewTaskDataOrPanic("input"), Dependencies: deps}})
	require.NoError(t, err)
	require.NotEmpty(t, handle.GoalID)
	require.NotEqual(t, handle.JobKey.JobId, handle.GoalID)
	return handle
}
func goalKey(h jobdb.JobHandle) jobdb.GoalKey {
	return jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}
}
func goalLease(t *testing.T, r jobdb.WorkflowRuntime, h jobdb.JobHandle) jobdb.ExecutionLease {
	t.Helper()
	lease, err := r.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	return lease
}
func goalComplete(t *testing.T, r jobdb.WorkflowRuntime, h jobdb.JobHandle, success bool) {
	t.Helper()
	lease := goalLease(t, r, h)
	require.NotNil(t, lease)
	require.Equal(t, h.GoalID, lease.Job().GoalID)
	var outcome jobdb.ChapterOutcome = jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}
	status := "success"
	if !success {
		status = "failed_app"
		outcome = jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "try again"}}
	}
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: outcome}}
	require.NoError(t, lease.Complete(context.Background(), jobdb.CompleteExecutionRequest{Status: status, Chapter: &chapter}))
}
func TestGoalsLifecycle(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		info, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.Equal(t, jobdb.GoalOpen, info.State)
		require.EqualValues(t, 1, info.Revision)
		consumer := goalSubmit(t, r, "consumer", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess})
		require.Nil(t, goalLease(t, r, consumer))
		goalComplete(t, r, a, false)
		after, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.Equal(t, info, after)
		require.Nil(t, goalLease(t, r, consumer))
		restart := jobdb.SubmitRestartJobRequest{RequestID: "replace-a", Job: jobdb.SubmitRestartJob{PriorJobKey: a.JobKey, ShareGoal: true, LastStepToKeep: 0}}
		b, err := r.SubmitRestartJob(ctx, restart)
		require.NoError(t, err)
		require.Equal(t, a.GoalID, b.GoalID)
		other := goalSubmit(t, r, "other")
		attached, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: other.JobKey.JobId})
		require.NoError(t, err)
		repeat, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: other.JobKey.JobId})
		require.NoError(t, err)
		require.Equal(t, attached, repeat)
		goalComplete(t, r, b, true)
		retry, err := r.SubmitRestartJob(ctx, restart)
		require.NoError(t, err)
		require.Equal(t, b, retry)
		wait, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "submission"})
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencySatisfied, wait.State)
		require.Equal(t, b.JobKey.JobId, wait.Dependencies[0].SatisfiedByJobID)
		require.NotNil(t, goalLease(t, r, consumer))
		goalComplete(t, r, other, true)
		winner, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.Equal(t, b.JobKey.JobId, winner.SuccessfulJobID)
		require.EqualValues(t, 2, winner.Revision)
		own, err := r.GetGoal(ctx, goalKey(other))
		require.NoError(t, err)
		require.Equal(t, other.JobKey.JobId, own.SuccessfulJobID)
		job, err := r.GetJob(ctx, other.JobKey)
		require.NoError(t, err)
		require.Equal(t, []string{a.GoalID}, job.OtherGoalIDs)
		list, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{a.GoalID, other.GoalID}, PageSize: 100})
		require.NoError(t, err)
		require.Len(t, list.Jobs, 3)
		_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(a), Message: "late"})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		fresh := goalSubmit(t, r, "fresh")
		_, err = r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: fresh.JobKey.JobId})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	})
}

func TestGoalRestartRequestIDConflicts(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		for _, mode := range []string{"direct", "lease"} {
			t.Run(mode, func(t *testing.T) {
				ctx := context.Background()
				submit := r.SubmitRestartJob
				if mode == "lease" {
					parent := goalSubmit(t, r, "restart-parent")
					lease := goalLease(t, r, parent)
					require.NotNil(t, lease)
					submit = lease.SubmitRestartJob
				}
				source := goalSubmit(t, r, mode+"-source")
				goalComplete(t, r, source, false)
				req := jobdb.SubmitRestartJobRequest{
					RequestID: mode + "-restart",
					Job:       jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, ShareGoal: true},
				}
				restarted, err := submit(ctx, req)
				require.NoError(t, err)
				require.NotEqual(t, source.JobKey, restarted.JobKey)
				require.Equal(t, source.GoalID, restarted.GoalID)
				assertRetry := func() {
					t.Helper()
					again, err := submit(ctx, req)
					require.NoError(t, err)
					require.Equal(t, restarted, again)
					changed := req
					changed.Job.ShareGoal = false
					_, err = submit(ctx, changed)
					require.ErrorIs(t, err, jobdb.ErrExistingJobMismatch)
					require.ErrorIs(t, err, jobdb.ErrConflict)
					require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				}
				assertRetry()
				goalComplete(t, r, restarted, false)
				assertRetry() // The destination has advanced beyond its copied prefix.
				_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(source), Message: "closed"})
				require.NoError(t, err)
				assertRetry() // Closure must not change the reserved request's result.
				info, err := r.GetJob(ctx, restarted.JobKey)
				require.NoError(t, err)
				require.Equal(t, restarted.GoalID, info.GoalID)
				require.Equal(t, jobdb.JobStatusCompleted, info.Status)
			})
		}
	})
}
func TestGoalFailureSnapshotAndCancellation(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		b := goalSubmit(t, r, "b")
		_, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: b.JobKey.JobId})
		require.NoError(t, err)
		consumer := goalSubmit(t, r, "consumer", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess})
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: a.JobKey, Reason: "first approach abandoned"}))
		req := jobdb.FailGoalRequest{GoalKey: goalKey(a), Message: "No acceptable solution"}
		failed, err := r.FailGoal(ctx, req)
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, failed.Failure.TerminalJobIDs)
		require.Equal(t, []string{"b"}, failed.Failure.NonTerminalJobIDs)
		wait, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "submission"})
		require.NoError(t, err)
		require.ErrorIs(t, wait.Err(), jobdb.ErrGoalFailed)
		var failure *jobdb.GoalFailedError
		require.True(t, errors.As(wait.Err(), &failure))
		require.Equal(t, *failed.Failure, failure.Failure)
		require.NotNil(t, goalLease(t, r, consumer))
		goalComplete(t, r, b, true)
		retry, err := r.FailGoal(ctx, req)
		require.NoError(t, err)
		require.Equal(t, failed, retry)
		req.Message = "different"
		_, err = r.FailGoal(ctx, req)
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	})
}
func TestGoalAttachSuccessCycleAndDurableWait(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		b := goalSubmit(t, r, "b")
		lease := goalLease(t, r, b)
		req := jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}, DependencyWait: &jobdb.DependencyWait{WaitID: "join-1", Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess}}}}
		require.NoError(t, lease.Reschedule(ctx, req))
		_, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: b.JobKey.JobId})
		require.ErrorIs(t, err, jobdb.ErrDependencyCycle)
		bi, err := r.GetJob(ctx, b.JobKey)
		require.NoError(t, err)
		require.Empty(t, bi.OtherGoalIDs)
		winner := goalSubmit(t, r, "winner")
		goalComplete(t, r, winner, true)
		_, err = r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: winner.JobKey.JobId})
		require.NoError(t, err)
		wait, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: b.JobKey, WaitID: "join-1"})
		require.NoError(t, err)
		require.Equal(t, winner.JobKey.JobId, wait.Dependencies[0].SatisfiedByJobID)
		lease = goalLease(t, r, b)
		require.NotNil(t, lease)
		require.NoError(t, lease.Reschedule(ctx, req)) // Same descriptor retains binding.
		lease = goalLease(t, r, b)
		require.NotNil(t, lease)
		req.DependencyWait.Dependencies[0].Target.GoalID = b.GoalID
		require.ErrorIs(t, lease.Reschedule(ctx, req), jobdb.ErrDependencyWaitConflict)
	})
}
func TestConcurrentGoalResolution(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		b := goalSubmit(t, r, "b")
		_, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: b.JobKey.JobId})
		require.NoError(t, err)
		leases := []jobdb.ExecutionLease{goalLease(t, r, a), goalLease(t, r, b)}
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for _, lease := range leases {
			wg.Add(1)
			go func(lease jobdb.ExecutionLease) {
				defer wg.Done()
				chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`true`)}}}}
				errs <- lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter})
			}(lease)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		info, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.Contains(t, []string{"a", "b"}, info.SuccessfulJobID)
		require.EqualValues(t, 2, info.Revision)
	})
}

func TestDependencyTimeoutCancellationAndValidation(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		consumer := goalSubmit(t, r, "consumer")
		lease := goalLease(t, r, consumer)
		past := time.Now().Add(-time.Second).UTC()
		wait := jobdb.DependencyWait{WaitID: "deadline", Deadline: &past, Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess}}}
		require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}, DependencyWait: &wait}))
		w, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "deadline"})
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencyTimedOut, w.State)
		goalComplete(t, r, a, true)
		after, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "deadline"})
		require.NoError(t, err)
		require.Equal(t, w, after)
		b := goalSubmit(t, r, "b")
		exact := goalSubmit(t, r, "exact", jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: b.JobKey.JobId}, Condition: jobdb.JobPrereqComplete})
		success := goalSubmit(t, r, "success", jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: b.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess})
		require.Nil(t, goalLease(t, r, exact))
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: b.JobKey}))
		for _, h := range []jobdb.JobHandle{exact, success} {
			outcome, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: h.JobKey, WaitID: "submission"})
			require.NoError(t, err)
			if h == exact {
				require.Equal(t, jobdb.DependencySatisfied, outcome.State)
			} else {
				require.Equal(t, jobdb.DependencyFailed, outcome.State)
				require.Error(t, outcome.Err())
			}
			require.NotNil(t, goalLease(t, r, h))
		}
		_, err = r.GetGoal(ctx, jobdb.GoalKey{TenantId: "another", GoalID: a.GoalID})
		require.ErrorIs(t, err, jobdb.ErrGoalNotFound)
		_, err = r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{JobID: "closed-restart", PriorJobKey: a.JobKey, ShareGoal: true}})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		invalid := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "invalid", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: "a", GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess}}}}
		_, err = r.SubmitJob(ctx, invalid)
		require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
	})
}
