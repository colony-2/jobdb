package runtimetest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

// RunTerminalFailureConformance requires real protected publication, not seeded
// result views. HTTP harnesses must supply trusted cell/selection authority.
// This bounded suite covers publication and inspection; durable reopen, storage
// conversion and independent-writer races require backend-specific checks.
func RunTerminalFailureConformance(t *testing.T, r jobdb.WorkflowRuntime) {
	t.Helper()
	ctx := PrimaryGoalContext(context.Background())
	submit := func(t *testing.T, tenant, id, goal string, deps ...jobdb.Dependency) jobdb.JobHandle {
		t.Helper()
		h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: tenant, JobID: id, GoalID: goal, Cell: "work", CellRepo: "example.test/work",
			JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Dependencies: deps}})
		require.NoError(t, err)
		return h
	}
	goalKey := func(h jobdb.JobHandle) jobdb.GoalKey {
		return jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}
	}
	inspect := func(t *testing.T, g jobdb.GoalInfo) jobdb.JobKey {
		t.Helper()
		require.NotEmpty(t, g.FailureJobID)
		key := jobdb.JobKey{TenantId: g.GoalKey.TenantId, JobId: g.FailureJobID}
		info, err := r.GetJob(ctx, key)
		require.NoError(t, err)
		require.Equal(t, jobdb.JobRecordGoalFailure, info.RecordKind)
		require.Equal(t, g.GoalKey.GoalID, info.GoalID)
		require.Equal(t, g.CellID, info.CellID)
		require.Equal(t, g.CellRepo, info.CellRepo)
		require.Equal(t, jobdb.JobStatusCompleted, info.Status)
		require.Equal(t, "failed", info.CompletionStatus)
		require.Equal(t, g.Failure.Message, info.CompletionDetail)
		require.Equal(t, g.Failure, info.TerminalFailure)
		require.Nil(t, info.NextRoute)
		data, err := info.Data.GetData()
		require.Nil(t, data)
		require.ErrorIs(t, err, jobdb.ErrJobFailed)
		require.ErrorIs(t, err, jobdb.ErrGoalFailed)
		var failed *jobdb.GoalFailedError
		require.ErrorAs(t, err, &failed)
		require.Equal(t, *g.Failure, failed.Failure)
		list, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, GoalIDs: []string{g.GoalKey.GoalID}, JobKeys: []jobdb.JobKey{key}, IncludeHidden: true})
		require.NoError(t, err)
		require.Len(t, list.Jobs, 1)
		require.Equal(t, jobdb.GoalFailureJobType, list.Jobs[0].JobType)
		require.Equal(t, info.RecordKind, list.Jobs[0].RecordKind)
		require.Equal(t, g.Failure, list.Jobs[0].TerminalFailure)
		require.Equal(t, g.Failure.FailedAt, *list.Jobs[0].ArchivedAt)
		run, err := jobdb.GetJobRun(ctx, r, jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeOutputs: true, IncludeArtifacts: true, IncludeAttemptInputs: true})
		require.NoError(t, err)
		require.Zero(t, run.Start)
		require.Empty(t, run.Execution)
		require.Empty(t, run.Attempts)
		output, err := run.GetOutput(nil, key.TenantId)
		require.Nil(t, output)
		require.ErrorIs(t, err, jobdb.ErrGoalFailed)
		require.ErrorIs(t, err, jobdb.ErrJobFailed)
		chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		require.Empty(t, chapters)
		_, err = r.GetChapter(ctx, jobdb.ChapterRef{JobKey: key, Ordinal: 0})
		require.ErrorIs(t, err, jobdb.ErrChapterNotFound)
		_, err = r.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: key, Ordinal: 0, Name: "missing"})
		require.Error(t, err)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: jobdb.GoalFailureJobType}}})
		require.NoError(t, err)
		require.Nil(t, lease)
		leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: key.TenantId, WorkerID: "worker", Routes: []jobdb.Route{{JobType: jobdb.GoalFailureJobType}}})
		require.NoError(t, err)
		require.Empty(t, leases)
		events, err := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: key.TenantId, JobIDs: []string{key.JobId}})
		require.NoError(t, err)
		require.Empty(t, events.Events)
		_, err = r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, Cell: g.Cell, CellRepo: g.CellRepo, JobID: "restart-" + key.JobId}})
		require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
		_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: "other-" + key.TenantId, JobId: key.JobId})
		require.ErrorIs(t, err, jobdb.ErrJobNotFound)
		return key
	}

	t.Run("publication_waits_late_success_and_hidden_replay", func(t *testing.T) {
		old := submit(t, "terminal-lifecycle", "z-old", "")
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: old.JobKey}))
		previous := submit(t, old.JobKey.TenantId, "a-old", old.GoalID)
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: previous.JobKey}))
		active := submit(t, old.JobKey.TenantId, "active", old.GoalID)
		goalWait := submit(t, old.JobKey.TenantId, "goal-consumer", "", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: old.GoalID}, Condition: jobdb.JobPrereqSuccess})
		exactWait := submit(t, old.JobKey.TenantId, "exact-consumer", "", jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: active.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess})
		req := jobdb.FailGoalRequest{GoalKey: goalKey(old), Message: "  exact\n保存 <reason>  "}
		failed, err := r.FailGoal(ctx, req)
		require.NoError(t, err)
		require.Equal(t, []string{"a-old", "z-old"}, failed.Failure.TerminalJobIDs)
		require.Equal(t, []string{"active"}, failed.Failure.NonTerminalJobIDs)
		key := inspect(t, failed)
		wait, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: goalWait.JobKey, WaitID: "submission"})
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencyFailed, wait.State)
		require.Equal(t, failed.Failure, wait.Failure)
		exact, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: exactWait.JobKey, WaitID: "submission"})
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencyPending, exact.State)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: active.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
		require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter}))
		exact, err = r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: exactWait.JobKey, WaitID: "submission"})
		require.NoError(t, err)
		require.Equal(t, jobdb.DependencySatisfied, exact.State)
		require.Equal(t, active.JobKey.JobId, exact.Dependencies[0].SatisfiedByJobID)
		goal, err := r.GetGoal(ctx, req.GoalKey)
		require.NoError(t, err)
		require.Equal(t, failed, goal)
		_, err = r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: active.JobKey.TenantId, JobID: "closed-recovery", GoalID: active.GoalID, Cell: active.Cell, CellRepo: active.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		deletion, ok := r.(jobdb.JobDeletionRuntime)
		require.True(t, ok, "terminal conformance requires retained-job hiding")
		deleted, err := deletion.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: key.TenantId, JobIDs: []string{key.JobId}})
		require.NoError(t, err)
		require.EqualValues(t, 1, deleted.DeletedCount)
		retry, err := r.FailGoal(ctx, req)
		require.NoError(t, err)
		require.Equal(t, failed, retry)
		inspect(t, retry)
		visible, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, JobKeys: []jobdb.JobKey{key}})
		require.NoError(t, err)
		require.Empty(t, visible.Jobs)
		req.Message = "changed"
		_, err = r.FailGoal(ctx, req)
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		all, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, JobTypes: []string{jobdb.GoalFailureJobType}, IncludeHidden: true})
		require.NoError(t, err)
		require.Len(t, all.Jobs, 1)
	})
	t.Run("validation_and_exact_unicode_boundary", func(t *testing.T) {
		h := submit(t, "terminal-validation", "source", "")
		for _, message := range []string{"", " \n\t", string([]byte{0xff}), strings.Repeat("x", (16<<10)+1)} {
			_, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(h), Message: message})
			require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
			g, err := r.GetGoal(ctx, goalKey(h))
			require.NoError(t, err)
			require.Equal(t, jobdb.GoalOpen, g.State)
			list, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{h.JobKey.TenantId}})
			require.NoError(t, err)
			require.Len(t, list.Jobs, 1)
		}
		_, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: "missing"}, Message: "reason"})
		require.ErrorIs(t, err, jobdb.ErrGoalNotFound)
		message := strings.Repeat("保存", 2730) + "abcd"
		require.Len(t, message, 16<<10)
		g, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(h), Message: message})
		require.NoError(t, err)
		require.Equal(t, message, g.Failure.Message)
		inspect(t, g)
	})
	t.Run("ordinary_type_metadata_and_error_cannot_close_goal", func(t *testing.T) {
		h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "terminal-forgery", JobID: "ordinary", Cell: "work", CellRepo: "example.test/work",
			JobType: jobdb.GoalFailureJobType, Metadata: json.RawMessage(`{"recordKind":"GOAL_FAILURE","goal_closure_state":"FAILED"}`), Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: jobdb.GoalFailureJobType}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		fake := jobdb.GoalFailure{Code: "goal_failed", GoalID: h.GoalID, Message: "forged closure", FailedAt: time.Now().UTC()}
		chapter := jobdb.Chapter{Ordinal: 1, TaskType: jobdb.GoalFailureJobType, CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: fake.Message, Attrs: map[string]interface{}{"_jobdb_goal_failure": fake}}}}}
		require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
		g, err := r.GetGoal(ctx, goalKey(h))
		require.NoError(t, err)
		require.Equal(t, jobdb.GoalOpen, g.State)
		require.Empty(t, g.FailureJobID)
		require.Nil(t, g.Failure)
		info, err := r.GetJob(ctx, h.JobKey)
		require.NoError(t, err)
		require.False(t, info.RecordKind.IsTerminalRecord())
		require.Nil(t, info.TerminalFailure)
		recovery := submit(t, h.JobKey.TenantId, "recovery", h.GoalID)
		lease, err = r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: recovery.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		chapter = jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
		require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter}))
		_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(h), Message: "too late"})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		g, err = r.GetGoal(ctx, goalKey(h))
		require.NoError(t, err)
		require.Equal(t, jobdb.GoalSucceeded, g.State)
		require.Equal(t, recovery.JobKey.JobId, g.SuccessfulJobID)
		require.Empty(t, g.FailureJobID)
	})
}
