package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLiveDeterminismFailurePreservesDurableHistory(t *testing.T) {
	for _, history := range []string{"tasks", "retry", "terminal-success", "terminal-failure", "lease-lost", "missing-hash"} {
		t.Run(history, func(t *testing.T) {
			ctx := context.Background()
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: "history-failure"}
			var taskRuns atomic.Int32
			job := twoTaskJob{name: "job", first: "task", second: "task"}
			ws := mustWorkSetForRunnerTest(t, job, countingTaskWorker{name: "task", counter: &taskRuns})
			policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 3}}
			seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
			lease := func() *fakeExecutionLease {
				return &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
			}
			_, err := newWorkerRunner(rt, ws, lease(), workerRunnerOptions{}).DoJob(ctx)
			require.NoError(t, err)
			final := rt.chapters[key][3]
			switch history {
			case "terminal-success":
			case "terminal-failure":
				final.Body = JobAttemptOutcomeChapter{Outcome: AppErrorOutcome{Error: AppErrorPayload{Message: "original terminal failure"}}}
				rt.chapters[key][3] = final
			case "retry":
				final.Body = JobAttemptOutcomeChapter{Outcome: AppErrorOutcome{Error: AppErrorPayload{Message: "original retry"}}}
				meta, err := chapterMetaFromChapter(final)
				require.NoError(t, err)
				retryable := true
				meta.Retryable = &retryable
				raw, err := json.Marshal(meta)
				require.NoError(t, err)
				final.Metadata = mustChapterMetadataForRunnerTest(t, raw)
				rt.chapters[key][3] = final
				task := cloneChapterForTest(rt.chapters[key][2])
				task.Ordinal = 4
				rt.chapters[key][4] = task
			default:
				delete(rt.chapters[key], 3)
			}
			if history == "missing-hash" {
				ch := rt.chapters[key][1]
				ch.InputHash = ""
				delete(ch.Metadata.Fields, "input_hash")
				rt.chapters[key][1] = ch
			}
			before, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			ws.JobWorker = replayHistoryJobOverride{JobWorker: job, run: func(ctx JobContext, in JobData) (JobData, error) {
				return ctx.DoTask(RunPolicy{}, "task", NewTaskDataOrPanic(99))
			}}
			current := lease()
			if history == "lease-lost" {
				current.completeErr = ErrExecutionLeaseLost
			}
			observer := &replayObserverRecorder{}
			_, original := newWorkerRunner(rt, ws, current, workerRunnerOptions{Observer: observer}).DoJob(ctx)
			if history == "missing-hash" {
				require.ErrorIs(t, original, ErrMissingInputHash)
			} else {
				require.ErrorIs(t, original, ErrWorkflowNotDeterministic)
				var mismatch TaskInputMismatchError
				require.ErrorAs(t, original, &mismatch)
				require.EqualValues(t, 1, mismatch.Ordinal)
			}
			after, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			_, _, complete, reschedules := current.snapshot()
			require.Len(t, complete, 1)
			require.Empty(t, reschedules)
			require.EqualValues(t, 2, taskRuns.Load(), "recovery must never run task workers")
			switch history {
			case "terminal-success", "terminal-failure":
				require.Equal(t, before, after)
				require.Equal(t, final, *complete[0].Chapter)
				require.Len(t, observer.jobEnds, 1)
				require.ErrorIs(t, observer.jobEnds[0].Err, ErrWorkflowNotDeterministic)
				// Public synchronous execution must not turn this mismatch into a
				// successful API result just because a previous outcome was recovered.
				_, err = classifyJobRunAfterRun(ctx, rt, key, nil, nil, original)
				require.ErrorIs(t, err, ErrWorkflowNotDeterministic)
			case "lease-lost":
				require.ErrorIs(t, original, ErrExecutionLeaseLost)
				require.Equal(t, before, after)
			default:
				require.Len(t, after, len(before)+1)
				require.Equal(t, before, after[:len(before)])
				_, storedErr := chapterToTaskData(rt, key, after[len(after)-1])
				require.EqualError(t, storedErr, original.Error())
				meta, err := chapterMetaFromChapter(after[len(after)-1])
				require.NoError(t, err)
				require.NotNil(t, meta.Retryable)
				require.False(t, *meta.Retryable)
				expectedAttempt := 1
				if history == "retry" {
					expectedAttempt = 2
				}
				require.Equal(t, expectedAttempt, meta.Attempt)
				// Even with retry budget left, crash recovery must not retry a completed failure.
				check := newWorkerRunner(rt, ws, current, workerRunnerOptions{})
				hash, err := computeInputHash(ctx, NewTaskDataOrPanic(42))
				require.NoError(t, err)
				_, _, cached, terminal, _, checkErr, _, _ := check.checkCachedJobResult(ctx, after[len(after)-1].Ordinal, hash, policy.Retry, time.Time{}, 0, nil)
				require.NoError(t, checkErr)
				require.True(t, cached)
				require.True(t, terminal)
			}
			replay := newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true})
			_, err = replay.DoJob(ctx)
			if history == "missing-hash" {
				require.ErrorIs(t, err, ErrMissingInputHash)
			} else {
				require.ErrorIs(t, err, ErrWorkflowNotDeterministic)
			}
			replayed, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, after, replayed)
		})
	}
}

func TestWorkerEngineReportsUnhandledRunnerError(t *testing.T) {
	rt := newRunnerTestRuntime()
	var logs bytes.Buffer
	job := countingJobWorker{name: "job", counter: &atomic.Int32{}}
	ws := mustWorkSetForRunnerTest(t, job)
	api, err := newWorkerEngine(rt, []WorkSet{*ws}, RuntimeBuildOptions{PollTenantId: "tenant", Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	require.NoError(t, err)
	// No initial chapter: execution fails before it can complete or reschedule.
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: JobKey{TenantId: "tenant", JobId: "broken"}}, route: Route{JobType: "job"}}
	api.(*workerEngine).runLease(context.Background(), lease, "worker")
	require.Contains(t, logs.String(), "job execution failed")
	require.Contains(t, logs.String(), `"tenantId":"tenant"`)
	require.Contains(t, logs.String(), `"jobId":"broken"`)
	require.Contains(t, logs.String(), "worker")
	require.Contains(t, logs.String(), ErrChapterNotFound.Error())
	_, _, complete, rescheduled := lease.snapshot()
	require.Empty(t, complete)
	require.Empty(t, rescheduled)
}
