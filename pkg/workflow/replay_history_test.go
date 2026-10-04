package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The replay contract includes the ordered observer history, not just the final
// result. Normalize data/error representations so live and decoded events compare.
type historyEvent struct {
	Kind, Task, Data, Error string
	JobKey                  JobKey
	Ordinal                 int64
	Attempt                 int
	At                      time.Time
}

type historyObserver struct{ events []historyEvent }

func historyData(data TaskData) string {
	if data == nil {
		return ""
	}
	raw, err := data.GetData()
	if err != nil {
		return "data error: " + err.Error()
	}
	return string(raw)
}

func historyError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (o *historyObserver) OnJobStart(e JobStartEvent) {
	o.events = append(o.events, historyEvent{Kind: "job-start", JobKey: e.JobKey, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Input)})
}
func (o *historyObserver) OnJobEnd(e JobEndEvent) {
	o.events = append(o.events, historyEvent{Kind: "job-end", JobKey: e.JobKey, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Output), Error: historyError(e.Err)})
}
func (o *historyObserver) OnTaskStart(e TaskStartEvent) {
	o.events = append(o.events, historyEvent{Kind: "task-start", JobKey: e.JobKey, Task: e.TaskType, Ordinal: e.Ordinal, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Input)})
}
func (o *historyObserver) OnTaskEnd(e TaskEndEvent) {
	o.events = append(o.events, historyEvent{Kind: "task-end", JobKey: e.JobKey, Task: e.TaskType, Ordinal: e.Ordinal, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Output), Error: historyError(e.Err)})
}

func ageReplayHistory(t *testing.T, rt *runnerTestRuntime, key JobKey, age time.Duration) {
	t.Helper()
	for ordinal, chapter := range rt.chapters[key] {
		meta, err := chapterMetaFromChapter(chapter)
		require.NoError(t, err)
		meta.CreatedAt = meta.CreatedAt.Add(-age)
		for _, at := range []*time.Time{meta.StartedAt, meta.FinishedAt, meta.NextAttemptAt} {
			if at != nil {
				*at = at.Add(-age)
			}
		}
		raw, err := json.Marshal(meta)
		require.NoError(t, err)
		chapter.CreatedAt = chapter.CreatedAt.Add(-age)
		chapter.Metadata = mustChapterMetadataForRunnerTest(t, raw)
		rt.chapters[key][ordinal] = chapter
	}
}

func TestReplayHistorySurvivesDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		failures, tasks, attempts int32
		failed                    bool
	}{
		{"success", 0, 1, 1, false},
		{"failure", 1, 1, 1, true},
		{"task-retry-success", 2, 3, 1, false},
		{"job-retry-success", 3, 4, 2, false},
		{"nine-failures", 9, 9, 3, true},
	} {
		for _, deadline := range []string{"job", "task", "both"} {
			t.Run(tc.name+"/"+deadline, func(t *testing.T) {
				rt := newRunnerTestRuntime()
				key := JobKey{TenantId: "tenant", JobId: "history"}
				var taskRuns atomic.Int32
				task := failThenSucceedTask{name: "task", counter: &taskRuns, failAttempts: tc.failures}
				policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 3, InitialInterval: Duration(time.Millisecond), BackoffCoefficient: 1}}
				taskPolicy := policy
				if tc.name == "failure" {
					policy.Retry.MaximumAttempts, taskPolicy.Retry.MaximumAttempts = 1, 1
				}
				if deadline != "task" {
					policy.TotalTimeout = AsDuration(time.Hour)
				}
				if deadline != "job" {
					taskPolicy.TotalTimeout = AsDuration(time.Hour)
				}
				job := taskTimeoutJob{name: "history", taskType: task.Name(), policy: taskPolicy}
				ws := mustWorkSetForRunnerTest(t, job, task)
				seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), policy)
				live := &historyObserver{}
				lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
				runner := newWorkerRunner(rt, ws, lease, workerRunnerOptions{JobPolicy: policy, Observer: live})
				output, runErr := runner.DoJob(context.Background())
				require.Equal(t, tc.failed, runErr != nil)
				require.Equal(t, tc.tasks, taskRuns.Load())
				require.Len(t, live.events, int(2*(tc.tasks+tc.attempts)))
				engine, err := newWorkerEngine(rt, []WorkSet{*ws}, RuntimeBuildOptions{PollTenantId: key.TenantId})
				require.NoError(t, err)
				var writes atomic.Int32
				rt.putChapterHook = func(PutChapterRequest) error {
					writes.Add(1)
					return errors.New("replay attempted a write")
				}

				// Replay the same completed history at different ages, without wall
				// clock sleeps. Every event and outcome must be invariant except
				// for the deliberate translation of persisted timestamps.
				for _, age := range []time.Duration{0, 2 * time.Hour, 24 * time.Hour} {
					t.Run(fmt.Sprint(age), func(t *testing.T) {
						ageReplayHistory(t, rt, key, age)
						defer ageReplayHistory(t, rt, key, -age)
						before, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
						require.NoError(t, err)
						observer := &historyObserver{}
						got, replayErr := engine.ReplayJobRun(context.Background(), ReplayRunRequest{JobKey: key, Observer: observer})
						require.Equal(t, historyError(runErr), historyError(replayErr))
						require.Equal(t, historyData(output), historyData(got))
						want := append([]historyEvent(nil), live.events...)
						for i := range want {
							want[i].At = want[i].At.Add(-age)
						}
						require.Equal(t, want, observer.events)
						after, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
						require.NoError(t, err)
						require.Equal(t, before, after)
						require.Equal(t, tc.tasks, taskRuns.Load(), "replay must not execute task workers")
						require.Zero(t, writes.Load())
					})
				}
			})
		}
	}
}

type replayHistoryJobOverride struct {
	JobWorker
	run func(JobContext, JobData) (JobData, error)
}

func (j replayHistoryJobOverride) Run(ctx JobContext, data JobData) (JobData, error) {
	return j.run(ctx, data)
}

func TestReplayInvocationBudgetAndCallerCancellation(t *testing.T) {
	for _, mode := range []string{"slow-replay", "cancel-before", "cancel-during", "caller-deadline"} {
		t.Run(mode, func(t *testing.T) {
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: mode}
			var runs atomic.Int32
			job := singleTaskJob{name: "history", taskType: "task"}
			ws := mustWorkSetForRunnerTest(t, job, countingTaskWorker{name: "task", counter: &runs})
			seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), RunPolicy{})
			lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
			_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(context.Background())
			require.NoError(t, err)
			// A historical execution budget shorter than the inspection work
			// must not interrupt replay, even while total timeout is disabled.
			ch := rt.chapters[key][0]
			meta, err := chapterMetaFromChapter(ch)
			require.NoError(t, err)
			meta.RunPolicy.InvocationTimeout = AsDuration(time.Nanosecond)
			raw, err := json.Marshal(meta)
			require.NoError(t, err)
			ch.Metadata = mustChapterMetadataForRunnerTest(t, raw)
			rt.chapters[key][0] = ch
			engine, err := newWorkerEngine(rt, []WorkSet{*ws}, RuntimeBuildOptions{PollTenantId: key.TenantId})
			require.NoError(t, err)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("caller stopped inspection")
			var expectedErr error
			if mode == "cancel-before" {
				cancel(cause)
				expectedErr = cause
			} else if mode == "cancel-during" {
				expectedErr = cause
			} else if mode == "caller-deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 20*time.Millisecond)
				defer stop()
				expectedErr = context.DeadlineExceeded
			}
			override := replayHistoryJobOverride{JobWorker: job, run: func(jc JobContext, data JobData) (JobData, error) {
				switch mode {
				case "slow-replay":
					time.Sleep(10 * time.Millisecond)
				case "cancel-during":
					cancel(cause)
				case "caller-deadline":
					<-ctx.Done()
				}
				return job.Run(jc, data)
			}}
			var writes atomic.Int32
			rt.putChapterHook = func(PutChapterRequest) error {
				writes.Add(1)
				return errors.New("unexpected write")
			}
			out, err := engine.ReplayJobRun(ctx, ReplayRunRequest{JobKey: key, JobWorker: override})
			if expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, "42", historyData(out))
			} else {
				require.ErrorIs(t, err, expectedErr)
			}
			require.EqualValues(t, 1, runs.Load())
			require.Zero(t, writes.Load())
		})
	}
}

func TestReplayExpiredIncompleteHistory(t *testing.T) {
	for _, missing := range []string{"task", "job"} {
		t.Run(missing, func(t *testing.T) {
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: missing}
			var runs atomic.Int32
			job := singleTaskJob{name: "history", taskType: "task"}
			ws := mustWorkSetForRunnerTest(t, job, countingTaskWorker{name: "task", counter: &runs})
			seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), RunPolicy{TotalTimeout: AsDuration(time.Hour)})
			lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
			_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(context.Background())
			require.NoError(t, err)
			delete(rt.chapters[key], 2)
			expectedOrdinal, expectedReason := int64(2), ReplayCacheMissJobResultMissing
			if missing == "task" {
				delete(rt.chapters[key], 1)
				expectedOrdinal, expectedReason = 1, ReplayCacheMissTaskResultMissing
			}
			ageReplayHistory(t, rt, key, 2*time.Hour)
			before, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			engine, err := newWorkerEngine(rt, []WorkSet{*ws}, RuntimeBuildOptions{PollTenantId: key.TenantId})
			require.NoError(t, err)
			observer := &historyObserver{}
			_, err = engine.ReplayJobRun(context.Background(), ReplayRunRequest{JobKey: key, Observer: observer})
			var miss ReplayCacheMissError
			require.ErrorAs(t, err, &miss)
			require.Equal(t, expectedReason, miss.Reason)
			require.Equal(t, expectedOrdinal, miss.Ordinal)
			require.Equal(t, 1, miss.Attempt)
			require.Equal(t, "job-end", observer.events[len(observer.events)-1].Kind)
			after, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.EqualValues(t, 1, runs.Load())
		})
	}
}

func TestAwaitUntilHistoricalDeadlines(t *testing.T) {
	for _, replay := range []bool{false, true} {
		for _, pending := range []bool{false, true} {
			t.Run(fmt.Sprintf("replay=%v/pending=%v", replay, pending), func(t *testing.T) {
				runner := &workerRunner{replay: replay, ctx: context.Background()}
				wakeAt := time.Now().Add(-time.Hour)
				if pending {
					wakeAt = time.Now().Add(time.Hour)
				}
				expired := time.Now().Add(-time.Minute)
				err := runner.awaitUntil(wakeAt, 7, 2, "task", nil, expired, expired, time.Second, time.Second)
				if !replay {
					var timeout TimeoutError
					require.ErrorAs(t, err, &timeout)
				} else if pending {
					var miss ReplayCacheMissError
					require.ErrorAs(t, err, &miss)
					require.Equal(t, ReplayCacheMissAwaitNotReady, miss.Reason)
					require.EqualValues(t, 7, miss.Ordinal)
					require.Equal(t, 2, miss.Attempt)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestCachedTimeoutEmitsJobEndAndCompletesRecoveredLease(t *testing.T) {
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "recorded-timeout"}
	var runs atomic.Int32
	wantErr := NewTimeoutError("job", time.Second, TimeoutScopeInvocation, nil, false)
	job := replayHistoryJobOverride{
		JobWorker: countingJobWorker{name: "history", counter: &runs},
		run: func(JobContext, JobData) (JobData, error) {
			runs.Add(1)
			return nil, wantErr
		},
	}
	ws := mustWorkSetForRunnerTest(t, job)
	policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 1}, TotalTimeout: AsDuration(time.Hour)}
	seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
	_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(context.Background())
	require.EqualError(t, err, wantErr.Error())
	ageReplayHistory(t, rt, key, 2*time.Hour)
	for _, replay := range []bool{true, false} {
		t.Run(fmt.Sprintf("replay=%v", replay), func(t *testing.T) {
			observer := &replayObserverRecorder{}
			// The live case models a crash after recording the result but before
			// completing the schedule. A cached failure must finish that lease.
			recovered := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
			var executionLease ExecutionLease = recovered
			if replay {
				executionLease = nil
			}
			runner := newWorkerRunner(rt, ws, executionLease, workerRunnerOptions{JobKey: key, Replay: replay, Observer: observer})
			_, err := runner.DoJob(context.Background())
			var timeout TimeoutError
			require.ErrorAs(t, err, &timeout)
			require.Equal(t, TimeoutScopeInvocation, timeout.Payload.Scope, "preserve the recorded timeout, not today's total deadline")
			require.Equal(t, "timeout_invocation", timeout.Payload.Code)
			require.Equal(t, Duration(time.Second), timeout.Payload.After)
			require.Len(t, observer.jobEnds, 1)
			require.EqualError(t, observer.jobEnds[0].Err, wantErr.Error())
			require.EqualValues(t, 1, runs.Load())
			_, _, completions, _ := recovered.snapshot()
			if replay {
				require.Empty(t, completions)
			} else {
				require.Len(t, completions, 1)
			}
		})
	}
}
