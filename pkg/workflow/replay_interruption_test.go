package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Model problem2 job 3KF5WM1xjsmh6MRcRllqBHj0w09 without its private
// recipe/source payloads: seven successful tasks, a job invocation timeout,
// another seven tasks and invocation timeout, then seven tasks and total timeout.
// Replay enters the interrupted task, which has no TaskAttemptOutcome chapter.
func TestReplayInterruptedJobAttempts(t *testing.T) {
	for _, replay := range []bool{true, false} {
		for _, catch := range []string{"propagate", "success", "nil"} {
			t.Run(fmt.Sprintf("replay=%v/catch=%s", replay, catch), func(t *testing.T) {
				rt := newRunnerTestRuntime()
				key := JobKey{TenantId: "tenant", JobId: "interrupted"}
				var taskRuns atomic.Int32
				task := countingTaskWorker{name: "task", counter: &taskRuns}
				var attempts int
				job := replayHistoryJobOverride{JobWorker: countingJobWorker{name: "job"}, run: func(ctx JobContext, in JobData) (JobData, error) {
					attempts++
					for i := 0; i < 7; i++ {
						if _, err := ctx.DoTask(RunPolicy{}, task.Name(), in); err != nil {
							return nil, err
						}
					}
					if attempts < 3 {
						return nil, NewTimeoutError("job", 10*time.Minute, TimeoutScopeInvocation, nil, true)
					}
					return nil, NewTimeoutError("job", 30*time.Minute, TimeoutScopeTotal, nil, false)
				}}
				policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 3}}
				ws := mustWorkSetForRunnerTest(t, job, task)
				seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
				lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
				_, original := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(context.Background())
				require.EqualValues(t, 21, taskRuns.Load())
				before, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Len(t, before, 25)
				job.run = func(ctx JobContext, in JobData) (JobData, error) {
					for i := 0; i < 8; i++ {
						if _, err := ctx.DoTask(RunPolicy{}, task.Name(), in); err != nil {
							if catch == "nil" {
								return nil, nil
							}
							if catch == "success" {
								return in, nil // cannot turn a recorded job timeout into success
							}
							return nil, fmt.Errorf("orchestration wrapper: %w", err)
						}
					}
					return in, nil
				}
				ws.JobWorker = job
				var executionLease ExecutionLease
				if !replay {
					executionLease = &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
				}
				var writes atomic.Int32
				rt.putChapterHook = func(PutChapterRequest) error { writes.Add(1); return errors.New("unexpected history write") }
				observer := &replayObserverRecorder{}
				runner := newWorkerRunner(rt, ws, executionLease, workerRunnerOptions{JobKey: key, Replay: replay, Observer: observer})
				_, err = runner.DoJob(context.Background())
				require.EqualError(t, err, original.Error())
				require.Len(t, observer.jobEnds, 3)
				require.Len(t, observer.taskStarts, 21)
				require.Len(t, observer.taskEnds, 21)
				for i, event := range observer.taskEnds {
					require.EqualValues(t, i+1+i/7, event.Ordinal)
					require.NoError(t, event.Err)
				}
				for i, event := range observer.jobEnds {
					var timeout TimeoutError
					require.ErrorAs(t, event.Err, &timeout)
					require.Equal(t, i+1, event.AttemptNumber)
					meta, err := chapterMetaFromChapter(before[(i+1)*8])
					require.NoError(t, err)
					require.Equal(t, metaEndAt(meta), event.At)
				}
				after, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.Zero(t, writes.Load())
				require.EqualValues(t, 21, taskRuns.Load())
			})
		}
	}
}

type interruptionTaskWorker struct {
	run func(TaskContext, TaskData) (TaskData, error)
}

func (interruptionTaskWorker) Name() string { return "task" }
func (w interruptionTaskWorker) Run(ctx TaskContext, in TaskData) (TaskData, error) {
	return w.run(ctx, in)
}

func TestTimedOutAttemptCannotWriteIntoRetry(t *testing.T) {
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "late-attempt"}
	releaseTask, releaseJob := make(chan struct{}), make(chan struct{})
	var releaseTaskOnce, releaseJobOnce sync.Once
	release := func() {
		releaseTaskOnce.Do(func() { close(releaseTask) })
		releaseJobOnce.Do(func() { close(releaseJob) })
	}
	t.Cleanup(release)
	taskExited, jobExited := make(chan struct{}), make(chan []error, 1)
	var taskRuns atomic.Int32
	task := interruptionTaskWorker{run: func(_ TaskContext, in TaskData) (TaskData, error) {
		if taskRuns.Add(1) == 1 {
			defer close(taskExited)
			<-releaseTask // ignores cancellation, returns after the retry completed
		}
		return in, nil
	}}
	job := replayHistoryJobOverride{JobWorker: countingJobWorker{name: "job"}, run: func(ctx JobContext, in JobData) (JobData, error) {
		out, err := ctx.DoTask(RunPolicy{InvocationTimeout: AsDuration(time.Hour)}, task.Name(), in)
		if errors.Is(err, context.Canceled) {
			<-releaseJob // delayed cleanup tries to use the expired attempt
			_, taskErr := ctx.DoTask(RunPolicy{}, task.Name(), in)
			_, submitErr := ctx.SubmitJob(context.Background(), SubmitJob{})
			_, restartErr := ctx.SubmitRestartJob(context.Background(), SubmitRestartJob{})
			yieldErr := ctx.Yield(context.Background(), RescheduleExecutionRequest{})
			jobExited <- []error{taskErr, submitErr, restartErr, yieldErr}
		}
		return out, err
	}}
	ws := mustWorkSetForRunnerTest(t, job, task)
	policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 2}, InvocationTimeout: AsDuration(100 * time.Millisecond)}
	seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(ctx)
	require.NoError(t, err)
	before, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	require.Len(t, before, 4) // start, interrupted job, successful task, successful job
	require.True(t, chapterIs(before[1], chapterTypeJobAttemptOutcome))
	require.True(t, chapterIs(before[2], chapterTypeTaskAttemptOutcome))
	release()
	select {
	case errs := <-jobExited:
		for _, err := range errs {
			require.ErrorIs(t, err, context.Canceled)
		}
	case <-ctx.Done():
		t.Fatal("timed-out job did not exit")
	}
	select {
	case <-taskExited:
	case <-ctx.Done():
		t.Fatal("timed-out task did not exit")
	}
	after, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	require.Equal(t, before, after)
	observer := &replayObserverRecorder{}
	_, err = newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: observer}).DoJob(ctx)
	require.NoError(t, err)
	require.Len(t, observer.jobEnds, 2)
	require.Len(t, observer.taskEnds, 1)
	require.EqualValues(t, 2, taskRuns.Load())
}

// Simulate a chapter store committing a task outcome, then losing the response
// when the caller's attempt is cancelled. Scheduling is an independent operation.
type interruptedAppendRuntime struct{ *runnerTestRuntime }

func (r interruptedAppendRuntime) PutChapter(ctx context.Context, req PutChapterRequest) error {
	if err := r.runnerTestRuntime.PutChapter(ctx, req); err != nil {
		return err
	}
	if chapterIs(req.Chapter, chapterTypeTaskAttemptOutcome) {
		<-ctx.Done()
		return context.Cause(ctx)
	}
	return nil
}

func TestJobTimeoutAfterTaskAppendLosesAcknowledgement(t *testing.T) {
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "committed-task"}
	var runs atomic.Int32
	job := singleTaskJob{name: "job", taskType: "task"}
	ws := mustWorkSetForRunnerTest(t, job, countingTaskWorker{name: "task", counter: &runs})
	policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 1}, InvocationTimeout: AsDuration(100 * time.Millisecond)}
	seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, original := newWorkerRunner(interruptedAppendRuntime{rt}, ws, lease, workerRunnerOptions{}).DoJob(ctx)
	var timeout TimeoutError
	require.ErrorAs(t, original, &timeout)
	require.Equal(t, "job", timeout.Payload.Kind)
	chapters, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	require.Len(t, chapters, 3)
	require.True(t, chapterIs(chapters[1], chapterTypeTaskAttemptOutcome))
	require.True(t, chapterIs(chapters[2], chapterTypeJobAttemptOutcome))
	observer := &replayObserverRecorder{}
	_, err = newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: observer}).DoJob(ctx)
	require.EqualError(t, err, original.Error())
	require.Len(t, observer.taskEnds, 1)
	require.Len(t, observer.jobEnds, 1)
	require.EqualValues(t, 1, runs.Load())
}

func TestReplayAfterActualJobTimerInterruptsOrchestration(t *testing.T) {
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "timer-interrupted"}
	var taskRuns atomic.Int32
	task := countingTaskWorker{name: "task", counter: &taskRuns}
	release, exited := make(chan struct{}), make(chan struct{})
	job := replayHistoryJobOverride{JobWorker: countingJobWorker{name: "job"}, run: func(ctx JobContext, in JobData) (JobData, error) {
		defer close(exited)
		if _, err := ctx.DoTask(RunPolicy{}, task.Name(), in); err != nil {
			return nil, err
		}
		<-release // timeout interrupts orchestration without a task outcome
		return in, nil
	}}
	ws := mustWorkSetForRunnerTest(t, job, task)
	policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 1}, InvocationTimeout: AsDuration(100 * time.Millisecond)}
	seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
	_, original := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(context.Background())
	close(release)
	<-exited
	var timeout TimeoutError
	require.ErrorAs(t, original, &timeout)
	require.EqualValues(t, 1, taskRuns.Load())
	job.run = func(ctx JobContext, in JobData) (JobData, error) {
		for i := 0; i < 2; i++ {
			if _, err := ctx.DoTask(RunPolicy{}, task.Name(), in); err != nil {
				return nil, err
			}
		}
		return in, nil
	}
	ws.JobWorker = job
	observer := &replayObserverRecorder{}
	_, err := newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: observer}).DoJob(context.Background())
	require.EqualError(t, err, original.Error())
	require.Len(t, observer.taskEnds, 1)
	require.Len(t, observer.jobEnds, 1)
	require.EqualValues(t, 1, taskRuns.Load())
}

func TestTaskBoundaryOnlyAcceptsRecordedJobTimeout(t *testing.T) {
	for _, kind := range []string{"invocation", "total", "task-timeout", "application-failure", "success", "input-mismatch"} {
		t.Run(kind, func(t *testing.T) {
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: "boundary"}
			var runs atomic.Int32
			job := singleTaskJob{name: "job", taskType: "task"}
			ws := mustWorkSetForRunnerTest(t, job, countingTaskWorker{name: "task", counter: &runs})
			seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), RunPolicy{Retry: RetryPolicy{MaximumAttempts: 1}})
			lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
			_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(context.Background())
			require.NoError(t, err)
			final := rt.chapters[key][2]
			var expected error
			switch kind {
			case "invocation", "total", "task-timeout":
				timeoutKind, scope := "job", kind
				if kind == "task-timeout" {
					timeoutKind, scope = "task", TimeoutScopeInvocation
				}
				expected = NewTimeoutError(timeoutKind, time.Minute, scope, nil, false)
				var timeout TimeoutError
				require.ErrorAs(t, expected, &timeout)
				final.Body = JobAttemptOutcomeChapter{Outcome: TimeoutOutcome{Timeout: timeout.Payload}}
			case "application-failure":
				final.Body = JobAttemptOutcomeChapter{Outcome: AppErrorOutcome{Error: AppErrorPayload{Message: "recorded failure"}}}
			}
			rt.chapters[key][2] = final
			ws.JobWorker = replayHistoryJobOverride{JobWorker: job, run: func(ctx JobContext, in JobData) (JobData, error) {
				if kind == "input-mismatch" {
					in = NewTaskDataOrPanic(99)
				}
				for i := 0; i < 2; i++ {
					if _, err := ctx.DoTask(RunPolicy{}, "task", in); err != nil {
						return nil, err
					}
				}
				return in, nil
			}}
			observer := &replayObserverRecorder{}
			runner := newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: observer})
			_, err = runner.DoJob(context.Background())
			if kind == "invocation" || kind == "total" {
				require.EqualError(t, err, expected.Error())
				require.Len(t, observer.jobEnds, 1)
				require.EqualValues(t, 3, runner.storyCounter)
			} else {
				require.ErrorIs(t, err, ErrWorkflowNotDeterministic)
				if kind == "input-mismatch" {
					var mismatch TaskInputMismatchError
					require.ErrorAs(t, err, &mismatch)
					require.EqualValues(t, 1, runner.storyCounter)
				} else {
					require.Contains(t, err.Error(), `unexpected chapter type "JobAttemptOutcome" at ordinal 2`)
					require.EqualValues(t, 2, runner.storyCounter)
				}
				require.Empty(t, observer.jobEnds, "a determinism error must not be reported as the cached job outcome")
			}
			require.EqualValues(t, 1, runs.Load())
		})
	}
}
