package workflow

import (
	"context"
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
	releaseTask := make(chan struct{})
	var releaseTaskOnce sync.Once
	release := func() {
		releaseTaskOnce.Do(func() { close(releaseTask) })
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
	job := replayHistoryJobOverride{JobWorker: countingJobWorker{name: "job"}, run: func(ctx ExecutionContext, in JobData) (JobData, error) {
		out, err := ctx.DoTask(TaskCall{Site: "task", Command: []byte("{}"), TaskType: task.Name(), ContractVersion: 1, Policy: RunPolicy{InvocationTimeout: AsDuration(time.Hour)}}, func(context.Context) (TaskData, error) { return in, nil })
		if context.Cause(ctx.Context()) != nil {
			// Orchestration unwinds before retry; expired calls still fail even
			// though the task worker remains blocked through the next attempt.
			_, taskErr := ctx.DoTask(TaskCall{Site: "task", Command: []byte("{}"), TaskType: task.Name(), ContractVersion: 1, Policy: RunPolicy{}}, func(context.Context) (TaskData, error) { return in, nil })
			_, submitErr := ctx.SubmitJob(context.Background(), SubmitJob{})
			_, restartErr := ctx.SubmitRestartJob(context.Background(), SubmitRestartJobRequest{Job: SubmitRestartJob{}})
			yieldErr := ctx.Yield(context.Background(), YieldRequest{Reschedule: RescheduleExecutionRequest{}})
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
	require.Len(t, before, 8) // execution setup, interrupted attempt, retry and final outcome
	require.True(t, chapterIs(before[len(before)-1], chapterTypeJobAttemptOutcome))
	release()
	select {
	case errs := <-jobExited:
		for _, err := range errs {
			var timeout *TimeoutError
			require.ErrorAs(t, err, &timeout)
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

func TestReplayAfterActualJobTimerCancelsOrchestration(t *testing.T) {
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "timer-interrupted"}
	var taskRuns atomic.Int32
	task := countingTaskWorker{name: "task", counter: &taskRuns}
	exited := make(chan struct{})
	job := replayHistoryJobOverride{JobWorker: countingJobWorker{name: "job"}, run: func(ctx ExecutionContext, in JobData) (JobData, error) {
		defer close(exited)
		if _, err := ctx.DoTask(TaskCall{Site: "task", Command: []byte("{}"), TaskType: task.Name(), ContractVersion: 1, Policy: RunPolicy{}}, func(context.Context) (TaskData, error) { return in, nil }); err != nil {
			return nil, err
		}
		<-ctx.Context().Done() // Orchestration cooperates with the job deadline.
		return in, nil
	}}
	ws := mustWorkSetForRunnerTest(t, job, task)
	policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 1}, InvocationTimeout: AsDuration(100 * time.Millisecond)}
	seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
	_, original := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(context.Background())
	<-exited
	var timeout TimeoutError
	require.ErrorAs(t, original, &timeout)
	require.EqualValues(t, 1, taskRuns.Load())
	job.run = func(ctx ExecutionContext, in JobData) (JobData, error) {
		for i := 0; i < 2; i++ {
			if _, err := ctx.DoTask(TaskCall{Site: "task", Command: []byte("{}"), TaskType: task.Name(), ContractVersion: 1, Policy: RunPolicy{}}, func(context.Context) (TaskData, error) { return in, nil }); err != nil {
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
