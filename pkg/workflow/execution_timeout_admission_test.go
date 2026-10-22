package workflow_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Delay the durable initial anchor so a short block budget is already exhausted
// before scope admission, without requiring a standalone scope/attempt append.
type expiredScopeAdmissionRuntime struct{ jobdb.WorkflowRuntime }

func (r expiredScopeAdmissionRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	if body, ok := req.Chapter.Body.(jobdb.ExecutionChapter); ok {
		rec, err := jobdb.DecodeExecutionRecord(body.Record)
		if err != nil {
			return err
		}
		if rec.Kind == "header" {
			timer := time.NewTimer(30 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return r.WorkflowRuntime.PutChapter(ctx, req)
}

func testScopeTimeoutAdmission(t *testing.T, base jobdb.WorkflowRuntime, admitted bool) {
	t.Helper()
	rt := base
	timeout := time.Hour
	if !admitted {
		rt = expiredScopeAdmissionRuntime{base}
		timeout = 20 * time.Millisecond // Exhausted by the wrapper, never a race to start.
	}
	release := make(chan struct{})
	exited := make(chan struct{})
	late := make(chan error, 1)
	var starts, handlers, matches, builds atomic.Int32
	var replaying atomic.Bool
	unblock := sync.OnceFunc(func() { close(release) })
	defer func() {
		unblock()
		if starts.Load() > 0 {
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Error("scope callback did not exit after release")
			}
		}
	}()
	worker := wf.ExecutionWorker{JobType: "scope-timeout", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("timed", wf.ScopeOptions{AttemptTimeout: timeout, Catch: []wf.CatchRule{{ID: "timeout", Match: func(err error) bool {
			matches.Add(1)
			var timeout *jobdb.TimeoutError
			return errors.As(err, &timeout)
		}, Handle: func(h wf.ExecutionContext, err error) (wf.CatchResult, error) {
			handlers.Add(1)
			if err := h.Context().Err(); err != nil {
				return wf.CatchResult{}, err
			}
			return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("caught")}, nil
		}}}}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
			lateTask := func() (jobdb.JobData, error) {
				return s.DoTask(invocation("late"), func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}
			// Replay revisits scope orchestration, but the recorded timeout must
			// unwind this call without constructing input or admitting a worker.
			if replaying.Load() {
				return lateTask()
			}
			starts.Add(1)
			defer close(exited)
			wf.ExpireExecutionAttemptForTest(s)
			<-release
			_, err := lateTask()
			late <- err
			return in, nil
		})
	}}
	key := submitExecutionTest(t, rt, worker)
	out, err := runBoundedTimeoutTest(t, rt, key, worker)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status, "job error: %v", out.JobError)
	raw, err := out.Output.GetData()
	require.NoError(t, err)
	require.JSONEq(t, `"caught"`, string(raw))
	require.EqualValues(t, 1, handlers.Load())
	require.EqualValues(t, 1, matches.Load())
	wantStarts := int32(0)
	if admitted {
		wantStarts = 1
	}
	require.Equal(t, wantStarts, starts.Load())
	unblock()
	if admitted {
		select {
		case err := <-late:
			var timeout *jobdb.TimeoutError
			require.ErrorAs(t, err, &timeout)
		case <-time.After(5 * time.Second):
			t.Fatal("admitted scope did not reject its late task call")
		}
	}
	require.Zero(t, builds.Load(), "expired scope must not construct late input")
	before, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	replaying.Store(true)
	value, err := replayExecutionTest(t, rt, key, worker)
	require.NoError(t, err)
	raw, err = value.GetData()
	require.NoError(t, err)
	require.JSONEq(t, `"caught"`, string(raw))
	after, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, wantStarts, starts.Load(), "replay must not execute live callback effects")
	require.EqualValues(t, 2, handlers.Load(), "replay revisits the selected handler's orchestration")
	require.EqualValues(t, 1, matches.Load(), "replay must reuse the recorded catch selection")
	require.Zero(t, builds.Load())
}

func runBoundedTimeoutTest(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, worker wf.ExecutionWorker, tasks ...wf.TaskWorker) (wf.JobRunOutcome, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: worker, TaskWorkers: tasks, WorkerID: "timeout-test", LeaseDuration: time.Minute})
	require.NoError(t, err)
	return run.Run(nil)
}

func testLateTaskRetry(t *testing.T, rt jobdb.WorkflowRuntime) {
	t.Helper()
	var runs, builds atomic.Int32
	release, exited := make(chan struct{}), make(chan struct{})
	scopes := make(chan wf.ExecutionContext, 1)
	observed := make(chan error, 1)
	unblock := sync.OnceFunc(func() { close(release) })
	defer func() {
		unblock()
		if runs.Load() > 0 {
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Error("late task did not exit after release")
			}
		}
	}()
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		if runs.Add(1) == 1 {
			defer close(exited)
			if !wf.ExpireExecutionTaskAttemptForTest(<-scopes, c) {
				return nil, errors.New("running task attempt not found")
			}
			<-release
			observed <- c.Context().Err()
			return jobdb.NewTaskDataOrPanic("late"), nil
		}
		return jobdb.NewTaskDataOrPanic("current"), nil
	}}
	call := invocation("retry")
	call.Policy.InvocationTimeout = jobdb.AsDuration(time.Hour)
	call.Policy.Retry.MaximumAttempts = 2
	worker := wf.ExecutionWorker{JobType: "late-task", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		if err := c.AwaitDuration(jobdb.Duration(time.Millisecond)); err != nil {
			return nil, err
		}
		return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); scopes <- c; return in, nil })
	}}
	key := submitExecutionTest(t, rt, worker)
	out, err := runBoundedTimeoutTest(t, rt, key, worker, task)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status, "job error: %v", out.JobError)
	require.EqualValues(t, 2, runs.Load())
	require.EqualValues(t, 1, builds.Load(), "task retries reuse the resolved input")
	unblock()
	select {
	case err := <-observed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("first task did not observe cancellation")
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("first task did not exit")
	}
	raw, err := out.Output.GetData()
	require.NoError(t, err)
	require.JSONEq(t, `"current"`, string(raw))
	value, err := replayExecutionTest(t, rt, key, worker, task)
	require.NoError(t, err)
	raw, err = value.GetData()
	require.NoError(t, err)
	require.JSONEq(t, `"current"`, string(raw))
	require.EqualValues(t, 2, runs.Load())
	require.EqualValues(t, 1, builds.Load())
}
