package workflow_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type interruptedReplayJob struct{}

func (interruptedReplayJob) Name() string { return "interrupted-replay" }
func (interruptedReplayJob) Run(ctx wf.JobContext, in jobdb.JobData) (jobdb.JobData, error) {
	for _, name := range []string{"prepare", "interrupt"} {
		if _, err := ctx.DoTask(jobdb.RunPolicy{InvocationTimeout: jobdb.AsDuration(time.Hour)}, name, in); err != nil {
			return nil, err
		}
	}
	return in, nil
}

type interruptedReplayTask struct {
	name    string
	runs    *atomic.Int32
	release <-chan struct{}
	exited  chan<- struct{}
}

func (w interruptedReplayTask) Name() string { return w.name }
func (w interruptedReplayTask) Run(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	if w.runs.Add(1) == 1 && w.release != nil {
		defer close(w.exited)
		<-w.release
	}
	return in, nil
}

type interruptionEvents struct{ events []string }

func (o *interruptionEvents) OnJobStart(e wf.JobStartEvent) {
	o.events = append(o.events, fmt.Sprintf("job-start:%d", e.AttemptNumber))
}
func (o *interruptionEvents) OnJobEnd(e wf.JobEndEvent) {
	o.events = append(o.events, fmt.Sprintf("job-end:%d:%v", e.AttemptNumber, e.Err != nil))
}
func (o *interruptionEvents) OnTaskStart(e wf.TaskStartEvent) {
	o.events = append(o.events, fmt.Sprintf("task-start:%d", e.Ordinal))
}
func (o *interruptionEvents) OnTaskEnd(e wf.TaskEndEvent) {
	o.events = append(o.events, fmt.Sprintf("task-end:%d", e.Ordinal))
}

// Generate history through the public run API and actual timers, then replay the
// same orchestration. This exercises adapter serialization and lease completion,
// including the missing task outcome at the first job's timeout boundary.
func TestInterruptedJobReplayAcrossRuntimes(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			release, exited := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var prepareRuns, interruptedRuns atomic.Int32
			job := interruptedReplayJob{}
			tasks := []wf.TaskWorker{
				interruptedReplayTask{name: "prepare", runs: &prepareRuns},
				interruptedReplayTask{name: "interrupt", runs: &interruptedRuns, release: release, exited: exited},
			}
			policy := jobdb.DefaultRunPolicy()
			policy.Retry.MaximumAttempts = 2
			policy.InvocationTimeout = jobdb.AsDuration(300 * time.Millisecond)
			handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
				TenantId: "tenant", JobType: job.Name(), Data: jobdb.NewTaskDataOrPanic(42), RunPolicy: policy,
			}})
			require.NoError(t, err)
			run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{
				JobKey: handle.JobKey, JobWorker: job, TaskWorkers: tasks, WorkerID: "worker", LeaseDuration: time.Second,
			})
			require.NoError(t, err)
			outcome, err := run.Run(nil)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, outcome.Status)
			unblock()
			select {
			case <-exited:
			case <-ctx.Done():
				t.Fatal("interrupted task did not exit")
			}
			before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			require.Len(t, before, 6)
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(job, tasks...).BuildEngine()
			require.NoError(t, err)
			observer := &interruptionEvents{}
			result, err := engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: handle.JobKey, JobWorker: job, Observer: observer})
			require.NoError(t, err)
			raw, err := result.GetData()
			require.NoError(t, err)
			require.JSONEq(t, "42", string(raw))
			require.Equal(t, []string{
				"job-start:1", "task-start:1", "task-end:1", "job-end:1:true",
				"job-start:2", "task-start:3", "task-end:3", "task-start:4", "task-end:4", "job-end:2:false",
			}, observer.events)
			after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.EqualValues(t, 2, prepareRuns.Load())
			require.EqualValues(t, 2, interruptedRuns.Load())
		})
	}
}
