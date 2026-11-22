package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffExecution(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, external := range []bool{false, true} {
			t.Run("B01_B02_B02b_B12/"+backend+map[bool]string{false: "/local", true: "/external"}[external], func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				ctx := celltest.Context(context.Background())
				var runs, builds, prior atomic.Int32
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					require.NotEmpty(t, c.InvocationID)
					require.Equal(t, 1, c.AttemptNumber)
					n := runs.Add(1)
					if n <= 7 {
						return nil, wf.EAGAINWait(c, wf.WithInitialWait(20*time.Millisecond), wf.WithMaxWait(20*time.Millisecond), wf.WithBackoffJitter(0), wf.WithCrashBudget(100))
					}
					return in, nil
				}}
				priorTask := executionTestTask{"prior", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { prior.Add(1); return in, nil }}
				call := invocation("work")
				call.Policy.InvocationTimeout = jobdb.AsDuration(time.Minute)
				w := wf.ExecutionWorker{JobType: "resource", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					p := invocation("prior")
					p.TaskType = "prior"
					if _, err := c.DoTask(p, func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
						return nil, err
					}
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
				}}
				key := submitExecutionTest(t, rt, w)
				if external {
					_, err := runResourceExecutionTest(t, rt, key, w, priorTask)
					require.NoError(t, err)
				}
				for i := 0; i < 7; i++ {
					out, err := runResourceExecutionTest(t, rt, key, w, task, priorTask)
					require.NoError(t, err)
					require.NotEqual(t, wf.JobRunCompleted, out.Status)
					info, err := rt.GetJob(ctx, key)
					require.NoError(t, err)
					require.Contains(t, []jobdb.JobStatus{jobdb.JobStatusAwaitingFuture, jobdb.JobStatusReady}, info.Status)
					require.NotNil(t, info.ExecutionState.ResourceResume)
					require.EqualValues(t, 100, *info.ExecutionState.CrashBudgetRemaining)
					time.Sleep(25 * time.Millisecond)
				}
				out, err := runResourceExecutionTest(t, rt, key, w, task, priorTask)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
				require.EqualValues(t, 1, prior.Load())
				require.EqualValues(t, 1, builds.Load())
				require.EqualValues(t, 8, runs.Load())
				records := executionRecords(t, rt, key)
				count := 0
				for _, rec := range records {
					if rec.Kind == "resource_checkpoint" {
						count++
						require.Equal(t, 1, rec.Resource.Attempt)
					}
				}
				require.Equal(t, 7, count)
			})
		}
	}
}
func TestResourceBackoffOptionsAndUnsupported(t *testing.T) {
	t.Run("C01a", func(t *testing.T) {
		require.ErrorIs(t, wf.EAGAINWait(wf.TaskContext{}), jobdb.ErrResourceBackoffUnsupported)
	})
	for _, opts := range [][]wf.ResourceBackoffOption{{wf.WithCrashBudget(0)}, {wf.WithBackoffJitter(.3)}, {wf.WithMaxWait(time.Minute)}, {wf.WithMaxWait(time.Second), wf.WithMaxWait(time.Second)}, {{}}} {
		require.ErrorIs(t, wf.EAGAINWait(wf.TaskContext{}, opts...), jobdb.ErrInvalidResourceBackoff)
	}
	require.False(t, errors.Is(wf.EAGAINWait(wf.TaskContext{}), context.DeadlineExceeded))
}

// Resource tests perform several real history/HTTP transactions per activation.
// Keep their lease independent of the short scheduler/deadline fixtures under test.
func runResourceExecutionTest(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, w wf.ExecutionWorker, tasks ...wf.TaskWorker) (wf.JobRunOutcome, error) {
	t.Helper()
	run, err := wf.GetJobForRun(celltest.Context(context.Background()), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: tasks, WorkerID: "resource-test", LeaseDuration: 30 * time.Second, ConsumerVersions: map[string]string{"c2j": "X", "pulse": "Y"}})
	require.NoError(t, err)
	return run.Run(nil)
}
