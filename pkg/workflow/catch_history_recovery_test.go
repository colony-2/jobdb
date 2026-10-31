package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestCatchHistorySuspendResume(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			var predicates, handlers atomic.Int32
			rules := []wf.CatchRule{{ID: "human-handler", Match: func(error) bool { predicates.Add(1); return true }, Handle: func(c wf.ExecutionContext, _ error) (wf.CatchResult, error) {
				handlers.Add(1)
				call := invocation("human")
				call.TaskType = "human"
				output, err := c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic("question"), nil })
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: output}, err
			}}}
			w := wf.ExecutionWorker{JobType: "handler-suspend", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("caught", wf.ScopeOptions{Catch: rules}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("original") })
			}}
			key := submitExecutionTest(t, recording, w)
			out, err := runExecutionTest(t, recording, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			before := readCatches(t, inspection, key)
			pending := catchAt(t, before, "caught")
			require.Equal(t, wf.CatchHandlingPending, pending.Handling)
			require.Nil(t, pending.Accepted)
			require.EqualValues(t, 1, predicates.Load())
			require.EqualValues(t, 1, handlers.Load())
			// Separate public engine only completes external work; inspection never uses it.
			completion, err := wf.NewEngineBuilder().WithRuntime(recording).BuildEngine()
			require.NoError(t, err)
			waiting, err := completion.GetWaitingTask(context.Background(), key)
			require.NoError(t, err)
			require.NoError(t, waiting.Finish(context.Background(), jobdb.NewTaskDataOrPanic("answer")))
			rules[0].Match = func(error) bool { panic("recorded predicate reevaluated") }
			out, err = runExecutionTest(t, recording, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			a := catchAt(t, readCatches(t, inspection, key), "caught")
			require.Equal(t, pending.Boundary, a.Boundary)
			require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
			require.JSONEq(t, `"answer"`, string(a.Accepted.Output.Data))
			h := handlers.Load()
			_ = readCatches(t, inspection, key)
			require.Equal(t, h, handlers.Load())
			require.EqualValues(t, 1, predicates.Load())
		})
	}
}

func TestCatchHistoryRestart(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			recoverRule := wf.CatchRule{ID: "recover", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("ok")}, nil
			}}
			w := wf.ExecutionWorker{JobType: "catch-restart", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				if _, err := c.DoScope("prior", wf.ScopeOptions{Catch: []wf.CatchRule{recoverRule}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("first") }); err != nil {
					return nil, err
				}
				return c.DoScope("restart-target", wf.ScopeOptions{Catch: []wf.CatchRule{recoverRule}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("second") })
			}}
			source := submitExecutionTest(t, recording, w)
			out, err := runExecutionTest(t, recording, source, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			before := readCatches(t, inspection, source)
			target := catchAt(t, before, "restart-target").Boundary.BoundaryID
			restarted, err := recording.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}})
			require.NoError(t, err)
			out, err = runExecutionTest(t, recording, restarted.JobKey, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			after := readCatches(t, inspection, restarted.JobKey)
			require.Len(t, after.Attempts, 2)
			for _, a := range after.Attempts {
				require.Equal(t, restarted.JobKey, a.Boundary.JobKey)
				require.NotEqual(t, source, a.Boundary.JobKey)
				require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
			}
			require.Equal(t, before, readCatches(t, inspection, source))
		})
	}
}
