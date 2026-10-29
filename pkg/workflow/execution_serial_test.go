package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Cancellation must not let catches, retries, or the caller overtake a scope
// body or input builder. The builder's timed-out output must never reach a task.
func TestExecutionOrchestrationFinishesBeforeCatchAndRetry(t *testing.T) {
	for _, builder := range []bool{false, true} {
		t.Run(fmt.Sprintf("builder=%t", builder), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rt := alternateRuntime(t, "toy")
				started, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				var events []string
				attempts, taskRuns := 0, 0
				callback := func(ctx context.Context, in jobdb.JobData) (jobdb.JobData, error) {
					attempts++
					n := attempts
					events = append(events, fmt.Sprintf("callback:%d", n))
					defer func() { events = append(events, fmt.Sprintf("cleanup:%d", n)) }()
					if n == 1 {
						close(started)
						<-ctx.Done()
						events = append(events, "cancelled")
						<-release
					}
					return in, nil // A late success must still be treated as a timeout.
				}
				catches := []wf.CatchRule{{ID: "timeout", Match: func(err error) bool {
					events = append(events, "match")
					var timeout *jobdb.TimeoutError
					return errors.As(err, &timeout)
				}, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
					events = append(events, "handler")
					return wf.CatchResult{Disposition: wf.CatchContinue}, nil
				}}}
				task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					taskRuns++
					return in, nil
				}}
				worker := wf.ExecutionWorker{JobType: "serial", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("outer", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						if builder {
							call := invocation("input")
							call.Policy.InvocationTimeout = jobdb.AsDuration(time.Second)
							call.Policy.Retry.MaximumAttempts = 2
							call.Catch = catches
							return s.DoTask(call, func(ctx context.Context) (jobdb.TaskData, error) { return callback(ctx, in) })
						}
						return s.DoScope("inner", wf.ScopeOptions{AttemptTimeout: time.Second, Retry: jobdb.RetryPolicy{MaximumAttempts: 2}, Catch: catches}, func(inner wf.ExecutionContext) (jobdb.JobData, error) {
							return callback(inner.Context(), in)
						})
					})
				}}
				key := submitExecutionTest(t, rt, worker)
				run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: worker, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "serial", LeaseDuration: time.Hour})
				require.NoError(t, err)
				type result struct {
					out wf.JobRunOutcome
					err error
				}
				done := make(chan result, 1)
				go func() { out, err := run.Run(nil); done <- result{out, err} }()
				<-started
				time.Sleep(2 * time.Second) // Virtual time expires the admitted callback.
				synctest.Wait()
				require.Equal(t, []string{"callback:1", "cancelled"}, events)
				require.Zero(t, taskRuns)
				select {
				case <-done:
					t.Fatal("Run returned while orchestration was still blocked")
				default:
				}
				unblock()
				res := <-done
				require.NoError(t, res.err)
				require.Equal(t, wf.JobRunCompleted, res.out.Status, "%v", res.out.JobError)
				require.Equal(t, []string{"callback:1", "cancelled", "cleanup:1", "match", "handler", "callback:2", "cleanup:2"}, events)
				wantTaskRuns := 0
				if builder {
					wantTaskRuns = 1
				}
				require.Equal(t, wantTaskRuns, taskRuns)
				before, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				value, err := replayExecutionTest(t, rt, key, worker, task)
				require.NoError(t, err)
				require.Equal(t, res.out.Output.GetDataOrPanic(), value.GetDataOrPanic())
				require.Equal(t, wantTaskRuns, taskRuns)
				after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		})
	}
}

func TestExecutionSynchronousCallbackPanics(t *testing.T) {
	for _, phase := range []string{"body", "builder", "match", "handler"} {
		t.Run(phase, func(t *testing.T) {
			rt := alternateRuntime(t, "toy")
			worker := wf.ExecutionWorker{JobType: "panic", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				if phase == "builder" {
					return c.DoTask(invocation("input"), func(context.Context) (jobdb.TaskData, error) { panic("broken builder") })
				}
				opts := wf.ScopeOptions{}
				if phase != "body" {
					opts.Catch = []wf.CatchRule{{ID: "failure", Match: func(error) bool {
						if phase == "match" {
							panic("broken match")
						}
						return true
					}, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) { panic("broken handler") }}}
				}
				return c.DoScope("scope", opts, func(wf.ExecutionContext) (jobdb.JobData, error) {
					if phase == "body" {
						panic("broken body")
					}
					return nil, errors.New("failure")
				})
			}}
			key := submitExecutionTest(t, rt, worker)
			out, err := runExecutionTest(t, rt, key, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.ErrorContains(t, out.JobError, "panic: broken "+phase)
			_, err = replayExecutionTest(t, rt, key, worker)
			require.EqualError(t, err, out.JobError.Error())
		})
	}
}
