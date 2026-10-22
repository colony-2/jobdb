package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestExecutionNestedDeadlinesAndHandlerBudget(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, handler := range []bool{false, true} {
			for _, slowAdmission := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/handler=%v/slow-admission=%v", backend, handler, slowAdmission), func(t *testing.T) {
					var rt jobdb.WorkflowRuntime = alternateRuntime(t, backend)
					if slowAdmission {
						rt = &delayedExecutionAdmissionRuntime{WorkflowRuntime: rt}
					}
					var builds, handlers, workers atomic.Int32
					task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
						workers.Add(1)
						<-c.Context().Done()
						return nil, c.Context().Err()
					}}
					w := wf.ExecutionWorker{JobType: "nested-deadline", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
						options := wf.ScopeOptions{TotalTimeout: 120 * time.Millisecond, Retry: jobdb.RetryPolicy{MaximumAttempts: 3}}
						if handler {
							options.AttemptTimeout = 40 * time.Millisecond
							options.Catch = []wf.CatchRule{{ID: "bounded-handler", Match: func(error) bool { return true }, Handle: func(h wf.ExecutionContext, err error) (wf.CatchResult, error) {
								handlers.Add(1)
								e := h.AwaitDuration(jobdb.Duration(time.Hour))
								return wf.CatchResult{Disposition: wf.CatchHandled, Output: in}, e
							}}}
						}
						return c.DoScope("parent", options, func(p wf.ExecutionContext) (jobdb.JobData, error) {
							return p.DoScope("child", wf.ScopeOptions{TotalTimeout: time.Hour, AttemptTimeout: time.Hour, Retry: jobdb.RetryPolicy{MaximumAttempts: 9}}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
								return s.DoTask(invocation("task"), func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
							})
						})
					}}
					key := submitExecutionTest(t, rt, w)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "nested-deadline", LeaseDuration: time.Second})
					require.NoError(t, err)
					out, err := run.Run(nil)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunFailed, out.Status)
					var timeout jobdb.TimeoutError
					require.ErrorAs(t, out.JobError, &timeout)
					// Storage and scheduling consume the budget too. Neither the worker nor
					// the handler is guaranteed to be admitted before its deadline. Started
					// worker/handler cancellation is covered separately with explicit gates.
					require.LessOrEqual(t, builds.Load(), int32(1), "expired parent must not admit child retries")
					require.LessOrEqual(t, workers.Load(), int32(1))
					require.LessOrEqual(t, handlers.Load(), int32(1))
					if !handler {
						require.Zero(t, handlers.Load())
					}
					if slowAdmission {
						require.Zero(t, builds.Load(), "storage delay exhausts the parent budget before input construction")
						require.Zero(t, workers.Load())
						require.Zero(t, handlers.Load(), "expired total budget must not admit a catch handler")
					}
					built := builds.Load()
					_, err = replayExecutionTest(t, rt, key, w, task)
					require.ErrorAs(t, err, &timeout)
					require.Equal(t, built, builds.Load(), "replay must not build interrupted work")
				})
			}
		}
	}
}

func TestExecutionPatchedContinuationRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committed=%v", backend, committed), func(t *testing.T) {
				base := alternateRuntime(t, backend)
				var runs atomic.Int32
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
				w := wf.ExecutionWorker{JobType: "patch-recovery", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("outer", wf.ScopeOptions{TotalTimeout: 5 * time.Second}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						if _, err := s.DoTask(invocation("prefix"), func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
							return nil, err
						}
						value := "old"
						return s.DoScope("patched", wf.ScopeOptions{OnRestart: func(raw json.RawMessage) error { return json.Unmarshal(raw, &value) }}, func(p wf.ExecutionContext) (jobdb.JobData, error) {
							call := invocation("suffix")
							call.Command, _ = json.Marshal(value)
							return p.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(value), nil })
						})
					})
				}}
				source := submitExecutionTest(t, base, w)
				out, err := runExecutionTest(t, base, source, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				original, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: source})
				require.NoError(t, err)
				h, err := base.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: executionRestartTarget(t, base, source, "patched"), Payload: json.RawMessage(`"patched-value"`)}}})
				require.NoError(t, err)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rt := &executionFaultRuntime{WorkflowRuntime: base, cancel: cancel, fail: func(r jobdb.ExecutionRecord) bool {
					if committed {
						return r.Kind == "result" && r.ID == `/scope:"$job"#1/attempt:1`
					}
					return r.Kind == "result" && strings.Contains(r.ID, `task:"suffix"`)
				}}
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "interrupted", LeaseDuration: 500 * time.Millisecond})
				require.NoError(t, err)
				_, err = run.Run(nil)
				require.Error(t, err)
				require.True(t, rt.fired.Load())
				interrupted, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: h.JobKey})
				require.NoError(t, err)
				var continuation json.RawMessage
				for _, ch := range interrupted {
					for _, r := range executionChapterRecords(t, ch) {
						if r.Kind == "continuation" {
							continuation, err = json.Marshal(r)
							require.NoError(t, err)
						}
					}
				}
				if committed {
					require.NotEmpty(t, continuation)
				} else {
					require.Empty(t, continuation, "uncommitted path metadata need not survive")
				}

				require.Eventually(t, func() bool {
					out, err = runExecutionTest(t, base, h.JobKey, w, task)
					require.NoError(t, err)
					return out.Status != wf.JobRunNotLeaseable
				}, 2*time.Second, 20*time.Millisecond)
				require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
				data, err := out.Output.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"patched-value"`, string(data))
				want := int32(4)
				if committed {
					want = 3
				}
				require.Equal(t, want, runs.Load())
				_, err = replayExecutionTest(t, base, h.JobKey, w, task)
				require.NoError(t, err)
				require.Equal(t, want, runs.Load())
				final, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: h.JobKey})
				require.NoError(t, err)
				continuations := 0
				for _, ch := range final {
					for _, r := range executionChapterRecords(t, ch) {
						if r.Kind == "continuation" {
							continuations++
							if committed {
								raw, err := json.Marshal(r)
								require.NoError(t, err)
								require.JSONEq(t, string(continuation), string(raw))
							}
						}
					}
				}
				require.Equal(t, 1, continuations, "recovery must retain continuation budget")
				after, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: source})
				require.NoError(t, err)
				require.Equal(t, original, after)
			})
		}
	}
}

func TestExecutionInvalidDeclarations(t *testing.T) {
	cases := []struct {
		name  string
		call  wf.TaskCall
		scope *wf.ScopeOptions
	}{
		{name: "missing-site", call: wf.TaskCall{TaskType: "echo", ContractVersion: 1, Command: json.RawMessage(`null`)}},
		{name: "missing-type", call: wf.TaskCall{Site: "task", ContractVersion: 1, Command: json.RawMessage(`null`)}},
		{name: "missing-contract", call: wf.TaskCall{Site: "task", TaskType: "echo", Command: json.RawMessage(`null`)}},
		{name: "invalid-command", call: wf.TaskCall{Site: "task", TaskType: "echo", ContractVersion: 1, Command: json.RawMessage(`{`)}},
		{name: "negative-total", scope: &wf.ScopeOptions{TotalTimeout: -time.Second}},
		{name: "negative-attempt", scope: &wf.ScopeOptions{AttemptTimeout: -time.Second}},
		{name: "negative-retries", scope: &wf.ScopeOptions{Retry: jobdb.RetryPolicy{MaximumAttempts: -1}}},
		{name: "negative-backoff", scope: &wf.ScopeOptions{Retry: jobdb.RetryPolicy{MaximumInterval: -1}}},
		{name: "missing-handler", scope: &wf.ScopeOptions{Catch: []wf.CatchRule{{ID: "rule", Match: func(error) bool { return true }}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := alternateRuntime(t, "sqlite")
			var bodies atomic.Int32
			w := wf.ExecutionWorker{JobType: "invalid", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				if tc.scope != nil {
					return c.DoScope("invalid", *tc.scope, func(wf.ExecutionContext) (jobdb.JobData, error) { bodies.Add(1); return in, nil })
				}
				return c.DoTask(tc.call, func(context.Context) (jobdb.TaskData, error) { bodies.Add(1); return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.Zero(t, bodies.Load())
			_, err = replayExecutionTest(t, rt, key, w)
			require.Error(t, err)
			require.Zero(t, bodies.Load())
		})
	}
}

func TestExecutionRestartPrerequisite(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			prerequisite := wf.ExecutionWorker{JobType: "failed-prerequisite", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
				return nil, fmt.Errorf("prerequisite failed")
			}}
			dependency := submitExecutionTest(t, rt, prerequisite)
			out, err := runExecutionTest(t, rt, dependency, prerequisite)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			var calls atomic.Int32
			w := wf.ExecutionWorker{JobType: "restart-prerequisite", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("target", wf.ScopeOptions{OnRestart: func(json.RawMessage) error { return nil }}, func(wf.ExecutionContext) (jobdb.JobData, error) { calls.Add(1); return in, nil })
			}}
			source := submitExecutionTest(t, rt, w)
			out, err = runExecutionTest(t, rt, source, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			h, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: executionRestartTarget(t, rt, source, "target")}, Prerequisites: []jobdb.JobPrerequisite{{JobID: dependency.JobId, Condition: jobdb.JobPrereqSuccess}}}})
			require.NoError(t, err)
			out, err = runExecutionTest(t, rt, h.JobKey, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.ErrorContains(t, out.JobError, "did not succeed")
			require.EqualValues(t, 1, calls.Load(), "rejected restart must not enter its target")
			_, err = replayExecutionTest(t, rt, h.JobKey, w)
			require.ErrorContains(t, err, "did not succeed")
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

// delayedExecutionAdmissionRuntime models storage work consuming an attempt's budget.
type delayedExecutionAdmissionRuntime struct{ jobdb.WorkflowRuntime }

func (r *delayedExecutionAdmissionRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	if body, ok := req.Chapter.Body.(jobdb.ExecutionChapter); ok {
		rec, err := jobdb.DecodeExecutionRecord(body.Record)
		if err != nil {
			return err
		}
		if rec.Kind == "header" {
			time.Sleep(150 * time.Millisecond)
		}
	}
	return r.WorkflowRuntime.PutChapter(ctx, req)
}
