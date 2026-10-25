package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type executionTestTask struct {
	name string
	run  func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error)
}

func (t executionTestTask) Name() string { return t.name }
func (t executionTestTask) Run(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	return t.run(c, in)
}
func invocation(site string) wf.TaskCall {
	return wf.TaskCall{Site: site, TaskType: "echo", ContractVersion: 1, Command: json.RawMessage(`{"operation":"echo"}`)}
}
func runExecutionTest(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, w wf.ExecutionWorker, tasks ...wf.TaskWorker) (wf.JobRunOutcome, error) {
	t.Helper()
	run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: tasks, WorkerID: "execution-test", LeaseDuration: time.Second, ConsumerVersions: map[string]string{"c2j": "X", "pulse": "Y"}})
	require.NoError(t, err)
	return run.Run(nil)
}
func submitExecutionTest(t *testing.T, rt jobdb.WorkflowRuntime, w wf.ExecutionWorker) jobdb.JobKey {
	t.Helper()
	h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("job")}})
	require.NoError(t, err)
	return h.JobKey
}
func replayExecutionTest(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, w wf.ExecutionWorker, tasks ...wf.TaskWorker) (jobdb.JobData, error) {
	t.Helper()
	e, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w, tasks...).BuildEngine()
	require.NoError(t, err)
	return e.ReplayJobRun(context.Background(), wf.ReplayRunRequest{JobKey: key})
}
func TestExecutionInvocationsAcrossRuntimes(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds, runs atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				runs.Add(1)
				require.NotNil(t, c.Context())
				return in, nil
			}}
			call := invocation("echo")
			w := wf.ExecutionWorker{JobType: "execution", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("outer", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
						builds.Add(1)
						return jobdb.NewTaskDataOrPanic("original"), nil
					})
				})
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			before, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			value, err := replayExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			raw, err := value.GetData()
			require.NoError(t, err)
			require.JSONEq(t, `"original"`, string(raw))
			require.EqualValues(t, 1, builds.Load())
			require.EqualValues(t, 1, runs.Load())
			after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, before, after)
			for _, change := range []string{"type", "command", "contract", "site"} {
				t.Run(change, func(t *testing.T) {
					old := call
					defer func() { call = old }()
					switch change {
					case "type":
						call.TaskType = "other"
					case "command":
						call.Command = json.RawMessage(`{"operation":"changed"}`)
					case "contract":
						call.ContractVersion = 2
					case "site":
						call.Site = "different"
					}
					_, err := replayExecutionTest(t, rt, key, w, task)
					require.ErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
					require.EqualValues(t, 1, builds.Load())
				})
			}
		})
	}
}
func TestExecutionRetryAndCatch(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds, runs, predicates atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				if runs.Add(1) == 1 {
					return nil, errors.New("retry me")
				}
				return in, nil
			}}
			call := invocation("echo")
			call.Policy.Retry.MaximumAttempts = 2
			call.Catch = []wf.CatchRule{{ID: "observe", Match: func(error) bool { predicates.Add(1); return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchContinue}, nil
			}}}
			w := wf.ExecutionWorker{JobType: "execution", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.EqualValues(t, 1, builds.Load())
			require.EqualValues(t, 2, runs.Load())
			require.EqualValues(t, 1, predicates.Load())
			_, err = replayExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.EqualValues(t, 1, predicates.Load())
			require.EqualValues(t, 2, runs.Load())
		})
	}
}

func TestExecutionScopeRetryAndTimeout(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var a, b atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				raw, _ := in.GetData()
				if string(raw) == `"A"` {
					a.Add(1)
					return in, nil
				}
				if b.Add(1) == 1 {
					return nil, errors.New("B failed")
				}
				return in, nil
			}}
			w := wf.ExecutionWorker{JobType: "scope-retry", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("block", wf.ScopeOptions{Retry: jobdb.RetryPolicy{MaximumAttempts: 2}}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					for _, name := range []string{"A", "B"} {
						if _, err := s.DoTask(invocation(name), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(name), nil }); err != nil {
							return nil, err
						}
					}
					return in, nil
				})
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.EqualValues(t, 2, a.Load())
			require.EqualValues(t, 2, b.Load())
			_, err = replayExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.EqualValues(t, 2, a.Load())
			for _, admitted := range []bool{false, true} {
				t.Run(fmt.Sprintf("timeout/admitted=%t", admitted), func(t *testing.T) {
					testScopeTimeoutAdmission(t, rt, admitted)
				})
			}
		})
	}
}

func TestExecutionExternalHandoff(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds atomic.Int32
			w := wf.ExecutionWorker{JobType: "external", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("scope", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					return s.DoTask(invocation("external"), func(context.Context) (jobdb.TaskData, error) {
						builds.Add(1)
						return jobdb.NewTaskDataOrPanic("explicit-input"), nil
					})
				})
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
			require.NoError(t, err)
			handle, err := engine.GetWaitingTask(context.Background(), key)
			require.NoError(t, err)
			input, err := handle.Data()
			require.NoError(t, err)
			raw, err := input.GetData()
			require.NoError(t, err)
			require.JSONEq(t, `"explicit-input"`, string(raw))
			_, err = replayExecutionTest(t, rt, key, w)
			require.Error(t, err)
			require.EqualValues(t, 1, builds.Load())
			require.NoError(t, handle.Finish(context.Background(), jobdb.NewTaskDataOrPanic("external-output")))
			require.Error(t, handle.Finish(context.Background(), jobdb.NewTaskDataOrPanic("stale-output")))
			out, err = runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.EqualValues(t, 1, builds.Load())
			_, err = replayExecutionTest(t, rt, key, w)
			require.NoError(t, err)
		})
	}
}

func TestExecutionPatchedRestart(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var runs atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
			w := wf.ExecutionWorker{JobType: "restart", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				if _, err := c.DoTask(invocation("prefix"), func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
					return nil, err
				}
				value := "old"
				return c.DoScope("patched", wf.ScopeOptions{OnRestart: func(raw json.RawMessage) error { return json.Unmarshal(raw, &value) }}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					call := invocation("suffix")
					call.Command, _ = json.Marshal(value)
					return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(value), nil })
				})
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			before, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			var target string
			for _, rec := range executionRecords(t, rt, key) {
				if rec.Kind == "scope" && strings.Contains(rec.ID, `"patched"`) {
					target = rec.ID
				}
			}
			require.NotEmpty(t, target)
			h, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target, Payload: json.RawMessage(`"new"`)}}})
			require.NoError(t, err)
			out, err = runExecutionTest(t, rt, h.JobKey, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			raw, err := out.Output.GetData()
			require.NoError(t, err)
			require.JSONEq(t, `"new"`, string(raw))
			require.EqualValues(t, 3, runs.Load())
			_, err = replayExecutionTest(t, rt, h.JobKey, w, task)
			require.NoError(t, err)
			require.EqualValues(t, 3, runs.Load())
			after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, err = rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: "missing"}}})
			require.Error(t, err)
		})
	}
}

type executionFaultRuntime struct {
	jobdb.WorkflowRuntime
	fail      func(jobdb.ExecutionRecord) bool
	after     bool
	fired     atomic.Bool
	committed atomic.Bool
	cancel    context.CancelFunc
}

func (f *executionFaultRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	body, ok := req.Chapter.Body.(jobdb.ExecutionChapter)
	if ok {
		rec, err := jobdb.DecodeExecutionRecord(body.Record)
		if err != nil {
			return err
		}
		matched := f.fail(rec)
		var prefix []jobdb.ExecutionRecord
		if raw := req.Chapter.Metadata.Fields["metadata"].Map["__execution_prefix_v3"].String; raw != "" {
			if err := json.Unmarshal([]byte(raw), &prefix); err != nil {
				return err
			}
		}
		for _, item := range prefix {
			matched = matched || f.fail(item)
		}
		if matched && f.fired.CompareAndSwap(false, true) {
			if f.after {
				if err := f.WorkflowRuntime.PutChapter(ctx, req); err != nil {
					return err
				}
				f.committed.Store(true)
			}
			if f.cancel != nil {
				f.cancel()
			}
			return errors.New("injected append failure")
		}
	}
	return f.WorkflowRuntime.PutChapter(ctx, req)
}
func TestExecutionUncertainAppend(t *testing.T) {
	for _, kind := range []string{"header", "activation", "scope", "attempt", "result", "catch", "handled", "decision"} {
		t.Run(kind, func(t *testing.T) {
			rt := &executionFaultRuntime{WorkflowRuntime: alternateRuntime(t, "sqlite"), after: true, fail: func(r jobdb.ExecutionRecord) bool { return r.Kind == kind }}
			var runs atomic.Int32
			task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
				runs.Add(1)
				return nil, errors.New("handled")
			}}
			call := invocation("task")
			call.Catch = []wf.CatchRule{{ID: "recover", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("ok")}, nil
			}}}
			w := wf.ExecutionWorker{JobType: "uncertain", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.True(t, rt.fired.Load())
			require.EqualValues(t, 1, runs.Load())
			_, err = replayExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
		})
	}
}
func TestExecutionRecoveryInputSnapshots(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committed=%v", backend, committed), func(t *testing.T) {
				base := alternateRuntime(t, backend)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rt := &executionFaultRuntime{WorkflowRuntime: base, cancel: cancel, fail: func(r jobdb.ExecutionRecord) bool {
					if committed {
						return r.Kind == "decision" && strings.Contains(r.ID, "task:")
					}
					return r.Kind == "result" && strings.Contains(r.ID, "task:")
				}}
				var builds, runs atomic.Int32
				source := "old"
				var seen []string
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					raw, _ := in.GetData()
					seen = append(seen, string(raw))
					if runs.Add(1) == 1 {
						return nil, errors.New("retry")
					}
					return in, nil
				}}
				call := invocation("task")
				call.Policy.Retry.MaximumAttempts = 2
				w := wf.ExecutionWorker{JobType: "recover-input", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
						builds.Add(1)
						return jobdb.NewTaskDataOrPanic(source), nil
					})
				}}
				key := submitExecutionTest(t, rt, w)
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "first", LeaseDuration: 60 * time.Millisecond})
				require.NoError(t, err)
				_, err = run.Run(nil)
				require.Error(t, err)
				require.True(t, rt.fired.Load())
				source = "new"
				var out wf.JobRunOutcome
				require.Eventually(t, func() bool {
					out, err = runExecutionTest(t, base, key, w, task)
					require.NoError(t, err)
					return out.Status != wf.JobRunNotLeaseable
				}, 2*time.Second, 25*time.Millisecond)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				if committed {
					require.EqualValues(t, 1, builds.Load())
					require.Equal(t, []string{`"old"`, `"old"`}, seen)
				} else {
					require.EqualValues(t, 2, builds.Load())
					require.Equal(t, []string{`"old"`, `"new"`}, seen)
				}
				_, err = replayExecutionTest(t, base, key, w, task)
				require.NoError(t, err)
			})
		}
	}
}
func TestExecutionBuilderFailureAndMutation(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	var builds, runs atomic.Int32
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		raw, _ := in.GetData()
		require.JSONEq(t, `{"value":"original"}`, string(raw))
		if runs.Add(1) == 1 {
			copy(raw, []byte(`{"value":"modified"}`))
			return nil, errors.New("retry worker")
		}
		return in, nil
	}}
	call := invocation("task")
	call.Policy.Retry.MaximumAttempts = 3
	w := wf.ExecutionWorker{JobType: "builder", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
			if builds.Add(1) == 1 {
				return nil, errors.New("retry builder")
			}
			return jobdb.NewTaskDataOrPanic(map[string]string{"value": "original"}), nil
		})
	}}
	key := submitExecutionTest(t, rt, w)
	out, err := runExecutionTest(t, rt, key, w, task)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status)
	require.EqualValues(t, 2, builds.Load())
	require.EqualValues(t, 2, runs.Load())
	_, err = replayExecutionTest(t, rt, key, w, task)
	require.NoError(t, err)
}

type executionReadOverride struct {
	jobdb.WorkflowRuntime
	change func(*jobdb.ExecutionRecord)
}

func (r executionReadOverride) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	chapters, err := r.WorkflowRuntime.ListChapters(ctx, req)
	if err != nil {
		return nil, err
	}
	for i, ch := range chapters {
		if b, ok := ch.Body.(jobdb.ExecutionChapter); ok {
			var record jobdb.ExecutionRecord
			if err = json.Unmarshal(b.Record, &record); err != nil {
				return nil, err
			}
			r.change(&record)
			raw, _ := json.Marshal(record)
			chapters[i].Body = jobdb.ExecutionChapter{Record: raw}
		}
	}
	return chapters, nil
}
func TestExecutionVersionsAndInspection(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var callbacks atomic.Int32
			w := wf.ExecutionWorker{JobType: "versions", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				callbacks.Add(1)
				v := c.ExecutionVersions()
				require.Equal(t, jobdb.ExecutionPatternVersion, v.ExecutionPatternVersion)
				return in, nil
			}}
			versions := map[string]string{"c2j": "X", "pulse": "Y"}
			ws, err := wf.AsWorkSetWithOptions(w, wf.WorkRegistrationOptions{ConsumerVersions: versions})
			require.NoError(t, err)
			versions["c2j"] = "mutated"
			require.Equal(t, "X", ws.Options.ConsumerVersions["c2j"])
			_, err = wf.AsWorkSetWithOptions(w, wf.WorkRegistrationOptions{ConsumerVersions: map[string]string{"c2j": ""}})
			require.Error(t, err)
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			info, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			require.NotEmpty(t, info.Execution)
			require.Equal(t, jobdb.ExecutionPatternVersion, info.Execution[0].Record.Versions.ExecutionPatternVersion)
			require.Equal(t, map[string]string{"c2j": "X", "pulse": "Y"}, info.Execution[0].Record.Versions.ConsumerVersions)
			for _, version := range []uint32{0, 99} {
				bad := executionReadOverride{WorkflowRuntime: rt, change: func(r *jobdb.ExecutionRecord) {
					if r.Kind == "header" {
						r.Version = version
					}
				}}
				_, err = replayExecutionTest(t, bad, key, w)
				require.Error(t, err)
				require.Contains(t, err.Error(), "unsupported execution version")
				require.EqualValues(t, 1, callbacks.Load())
			}
			bad := executionReadOverride{WorkflowRuntime: rt, change: func(r *jobdb.ExecutionRecord) {
				if r.Kind == "header" {
					r.Versions = nil
				}
			}}
			_, err = replayExecutionTest(t, bad, key, w)
			require.Error(t, err)
			require.EqualValues(t, 1, callbacks.Load())
		})
	}
}
func TestExecutionNegativeAndCatchOrdering(t *testing.T) {
	for _, mode := range []string{"handled", "rewritten", "propagate", "handler-error", "bad-disposition", "swallowed-mismatch"} {
		t.Run(mode, func(t *testing.T) {
			rt := alternateRuntime(t, "sqlite")
			var runs, predicates, handlers atomic.Int32
			task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
				runs.Add(1)
				return nil, errors.New("original")
			}}
			call := invocation("task")
			call.Policy.Retry.MaximumAttempts = 2
			call.Catch = []wf.CatchRule{{ID: "first", Match: func(error) bool { predicates.Add(1); return true }, Handle: func(c wf.ExecutionContext, err error) (wf.CatchResult, error) {
				handlers.Add(1)
				switch mode {
				case "handled", "swallowed-mismatch":
					return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("ok")}, nil
				case "propagate":
					return wf.CatchResult{Disposition: wf.CatchPropagate}, nil
				case "handler-error":
					return wf.CatchResult{}, errors.New("handler failed")
				case "bad-disposition":
					return wf.CatchResult{Disposition: "invalid"}, nil
				default:
					return wf.CatchResult{Disposition: wf.CatchContinue, Err: errors.New("rewritten")}, nil
				}
			}}, {ID: "second", Match: func(error) bool { t.Error("second predicate must not run"); return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) { panic("unreachable") }}}
			w := wf.ExecutionWorker{JobType: "negative", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			if mode == "handled" || mode == "swallowed-mismatch" {
				require.Equal(t, wf.JobRunCompleted, out.Status)
			} else {
				require.Equal(t, wf.JobRunFailed, out.Status)
			}
			want := int32(1)
			if mode == "rewritten" {
				want = 2
			}
			require.Equal(t, want, runs.Load())
			require.Equal(t, want, handlers.Load())
			if mode == "swallowed-mismatch" {
				w.Execute = func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					changed := call
					changed.Command = json.RawMessage(`"changed"`)
					_, _ = c.DoTask(changed, func(context.Context) (jobdb.TaskData, error) { t.Error("builder must not run"); return in, nil })
					return in, nil
				}
				_, err = replayExecutionTest(t, rt, key, w, task)
				require.ErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
			} else {
				_, err = replayExecutionTest(t, rt, key, w, task)
				if mode == "handled" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
			require.Equal(t, want, predicates.Load())
			require.Equal(t, want, runs.Load())
		})
	}
}

func TestExecutionExternalDeadlineAndReplacement(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds atomic.Int32
			call := invocation("external")
			call.Policy.InvocationTimeout = jobdb.AsDuration(80 * time.Millisecond)
			call.Policy.Retry.MaximumAttempts = 2
			w := wf.ExecutionWorker{JobType: "external-timeout", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
			require.NoError(t, err)
			stale, err := engine.GetWaitingTask(context.Background(), key)
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
			// Let timeout processing replace the waiting generation before
			// attempting the stale answer; expiry alone leaves the slot open.
			out, err = runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			require.EqualValues(t, 1, builds.Load())
			fresh, err := engine.GetWaitingTask(context.Background(), key)
			require.NoError(t, err)
			require.NotEqual(t, stale.TaskOrdinalToComplete(), fresh.TaskOrdinalToComplete())
			require.Error(t, stale.Finish(context.Background(), jobdb.NewTaskDataOrPanic("old-generation")))
			require.NoError(t, fresh.Finish(context.Background(), jobdb.NewTaskDataOrPanic("fresh")))
			out, err = runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			_, err = replayExecutionTest(t, rt, key, w)
			require.NoError(t, err)
		})
	}
}

func TestExecutionRecoveryDuringHandler(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fault := &executionFaultRuntime{WorkflowRuntime: rt, cancel: cancel, fail: func(r jobdb.ExecutionRecord) bool { return r.Kind == "result" && strings.Contains(r.ID, `"second"`) }}
	var first, second, predicates atomic.Int32
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		raw, _ := in.GetData()
		switch string(raw) {
		case `"fail"`:
			return nil, errors.New("body failed")
		case `"first"`:
			first.Add(1)
		case `"second"`:
			second.Add(1)
		}
		return in, nil
	}}
	rules := []wf.CatchRule{{ID: "handler", Match: func(error) bool { predicates.Add(1); return true }, Handle: func(h wf.ExecutionContext, err error) (wf.CatchResult, error) {
		for _, name := range []string{"first", "second"} {
			if _, err := h.DoTask(invocation(name), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(name), nil }); err != nil {
				return wf.CatchResult{}, err
			}
		}
		return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("recovered")}, nil
	}}}
	w := wf.ExecutionWorker{JobType: "handler-recovery", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("block", wf.ScopeOptions{Catch: rules}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
			return s.DoTask(invocation("fail"), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic("fail"), nil })
		})
	}}
	key := submitExecutionTest(t, rt, w)
	run, err := wf.GetJobForRun(ctx, fault, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "first", LeaseDuration: 50 * time.Millisecond})
	require.NoError(t, err)
	_, err = run.Run(nil)
	require.Error(t, err)
	require.True(t, fault.fired.Load())
	rules[0].Match = func(error) bool { t.Error("recorded selection must bypass changed predicate"); return false }
	var out wf.JobRunOutcome
	require.Eventually(t, func() bool {
		out, err = runExecutionTest(t, rt, key, w, task)
		require.NoError(t, err)
		return out.Status != wf.JobRunNotLeaseable
	}, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, wf.JobRunCompleted, out.Status)
	require.EqualValues(t, 1, first.Load())
	require.EqualValues(t, 2, second.Load())
	require.EqualValues(t, 1, predicates.Load())
	_, err = replayExecutionTest(t, rt, key, w, task)
	require.NoError(t, err)
}

func TestExecutionScopeDeadlineSurvivesRecovery(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fault := &executionFaultRuntime{WorkflowRuntime: rt, cancel: cancel, fail: func(r jobdb.ExecutionRecord) bool {
		return r.Kind == "result" && r.ID == `/scope:"$job"#1/attempt:1`
	}}
	var runs atomic.Int32
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
	w := wf.ExecutionWorker{JobType: "durable-timeout", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("timed", wf.ScopeOptions{TotalTimeout: 70 * time.Millisecond}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
			return s.DoTask(invocation("completed"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
		})
	}}
	key := submitExecutionTest(t, rt, w)
	run, err := wf.GetJobForRun(ctx, fault, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "first", LeaseDuration: 40 * time.Millisecond})
	require.NoError(t, err)
	_, err = run.Run(nil)
	require.Error(t, err)
	require.True(t, fault.fired.Load())
	time.Sleep(90 * time.Millisecond)
	out, err := runExecutionTest(t, rt, key, w, task)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunFailed, out.Status)
	require.EqualValues(t, 1, runs.Load())
	_, err = replayExecutionTest(t, rt, key, w, task)
	require.Error(t, err)
	require.EqualValues(t, 1, runs.Load())
}

func executionRestartTarget(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, site string) string {
	t.Helper()
	info, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
	require.NoError(t, err)
	for _, entry := range info.Execution {
		if entry.Record.Kind == "scope" && strings.HasSuffix(entry.Record.ID, fmt.Sprintf("scope:%q#1", site)) {
			return entry.Record.ID
		}
	}
	t.Fatalf("scope %s not found", site)
	return ""
}
func TestExecutionRestartBudgetsAndRepeatedFork(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		scenario := func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var runs atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
			w := wf.ExecutionWorker{JobType: "forks", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("outer", wf.ScopeOptions{TotalTimeout: time.Minute}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					value := "old"
					var out jobdb.TaskData
					for _, name := range []string{"one", "two"} {
						var err error
						out, err = s.DoScope(name, wf.ScopeOptions{OnRestart: func(raw json.RawMessage) error { return json.Unmarshal(raw, &value) }}, func(n wf.ExecutionContext) (jobdb.JobData, error) {
							call := invocation(name)
							call.Command, _ = json.Marshal(value)
							return n.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(value), nil })
						})
						if err != nil {
							return nil, err
						}
					}
					return out, nil
				})
			}}
			source := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, source, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status, "error: %v", out.JobError)
			for i, site := range []string{"one", "two"} {
				// Virtual time covers expired source deadlines on every fork.
				// Real transports use a generous live budget and verify rebasing
				// directly, rather than racing storage against a short timeout.
				if backend == "toy" {
					time.Sleep(2 * time.Minute)
				}
				original, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: source})
				require.NoError(t, err)
				outer := executionRestartTarget(t, rt, source, "outer") + "/attempt:1"
				if backend == "toy" {
					info, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: source})
					require.NoError(t, err)
					for _, entry := range info.Execution {
						if !entry.Record.TotalDeadline.IsZero() {
							require.True(t, entry.Record.TotalDeadline.Before(time.Now()), "source deadline must have expired")
						}
					}
				}
				target := executionRestartTarget(t, rt, source, site)
				patch, _ := json.Marshal(fmt.Sprintf("patch-%d", i))
				req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, JobID: fmt.Sprintf("fork-%d", i), ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target, Payload: patch}}}
				h, err := rt.SubmitRestartJob(context.Background(), req)
				require.NoError(t, err)
				same, err := rt.SubmitRestartJob(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, h.JobKey, same.JobKey)
				changed := req
				copy := *req.Job.ExecutionRestart
				copy.Payload = json.RawMessage(`"different"`)
				changed.Job.ExecutionRestart = &copy
				_, err = rt.SubmitRestartJob(context.Background(), changed)
				require.Error(t, err)
				restartedAt := time.Now()
				out, err = runExecutionTest(t, rt, h.JobKey, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status, "error: %v", out.JobError)
				raw, err := out.Output.GetData()
				require.NoError(t, err)
				require.JSONEq(t, string(patch), string(raw))
				_, err = replayExecutionTest(t, rt, h.JobKey, w, task)
				require.NoError(t, err)
				info, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: h.JobKey})
				require.NoError(t, err)
				var deadline time.Time
				for _, entry := range info.Execution {
					if entry.Record.Kind == "continuation" && entry.Record.ID == target {
						var budgets map[string]struct {
							TotalDeadline time.Time `json:"total_deadline"`
						}
						require.NoError(t, json.Unmarshal(entry.Record.Payload, &budgets))
						deadline = budgets[outer].TotalDeadline
					}
				}
				require.False(t, deadline.IsZero(), "continuation must refresh the ancestor's budget")
				require.False(t, deadline.Before(restartedAt.Add(time.Minute)), "continuation must receive a full new budget")
				after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: source})
				require.NoError(t, err)
				require.Equal(t, original, after, "restart must not rewrite source history")
				source = h.JobKey
			}
			require.EqualValues(t, 5, runs.Load())
		}
		t.Run(backend, func(t *testing.T) {
			if backend == "toy" {
				synctest.Test(t, scenario)
			} else {
				scenario(t)
			}
		})
	}
}
func TestExecutionBackoffAndVersionRecovery(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	var builds, runs atomic.Int32
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		if runs.Add(1) == 1 {
			return nil, errors.New("retry")
		}
		return in, nil
	}}
	call := invocation("task")
	call.Policy.Retry = jobdb.RetryPolicy{MaximumAttempts: 2, InitialInterval: jobdb.Duration(80 * time.Millisecond)}
	w := wf.ExecutionWorker{JobType: "backoff", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
	}}
	key := submitExecutionTest(t, rt, w)
	run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "old", LeaseDuration: time.Second, AwaitThreshold: time.Millisecond, ConsumerVersions: map[string]string{"c2j": "old"}})
	require.NoError(t, err)
	out, err := run.Run(nil)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunSuspended, out.Status)
	time.Sleep(95 * time.Millisecond)
	run, err = wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "new", LeaseDuration: time.Second, ConsumerVersions: map[string]string{"c2j": "new", "pulse": "Y"}})
	require.NoError(t, err)
	out, err = run.Run(nil)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status, "error: %v", out.JobError)
	require.EqualValues(t, 1, builds.Load())
	require.EqualValues(t, 2, runs.Load())
	info, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
	require.NoError(t, err)
	require.Equal(t, "old", info.Execution[0].Record.Versions.ConsumerVersions["c2j"])
	var versions []string
	for _, entry := range info.Execution {
		if entry.Record.Kind == "activation" {
			versions = append(versions, entry.Record.Versions.ConsumerVersions["c2j"])
		}
	}
	require.Equal(t, []string{"old", "new"}, versions)
	_, err = replayExecutionTest(t, rt, key, w, task)
	require.NoError(t, err)
}

func TestExecutionArtifactsAndSuppliedLease(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			artifactPath := filepath.Join(t.TempDir(), "result.txt")
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				if err := os.WriteFile(artifactPath, []byte("durable artifact"), 0600); err != nil {
					return nil, err
				}
				artifact, err := jobdb.NewArtifactFromFile("result.txt", artifactPath)
				if err != nil {
					return nil, err
				}
				return &jobdb.SimpleTaskData{Data: jobdb.Data(`"output"`), Artifacts: []jobdb.Artifact{artifact}}, nil
			}}
			w := wf.ExecutionWorker{JobType: "artifacts", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(invocation("artifact"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			lease, err := rt.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "supplied", Routes: []jobdb.Route{{JobType: w.Name()}}, LeaseDuration: time.Second})
			require.NoError(t, err)
			run, err := wf.GetJobForRunWithLease(context.Background(), rt, lease, wf.GetJobForRunRequest{JobKey: key, JobWorker: &w, TaskWorkers: []wf.TaskWorker{task}, ConsumerVersions: map[string]string{"pulse": "supplied"}})
			require.NoError(t, err)
			out, err := run.Run(nil)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			_, err = os.Stat(artifactPath)
			require.True(t, os.IsNotExist(err), "snapshot must release the worker's temporary file")
			value, err := replayExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			artifacts, err := value.GetArtifacts()
			require.NoError(t, err)
			require.Len(t, artifacts, 1)
			keyref, err := artifacts[0].ArtifactKey()
			require.NoError(t, err)
			artifact, err := rt.OpenArtifact(context.Background(), jobdb.ArtifactRef{JobKey: key, Ordinal: keyref.TaskOrdinal, Name: keyref.Name})
			require.NoError(t, err)
			reader, err := artifact.Open()
			require.NoError(t, err)
			defer reader.Close()
			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, "durable artifact", string(data))
			info, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, "supplied", info.Execution[0].Record.Versions.ConsumerVersions["pulse"])
		})
	}
}

func TestExecutionInterruptedTaskNeverBuildsDuringReplay(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, admitted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/admitted=%t", backend, admitted), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var builds, runs atomic.Int32
				release := make(chan struct{})
				exited := make(chan struct{})
				scopes := make(chan wf.ExecutionContext, 1)
				// Always release a late worker, including on assertion failure.
				defer func() {
					close(release)
					if runs.Load() > 0 {
						select {
						case <-exited:
						case <-time.After(5 * time.Second):
							t.Error("interrupted worker did not exit after release")
						}
					}
				}()
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					runs.Add(1)
					defer close(exited)
					// Expire only after admission, then keep the worker alive through
					// replay to check that its missing result cannot trigger a rebuild.
					wf.ExpireExecutionScopeForTest(<-scopes)
					<-release
					return in, nil
				}}
				w := wf.ExecutionWorker{JobType: "interrupted-task", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("timed", wf.ScopeOptions{TotalTimeout: time.Minute}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						if admitted {
							scopes <- s
						} else {
							wf.ExpireExecutionScopeForTest(s)
						}
						return s.DoTask(invocation("slow"), func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
					})
				}}
				key := submitExecutionTest(t, rt, w)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "interrupted-test", LeaseDuration: time.Second})
				require.NoError(t, err)
				out, err := run.Run(nil)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunFailed, out.Status)
				var timeout jobdb.TimeoutError
				require.ErrorAs(t, out.JobError, &timeout)
				_, err = replayExecutionTest(t, rt, key, w, task)
				require.ErrorAs(t, err, &timeout)
				want := int32(0)
				if admitted {
					want = 1
				}
				require.Equal(t, want, builds.Load())
				require.Equal(t, want, runs.Load())
			})
		}
	}
}
func TestExecutionChildJoinOutcomes(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			rt := alternateRuntime(t, "sqlite")
			var submits, after atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				submits.Add(1)
				keyHandle, err := c.SubmitJob(c.Context(), jobdb.SubmitJob{JobID: "child", JobType: "child", Data: in})
				key := keyHandle.JobKey
				if err != nil {
					return nil, err
				}
				return jobdb.NewTaskData(key)
			}}
			parent := wf.ExecutionWorker{JobType: "parent", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				out, err := c.DoTask(invocation("submit"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				if err != nil {
					return nil, err
				}
				raw, _ := out.GetData()
				var key jobdb.JobKey
				if err = json.Unmarshal(raw, &key); err != nil {
					return nil, err
				}
				err = c.AwaitJobs(key.JobId)
				if err != nil {
					after.Add(1)
					return jobdb.NewTaskDataOrPanic("failed-child-handled"), nil
				}
				after.Add(1)
				return jobdb.NewTaskDataOrPanic("child-complete"), nil
			}}
			key := submitExecutionTest(t, rt, parent)
			out, err := runExecutionTest(t, rt, key, parent, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			child := wf.ExecutionWorker{JobType: "child", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
				if failed {
					return nil, errors.New("child failed")
				}
				return jobdb.NewTaskDataOrPanic("ok"), nil
			}}
			_, err = runExecutionTest(t, rt, jobdb.JobKey{TenantId: "tenant", JobId: "child"}, child)
			require.NoError(t, err)
			out, err = runExecutionTest(t, rt, key, parent, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			want, _ := out.Output.GetData()
			replay, err := replayExecutionTest(t, rt, key, parent, task)
			require.NoError(t, err)
			got, _ := replay.GetData()
			require.JSONEq(t, string(want), string(got))
			require.EqualValues(t, 1, submits.Load())
		})
	}
}
func TestExecutionWaitAndLateTaskRetry(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) { testLateTaskRetry(t, alternateRuntime(t, backend)) })
	}
}

type executionCacheRuntime struct {
	jobdb.WorkflowRuntime
	config, secret atomic.Int32
}
type executionCacheLease struct {
	jobdb.ExecutionLease
	r *executionCacheRuntime
}

func (r *executionCacheRuntime) GetJobLease(c context.Context, q jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	l, e := r.WorkflowRuntime.GetJobLease(c, q)
	if e != nil {
		return nil, e
	}
	return &executionCacheLease{l, r}, nil
}
func (l *executionCacheLease) GetConfiguration(context.Context) (jobdb.ConfigurationSnapshot, error) {
	n := l.r.config.Add(1)
	return jobdb.ConfigurationSnapshot{Revision: fmt.Sprint(n), Values: map[string]json.RawMessage{"value": json.RawMessage(`"configuration"`)}}, nil
}
func (l *executionCacheLease) ResolveSecrets(context.Context, []string) (jobdb.SecretResolveResult, error) {
	n := l.r.secret.Add(1)
	return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"foo": {Value: fmt.Sprintf("never-record-secret-%d", n)}}}, nil
}
func TestExecutionCacheLifetime(t *testing.T) {
	rt := &executionCacheRuntime{WorkflowRuntime: alternateRuntime(t, "sqlite")}
	var a, b atomic.Int32
	var revisions []string
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		config, err := c.GetConfiguration(c.Context())
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, config.Revision)
		if _, err = c.ResolveSecrets(c.Context(), []string{"foo", "absent"}); err != nil {
			return nil, err
		}
		if _, err = c.ResolveSecrets(c.Context(), []string{"absent"}); err != nil {
			return nil, err
		}
		raw, _ := in.GetData()
		if string(raw) == `"A"` {
			if a.Add(1) == 1 {
				return nil, errors.New("retry A")
			}
		} else if b.Add(1) == 1 {
			return nil, errors.New("retry job")
		}
		return in, nil
	}}
	w := wf.ExecutionWorker{JobType: "cache-lifetime", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("nested", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
			for _, name := range []string{"A", "B"} {
				call := invocation(name)
				if name == "A" {
					call.Policy.Retry.MaximumAttempts = 2
				}
				if _, err := s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(name), nil }); err != nil {
					return nil, err
				}
			}
			return in, nil
		})
	}}
	h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("job"), RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 2}}}})
	require.NoError(t, err)
	out, err := runExecutionTest(t, rt, h.JobKey, w, task)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status)
	require.Equal(t, []string{"1", "1", "1", "2", "2"}, revisions)
	require.EqualValues(t, 2, rt.config.Load())
	require.EqualValues(t, 2, rt.secret.Load())
	_, err = replayExecutionTest(t, rt, h.JobKey, w, task)
	require.NoError(t, err)
	require.EqualValues(t, 2, rt.secret.Load())
	chapters, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: h.JobKey})
	require.NoError(t, err)
	raw, err := json.Marshal(chapters)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "never-record-secret")
}
