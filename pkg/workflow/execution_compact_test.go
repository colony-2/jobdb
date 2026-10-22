package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Match story3's 235 scopes and 29 task results, including two human waits.
// Pure structure and restart handlers must not introduce physical chapters.
func TestExecutionCompactStoryVolume(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"flat", "structural", "restartable"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var builds, runs atomic.Int32
				worker := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
				w := wf.ExecutionWorker{JobType: "compact-story", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					var out jobdb.JobData = in
					for i := 0; i < 235; i++ {
						body := func(s wf.ExecutionContext) (jobdb.JobData, error) {
							if i >= 29 {
								return out, nil
							}
							call := invocation(fmt.Sprintf("task-%d", i))
							if i == 9 || i == 26 {
								call.TaskType = "human"
							}
							return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
						}
						var err error
						if mode == "flat" {
							out, err = body(c)
						} else {
							opts := wf.ScopeOptions{}
							if mode == "restartable" {
								opts.OnRestart = func(json.RawMessage) error { return nil }
							}
							out, err = c.DoScope(fmt.Sprintf("scope-%d", i), opts, body)
						}
						if err != nil {
							return out, err
						}
					}
					return out, nil
				}}
				key := submitExecutionTest(t, rt, w)
				for i := 0; i < 3; i++ {
					out, err := runExecutionTest(t, rt, key, w, worker)
					require.NoError(t, err)
					if i == 2 {
						require.Equal(t, wf.JobRunCompleted, out.Status)
						break
					}
					require.Equal(t, wf.JobRunSuspended, out.Status)
					engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
					require.NoError(t, err)
					wait, err := engine.GetWaitingTask(context.Background(), key)
					require.NoError(t, err)
					require.NoError(t, wait.Finish(context.Background(), jobdb.NewTaskDataOrPanic("answer")))
				}
				chapters, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Len(t, chapters, 38, "29 results, two waits, three activations, header, job result/outcome and submission")
				taskResults := 0
				for _, ch := range chapters {
					if b, ok := ch.Body.(jobdb.ExecutionChapter); ok {
						r, err := jobdb.DecodeExecutionRecord(b.Record)
						require.NoError(t, err)
						require.NotContains(t, []string{"scope", "task", "attempt", "catch", "handled", "decision", "end"}, r.Kind)
						if r.Kind == "result" && strings.Contains(r.ID, "/task:") {
							taskResults++
						}
					}
				}
				require.Equal(t, 29, taskResults)
				require.EqualValues(t, 29, builds.Load())
				require.EqualValues(t, 27, runs.Load())
				if mode == "structural" {
					for _, r := range executionRecords(t, rt, key) {
						if r.Kind == "scope" {
							require.Equal(t, `/scope:"$job"#1`, r.ID, "organizational scopes need no metadata")
						}
					}
				}
				_, err = replayExecutionTest(t, rt, key, w, worker)
				require.NoError(t, err)
				require.EqualValues(t, 29, builds.Load())
				require.EqualValues(t, 27, runs.Load())
				after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, chapters, after)
				t.Logf("235 scopes / 29 task results / two input waits: %d chapters", len(chapters))
			})
		}
	}
}

// Simulate a valid contender committing another result into the expected slot.
// A different attempt must be rejected without retrying at a later position.
type compactContenderRuntime struct {
	jobdb.WorkflowRuntime
	wrongAttempt bool
	fired        bool
}

func (r *compactContenderRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	body, ok := req.Chapter.Body.(jobdb.ExecutionChapter)
	if !ok {
		return r.WorkflowRuntime.PutChapter(ctx, req)
	}
	rec, err := jobdb.DecodeExecutionRecord(body.Record)
	if err != nil {
		return err
	}
	if !r.fired && rec.Kind == "result" && strings.Contains(rec.ID, "/task:") {
		r.fired = true
		rec.Output = &jobdb.ExecutionSnapshot{Data: json.RawMessage(`"winner"`)}
		if r.wrongAttempt {
			rec.ID += "/another-attempt"
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		req.Chapter.Body = jobdb.ExecutionChapter{Record: raw}
		if err := r.WorkflowRuntime.PutChapter(ctx, req); err != nil {
			return err
		}
		return jobdb.ErrConflict
	}
	return r.WorkflowRuntime.PutChapter(ctx, req)
}
func TestExecutionCompactCommitWinner(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrong=%v", wrong), func(t *testing.T) {
			base := alternateRuntime(t, "sqlite")
			rt := &compactContenderRuntime{WorkflowRuntime: base, wrongAttempt: wrong}
			var calls atomic.Int32
			worker := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
				calls.Add(1)
				return jobdb.NewTaskDataOrPanic("loser"), nil
			}}
			w := wf.ExecutionWorker{JobType: "contender", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, worker)
			require.True(t, rt.fired)
			require.EqualValues(t, 1, calls.Load())
			if wrong {
				require.ErrorContains(t, err, "conflicts with committed history")
				chapters, e := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, e)
				require.Len(t, chapters, 4, "stale writer must not append elsewhere")
			} else {
				require.NoError(t, err)
				data, err := out.Output.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"winner"`, string(data))
				out, err := replayExecutionTest(t, base, key, w, worker)
				require.NoError(t, err)
				data, err = out.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"winner"`, string(data))
				require.EqualValues(t, 1, calls.Load())
			}
		})
	}
}

func TestExecutionCompactUncommittedLocalTimeoutRestarts(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fault := &executionFaultRuntime{WorkflowRuntime: rt, cancel: cancel, fail: func(r jobdb.ExecutionRecord) bool { return r.Kind == "result" && strings.Contains(r.ID, "/task:") }}
	var calls atomic.Int32
	worker := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { calls.Add(1); return in, nil }}
	w := wf.ExecutionWorker{JobType: "local-budget", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		call := invocation("task")
		call.Policy.InvocationTimeout = jobdb.AsDuration(100 * time.Millisecond)
		return c.DoScope("block", wf.ScopeOptions{TotalTimeout: time.Hour}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
			return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
		})
	}}
	key := submitExecutionTest(t, rt, w)
	run, err := wf.GetJobForRun(ctx, fault, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{worker}, WorkerID: "first", LeaseDuration: 500 * time.Millisecond})
	require.NoError(t, err)
	_, err = run.Run(nil)
	require.Error(t, err)
	require.True(t, fault.fired.Load())
	var out wf.JobRunOutcome
	require.Eventually(t, func() bool {
		out, err = runExecutionTest(t, rt, key, w, worker)
		return err == nil && out.Status != wf.JobRunNotLeaseable
	}, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
	require.EqualValues(t, 2, calls.Load())
	_, err = replayExecutionTest(t, rt, key, w, worker)
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load())
}

func TestExecutionCompactCatchChoiceCommittedWithHandlerTask(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	var predicates, repairs atomic.Int32
	rule := wf.CatchRule{ID: "repair", Match: func(error) bool { predicates.Add(1); return true }, Handle: func(c wf.ExecutionContext, _ error) (wf.CatchResult, error) {
		out, err := c.DoTask(invocation("repair"), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic("fixed"), nil })
		return wf.CatchResult{Disposition: wf.CatchHandled, Output: out}, err
	}}
	w := wf.ExecutionWorker{JobType: "catch-prefix", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("block", wf.ScopeOptions{Catch: []wf.CatchRule{rule}}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
			return s.DoTask(invocation("fail"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
		})
	}}
	worker := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		if strings.Contains(c.InvocationID, `task:"fail"`) {
			return nil, errors.New("failure")
		}
		repairs.Add(1)
		return in, nil
	}}
	key := submitExecutionTest(t, rt, w)
	out, err := runExecutionTest(t, rt, key, w, worker)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status)
	chapters, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	for _, ch := range chapters {
		if b, ok := ch.Body.(jobdb.ExecutionChapter); ok {
			r, err := jobdb.DecodeExecutionRecord(b.Record)
			require.NoError(t, err)
			require.NotEqual(t, "catch", r.Kind)
		}
	}
	rule.Match = func(error) bool { t.Error("committed selection must precede path reconstruction"); return false }
	_, err = replayExecutionTest(t, rt, key, w, worker)
	require.NoError(t, err)
	require.EqualValues(t, 1, predicates.Load())
	require.EqualValues(t, 1, repairs.Load())
}

func TestExecutionCompactTimeoutPropagation(t *testing.T) {
	for _, depth := range []int{0, 50} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			rt := alternateRuntime(t, "sqlite")
			w := wf.ExecutionWorker{JobType: "timeout-propagation", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				var walk func(wf.ExecutionContext, int) (jobdb.JobData, error)
				walk = func(s wf.ExecutionContext, n int) (jobdb.JobData, error) {
					if n > 0 {
						return s.DoScope("structure", wf.ScopeOptions{}, func(child wf.ExecutionContext) (jobdb.JobData, error) { return walk(child, n-1) })
					}
					return s.DoScope("timed", wf.ScopeOptions{TotalTimeout: time.Hour}, func(child wf.ExecutionContext) (jobdb.JobData, error) {
						wf.ExpireExecutionScopeForTest(child)
						return child.DoTask(invocation("never"), func(context.Context) (jobdb.TaskData, error) { t.Error("timed out work admitted"); return in, nil })
					})
				}
				return walk(c, depth)
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			chapters, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Len(t, chapters, 6)
			_, err = replayExecutionTest(t, rt, key, w)
			var timeout *jobdb.TimeoutError
			require.ErrorAs(t, err, &timeout)
		})
	}
}

func TestExecutionCompactTaskBoundaryRestart(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var first, second, third atomic.Int32
			worker := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				switch {
				case strings.Contains(c.InvocationID, `task:"A"`):
					first.Add(1)
				case strings.Contains(c.InvocationID, `task:"B"`):
					second.Add(1)
				default:
					third.Add(1)
				}
				return in, nil
			}}
			w := wf.ExecutionWorker{JobType: "task-restart", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				for _, name := range []string{"A", "B", "C"} {
					_, err := c.DoScope("organize", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoScope("inner", wf.ScopeOptions{}, func(n wf.ExecutionContext) (jobdb.JobData, error) {
							return n.DoTask(invocation(name), func(context.Context) (jobdb.TaskData, error) { return in, nil })
						})
					})
					if err != nil {
						return nil, err
					}
				}
				return in, nil
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			info, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			var target string
			var ordinal int64
			for _, entry := range info.Execution {
				if entry.Record.Kind == "task" && strings.Contains(entry.Record.ID, `task:"B"`) {
					target = entry.Record.ID
				}
				if entry.Record.Kind == "result" && strings.Contains(entry.Record.ID, `task:"B"`) {
					ordinal = entry.Ordinal
				}
			}
			require.NotEmpty(t, target)
			require.Positive(t, ordinal)
			before, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}})
			require.NoError(t, err)
			out, err = runExecutionTest(t, rt, before.JobKey, w, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
			require.EqualValues(t, 1, first.Load())
			require.EqualValues(t, 2, second.Load())
			require.EqualValues(t, 2, third.Load())
			after, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, LastStepToKeep: ordinal}})
			require.NoError(t, err)
			out, err = runExecutionTest(t, rt, after.JobKey, w, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
			require.EqualValues(t, 1, first.Load())
			require.EqualValues(t, 2, second.Load())
			require.EqualValues(t, 3, third.Load())
			_, err = replayExecutionTest(t, rt, before.JobKey, w, worker)
			require.NoError(t, err)
			_, err = replayExecutionTest(t, rt, after.JobKey, w, worker)
			require.NoError(t, err)
		})
	}
}

func TestExecutionCompactCatchChoiceSurvivesYield(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var predicates, repairs, yields atomic.Int32
			rule := wf.CatchRule{ID: "repair", Match: func(error) bool { predicates.Add(1); return true }, Handle: func(c wf.ExecutionContext, _ error) (wf.CatchResult, error) {
				if yields.Add(1) == 1 {
					return wf.CatchResult{}, c.Yield(c.Context(), wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "catch-yield"}}})
				}
				out, err := c.DoTask(invocation("repair"), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic("fixed"), nil })
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: out}, err
			}}
			w := wf.ExecutionWorker{JobType: "catch-yield", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("block", wf.ScopeOptions{Catch: []wf.CatchRule{rule}}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					return s.DoTask(invocation("fail"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				})
			}}
			worker := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				if strings.Contains(c.InvocationID, `task:"fail"`) {
					return nil, errors.New("failure")
				}
				repairs.Add(1)
				return in, nil
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			rule.Match = func(error) bool { t.Error("handoff must preserve its selected path"); return false }
			out, err = runExecutionTest(t, rt, key, w, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
			_, err = replayExecutionTest(t, rt, key, w, worker)
			require.NoError(t, err)
			require.EqualValues(t, 1, predicates.Load())
			require.EqualValues(t, 1, repairs.Load())
		})
	}
}

func TestExecutionCompactLocalTimerAfterCheckpoint(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	var runs atomic.Int32
	worker := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		if runs.Add(1) == 1 {
			return nil, c.Yield(c.Context(), wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "timer-checkpoint"}, WaitUntil: func() *time.Time { v := time.Now().Add(150 * time.Millisecond); return &v }()}})
		}
		return in, nil
	}}
	w := wf.ExecutionWorker{JobType: "timer-checkpoint", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		call := invocation("work")
		call.Policy.InvocationTimeout = jobdb.AsDuration(100 * time.Millisecond)
		return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
	}}
	key := submitExecutionTest(t, rt, w)
	out, err := runExecutionTest(t, rt, key, w, worker)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunSuspended, out.Status)
	require.Eventually(t, func() bool {
		out, err = runExecutionTest(t, rt, key, w, worker)
		return err == nil && (out.Status == wf.JobRunCompleted || out.Status == wf.JobRunFailed)
	}, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
	require.EqualValues(t, 2, runs.Load())
	_, err = replayExecutionTest(t, rt, key, w, worker)
	require.NoError(t, err)
	require.EqualValues(t, 2, runs.Load())
}
