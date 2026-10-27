package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Restart the same logical boundary through three generations. Virtual time
// expires old budgets; real transports assert rebasing with generous budgets.
func TestExecutionRepeatedRestartSameBoundary(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, site := range []string{"parent", "child", "work"} {
			for _, finiteParent := range []bool{false, true} {
				scenario := func(t *testing.T) {
					rt := alternateRuntime(t, backend)
					ctx := context.Background()
					var priorRuns, workRuns atomic.Int32
					task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
						if strings.Contains(c.InvocationID, `task:"prior"`) {
							priorRuns.Add(1)
						} else {
							workRuns.Add(1)
						}
						return in, nil
					}}
					parent := wf.ScopeOptions{OnRestart: func(json.RawMessage) error { return nil }}
					budget := time.Hour
					if finiteParent {
						parent.TotalTimeout = 30 * time.Minute
						parent.AttemptTimeout = 20 * time.Minute
						budget = 20 * time.Minute
					}
					w := wf.ExecutionWorker{JobType: "repeated-restart", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
						if _, err := c.DoTask(invocation("prior"), func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
							return nil, err
						}
						return c.DoScope("parent", parent, func(s wf.ExecutionContext) (jobdb.JobData, error) {
							return s.DoScope("child", wf.ScopeOptions{TotalTimeout: time.Hour}, func(n wf.ExecutionContext) (jobdb.JobData, error) {
								call := invocation("work")
								call.Policy.TotalTimeout = jobdb.AsDuration(time.Hour)
								return n.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
							})
						})
					}}
					source := submitExecutionTest(t, rt, w)
					out, err := runExecutionTest(t, rt, source, w, task)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunCompleted, out.Status)
					var target string
					for _, rec := range executionRecords(t, rt, source) {
						if (rec.Kind == "scope" || rec.Kind == "task") && strings.HasSuffix(rec.ID, fmt.Sprintf(":%q#1", site)) {
							target = rec.ID
						}
					}
					require.NotEmpty(t, target)
					var lastContinuation time.Time
					for generation := 1; generation <= 3; generation++ {
						if backend == "toy" {
							time.Sleep(2 * time.Hour)
						}
						original, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source})
						require.NoError(t, err)
						h, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}})
						require.NoError(t, err)
						started := time.Now()
						out, err = runExecutionTest(t, rt, h.JobKey, w, task)
						require.NoError(t, err)
						require.Equal(t, wf.JobRunCompleted, out.Status, "generation %d: %v", generation, out.JobError)
						var continuation, workDeadline time.Time
						continuations := 0
						for _, rec := range executionRecords(t, rt, h.JobKey) {
							if rec.Kind == "continuation" && rec.ID == target {
								continuations++
								continuation = rec.At
							}
							if rec.Kind == "attempt" && strings.HasSuffix(rec.ID, `task:"work"#1/attempt:1`) {
								workDeadline = rec.Deadline
							}
						}
						require.Equal(t, 1, continuations, "superseded continuation must not be copied")
						require.False(t, continuation.Before(started))
						require.True(t, continuation.After(lastContinuation))
						require.False(t, workDeadline.Before(started.Add(budget)), "new work must receive a fresh budget")
						require.WithinDuration(t, continuation.Add(budget), workDeadline, time.Second, "ancestor budget must still bound the task")
						lastContinuation = continuation
						// Completed replay after the deadline must preserve the outcome and history.
						if backend == "toy" {
							time.Sleep(2 * time.Hour)
						}
						before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
						require.NoError(t, err)
						for i := 0; i < 2; i++ {
							_, err = replayExecutionTest(t, rt, h.JobKey, w, task)
							require.NoError(t, err)
						}
						after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
						require.NoError(t, err)
						require.Equal(t, before, after)
						unchanged, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source})
						require.NoError(t, err)
						require.Equal(t, original, unchanged)
						require.EqualValues(t, 1, priorRuns.Load(), "work before the target must stay replayed")
						require.EqualValues(t, generation+1, workRuns.Load())
						source = h.JobKey
					}
				}
				t.Run(fmt.Sprintf("%s/%s/finite-parent=%t", backend, site, finiteParent), func(t *testing.T) {
					if backend == "toy" {
						synctest.Test(t, scenario)
					} else {
						scenario(t)
					}
				})
			}
		}
	}
}

// A committed continuation belongs to this job: recovering it must not renew
// budgets. External dispatch persists the continuation before a second worker.
func TestExecutionRepeatedRestartRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		scenario := func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var runs atomic.Int32
			task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
			w := wf.ExecutionWorker{JobType: "restart-recovery", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("target", wf.ScopeOptions{TotalTimeout: time.Hour}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					return s.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				})
			}}
			source := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, source, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			target := executionRestartTarget(t, rt, source, "target")
			for generation := 1; generation <= 3; generation++ {
				if backend == "toy" {
					time.Sleep(2 * time.Hour)
				}
				h, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}})
				require.NoError(t, err)
				out, err = runExecutionTest(t, rt, h.JobKey, w) // persist external wait
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				wait := executionWait(t, rt, h.JobKey)
				before := executionRecords(t, rt, h.JobKey)
				if backend == "toy" {
					time.Sleep(10 * time.Minute)
				}
				for i := 0; i < 2; i++ {
					_, err = replayExecutionTest(t, rt, h.JobKey, w, task)
					var miss wf.ReplayCacheMissError
					require.ErrorAs(t, err, &miss)
					require.Equal(t, wf.ReplayCacheMissTaskResultMissing, miss.Reason)
				}
				require.Equal(t, before, executionRecords(t, rt, h.JobKey))
				require.EqualValues(t, generation, runs.Load(), "inspection must not run the waiting task")
				out, err = runExecutionTest(t, rt, h.JobKey, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
				after := executionRecords(t, rt, h.JobKey)
				require.Equal(t, before, after[:len(before)], "recovery must preserve continuation and deadlines")
				require.Equal(t, wait, executionWait(t, rt, h.JobKey))
				var continuations int
				for _, rec := range after {
					if rec.Kind == "continuation" && rec.ID == target {
						continuations++
					}
				}
				require.Equal(t, 1, continuations)
				require.EqualValues(t, generation+1, runs.Load())
				source = h.JobKey
			}
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

func TestExecutionRepeatedRestartStillTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := alternateRuntime(t, "toy")
		var runs atomic.Int32
		task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
			if runs.Add(1) > 1 {
				<-c.Context().Done()
				return nil, context.Cause(c.Context())
			}
			return in, nil
		}}
		w := wf.ExecutionWorker{JobType: "restart-timeout", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			return c.DoScope("target", wf.ScopeOptions{TotalTimeout: time.Minute}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
				return s.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
			})
		}}
		source := submitExecutionTest(t, rt, w)
		out, err := runExecutionTest(t, rt, source, w, task)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunCompleted, out.Status)
		target := executionRestartTarget(t, rt, source, "target")
		for generation := 1; generation <= 3; generation++ {
			time.Sleep(2 * time.Minute)
			h, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}})
			require.NoError(t, err)
			start := time.Now()
			run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "timeout", LeaseDuration: time.Hour})
			require.NoError(t, err)
			out, err = run.Run(nil)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			var timeout jobdb.TimeoutError
			require.ErrorAs(t, out.JobError, &timeout)
			require.Equal(t, time.Minute, time.Since(start), "fresh budget must expire normally, not immediately")
			require.EqualValues(t, generation+1, runs.Load())
			source = h.JobKey
		}
	})
}

// Each new restart evaluates its own admission. A previously successful gate
// must neither leak into a gate-free restart nor satisfy a different dependency.
func TestExecutionRepeatedRestartAdmission(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"prerequisite", "goal"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				ctx := context.Background()
				var calls atomic.Int32
				task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { calls.Add(1); return in, nil }}
				w := wf.ExecutionWorker{JobType: "repeated-admission", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("target", wf.ScopeOptions{OnRestart: func(json.RawMessage) error { return nil }}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
					})
				}}
				producer := wf.ExecutionWorker{JobType: "producer", Execute: func(_ wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) { return in, nil }}
				success := submitExecutionTest(t, rt, producer)
				out, err := runExecutionTest(t, rt, success, producer)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				successInfo, err := rt.GetJob(ctx, success)
				require.NoError(t, err)
				failure := submitExecutionTest(t, rt, producer)
				failureInfo, err := rt.GetJob(ctx, failure)
				require.NoError(t, err)
				if mode == "goal" {
					_, err = rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: failureInfo.GoalID}, Message: "replacement dependency failed"})
					require.NoError(t, err)
				} else {
					producer.Execute = func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
						return nil, fmt.Errorf("replacement dependency failed")
					}
					out, err = runExecutionTest(t, rt, failure, producer)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunFailed, out.Status)
				}
				source := submitExecutionTest(t, rt, w)
				out, err = runExecutionTest(t, rt, source, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				target := executionRestartTarget(t, rt, source, "target")
				for generation := 1; generation <= 3; generation++ {
					request := jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}
					if generation != 2 {
						dependency, goal := success, successInfo.GoalID
						if generation == 3 {
							dependency, goal = failure, failureInfo.GoalID
						}
						if mode == "goal" {
							request.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: goal}, Condition: jobdb.JobPrereqSuccess}}
						} else {
							request.Prerequisites = []jobdb.JobPrerequisite{{JobID: dependency.JobId, Condition: jobdb.JobPrereqSuccess}}
						}
					}
					h, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: request})
					require.NoError(t, err)
					out, err = runExecutionTest(t, rt, h.JobKey, w, task)
					require.NoError(t, err)
					if generation < 3 {
						require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
					} else {
						require.Equal(t, wf.JobRunFailed, out.Status)
						if mode == "goal" {
							require.ErrorIs(t, out.JobError, jobdb.ErrGoalFailed)
						} else {
							require.ErrorContains(t, out.JobError, "did not succeed")
						}
					}
					if generation == 2 {
						// Also fork directly from the successful gated restart
						// with a failing replacement gate, before advancing source.
						replacement := jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}
						if mode == "goal" {
							replacement.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: failureInfo.GoalID}, Condition: jobdb.JobPrereqSuccess}}
						} else {
							replacement.Prerequisites = []jobdb.JobPrerequisite{{JobID: failure.JobId, Condition: jobdb.JobPrereqSuccess}}
						}
						rejected, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: replacement})
						require.NoError(t, err)
						outcome, err := runExecutionTest(t, rt, rejected.JobKey, w, task)
						require.NoError(t, err)
						require.Equal(t, wf.JobRunFailed, outcome.Status)
						if mode == "goal" {
							require.ErrorIs(t, outcome.JobError, jobdb.ErrGoalFailed)
						} else {
							require.ErrorContains(t, outcome.JobError, "did not succeed")
						}
						for _, rec := range executionRecords(t, rt, h.JobKey) {
							require.NotEqual(t, "admission", rec.Kind, "old restart admission must not survive a new gate-free restart")
						}
					}
					before := executionRecords(t, rt, h.JobKey)
					_, replayErr := replayExecutionTest(t, rt, h.JobKey, w, task)
					if generation < 3 {
						require.NoError(t, replayErr)
					} else {
						require.Error(t, replayErr)
					}
					require.Equal(t, before, executionRecords(t, rt, h.JobKey))
					source = h.JobKey
				}
				require.EqualValues(t, 3, calls.Load(), "failed replacement admission must prevent execution")
			})
		}
	}
}
