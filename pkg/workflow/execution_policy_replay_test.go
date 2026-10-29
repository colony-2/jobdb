package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Record with one version of a consumer's defaults, then reconstruct with a
// different version. Same-version round trips cannot catch this regression.
func TestExecutionRecordedPolicyReplay(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"removed", "added", "changed", "retry-removed", "restartable"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				opts := wf.ScopeOptions{TotalTimeout: time.Hour, AttemptTimeout: 30 * time.Minute}
				call := invocation("work")
				call.Policy = jobdb.RunPolicy{TotalTimeout: jobdb.AsDuration(10 * time.Minute), InvocationTimeout: jobdb.AsDuration(time.Minute)}
				if mode == "added" {
					opts = wf.ScopeOptions{}
					call.Policy = jobdb.RunPolicy{}
				}
				if mode == "retry-removed" {
					call.Policy.Retry.MaximumAttempts = 2
					opts.Retry.MaximumAttempts = 2
				}
				if mode == "restartable" {
					opts.OnRestart = func(json.RawMessage) error { return nil }
				}
				var builds, runs atomic.Int32
				task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					n := runs.Add(1)
					if mode == "retry-removed" && n == 1 {
						return nil, errors.New("transient")
					}
					return in, nil
				}}
				worker := wf.ExecutionWorker{JobType: "policy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("operation", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
					})
				}}
				key := submitExecutionTest(t, rt, worker)
				out, err := runExecutionTest(t, rt, key, worker, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
				before, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				wantBuilds, wantRuns := builds.Load(), runs.Load()
				opts.TotalTimeout, opts.AttemptTimeout, opts.Retry = 0, 0, jobdb.RetryPolicy{}
				call.Policy = jobdb.RunPolicy{}
				if mode == "added" || mode == "changed" {
					opts.TotalTimeout, opts.AttemptTimeout = time.Nanosecond, time.Nanosecond
					opts.Retry.MaximumAttempts = 3
					call.Policy = jobdb.RunPolicy{TotalTimeout: jobdb.AsDuration(time.Nanosecond), InvocationTimeout: jobdb.AsDuration(time.Nanosecond), Retry: jobdb.RetryPolicy{MaximumAttempts: 3}}
				}
				replay, err := replayExecutionTest(t, rt, key, worker, task)
				require.NoError(t, err)
				raw, err := replay.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"job"`, string(raw))
				require.Equal(t, wantBuilds, builds.Load())
				require.Equal(t, wantRuns, runs.Load())
				after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after, "inspection cannot rewrite policy or history")
			})
		}
	}
}

func TestExecutionRecordedPolicyTimeoutOutcome(t *testing.T) {
	for _, kind := range []string{"scope-total", "scope-attempt", "task-total", "task-attempt"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rt := alternateRuntime(t, "toy")
				opts := wf.ScopeOptions{}
				call := invocation("blocked")
				switch kind {
				case "scope-total":
					opts.TotalTimeout = time.Minute
				case "scope-attempt":
					opts.AttemptTimeout = time.Minute
				case "task-total":
					call.Policy.TotalTimeout = jobdb.AsDuration(time.Minute)
				case "task-attempt":
					call.Policy.InvocationTimeout = jobdb.AsDuration(time.Minute)
				}
				var builds, runs atomic.Int32
				done := make(chan struct{})
				task := executionTestTask{"echo", func(c wf.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
					runs.Add(1)
					defer close(done)
					<-c.Context().Done()
					return nil, context.Cause(c.Context())
				}}
				worker := wf.ExecutionWorker{JobType: "timeout-policy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("outer", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoScope("organizational", wf.ScopeOptions{}, func(n wf.ExecutionContext) (jobdb.JobData, error) {
							return n.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
						})
					})
				}}
				key := submitExecutionTest(t, rt, worker)
				out, err := runExecutionTest(t, rt, key, worker, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunFailed, out.Status)
				<-done
				var timeout jobdb.TimeoutError
				require.ErrorAs(t, out.JobError, &timeout)
				before := executionRecords(t, rt, key)
				opts = wf.ScopeOptions{}
				call.Policy = jobdb.RunPolicy{}
				time.Sleep(24 * time.Hour)
				_, err = replayExecutionTest(t, rt, key, worker, task)
				require.ErrorAs(t, err, &timeout)
				require.NotErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
				require.EqualValues(t, 1, builds.Load())
				require.EqualValues(t, 1, runs.Load())
				require.Equal(t, before, executionRecords(t, rt, key))
			})
		})
	}
}

// Recovery must preserve the existing scope's budget, while the first entry
// into a new sibling scope uses the new consumer's declaration.
func TestExecutionRecordedPolicyRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, originallyTimed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/timed=%t", backend, originallyTimed), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				opts := wf.ScopeOptions{}
				if originallyTimed {
					opts.TotalTimeout = time.Hour
					opts.AttemptTimeout = 30 * time.Minute
				}
				var builds atomic.Int32
				worker := wf.ExecutionWorker{JobType: "recover-policy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					build := func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil }
					_, err := c.DoScope("existing", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						if _, err := s.DoTask(invocation("first"), build); err != nil {
							return nil, err
						}
						return s.DoTask(invocation("second"), build)
					})
					if err != nil {
						return nil, err
					}
					return c.DoScope("new", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) { return s.DoTask(invocation("third"), build) })
				}}
				key := submitExecutionTest(t, rt, worker)
				out, err := runExecutionTest(t, rt, key, worker)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				first := executionWait(t, rt, key)
				opts.TotalTimeout, opts.AttemptTimeout = 2*time.Hour, 90*time.Minute
				if originallyTimed {
					opts.TotalTimeout, opts.AttemptTimeout = 0, 0
				}
				before := executionRecords(t, rt, key)
				_, err = replayExecutionTest(t, rt, key, worker)
				var miss wf.ReplayCacheMissError
				require.ErrorAs(t, err, &miss)
				require.Equal(t, before, executionRecords(t, rt, key))
				require.EqualValues(t, 1, builds.Load())
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
				require.NoError(t, err)
				finish := func() {
					h, err := engine.GetWaitingTask(context.Background(), key)
					require.NoError(t, err)
					require.NoError(t, h.Finish(context.Background(), jobdb.NewTaskDataOrPanic("answer")))
				}
				finish()
				out, err = runExecutionTest(t, rt, key, worker)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status, "%v", out.JobError)
				var second jobdb.ExecutionRecord
				for _, r := range executionRecords(t, rt, key) {
					if r.Kind == "wait" && strings.Contains(r.ID, `task:"second"`) {
						second = r
					}
				}
				require.NotEmpty(t, second.ID)
				require.Equal(t, first.Deadline, second.Deadline, "existing scope deadline must not change")
				finish()
				out, err = runExecutionTest(t, rt, key, worker)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status, "%v", out.JobError)
				var third jobdb.ExecutionRecord
				for _, r := range executionRecords(t, rt, key) {
					if r.Kind == "wait" && strings.Contains(r.ID, `task:"third"`) {
						third = r
					}
				}
				require.NotEmpty(t, third.ID)
				if originallyTimed {
					require.True(t, third.Deadline.IsZero())
				} else {
					require.WithinDuration(t, third.At.Add(90*time.Minute), third.Deadline, time.Second)
				}
				finish()
				out, err = runExecutionTest(t, rt, key, worker)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
			})
		}
	}
}

// Captured with JobDB 0bd5f2c, before policy binding changed. Keep the fixture
// fixed: generating it with the reader under test would hide protocol drift.
func TestExecutionRecordedPolicyHistoricalFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/execution-recorded-policy-v4.json")
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	require.NoError(t, json.Unmarshal(raw, &records))
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend)
			worker := wf.ExecutionWorker{JobType: "policy-fixture", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("operation", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					return s.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) {
						return nil, errors.New("historical input builder must not execute")
					})
				})
			}}
			key := submitExecutionTest(t, rt, worker)
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "fixture", Routes: []jobdb.Route{{JobType: worker.Name()}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			token := ""
			if l, ok := lease.(interface{ LeaseToken() string }); ok {
				token = l.LeaseToken()
			}
			for i, r := range records {
				raw, err := json.Marshal(r)
				require.NoError(t, err)
				ordinal := int64(i + 1)
				require.NoError(t, rt.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: key, Ordinal: ordinal}, Chapter: jobdb.Chapter{Ordinal: ordinal, TaskType: "__execution__", CreatedAt: r.At, Body: jobdb.ExecutionChapter{Record: raw}}}))
			}
			require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "completed", Chapter: &jobdb.Chapter{Ordinal: int64(len(records) + 1), TaskType: worker.Name(), CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"job"`)}}}}}))
			before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			out, err := replayExecutionTest(t, rt, key, worker)
			require.NoError(t, err)
			require.JSONEq(t, `"job"`, string(out.GetDataOrPanic()))
			after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestExecutionRecordedPolicyExpiredRecovery(t *testing.T) {
	for _, kind := range []string{"scope", "task"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rt := alternateRuntime(t, "toy")
				opts := wf.ScopeOptions{}
				call := invocation("external")
				if kind == "scope" {
					opts.TotalTimeout = time.Minute
				} else {
					call.Policy.TotalTimeout = jobdb.AsDuration(time.Minute)
				}
				var builds atomic.Int32
				worker := wf.ExecutionWorker{JobType: "expired-policy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("existing", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
					})
				}}
				key := submitExecutionTest(t, rt, worker)
				out, err := runExecutionTest(t, rt, key, worker)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				opts.TotalTimeout = 0
				call.Policy.TotalTimeout = nil
				time.Sleep(2 * time.Minute)
				out, err = runExecutionTest(t, rt, key, worker)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunFailed, out.Status, "%v", out.JobError)
				var timeout jobdb.TimeoutError
				require.ErrorAs(t, out.JobError, &timeout)
				require.EqualValues(t, 1, builds.Load())
				_, err = replayExecutionTest(t, rt, key, worker)
				require.ErrorAs(t, err, &timeout)
			})
		})
	}
}

func TestExecutionRecordedPolicyRetryRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, kind := range []string{"scope", "task"} {
			for _, attempts := range []int32{1, 2} {
				t.Run(fmt.Sprintf("%s/%s/attempts=%d", backend, kind, attempts), func(t *testing.T) {
					rt := alternateRuntime(t, backend)
					opts := wf.ScopeOptions{}
					call := invocation("external")
					if kind == "scope" {
						opts.Retry.MaximumAttempts = attempts
					} else {
						call.Policy.Retry.MaximumAttempts = attempts
					}
					worker := wf.ExecutionWorker{JobType: "retry-policy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
						return c.DoScope("existing", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) {
							return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
						})
					}}
					key := submitExecutionTest(t, rt, worker)
					out, err := runExecutionTest(t, rt, key, worker)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunSuspended, out.Status)
					if kind == "scope" {
						opts.Retry.MaximumAttempts = 3 - attempts
					} else {
						call.Policy.Retry.MaximumAttempts = 3 - attempts
					}
					var runs atomic.Int32
					task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
						if runs.Add(1) == 1 {
							return nil, errors.New("transient")
						}
						return in, nil
					}}
					out, err = runExecutionTest(t, rt, key, worker, task)
					require.NoError(t, err)
					want := wf.JobRunFailed
					if attempts == 2 {
						want = wf.JobRunCompleted
					}
					require.Equal(t, want, out.Status, "%v", out.JobError)
					require.Equal(t, attempts, runs.Load())
					before := executionRecords(t, rt, key)
					_, err = replayExecutionTest(t, rt, key, worker, task)
					if attempts == 2 {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "transient")
						require.NotErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
					}
					require.Equal(t, before, executionRecords(t, rt, key))
					require.Equal(t, attempts, runs.Load())
				})
			}
		}
	}
}

func TestExecutionRecordedPolicyRestartAncestor(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			opts := wf.ScopeOptions{TotalTimeout: time.Hour, AttemptTimeout: 30 * time.Minute}
			worker := wf.ExecutionWorker{JobType: "restart-policy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("existing", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					if _, err := s.DoTask(invocation("first"), func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
						return nil, err
					}
					return s.DoTask(invocation("second"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				})
			}}
			var runs atomic.Int32
			task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
			key := submitExecutionTest(t, rt, worker)
			out, err := runExecutionTest(t, rt, key, worker, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			var target string
			for _, r := range executionRecords(t, rt, key) {
				if r.Kind == "task" && strings.Contains(r.ID, `task:"second"`) {
					target = r.ID
				}
			}
			require.NotEmpty(t, target)
			opts = wf.ScopeOptions{}
			restarted, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target}}})
			require.NoError(t, err)
			started := time.Now()
			out, err = runExecutionTest(t, rt, restarted.JobKey, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status, "%v", out.JobError)
			wait := executionWait(t, rt, restarted.JobKey)
			require.WithinDuration(t, started.Add(30*time.Minute), wait.Deadline, time.Second, "restart rebases the retained ancestor's original budget")
			require.EqualValues(t, 2, runs.Load(), "completed work must not rerun during recovery")
			before := executionRecords(t, rt, restarted.JobKey)
			_, err = replayExecutionTest(t, rt, restarted.JobKey, worker)
			var miss wf.ReplayCacheMissError
			require.ErrorAs(t, err, &miss)
			require.Equal(t, before, executionRecords(t, rt, restarted.JobKey))
		})
	}
}
