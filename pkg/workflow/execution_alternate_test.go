package workflow_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func executionRecords(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey) []jobdb.ExecutionRecord {
	t.Helper()
	chapters, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	for _, chapter := range chapters {
		records = append(records, executionChapterRecords(t, chapter)...)
	}
	return records
}

// Includes reconstruction metadata while retaining raw chapters for volume tests.
func executionChapterRecords(t *testing.T, chapter jobdb.Chapter) []jobdb.ExecutionRecord {
	t.Helper()
	var record *jobdb.ExecutionRecord
	rawPrefix := chapter.Metadata.Fields["metadata"].Map[executionmeta.Key].String
	if body, ok := chapter.Body.(jobdb.ExecutionChapter); ok {
		r, err := jobdb.DecodeExecutionRecord(body.Record)
		require.NoError(t, err)
		record = &r
		if r.Kind == "restart" && len(r.Descriptor) > 0 {
			rawPrefix = string(r.Descriptor)
		}
	}
	raw, err := executionmeta.Decode(rawPrefix)
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	for _, item := range raw {
		r, err := jobdb.DecodeExecutionRecord(item)
		require.NoError(t, err)
		records = append(records, r)
	}
	if record != nil {
		records = append(records, *record)
	}
	return records
}

func executionWait(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey) jobdb.ExecutionRecord {
	t.Helper()
	for _, record := range executionRecords(t, rt, key) {
		if record.Kind == "wait" {
			return record
		}
	}
	t.Fatal("missing durable wait")
	return jobdb.ExecutionRecord{}
}

func TestExecutionAlternateLifecycle(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		modes := []string{"fallback", "local-primary", "leased-primary", "human-before", "human-late", "claimed-crash", "primary-lease-expired", "retry"}
		for _, mode := range modes {
			controlledExpiry := mode == "primary-lease-expired" || mode == "claimed-crash" || mode == "retry"
			scenario := func(t *testing.T) {
				ctx := context.Background()
				var rt jobdb.WorkflowRuntime
				var setSchedulerTimes func(jobdb.JobKey, bool)
				if controlledExpiry && backend != "toy" {
					rt, setSchedulerTimes = alternateExpiryRuntime(t, backend)
				} else {
					rt = alternateRuntime(t, backend)
				}
				var builds, primaryRuns, fallbackRuns atomic.Int32
				call := invocation("answer")
				call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback", After: 150 * time.Millisecond}
				if mode == "human-before" || mode == "leased-primary" {
					call.Alternate.After = time.Hour
				}
				if mode == "primary-lease-expired" && setSchedulerTimes != nil {
					call.Alternate.After = 0
				}
				if mode == "retry" {
					call.Policy.Retry.MaximumAttempts = 2
				}
				w := wf.ExecutionWorker{JobType: "timed-answer", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
						builds.Add(1)
						return jobdb.NewTaskDataOrPanic("original"), nil
					})
				}}
				primary := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					primaryRuns.Add(1)
					return jobdb.NewTaskDataOrPanic("primary"), nil
				}}
				fallback := executionTestTask{"fallback", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					raw, err := in.GetData()
					require.NoError(t, err)
					require.JSONEq(t, `"original"`, string(raw))
					if fallbackRuns.Add(1) == 1 && mode == "retry" {
						return nil, errors.New("try again")
					}
					return jobdb.NewTaskDataOrPanic("fallback"), nil
				}}
				key := submitExecutionTest(t, rt, w)
				expected := "fallback"
				if mode == "local-primary" {
					out, err := runExecutionTest(t, rt, key, w, primary, fallback)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunCompleted, out.Status)
					expected = "primary"
					for _, record := range executionRecords(t, rt, key) {
						require.NotEqual(t, "wait", record.Kind)
					}
				} else {
					out, err := runExecutionTest(t, rt, key, w, fallback)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunSuspended, out.Status)
					if mode == "primary-lease-expired" && setSchedulerTimes != nil {
						// Hold scheduler eligibility until the primary is claimed. The
						// recorded due time is already past and remains unchanged.
						setSchedulerTimes(key, false)
					}
					wait := executionWait(t, rt, key)
					require.NotNil(t, wait.AlternateAt)
					require.Equal(t, "echo", wait.ExecutionTaskType)
					require.WithinDuration(t, wait.At.Add(call.Alternate.After), *wait.AlternateAt, 100*time.Millisecond)
					engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
					require.NoError(t, err)
					pending, err := engine.GetWaitingTask(ctx, key)
					require.NoError(t, err)
					require.Equal(t, "echo", pending.TaskType())
					finish := func() error { return pending.Finish(ctx, jobdb.NewTaskDataOrPanic("human")) }
					before := executionRecords(t, rt, key)
					_, err = replayExecutionTest(t, rt, key, w, primary, fallback)
					var miss wf.ReplayCacheMissError
					require.ErrorAs(t, err, &miss)
					require.Equal(t, before, executionRecords(t, rt, key))
					require.Zero(t, primaryRuns.Load())
					require.Zero(t, fallbackRuns.Load())
					tasks := []wf.TaskWorker{fallback}
					switch mode {
					case "human-before":
						require.NoError(t, finish())
						expected = "human"
					case "leased-primary":
						tasks = []wf.TaskWorker{primary}
						expected = "primary"
					case "primary-lease-expired":
						duration := time.Hour
						if backend == "toy" {
							duration = 200 * time.Millisecond // synctest's virtual clock
						}
						lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "primary", Routes: []jobdb.Route{{JobType: w.Name(), TaskType: "echo"}}, LeaseDuration: duration})
						require.NoError(t, err)
						require.NotNil(t, lease)
						require.Error(t, finish())
						if setSchedulerTimes != nil {
							setSchedulerTimes(key, true)
						} else {
							waitAlternate(time.Now().Add(250 * time.Millisecond))
						}
					default:
						waitAlternate(*wait.AlternateAt)
						if mode == "human-late" {
							require.NoError(t, finish())
							expected = "human"
						}
						if mode == "claimed-crash" || mode == "retry" {
							// The scheduler applies the relative delay after transport and
							// storage latency. Observe eligibility rather than assuming the
							// first request at the client's due time must acquire a lease.
							duration := time.Hour
							if backend == "toy" {
								duration = 80 * time.Millisecond // synctest's virtual clock
							}
							require.Eventually(t, func() bool {
								lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "crashed", Routes: []jobdb.Route{{JobType: w.Name(), TaskType: "fallback"}}, LeaseDuration: duration})
								require.NoError(t, err)
								return lease != nil
							}, 3*time.Second, 25*time.Millisecond)
							require.Error(t, finish())
							if setSchedulerTimes != nil {
								setSchedulerTimes(key, true)
							} else {
								waitAlternate(time.Now().Add(120 * time.Millisecond))
							}
							// The committed B route must survive both lease loss and retries,
							// even when A becomes available in this worker process.
							tasks = []wf.TaskWorker{primary, fallback}
						}
					}
					require.Eventually(t, func() bool {
						out, err = runExecutionTest(t, rt, key, w, tasks...)
						require.NoError(t, err)
						return out.Status == wf.JobRunCompleted || out.Status == wf.JobRunFailed
					}, 3*time.Second, 25*time.Millisecond)
					require.Equal(t, wf.JobRunCompleted, out.Status)
					require.Equal(t, wait, executionWait(t, rt, key))
					require.Error(t, finish(), "old answer cannot replace the committed outcome")
				}
				before := executionRecords(t, rt, key)
				// Replay needs neither A nor B and must not rebuild input.
				value, err := replayExecutionTest(t, rt, key, w)
				require.NoError(t, err)
				raw, err := value.GetData()
				require.NoError(t, err)
				require.JSONEq(t, fmt.Sprintf("%q", expected), string(raw))
				require.Equal(t, before, executionRecords(t, rt, key))
				require.EqualValues(t, 1, builds.Load())
				wantPrimary, wantFallback := int32(0), int32(0)
				if expected == "primary" {
					wantPrimary = 1
				}
				if expected == "fallback" {
					wantFallback = 1
				}
				if mode == "retry" {
					wantFallback = 2
				}
				require.Equal(t, wantPrimary, primaryRuns.Load())
				require.Equal(t, wantFallback, fallbackRuns.Load())
				original := *call.Alternate
				for _, changed := range []wf.TaskAlternateAfter{{TaskType: "other", After: original.After}, {TaskType: original.TaskType, After: original.After + time.Second}} {
					call.Alternate = &changed
					_, err = replayExecutionTest(t, rt, key, w)
					require.ErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
				}
			}
			t.Run(backend+"/"+mode, func(t *testing.T) {
				if backend == "toy" && controlledExpiry {
					// Inspection/replay cannot consume the primary claim window.
					// Only explicit waits advance the in-memory runtime's clock.
					synctest.Test(t, scenario)
				} else {
					scenario(t)
				}
			})
		}
	}
}

func TestExecutionAlternatePendingRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend)
			var builds atomic.Int32
			call := invocation("answer")
			call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback", After: time.Hour}
			w := wf.ExecutionWorker{JobType: "pending-recovery", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}}
			fallback := executionTestTask{"fallback", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
				t.Error("fallback ran early")
				return nil, errors.New("early")
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			wait := executionWait(t, rt, key)
			out, err = runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			require.False(t, out.LeaseAcquired)
			// Re-admit orchestration after a scheduler handoff. Replaying the
			// pending wait must not move the due time or rebuild its input.
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "handoff", Routes: []jobdb.Route{{JobType: w.Name(), TaskType: "echo"}}, LeaseDuration: time.Second})
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: w.Name()}}))
			out, err = runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			require.Equal(t, wait, executionWait(t, rt, key))
			require.EqualValues(t, 1, builds.Load())
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
			require.NoError(t, err)
			pending, err := engine.GetWaitingTask(ctx, key)
			require.NoError(t, err)
			require.NoError(t, pending.Finish(ctx, jobdb.NewTaskDataOrPanic("answer")))
			out, err = runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
		})
	}
}

func TestExecutionAlternateBackoffRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds, runs atomic.Int32
			call := invocation("answer")
			call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback"}
			call.Policy.Retry = jobdb.RetryPolicy{MaximumAttempts: 2, InitialInterval: jobdb.Duration(200 * time.Millisecond)}
			w := wf.ExecutionWorker{JobType: "alternate-backoff", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}}
			fallback := executionTestTask{"fallback", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				if runs.Add(1) == 1 {
					return nil, errors.New("retry")
				}
				return in, nil
			}}
			primary := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
				t.Error("retried A after selecting B")
				return nil, errors.New("wrong implementation")
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{fallback}, WorkerID: "backoff", LeaseDuration: time.Second, AwaitThreshold: time.Millisecond})
			require.NoError(t, err)
			out, err = run.Run(nil)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			require.Eventually(t, func() bool {
				out, err = runExecutionTest(t, rt, key, w, primary, fallback)
				require.NoError(t, err)
				return out.Status == wf.JobRunCompleted || out.Status == wf.JobRunFailed
			}, 3*time.Second, 25*time.Millisecond)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.EqualValues(t, 2, runs.Load())
			require.EqualValues(t, 1, builds.Load())
			_, err = replayExecutionTest(t, rt, key, w)
			require.NoError(t, err)
		})
	}
}

func TestExecutionAlternateDeadlines(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"deadline-first", "alternate-first", "scope", "completed"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				call := invocation("answer")
				call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback", After: time.Hour}
				call.Policy.TotalTimeout = jobdb.AsDuration(250 * time.Millisecond)
				if mode == "alternate-first" {
					call.Alternate.After = 0
				}
				if mode == "scope" {
					call.Policy.TotalTimeout = nil
				}
				w := wf.ExecutionWorker{JobType: "alternate-deadline", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					task := func(c wf.ExecutionContext) (jobdb.JobData, error) {
						return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
					}
					if mode == "scope" {
						return c.DoScope("budget", wf.ScopeOptions{TotalTimeout: 250 * time.Millisecond}, task)
					}
					return task(c)
				}}
				fallback := executionTestTask{"fallback", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
					t.Error("expired task executed")
					return nil, errors.New("expired")
				}}
				key := submitExecutionTest(t, rt, w)
				out, err := runExecutionTest(t, rt, key, w, fallback)
				require.NoError(t, err)
				if out.Status == wf.JobRunSuspended {
					wait := executionWait(t, rt, key)
					engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
					require.NoError(t, err)
					pending, err := engine.GetWaitingTask(context.Background(), key)
					require.NoError(t, err)
					if mode == "completed" {
						require.NoError(t, pending.Finish(context.Background(), jobdb.NewTaskDataOrPanic("answered")))
					}
					waitAlternate(wait.Deadline.Add(time.Millisecond))
					tasks := []wf.TaskWorker{}
					if mode == "alternate-first" {
						tasks = append(tasks, fallback)
					}
					out, err = runExecutionTest(t, rt, key, w, tasks...)
					require.NoError(t, err)
					if mode != "completed" {
						// The timeout has now won the waiting slot and recorded an
						// outcome. Wall-clock expiry alone does not reject answers.
						require.Error(t, pending.Finish(context.Background(), jobdb.NewTaskDataOrPanic("too late")))
					}
				}
				if mode == "completed" {
					require.Equal(t, wf.JobRunCompleted, out.Status)
					_, err = replayExecutionTest(t, rt, key, w)
					require.NoError(t, err)
				} else {
					require.Equal(t, wf.JobRunFailed, out.Status)
					var timeout jobdb.TimeoutError
					require.ErrorAs(t, out.JobError, &timeout)
					_, err = replayExecutionTest(t, rt, key, w)
					require.ErrorAs(t, err, &timeout)
				}
			})
		}
	}
}

func TestExecutionAlternateInvalidDeclarations(t *testing.T) {
	for _, alternate := range []wf.TaskAlternateAfter{{}, {TaskType: "echo"}, {TaskType: "fallback", After: -time.Second}} {
		t.Run(fmt.Sprintf("%+v", alternate), func(t *testing.T) {
			rt := alternateRuntime(t, "toy")
			call := invocation("answer")
			call.Alternate = &alternate
			w := wf.ExecutionWorker{JobType: "invalid-alternate", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
					t.Error("invalid declaration built inputs")
					return in, nil
				})
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.ErrorContains(t, out.JobError, "alternate requires")
		})
	}
}

func TestExecutionAlternateCompletionRace(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var runs atomic.Int32
			call := invocation("answer")
			call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback"}
			w := wf.ExecutionWorker{JobType: "answer-race", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			fallback := executionTestTask{"fallback", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				runs.Add(1)
				return jobdb.NewTaskDataOrPanic("fallback"), nil
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
			require.NoError(t, err)
			pending, err := engine.GetWaitingTask(context.Background(), key)
			require.NoError(t, err)
			answered := make(chan error, 1)
			go func() { answered <- pending.Finish(context.Background(), jobdb.NewTaskDataOrPanic("human")) }()
			_, err = runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			humanErr := <-answered
			_, err = runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			value, err := replayExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			raw, err := value.GetData()
			require.NoError(t, err)
			if humanErr == nil {
				require.JSONEq(t, `"human"`, string(raw))
				require.Zero(t, runs.Load())
			} else {
				require.JSONEq(t, `"fallback"`, string(raw))
				require.EqualValues(t, 1, runs.Load())
			}
		})
	}
}

func TestExecutionAlternateExpiredAttemptKeepsSelection(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds, runs atomic.Int32
			call := invocation("answer")
			call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback"}
			call.Policy.InvocationTimeout = jobdb.AsDuration(time.Second)
			call.Policy.Retry.MaximumAttempts = 2
			w := wf.ExecutionWorker{JobType: "expired-at-pickup", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}}
			primary := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
				t.Error("timeout returned selection to A")
				return nil, errors.New("wrong implementation")
			}}
			fallback := executionTestTask{"fallback", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			waitAlternate(executionWait(t, rt, key).Deadline.Add(time.Millisecond))
			out, err = runExecutionTest(t, rt, key, w, primary, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.EqualValues(t, 1, builds.Load())
			require.EqualValues(t, 1, runs.Load())
			_, err = replayExecutionTest(t, rt, key, w)
			require.NoError(t, err)
		})
	}
}

func TestExecutionAlternateCancellation(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, claimed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/claimed=%v", backend, claimed), func(t *testing.T) {
				ctx := context.Background()
				rt := alternateRuntime(t, backend)
				call := invocation("answer")
				call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback"}
				w := wf.ExecutionWorker{JobType: "cancel-alternate", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
				}}
				fallback := executionTestTask{"fallback", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
					t.Error("cancelled task executed")
					return nil, errors.New("cancelled")
				}}
				key := submitExecutionTest(t, rt, w)
				out, err := runExecutionTest(t, rt, key, w, fallback)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
				require.NoError(t, err)
				pending, err := engine.GetWaitingTask(ctx, key)
				require.NoError(t, err)
				var lease jobdb.ExecutionLease
				if claimed {
					lease, err = rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "claimed", Routes: []jobdb.Route{{JobType: w.Name(), TaskType: "fallback"}}, LeaseDuration: time.Second})
					require.NoError(t, err)
					require.NotNil(t, lease)
				}
				require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key}))
				require.Error(t, pending.Finish(ctx, jobdb.NewTaskDataOrPanic("too late")))
				if claimed {
					require.Error(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: w.Name()}}))
				}
				out, err = runExecutionTest(t, rt, key, w, fallback)
				require.NoError(t, err)
				require.False(t, out.LeaseAcquired)
			})
		}
	}
}

func TestExecutionAlternateDispatchRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"dispatch-uncommitted", "dispatch-committed", "dispatch-ack-lost", "result-uncommitted", "result-committed"} {
			scenario := func(t *testing.T) {
				const leaseDuration = 10 * time.Second
				var base jobdb.WorkflowRuntime
				var setSchedulerTimes func(jobdb.JobKey, bool)
				if backend == "toy" {
					base = alternateRuntime(t, backend)
				} else {
					base, setSchedulerTimes = alternateExpiryRuntime(t, backend)
				}
				var builds, runs atomic.Int32
				call := invocation("answer")
				call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback"}
				w := wf.ExecutionWorker{JobType: "dispatch-recovery", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
				}}
				fallback := executionTestTask{"fallback", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
				key := submitExecutionTest(t, base, w)
				out, err := runExecutionTest(t, base, key, w, fallback)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				kind := strings.Split(mode, "-")[0]
				fault := &executionFaultRuntime{WorkflowRuntime: base, after: strings.HasSuffix(mode, "-committed") || mode == "dispatch-ack-lost", fail: func(r jobdb.ExecutionRecord) bool { return r.Kind == kind && strings.Contains(r.ID, "task:") }}
				if mode != "dispatch-ack-lost" {
					fault.cancel = cancel
				}
				run, err := wf.GetJobForRun(ctx, fault, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{fallback}, WorkerID: "interrupted", LeaseDuration: leaseDuration})
				require.NoError(t, err)
				out, err = run.Run(nil)
				require.True(t, fault.fired.Load())
				if fault.after {
					require.True(t, fault.committed.Load(), "fault injection must follow a successful append; run error: %v", err)
				}
				if mode == "dispatch-ack-lost" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					before := executionRecords(t, base, key)
					_, err = replayExecutionTest(t, base, key, w, fallback)
					var miss wf.ReplayCacheMissError
					require.ErrorAs(t, err, &miss)
					require.Equal(t, before, executionRecords(t, base, key))
					// Run has stopped renewal. Expire only now, after verifying the
					// intended crash point, independently of storage/HTTP speed.
					if setSchedulerTimes != nil {
						setSchedulerTimes(key, true)
					} else {
						time.Sleep(leaseDuration + time.Millisecond)
					}
					require.Eventually(t, func() bool {
						out, err = runExecutionTest(t, base, key, w, fallback)
						require.NoError(t, err)
						return out.Status != wf.JobRunNotLeaseable
					}, 3*time.Second, 25*time.Millisecond)
				}
				require.Equal(t, wf.JobRunCompleted, out.Status)
				require.EqualValues(t, 1, builds.Load())
				wantRuns := int32(1)
				if mode == "result-uncommitted" {
					wantRuns = 2
				}
				require.Equal(t, wantRuns, runs.Load())
				_, err = replayExecutionTest(t, base, key, w)
				require.NoError(t, err)
				require.Equal(t, wantRuns, runs.Load())
			}
			t.Run(backend+"/"+mode, func(t *testing.T) {
				if backend == "toy" {
					synctest.Test(t, scenario)
				} else {
					scenario(t)
				}
			})
		}
	}
}

// Keep the primary claim and lease expiry independent of HTTP/storage speed.
// Only scheduler timestamps change; the durable wait, input and hashes do not.
func alternateExpiryRuntime(t *testing.T, backend string) (jobdb.WorkflowRuntime, func(jobdb.JobKey, bool)) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "expiry.db")
	base, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close(ctx)) })
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var runtime jobdb.WorkflowRuntime = base
	if backend == "remote" {
		server := httptest.NewServer(remote.NewServer(base))
		t.Cleanup(server.Close)
		runtime, err = remote.New(server.URL, server.Client())
		require.NoError(t, err)
	}
	return runtime, func(key jobdb.JobKey, expire bool) {
		t.Helper()
		query := "UPDATE jobdb_jobs SET alternate_at_ns=? WHERE tenant_id=? AND job_id=?"
		at := time.Now().Add(time.Hour).UnixNano()
		if expire {
			query = "UPDATE jobdb_jobs SET alternate_at_ns=?, lease_expires_at_ns=0 WHERE tenant_id=? AND job_id=?"
			at = 0
		}
		result, err := db.ExecContext(ctx, query, at, key.TenantId, key.JobId)
		require.NoError(t, err)
		n, err := result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	}
}
