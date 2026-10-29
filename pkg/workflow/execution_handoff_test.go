package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// These reads cannot reliably tell us what the current invocation acknowledged.
// They may fail, or a subsequent worker may already have changed the job.
type handoffUnreadableRuntime struct{ jobdb.WorkflowRuntime }

func (r handoffUnreadableRuntime) GetJob(context.Context, jobdb.JobKey) (jobdb.JobInfo, error) {
	return jobdb.JobInfo{}, errors.New("scheduler unavailable after handoff")
}
func (r handoffUnreadableRuntime) ListJobs(context.Context, jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	return jobdb.ListJobsResponse{}, errors.New("scheduler unavailable after handoff")
}

func TestExecutionHandoffWaitsForOrchestrationButNotTask(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, taskLevel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/task=%t", backend, taskLevel), func(t *testing.T) {
				base := alternateRuntime(t, backend)
				rt := handoffUnreadableRuntime{base}
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				exited := make(chan struct{})
				returned := make(chan error, 1)
				// Always release abandoned user code, including if an assertion fails.
				defer func() {
					unblock()
					select {
					case <-exited:
					case <-time.After(5 * time.Second):
						t.Error("Yield caller did not exit")
					}
				}()
				metadata := json.RawMessage(`{"kind":"environment_required","environment":"gpu","marker":"handoff-only"}`)
				original := append(json.RawMessage(nil), metadata...)
				handoff := func(ctx context.Context, yield func(context.Context, wf.YieldRequest) error) error {
					defer close(exited)
					err := yield(ctx, wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "receiver"}}, Metadata: metadata})
					returned <- err
					<-release // Only a task worker may outlive Run after Yield.
					return err
				}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					return nil, handoff(c.Context(), c.Yield)
				}}
				worker := wf.ExecutionWorker{JobType: "sender", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("nested", wf.ScopeOptions{}, func(c wf.ExecutionContext) (jobdb.JobData, error) {
						if taskLevel {
							return c.DoTask(invocation("handoff"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
						}
						return nil, handoff(c.Context(), c.Yield)
					})
				}}
				key := submitExecutionTest(t, base, worker)
				runnable, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: worker, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "sender", LeaseDuration: time.Minute})
				require.NoError(t, err)
				type result struct {
					out wf.JobRunOutcome
					err error
				}
				done := make(chan result, 1)
				go func() { out, err := runnable.Run(nil); done <- result{out, err} }()
				select {
				case err := <-returned:
					require.True(t, wf.IsExecutionSuspended(err))
				case <-time.After(5 * time.Second):
					t.Fatal("Yield did not return control error")
				}
				if !taskLevel {
					select {
					case <-done:
						t.Fatal("Run returned before orchestration exited")
					default:
					}
					unblock()
				}
				var out wf.JobRunOutcome
				select {
				case res := <-done:
					require.NoError(t, res.err)
					out = res.out
				case <-time.After(5 * time.Second):
					t.Fatal("Run waited for user code after Yield")
				}
				require.Equal(t, wf.JobRunSuspended, out.Status)
				require.NotNil(t, out.Handoff)
				require.Equal(t, jobdb.Route{JobType: "receiver"}, out.Handoff.NextRoute)
				require.JSONEq(t, string(original), string(out.Handoff.Metadata))
				if taskLevel {
					select {
					case <-exited:
						t.Fatal("task caller bypassed gate")
					default:
					}
				} else {
					select {
					case <-exited:
					default:
						t.Fatal("Run returned before orchestration cleanup")
					}
				}
				// Request, returned outcome, cached Outcome(), and repeat Run() own copies.
				metadata[0] = '!'
				out.Handoff.Metadata[0] = '!'
				out.Handoff.NextRoute.JobType = "mutated"
				cached, ok := runnable.Outcome()
				require.True(t, ok)
				require.JSONEq(t, string(original), string(cached.Handoff.Metadata))
				require.Equal(t, "receiver", cached.Handoff.NextRoute.JobType)
				cached.Handoff.Metadata[0] = '!'
				again, err := runnable.Run(nil)
				require.NoError(t, err)
				require.JSONEq(t, string(original), string(again.Handoff.Metadata))
				for _, record := range executionRecords(t, base, key) {
					require.NotEqual(t, "result", record.Kind)
					data, err := json.Marshal(record)
					require.NoError(t, err)
					require.NotContains(t, string(data), "handoff-only")
				}
				// The handoff remains this invocation's receipt after the job changes again.
				require.NoError(t, base.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: key}))
				again, err = runnable.Run(nil)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, again.Status)
				require.NotNil(t, again.Handoff)
				next, err := wf.GetJobForRun(context.Background(), base, wf.GetJobForRunRequest{JobKey: key, JobWorker: worker, WorkerID: "later"})
				require.NoError(t, err)
				nextOut, err := next.Run(nil)
				require.NoError(t, err)
				require.Nil(t, nextOut.Handoff)
			})
		}
	}
}

func TestExecutionHandoffRecoveryAndReplay(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var runs atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				if runs.Add(1) == 1 {
					return nil, c.Yield(c.Context(), wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "recover"}}, Metadata: json.RawMessage(`{"kind":"environment_required"}`)})
				}
				return in, nil
			}}
			worker := wf.ExecutionWorker{JobType: "recover", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(invocation("yield"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, worker)
			out, err := runExecutionTest(t, rt, key, worker, task)
			require.NoError(t, err)
			require.NotNil(t, out.Handoff)
			// Inspecting a job without acquiring it is not confirmation of a new Yield.
			lease, err := rt.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "replacement", Routes: []jobdb.Route{{JobType: worker.Name()}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			waiting, err := runExecutionTest(t, rt, key, worker, task)
			require.NoError(t, err)
			require.Nil(t, waiting.Handoff)
			runnable, err := wf.GetJobForRunWithLease(context.Background(), rt, lease, wf.GetJobForRunRequest{JobKey: key, JobWorker: worker, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "replacement"})
			require.NoError(t, err)
			out, err = runnable.Run(nil)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.Nil(t, out.Handoff)
			before := executionRecords(t, rt, key)
			replay, err := replayExecutionTest(t, rt, key, worker)
			require.NoError(t, err)
			require.Equal(t, out.Output.GetDataOrPanic(), replay.GetDataOrPanic())
			require.EqualValues(t, 2, runs.Load())
			require.Equal(t, before, executionRecords(t, rt, key))
		})
	}
}

func TestExecutionHandoffInvalidMetadata(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, taskLevel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/task=%t", backend, taskLevel), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				req := wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "receiver"}}, Metadata: json.RawMessage(`{invalid`)}
				check := func(err error) {
					require.ErrorContains(t, err, "metadata must be valid JSON")
					require.False(t, wf.IsExecutionSuspended(err))
				}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					check(c.Yield(c.Context(), req))
					return in, nil
				}}
				worker := wf.ExecutionWorker{JobType: "invalid", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if taskLevel {
						return c.DoTask(invocation("invalid"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
					}
					check(c.Yield(c.Context(), req))
					return in, nil
				}}
				key := submitExecutionTest(t, rt, worker)
				out, err := runExecutionTest(t, rt, key, worker, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				require.Nil(t, out.Handoff)
			})
		}
	}
}

// Model an uncooperative transport that commits but withholds acknowledgement
// past run cancellation. Hide renewal (and use no-op legacy keepalive) so this
// fault tests the outcome boundary independently of lease-session I/O locking.
type lateHandoffRuntime struct {
	jobdb.WorkflowRuntime
	committed chan struct{}
	release   chan struct{}
}

func (r lateHandoffRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	lease, err := r.WorkflowRuntime.GetJobLease(ctx, req)
	if err != nil || lease == nil {
		return lease, err
	}
	return lateHandoffLease{ExecutionLease: lease, committed: r.committed, release: r.release}, nil
}

type lateHandoffLease struct {
	jobdb.ExecutionLease
	committed chan struct{}
	release   chan struct{}
}

func (l lateHandoffLease) LeaseToken() string {
	if token, ok := l.ExecutionLease.(interface{ LeaseToken() string }); ok {
		return token.LeaseToken()
	}
	return ""
}
func (l lateHandoffLease) KeepAlive(context.Context) error { return nil }
func (l lateHandoffLease) StopKeepAlive()                  {}
func (l lateHandoffLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	if err := l.ExecutionLease.Reschedule(ctx, req); err != nil {
		return err
	}
	close(l.committed)
	<-l.release
	return nil
}

func TestExecutionHandoffLateAcknowledgementCannotChangeOutcome(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, taskLevel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/task=%t", backend, taskLevel), func(t *testing.T) {
				base := alternateRuntime(t, backend)
				committed := make(chan struct{})
				release := make(chan struct{})
				yieldReturned := make(chan error, 1)
				var once atomic.Bool
				unblock := func() {
					if once.CompareAndSwap(false, true) {
						close(release)
					}
				}
				defer unblock()
				rt := lateHandoffRuntime{WorkflowRuntime: base, committed: committed, release: release}
				req := wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "receiver"}}}
				call := func(ctx context.Context, yield func(context.Context, wf.YieldRequest) error) error {
					err := yield(ctx, req)
					yieldReturned <- err
					return err
				}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					return nil, call(c.Context(), c.Yield)
				}}
				worker := wf.ExecutionWorker{JobType: "sender", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if taskLevel {
						return c.DoTask(invocation("late"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
					}
					return nil, call(c.Context(), c.Yield)
				}}
				key := submitExecutionTest(t, base, worker)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				runnable, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: worker, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "cancelled", LeaseDuration: time.Minute})
				require.NoError(t, err)
				type result struct {
					out wf.JobRunOutcome
					err error
				}
				done := make(chan result, 1)
				go func() { out, err := runnable.Run(nil); done <- result{out, err} }()
				select {
				case <-committed:
				case <-time.After(5 * time.Second):
					t.Fatal("reschedule did not commit")
				}
				cancel()
				if !taskLevel {
					// A synchronous scope waits for the transport to return. Its
					// cancelled response must still never confirm a handoff.
					unblock()
				}
				select {
				case res := <-done:
					require.ErrorIs(t, res.err, context.Canceled)
					require.Nil(t, res.out.Handoff)
				case <-time.After(5 * time.Second):
					t.Fatal("Run waited indefinitely for unacknowledged Yield")
				}
				unblock()
				select {
				case err := <-yieldReturned:
					require.False(t, wf.IsExecutionSuspended(err))
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(5 * time.Second):
					t.Fatal("late Yield did not return")
				}
				out, err := runnable.Run(nil)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, out.Handoff)
				_, ok := runnable.Outcome()
				require.False(t, ok)
				for _, record := range executionRecords(t, base, key) {
					require.NotEqual(t, "result", record.Kind)
				}
			})
		}
	}
}

func TestJobWorkerYieldHandoffOutcome(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, taskLevel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/task=%t", backend, taskLevel), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				req := wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "receiver"}}, Metadata: json.RawMessage(`{"kind":"environment_required"}`)}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					return nil, c.Yield(c.Context(), req)
				}}
				worker := suppliedJob{run: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if taskLevel {
						return c.DoTask(wf.TaskCall{Site: "task", Command: []byte("{}"), TaskType: "echo", ContractVersion: 1, Policy: jobdb.RunPolicy{}}, func(context.Context) (wf.TaskData, error) { return in, nil })
					}
					return nil, c.Yield(context.Background(), req)
				}}
				h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("job")}})
				require.NoError(t, err)
				runnable, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: worker, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "job-worker", LeaseDuration: time.Minute})
				require.NoError(t, err)
				out, err := runnable.Run(nil)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				require.NotNil(t, out.Handoff)
				require.Equal(t, req.Reschedule.NextRoute, out.Handoff.NextRoute)
				require.Equal(t, req.Metadata, out.Handoff.Metadata)
			})
		}
	}
}

func TestExecutionHandoffSurvivesConcurrentJobChange(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			base := alternateRuntime(t, backend)
			worker := wf.ExecutionWorker{JobType: "sender", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return nil, c.Yield(c.Context(), wf.YieldRequest{Reschedule: jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "receiver"}}})
			}}
			key := submitExecutionTest(t, base, worker)
			rt := yieldFaultRuntime{WorkflowRuntime: base, afterCommit: func() error {
				// Change the job after the scheduling commit but before acknowledgement
				// reaches Yield. Run must still report its own acknowledged transition.
				return base.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: key})
			}}
			out, err := runExecutionTest(t, rt, key, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			require.NotNil(t, out.Handoff)
			require.Equal(t, "receiver", out.Handoff.NextRoute.JobType)
			info, err := base.GetJob(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusCancelled, info.Status)
		})
	}
}
