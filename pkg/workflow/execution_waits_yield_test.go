package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Exercise real default deadlines: a zero policy hid the SQLite join bug.
func TestExecutionChildWaitDeadlineAndRecovery(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, taskWait := range []bool{false, true} {
			for _, terminal := range []string{"success", "failure", "cancel"} {
				t.Run(fmt.Sprintf("%s/task=%t/%s", backend, taskWait, terminal), func(t *testing.T) {
					ctx := context.Background()
					rt := alternateRuntime(t, backend)
					child := wf.ExecutionWorker{JobType: "child", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
						if terminal == "failure" {
							return nil, errors.New("child failure")
						}
						return jobdb.NewTaskDataOrPanic("done"), nil
					}}
					children := []jobdb.JobKey{submitExecutionTest(t, rt, child), submitExecutionTest(t, rt, child)}
					ids := []string{children[0].JobId, children[1].JobId}
					var calls atomic.Int32
					task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
						calls.Add(1)
						if err := c.AwaitJobs(ids...); err != nil {
							return nil, err
						}
						return in, nil
					}}
					parent := wf.ExecutionWorker{JobType: "parent", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
						if taskWait {
							return c.DoTask(invocation("join"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
						}
						if err := c.AwaitJobs(ids...); err != nil {
							return nil, err
						}
						return in, nil
					}}
					policy := jobdb.DefaultRunPolicy()
					policy.Retry.MaximumAttempts = 1
					h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: parent.Name(), Data: jobdb.NewTaskDataOrPanic("parent input"), RunPolicy: policy}})
					require.NoError(t, err)
					run := func(worker string) wf.JobRunOutcome {
						r, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: parent, TaskWorkers: []wf.TaskWorker{task}, WorkerID: worker, LeaseDuration: time.Minute})
						require.NoError(t, err)
						out, err := r.Run(nil)
						require.NoError(t, err)
						return out
					}
					require.Equal(t, wf.JobRunSuspended, run("original").Status)
					for i, key := range children {
						if terminal == "cancel" {
							require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key}))
						} else {
							out, err := runExecutionTest(t, rt, key, child)
							require.NoError(t, err)
							if terminal == "failure" {
								require.Equal(t, wf.JobRunFailed, out.Status)
							} else {
								require.Equal(t, wf.JobRunCompleted, out.Status)
							}
						}
						if i == 0 {
							require.Equal(t, wf.JobRunSuspended, run("too-early").Status)
						}
					}
					out := run("replacement")
					require.Equal(t, wf.JobRunCompleted, out.Status)
					before := executionRecords(t, rt, h.JobKey)
					count := calls.Load()
					replay, err := replayExecutionTest(t, rt, h.JobKey, parent, task)
					require.NoError(t, err)
					require.Equal(t, out.Output.GetDataOrPanic(), replay.GetDataOrPanic())
					require.Equal(t, count, calls.Load())
					require.Equal(t, before, executionRecords(t, rt, h.JobKey))
				})
			}
		}
	}
}

// Inject reschedule failures at the lease boundary, including a lost response
// after the underlying runtime has committed the handoff.
type yieldFaultRuntime struct {
	jobdb.WorkflowRuntime
	mode string
}

func (r yieldFaultRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	l, err := r.WorkflowRuntime.GetJobLease(ctx, req)
	if err != nil || l == nil {
		return l, err
	}
	return yieldFaultLease{RenewableExecutionLease: l.(jobdb.RenewableExecutionLease), mode: r.mode}, nil
}

type yieldFaultLease struct {
	jobdb.RenewableExecutionLease
	mode string
}

func (l yieldFaultLease) LeaseToken() string {
	if token, ok := l.RenewableExecutionLease.(interface{ LeaseToken() string }); ok {
		return token.LeaseToken()
	}
	return ""
}

func (l yieldFaultLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	next, err := l.RenewableExecutionLease.Renew(ctx)
	if err != nil {
		return nil, err
	}
	return yieldFaultLease{RenewableExecutionLease: next, mode: l.mode}, nil
}

func (l yieldFaultLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	switch l.mode {
	case "rejected":
		return jobdb.ErrConflict
	case "lease-lost":
		return jobdb.ErrExecutionLeaseLost
	case "uncertain":
		return errors.New("transport response unavailable")
	case "committed-response-lost":
		if err := l.RenewableExecutionLease.Reschedule(ctx, req); err != nil {
			return err
		}
		return errors.New("transport response unavailable after commit")
	default:
		return l.RenewableExecutionLease.Reschedule(ctx, req)
	}
}

func TestExecutionYieldConfirmation(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, taskYield := range []bool{false, true} {
			for _, mode := range []string{"accepted", "rejected", "lease-lost", "uncertain", "committed-response-lost", "cancelled"} {
				t.Run(fmt.Sprintf("%s/task=%t/%s", backend, taskYield, mode), func(t *testing.T) {
					base := alternateRuntime(t, backend)
					rt := yieldFaultRuntime{WorkflowRuntime: base, mode: mode}
					observed := make(chan error, 1)
					var notifications atomic.Int32
					perform := func(ctx context.Context, yield func(context.Context, jobdb.RescheduleExecutionRequest) error) error {
						if mode == "cancelled" {
							var cancel context.CancelFunc
							ctx, cancel = context.WithCancel(ctx)
							cancel()
						}
						err := yield(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "receiver"}})
						wrapped := fmt.Errorf("handoff: %w", err)
						if wf.IsExecutionSuspended(wrapped) {
							notifications.Add(1)
						}
						observed <- wrapped
						return wrapped
					}
					task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
						return nil, perform(c.Context(), c.Yield)
					}}
					worker := wf.ExecutionWorker{JobType: "sender", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
						if taskYield {
							return c.DoTask(invocation("handoff"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
						}
						return nil, perform(c.Context(), c.Yield)
					}}
					key := submitExecutionTest(t, base, worker)
					out, runErr := runExecutionTest(t, rt, key, worker, task)
					var err error
					select {
					case err = <-observed:
					case <-time.After(5 * time.Second):
						t.Fatalf("Yield did not return to its caller: run outcome=%+v error=%v", out, runErr)
					}
					require.Error(t, err)
					require.Equal(t, mode == "accepted", wf.IsExecutionSuspended(err))
					require.Equal(t, mode == "accepted", wf.IsExecutionSuspended(errors.Join(errors.New("wrapper"), err)))
					if mode == "accepted" {
						require.NoError(t, runErr)
						require.Equal(t, wf.JobRunSuspended, out.Status)
						require.EqualValues(t, 1, notifications.Load())
					} else {
						require.Error(t, runErr)
						require.Zero(t, notifications.Load())
						if mode == "cancelled" {
							require.ErrorIs(t, err, context.Canceled)
						}
						if mode == "lease-lost" {
							require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
						}
					}
					for _, record := range executionRecords(t, base, key) {
						require.NotEqual(t, "result", record.Kind, "Yield must not complete a task or job")
					}
					lease, err := base.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "receiver", Routes: []jobdb.Route{{JobType: "receiver"}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					if mode == "accepted" || mode == "committed-response-lost" {
						require.NotNil(t, lease)
					} else {
						require.Nil(t, lease)
					}
				})
			}
		}
	}
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, jobdb.ErrExecutionLeaseLost, errors.New("execution suspended")} {
		require.False(t, wf.IsExecutionSuspended(err))
	}
}

func TestJoinRescheduleDeadlineAndReset(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, poll := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/poll=%t", backend, poll), func(t *testing.T) {
				ctx := context.Background()
				rt := alternateRuntime(t, backend)
				key := submitExecutionTest(t, rt, wf.ExecutionWorker{JobType: "parent"})
				child := submitExecutionTest(t, rt, wf.ExecutionWorker{JobType: "child"})
				route := jobdb.Route{JobType: "parent"}
				acquire := func() jobdb.ExecutionLease {
					t.Helper()
					if poll {
						leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "replacement", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
						require.NoError(t, err)
						if len(leases) == 0 {
							return nil
						}
						require.Len(t, leases, 1)
						return leases[0]
					}
					lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "replacement", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					return lease
				}
				lease := acquire()
				require.NotNil(t, lease)
				deadline := time.Now().Add(-time.Hour)
				req := jobdb.RescheduleExecutionRequest{NextRoute: route, WaitForJobIDs: []string{child.JobId}, WaitUntil: &deadline}
				require.NoError(t, lease.Reschedule(ctx, req))
				lease = acquire()
				require.NotNil(t, lease, "unfinished join must wake at its deadline")
				// A later wait without a deadline must not inherit the expired deadline.
				req.WaitUntil = nil
				require.NoError(t, lease.Reschedule(ctx, req))
				require.Nil(t, acquire())
				require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: child}))
				lease = acquire()
				require.NotNil(t, lease)
				// A time-only wait remains a not-before time.
				future := time.Now().Add(time.Hour)
				require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route, WaitUntil: &future}))
				require.Nil(t, acquire())
			})
		}
	}
}
