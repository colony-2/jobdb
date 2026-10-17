package workflow_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Verify omission and explicit declarations through storage and HTTP, including
// the durable deadline on an external task rather than just the policy struct.
func TestExecutionTimeoutPolicyPersistence(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"omitted", "default", "attempt-only", "total-only"} {
			t.Run(fmt.Sprintf("%s/%s", backend, mode), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				policy := jobdb.DefaultRunPolicy()
				switch mode {
				case "omitted":
					policy = jobdb.RunPolicy{}
				case "attempt-only":
					policy.InvocationTimeout = jobdb.AsDuration(time.Hour)
				case "total-only":
					policy.TotalTimeout = jobdb.AsDuration(2 * time.Hour)
				}
				call := invocation("external")
				if mode == "default" {
					call.Policy = wf.DefaultRunPolicy()
				}
				worker := wf.ExecutionWorker{JobType: "timeout-policy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoScope("nested", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
					})
				}}
				h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("input"), RunPolicy: policy}})
				require.NoError(t, err)
				info, err := rt.GetJob(context.Background(), h.JobKey)
				require.NoError(t, err)
				stored := info.ExecutionState.RunPolicy
				if mode == "attempt-only" {
					require.Equal(t, jobdb.AsDuration(time.Hour), stored.InvocationTimeout)
				} else {
					require.Nil(t, stored.InvocationTimeout)
				}
				if mode == "total-only" {
					require.Equal(t, jobdb.AsDuration(2*time.Hour), stored.TotalTimeout)
				} else {
					require.Nil(t, stored.TotalTimeout)
				}
				out, err := runExecutionTest(t, rt, h.JobKey, worker)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				records := executionRecords(t, rt, h.JobKey)
				if mode == "omitted" || mode == "default" {
					for _, rec := range records {
						require.True(t, rec.Deadline.IsZero(), "unexpected attempt deadline: %+v", rec)
						require.True(t, rec.TotalDeadline.IsZero(), "unexpected total deadline: %+v", rec)
					}
				} else {
					var rootAttempt jobdb.ExecutionRecord
					for _, rec := range records {
						if rec.Kind == "attempt" && rec.ID == `/scope:"$job"#1/attempt:1` {
							rootAttempt = rec
						}
					}
					require.False(t, rootAttempt.Deadline.IsZero())
					if mode == "attempt-only" {
						require.True(t, rootAttempt.TotalDeadline.IsZero())
					} else {
						require.Equal(t, rootAttempt.TotalDeadline, rootAttempt.Deadline)
					}
					require.Equal(t, rootAttempt.Deadline, executionWait(t, rt, h.JobKey).Deadline, "explicit parent budget must still bound the external wait")
				}
			})
		}
	}
}

type timeoutPolicyLegacyJob struct{ policy jobdb.RunPolicy }

func (timeoutPolicyLegacyJob) Name() string { return "timeout-policy-legacy" }
func (w timeoutPolicyLegacyJob) Run(c wf.JobContext, in jobdb.JobData) (jobdb.JobData, error) {
	return c.DoTask(w.policy, "echo", in)
}

func TestDefaultPoliciesPermitLongRunningWork(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		for _, limit := range []string{"none", "job-attempt", "job-total", "task-attempt", "task-total"} {
			t.Run(fmt.Sprintf("versioned=%t/%s", versioned, limit), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					rt := alternateRuntime(t, "toy")
					policy, taskPolicy := jobdb.DefaultRunPolicy(), wf.DefaultRunPolicy()
					policy.Retry.MaximumAttempts = 1
					taskPolicy.Retry.MaximumAttempts = 1
					switch limit {
					case "job-attempt":
						policy.InvocationTimeout = jobdb.AsDuration(5 * time.Minute)
					case "job-total":
						policy.TotalTimeout = jobdb.AsDuration(5 * time.Minute)
					case "task-attempt":
						taskPolicy.InvocationTimeout = jobdb.AsDuration(5 * time.Minute)
					case "task-total":
						taskPolicy.TotalTimeout = jobdb.AsDuration(5 * time.Minute)
					}
					var runs atomic.Int32
					exited := make(chan struct{})
					defer func() {
						if runs.Load() > 0 {
							select {
							case <-exited:
							case <-time.After(2 * time.Hour): // Virtual time, including assertion-failure cleanup.
								t.Error("long-running task did not exit")
							}
						}
					}()
					task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
						defer close(exited)
						runs.Add(1)
						time.Sleep(time.Hour)
						return in, nil
					}}
					var worker wf.JobWorker = timeoutPolicyLegacyJob{taskPolicy}
					if versioned {
						call := invocation("long-task")
						call.Policy = taskPolicy
						worker = wf.ExecutionWorker{JobType: "timeout-policy-execution", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
							return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
						}}
					}
					h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("finished"), RunPolicy: policy}})
					require.NoError(t, err)
					run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: worker, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "long-running-test", LeaseDuration: 24 * time.Hour})
					require.NoError(t, err)
					out, err := run.Run(nil)
					require.NoError(t, err)
					if limit == "none" {
						require.Equal(t, wf.JobRunCompleted, out.Status, "job error: %v", out.JobError)
					} else {
						require.Equal(t, wf.JobRunFailed, out.Status)
						var timeout jobdb.TimeoutError
						require.ErrorAs(t, out.JobError, &timeout)
					}
					require.EqualValues(t, 1, runs.Load())
					engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(worker, task).BuildEngine()
					require.NoError(t, err)
					value, err := engine.ReplayJobRun(context.Background(), wf.ReplayRunRequest{JobKey: h.JobKey})
					if limit == "none" {
						require.NoError(t, err)
						require.JSONEq(t, `"finished"`, string(value.GetDataOrPanic()))
					} else {
						var timeout jobdb.TimeoutError
						require.ErrorAs(t, err, &timeout)
					}
					require.EqualValues(t, 1, runs.Load(), "replay must not execute the worker")
				})
			})
		}
	}
}
