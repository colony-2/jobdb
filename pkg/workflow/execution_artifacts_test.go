package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func readExecutionArtifact(t *testing.T, rt jobdb.WorkflowRuntime, tenant string, key jobdb.ArtifactKey) string {
	t.Helper()
	artifact, err := rt.OpenArtifact(context.Background(), jobdb.ArtifactRef{JobKey: jobdb.JobKey{TenantId: tenant, JobId: key.JobId}, Ordinal: key.TaskOrdinal, Name: key.Name})
	require.NoError(t, err)
	r, err := artifact.Open()
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(data)
}

func TestExecutionProducerArtifactReferences(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"local", "recover", "local-wait", "external-yield", "external-requeue"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				base := alternateRuntime(t, backend)
				var runs atomic.Int32
				var identities []string
				var attempts []int
				yields := 0
				if strings.HasPrefix(mode, "external-") {
					yields = 1
				}
				if mode == "external-requeue" {
					yields = 2
				}
				w := wf.ExecutionWorker{JobType: "producer-references", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(invocation("produce"), func(context.Context) (jobdb.TaskData, error) {
						return jobdb.NewTaskDataOrPanic("input", jobdb.NewArtifactFromBytes("result.txt", []byte("input bytes"))), nil
					})
				}}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					identities = append(identities, c.InvocationID)
					attempts = append(attempts, c.AttemptNumber)
					n := runs.Add(1)
					if n <= int32(yields) {
						return nil, c.Yield(c.Context(), jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: w.Name()}})
					}
					if n == 1 && mode == "local-wait" {
						if err := c.AwaitDuration(jobdb.Duration(200 * time.Millisecond)); err != nil {
							return nil, err
						}
					}
					if err := c.AwaitDuration(jobdb.Duration(time.Millisecond)); err != nil {
						return nil, err
					}
					key := jobdb.ArtifactKey{JobId: c.JobKey.JobId, TaskOrdinal: c.Step, Name: "result.txt", SizeBytes: 4}
					return jobdb.NewTaskDataOrPanic(map[string]any{"reference": key},
						jobdb.NewArtifactFromBytes("result.txt", []byte("data")),
						jobdb.NewArtifactFromBytes("execution-input-0-result.txt", []byte("output collision"))), nil
				}}
				key := submitExecutionTest(t, base, w)
				if yields > 0 {
					out, err := runExecutionTest(t, base, key, w)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunSuspended, out.Status)
					for i := 0; i < yields; i++ {
						out, err = runExecutionTest(t, base, key, w, task)
						require.NoError(t, err)
						require.Equal(t, wf.JobRunSuspended, out.Status)
					}
					if mode == "external-requeue" {
						out, err = runExecutionTest(t, base, key, w)
						require.NoError(t, err)
						require.Equal(t, wf.JobRunSuspended, out.Status)
					}
				}
				if mode == "local-wait" {
					run, err := wf.GetJobForRun(context.Background(), base, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "waiting", LeaseDuration: time.Second, AwaitThreshold: time.Millisecond})
					require.NoError(t, err)
					out, err := run.Run(nil)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunSuspended, out.Status)
				}
				if mode == "recover" {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					fault := &executionFaultRuntime{WorkflowRuntime: base, cancel: cancel, fail: func(r jobdb.ExecutionRecord) bool { return r.Kind == "result" && strings.Contains(r.ID, "task:") }}
					run, err := wf.GetJobForRun(ctx, fault, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "crashed", LeaseDuration: 100 * time.Millisecond})
					require.NoError(t, err)
					_, err = run.Run(nil)
					require.Error(t, err)
					require.True(t, fault.fired.Load())
				}
				var out wf.JobRunOutcome
				require.Eventually(t, func() bool {
					var err error
					out, err = runExecutionTest(t, base, key, w, task)
					require.NoError(t, err)
					return out.Status == wf.JobRunCompleted || out.Status == wf.JobRunFailed
				}, 3*time.Second, 25*time.Millisecond)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				info, err := base.GetJob(context.Background(), key)
				require.NoError(t, err)
				var live struct {
					Reference jobdb.ArtifactKey `json:"reference"`
				}
				require.NoError(t, json.Unmarshal(info.Data.GetDataOrPanic(), &live))
				require.Equal(t, "data", readExecutionArtifact(t, base, key.TenantId, live.Reference))
				before := executionRecords(t, base, key)
				value, err := replayExecutionTest(t, base, key, w)
				require.NoError(t, err)
				var result struct {
					Reference jobdb.ArtifactKey `json:"reference"`
				}
				require.NoError(t, json.Unmarshal(value.GetDataOrPanic(), &result))
				require.Equal(t, "data", readExecutionArtifact(t, base, key.TenantId, result.Reference))
				for _, record := range before {
					if record.Kind == "result" && strings.Contains(record.ID, "task:") {
						require.Equal(t, result.Reference, *record.Output.Artifacts[0].Key)
						require.Len(t, record.Input.Artifacts, 1)
						require.Equal(t, "input bytes", readExecutionArtifact(t, base, key.TenantId, *record.Input.Artifacts[0].Key))
						require.Equal(t, "output collision", readExecutionArtifact(t, base, key.TenantId, *record.Output.Artifacts[1].Key))
					}
				}
				require.Equal(t, before, executionRecords(t, base, key))
				wantRuns := int32(1)
				if mode != "local" {
					wantRuns = 2
				}
				if mode == "external-requeue" {
					wantRuns = 3
				}
				require.Equal(t, wantRuns, runs.Load())
				require.Len(t, identities, int(wantRuns))
				require.NotEmpty(t, identities[0])
				for i := range identities {
					require.Equal(t, identities[0], identities[i], "unfinished work keeps the logical invocation")
					require.Equal(t, 1, attempts[i])
				}
			})
		}
	}
}

type executionArtifactObserver struct{ tasks []wf.TaskEndEvent }

func (*executionArtifactObserver) OnJobStart(wf.JobStartEvent)   {}
func (*executionArtifactObserver) OnTaskStart(wf.TaskStartEvent) {}
func (o *executionArtifactObserver) OnTaskEnd(e wf.TaskEndEvent) { o.tasks = append(o.tasks, e) }
func (*executionArtifactObserver) OnJobEnd(wf.JobEndEvent)       {}

func TestExecutionFailureDiagnostics(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retry=%v", backend, retry), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				diagnosticPath := filepath.Join(t.TempDir(), "stderr.txt")
				var runs atomic.Int32
				call := invocation("diagnostic")
				if retry {
					call.Policy.Retry.MaximumAttempts = 2
				}
				w := wf.ExecutionWorker{JobType: "failure-diagnostics", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
				}}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					if runs.Add(1) > 1 {
						return jobdb.NewTaskDataOrPanic("success"), nil
					}
					if err := os.WriteFile(diagnosticPath, []byte("sanitized stderr"), 0600); err != nil {
						return nil, err
					}
					artifact, err := jobdb.NewArtifactFromFile("stderr.txt", diagnosticPath)
					if err != nil {
						return nil, err
					}
					return jobdb.NewTaskDataOrPanic(map[string]string{"diagnostic": "failed"}, artifact), jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "application failed"}}
				}}
				key := submitExecutionTest(t, rt, w)
				out, err := runExecutionTest(t, rt, key, w, task)
				require.NoError(t, err)
				_, err = os.Stat(diagnosticPath)
				require.True(t, os.IsNotExist(err), "failed output snapshots must clean temporary files")
				if retry {
					require.Equal(t, wf.JobRunCompleted, out.Status)
				} else {
					require.Equal(t, wf.JobRunFailed, out.Status)
				}
				for _, record := range executionRecords(t, rt, key) {
					if record.Kind == "result" && strings.Contains(record.ID, "task:") && record.ErrorKind != "" {
						require.NotNil(t, record.Output, "failed task discarded diagnostics")
						require.JSONEq(t, `{"diagnostic":"failed"}`, string(record.Output.Data))
						require.Len(t, record.Output.Artifacts, 1)
						require.Equal(t, "sanitized stderr", readExecutionArtifact(t, rt, key.TenantId, *record.Output.Artifacts[0].Key))
					}
				}
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
				require.NoError(t, err)
				observer := &executionArtifactObserver{}
				value, err := engine.ReplayJobRun(context.Background(), wf.ReplayRunRequest{JobKey: key, Observer: observer})
				if retry {
					require.NoError(t, err)
				} else {
					var app jobdb.AppError
					require.ErrorAs(t, err, &app)
					require.NotNil(t, value)
					require.JSONEq(t, `{"diagnostic":"failed"}`, string(value.GetDataOrPanic()))
				}
				require.NotEmpty(t, observer.tasks)
				first := observer.tasks[0]
				require.Error(t, first.Err)
				require.NotNil(t, first.Output)
				require.JSONEq(t, `{"diagnostic":"failed"}`, string(first.Output.GetDataOrPanic()))
				artifacts, err := first.Output.GetArtifacts()
				require.NoError(t, err)
				require.Len(t, artifacts, 1)
				data, err := artifacts[0].Bytes(context.Background())
				require.NoError(t, err)
				require.Equal(t, "sanitized stderr", string(data))
				wantRuns := int32(1)
				if retry {
					wantRuns = 2
				}
				require.Equal(t, wantRuns, runs.Load())
			})
		}
	}
}

func TestExecutionTaskInvocationIdentity(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, retryOwner := range []string{"task", "scope", "job"} {
			t.Run(backend+"/"+retryOwner, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var identities []string
				var attempts []int
				call := invocation("same-site")
				if retryOwner == "task" {
					call.Policy.Retry.MaximumAttempts = 2
				}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					identities = append(identities, c.InvocationID)
					attempts = append(attempts, c.AttemptNumber)
					if len(identities) == 1 {
						return nil, fmt.Errorf("retry the %s", retryOwner)
					}
					return in, nil
				}}
				w := wf.ExecutionWorker{JobType: "invocation-identity", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					body := func(c wf.ExecutionContext) (jobdb.JobData, error) {
						return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
					}
					if retryOwner == "scope" {
						return c.DoScope("block", wf.ScopeOptions{Retry: jobdb.RetryPolicy{MaximumAttempts: 2}}, body)
					}
					return body(c)
				}}
				policy := jobdb.DefaultRunPolicy()
				if retryOwner == "job" {
					policy.Retry.MaximumAttempts = 2
				}
				h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("input"), RunPolicy: policy}})
				require.NoError(t, err)
				out, err := runExecutionTest(t, rt, h.JobKey, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				require.Len(t, identities, 2)
				require.NotEmpty(t, identities[0])
				if retryOwner == "task" {
					require.Equal(t, identities[0], identities[1])
					require.Equal(t, []int{1, 2}, attempts)
				} else {
					require.NotEqual(t, identities[0], identities[1])
					require.Equal(t, []int{1, 1}, attempts)
				}
				_, err = replayExecutionTest(t, rt, h.JobKey, w)
				require.NoError(t, err)
				require.Len(t, identities, 2)
			})
		}
	}
}
