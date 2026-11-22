package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffReplay(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			t.Run("B11_original_deadline", func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var runs atomic.Int32
				call := invocation("timeout")
				call.Policy.InvocationTimeout = jobdb.AsDuration(3 * time.Second)
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					runs.Add(1)
					return nil, wf.EAGAINWait(c, wf.WithInitialWait(30*time.Second), wf.WithBackoffJitter(0))
				}}
				w := wf.ExecutionWorker{JobType: "resource-timeout", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
				}}
				key := submitExecutionTest(t, rt, w)
				_, err := runResourceExecutionTest(t, rt, key, w, task)
				require.NoError(t, err)
				var deadline time.Time
				for _, r := range executionRecords(t, rt, key) {
					if r.Kind == "resource_checkpoint" {
						deadline = r.Deadline
					}
				}
				require.False(t, deadline.IsZero())
				time.Sleep(max(time.Until(deadline)+10*time.Millisecond, 0))
				out, err := runResourceExecutionTest(t, rt, key, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunFailed, out.Status)
				require.EqualValues(t, 1, runs.Load())
				timeout := false
				for _, r := range executionRecords(t, rt, key) {
					if r.ErrorKind == "Timeout" {
						timeout = true
					}
				}
				require.True(t, timeout)
			})
			t.Run("B02a_current_recipe_attempt", func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var runs atomic.Int32
				call := invocation("attempt")
				call.Policy.Retry = wf.RetryPolicy{MaximumAttempts: 2, InitialInterval: jobdb.Duration(time.Millisecond), MaximumInterval: jobdb.Duration(time.Millisecond), BackoffCoefficient: 1}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					switch runs.Add(1) {
					case 1:
						return nil, errors.New("ordinary retry")
					case 2:
						require.Equal(t, 2, c.AttemptNumber)
						return nil, wf.EAGAINWait(c, wf.WithInitialWait(20*time.Millisecond), wf.WithMaxWait(20*time.Millisecond), wf.WithBackoffJitter(0))
					default:
						require.Equal(t, 2, c.AttemptNumber)
						return in, nil
					}
				}}
				w := wf.ExecutionWorker{JobType: "resource-attempt", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
				}}
				key := submitExecutionTest(t, rt, w)
				// No task worker on the first activation creates a genuinely dispatched task.
				_, err := runResourceExecutionTest(t, rt, key, w)
				require.NoError(t, err)
				_, err = runResourceExecutionTest(t, rt, key, w, task)
				require.NoError(t, err)
				records := executionRecords(t, rt, key)
				found := false
				for _, r := range records {
					if r.Kind == "resource_checkpoint" {
						found = true
						require.Equal(t, 2, r.Resource.Attempt)
						require.Contains(t, r.ID, "/attempt:2")
					}
				}
				require.True(t, found)
				time.Sleep(25 * time.Millisecond)
				out, err := runResourceExecutionTest(t, rt, key, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
			})
		})
	}
	t.Run("B08a_reopen_after_wake_claim", func(t *testing.T) {
		ctx := celltest.Context(context.Background())
		cfg := sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "crash.db")}
		rt, err := sqlite.NewFromConfig(ctx, cfg)
		require.NoError(t, err)
		defer func() { require.NoError(t, rt.Close(ctx)) }()
		var runs, builds atomic.Int32
		task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
			if runs.Add(1) == 1 {
				return nil, wf.EAGAINWait(c, wf.WithInitialWait(20*time.Millisecond), wf.WithMaxWait(20*time.Millisecond), wf.WithBackoffJitter(0))
			}
			return in, nil
		}}
		w := wf.ExecutionWorker{JobType: "resource-crash", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			return c.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
		}}
		key := submitExecutionTest(t, rt, w)
		_, err = runResourceExecutionTest(t, rt, key, w, task)
		require.NoError(t, err)
		time.Sleep(25 * time.Millisecond)
		lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "crash", Routes: []jobdb.Route{{JobType: w.Name()}}, LeaseDuration: 20 * time.Millisecond})
		require.NoError(t, err)
		require.NotNil(t, lease)
		selected := lease.ExecutionState().ResourceResume
		require.NotNil(t, selected)
		require.NoError(t, rt.Close(ctx))
		rt, err = sqlite.NewFromConfig(ctx, cfg)
		require.NoError(t, err)
		time.Sleep(25 * time.Millisecond)
		out, err := runResourceExecutionTest(t, rt, key, w, task)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunCompleted, out.Status)
		require.EqualValues(t, 1, builds.Load())
		require.EqualValues(t, 2, runs.Load())
	})
}

type resourceAckLease struct {
	jobdb.RenewableExecutionLease
	commit bool
}

func (l resourceAckLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	next, err := l.RenewableExecutionLease.Renew(ctx)
	l.RenewableExecutionLease = next
	return l, err
}
func (l resourceAckLease) RescheduleResourceBackoff(ctx context.Context, r jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	if l.commit {
		if _, err := l.RenewableExecutionLease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, r); err != nil {
			return jobdb.ResourceBackoffReceipt{}, err
		}
	}
	return jobdb.ResourceBackoffReceipt{}, errors.New("acknowledgment unavailable")
}

type resourcePoisonData struct {
	jobdb.TaskData
	reads *atomic.Int32
}

func (d resourcePoisonData) GetData() (json.RawMessage, error) {
	d.reads.Add(1)
	return nil, errors.New("output must not serialize")
}
func (d resourcePoisonData) GetArtifacts() ([]jobdb.Artifact, error) {
	d.reads.Add(1)
	return nil, errors.New("artifacts must not prepare")
}

func TestResourceBackoffAcknowledgment(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "B01a_B05b_prepared_unselected", true: "B14a_unresolved_committed"}[commit], func(t *testing.T) {
			rt := alternateRuntime(t, "sqlite")
			ctx := celltest.Context(context.Background())
			var reads, prior, runs, catches atomic.Int32
			call := invocation("resource")
			call.Policy.Retry.MaximumAttempts = 3
			call.Catch = []wf.CatchRule{{ID: "never", Match: func(error) bool { catches.Add(1); return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled}, nil
			}}}
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				if runs.Add(1) == 1 {
					return resourcePoisonData{reads: &reads}, wf.EAGAINWait(c, wf.WithInitialWait(20*time.Millisecond), wf.WithMaxWait(20*time.Millisecond), wf.WithBackoffJitter(0))
				}
				return in, nil
			}}
			done := executionTestTask{"prior", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { prior.Add(1); return in, nil }}
			w := wf.ExecutionWorker{JobType: "resource-ack", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				p := invocation("prior")
				p.TaskType = "prior"
				if _, err := c.DoTask(p, func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
					return nil, err
				}
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "ack", Routes: []jobdb.Route{{JobType: w.Name()}}, LeaseDuration: 2 * time.Second})
			require.NoError(t, err)
			run, err := wf.GetJobForRunWithLease(ctx, rt, resourceAckLease{lease.(jobdb.RenewableExecutionLease), commit}, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task, done}})
			require.NoError(t, err)
			_, err = run.Run(nil)
			require.True(t, jobdb.IsExecutionInfrastructureError(err), "%v", err)
			require.Zero(t, reads.Load())
			require.Zero(t, catches.Load())
			for _, r := range executionRecords(t, rt, key) {
				if r.Kind == "result" {
					require.NotContains(t, r.ID, "resource")
				}
			}
			info, err := rt.GetJob(ctx, key)
			require.NoError(t, err)
			require.Equal(t, commit, info.ExecutionState.ResourceResume != nil)
			if commit {
				time.Sleep(25 * time.Millisecond)
			} else {
				time.Sleep(2100 * time.Millisecond)
			}
			out, err := runResourceExecutionTest(t, rt, key, w, task, done)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.EqualValues(t, 1, prior.Load())
			require.EqualValues(t, 2, runs.Load())
		})
	}
}

func TestResourceBackoffUnjoinedTaskB01e(t *testing.T) {
	rt := alternateRuntime(t, "toy")
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	exited := make(chan struct{})
	slow := executionTestTask{"slow", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		defer close(exited)
		<-release
		return in, nil
	}}
	resource := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { return nil, wf.EAGAINWait(c) }}
	first := invocation("slow")
	first.TaskType = "slow"
	first.Policy.InvocationTimeout = jobdb.AsDuration(10 * time.Millisecond)
	first.Catch = []wf.CatchRule{{ID: "timeout", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
		return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("handled")}, nil
	}}}
	w := wf.ExecutionWorker{JobType: "resource-quiescence", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		if _, err := c.DoTask(first, func(context.Context) (jobdb.TaskData, error) { return in, nil }); err != nil {
			return nil, err
		}
		return c.DoTask(invocation("resource"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
	}}
	key := submitExecutionTest(t, rt, w)
	_, err := runResourceExecutionTest(t, rt, key, w, slow, resource)
	require.True(t, jobdb.IsExecutionInfrastructureError(err), "%v", err)
	info, err := rt.GetJob(celltest.Context(context.Background()), key)
	require.NoError(t, err)
	require.Nil(t, info.ExecutionState.ResourceResume)
	for _, r := range executionRecords(t, rt, key) {
		require.NotEqual(t, "resource_checkpoint", r.Kind)
	}
	unblock()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("task did not stop")
	}
}

func TestResourceBackoffAncestorIdentityB02a(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	var calls atomic.Int32
	var identities []string
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		identities = append(identities, c.InvocationID)
		if calls.Add(1)%2 == 1 {
			return nil, wf.EAGAINWait(c, wf.WithInitialWait(20*time.Millisecond), wf.WithMaxWait(20*time.Millisecond), wf.WithBackoffJitter(0))
		}
		return in, nil
	}}
	var successes atomic.Int32
	w := wf.ExecutionWorker{JobType: "resource-ancestor", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("parent", wf.ScopeOptions{Retry: wf.RetryPolicy{MaximumAttempts: 2, InitialInterval: jobdb.Duration(time.Millisecond), MaximumInterval: jobdb.Duration(time.Millisecond), BackoffCoefficient: 1}}, func(c wf.ExecutionContext) (jobdb.JobData, error) {
			out, err := c.DoTask(invocation("same-name"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
			if err != nil {
				return nil, err
			}
			if successes.Add(1) == 1 {
				return nil, errors.New("retry parent")
			}
			return out, nil
		})
	}}
	key := submitExecutionTest(t, rt, w)
	for range 2 {
		_, err := runResourceExecutionTest(t, rt, key, w, task)
		require.NoError(t, err)
		time.Sleep(25 * time.Millisecond)
	}
	out, err := runResourceExecutionTest(t, rt, key, w, task)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status)
	require.Len(t, identities, 4)
	require.Equal(t, identities[0], identities[1])
	require.Equal(t, identities[2], identities[3])
	require.NotEqual(t, identities[0], identities[2])
	require.True(t, strings.Contains(identities[0], `scope:"parent"#1/attempt:1`))
	require.True(t, strings.Contains(identities[2], `scope:"parent"#1/attempt:2`))
}

func TestResourceBackoffSelectedImplementationB02b(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			ctx := celltest.Context(context.Background())
			var builds, fallbackRuns, primaryRuns atomic.Int32
			call := invocation("selected")
			call.Alternate = &wf.TaskAlternateAfter{TaskType: "fallback", After: 0}
			call.Policy.InvocationTimeout = jobdb.AsDuration(time.Minute)
			call.Policy.TotalTimeout = jobdb.AsDuration(2 * time.Minute)
			w := wf.ExecutionWorker{JobType: "resource-selected", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
					builds.Add(1)
					return jobdb.NewTaskDataOrPanic("durable", jobdb.NewArtifactFromBytes("input.txt", []byte("original artifact"))), nil
				})
			}}
			primary := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { primaryRuns.Add(1); return in, nil }}
			fallback := executionTestTask{"fallback", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				raw, err := in.GetData()
				require.NoError(t, err)
				require.JSONEq(t, `"durable"`, string(raw))
				arts, err := in.GetArtifacts()
				require.NoError(t, err)
				require.Len(t, arts, 1)
				data, err := arts[0].Bytes(ctx)
				require.NoError(t, err)
				require.Equal(t, "original artifact", string(data))
				require.JSONEq(t, `{"cursor":9007199254740993}`, string(c.ClientPayload()))
				require.EqualValues(t, 1, c.ClientPayloadRevision())
				if fallbackRuns.Add(1) == 1 {
					return nil, wf.EAGAINWait(c, wf.WithInitialWait(20*time.Millisecond), wf.WithMaxWait(20*time.Millisecond), wf.WithBackoffJitter(0))
				}
				return in, nil
			}}
			h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("job"), ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"cursor":9007199254740993}`)}}})
			require.NoError(t, err)
			_, err = runResourceExecutionTest(t, rt, h.JobKey, w, fallback)
			require.NoError(t, err)
			wait := executionWait(t, rt, h.JobKey)
			_, err = runResourceExecutionTest(t, rt, h.JobKey, w, fallback)
			require.NoError(t, err)
			for _, rec := range executionRecords(t, rt, h.JobKey) {
				if rec.Kind == "resource_checkpoint" {
					require.Equal(t, "fallback", rec.Resource.ExecutionTaskType)
					require.Equal(t, wait.Deadline, rec.Deadline)
					require.Equal(t, wait.TotalDeadline, rec.TotalDeadline)
				}
			}
			time.Sleep(25 * time.Millisecond)
			out, err := runResourceExecutionTest(t, rt, h.JobKey, w, primary, fallback)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.Zero(t, primaryRuns.Load())
			require.EqualValues(t, 2, fallbackRuns.Load())
			require.EqualValues(t, 1, builds.Load())
		})
	}
}
