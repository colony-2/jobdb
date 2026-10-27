package workflow_test

import (
	"context"
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

type unfinishedObserver struct {
	starts  []wf.TaskStartEvent
	ends    []wf.TaskEndEvent
	jobEnds []wf.JobEndEvent
}

func (o *unfinishedObserver) OnJobStart(wf.JobStartEvent)     {}
func (o *unfinishedObserver) OnJobEnd(e wf.JobEndEvent)       { o.jobEnds = append(o.jobEnds, e) }
func (o *unfinishedObserver) OnTaskStart(e wf.TaskStartEvent) { o.starts = append(o.starts, e) }
func (o *unfinishedObserver) OnTaskEnd(e wf.TaskEndEvent)     { o.ends = append(o.ends, e) }

// Inspect from an independent engine with no registered task implementation.
// Barriers keep the durable frontier fixed while we compare history and state.
func TestExecutionUnfinishedInspection(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"first", "later", "retry", "external", "external-retry"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				entered, release := make(chan wf.TaskContext, 1), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				var builds, runs, priorBuilds, priorRuns atomic.Int32
				retry := mode == "retry" || mode == "external-retry"
				external := mode == "external" || mode == "external-retry"
				call := invocation("active")
				if retry {
					call.Policy.Retry.MaximumAttempts = 2
				}
				w := wf.ExecutionWorker{JobType: "unfinished", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if mode == "later" {
						prior := invocation("prior")
						prior.TaskType = "prior"
						var err error
						in, err = c.DoTask(prior, func(context.Context) (jobdb.TaskData, error) { priorBuilds.Add(1); return in, nil })
						if err != nil {
							return nil, err
						}
					}
					return c.DoScope("decorative", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoScope("policy", wf.ScopeOptions{TotalTimeout: time.Hour, Retry: jobdb.RetryPolicy{MaximumAttempts: 2}}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
							return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
								builds.Add(1)
								return jobdb.NewTaskDataOrPanic(map[string]any{"from": string(in.GetDataOrPanic()), "resolved": true}, jobdb.NewArtifactFromBytes("input.txt", []byte("input"))), nil
							})
						})
					})
				}}
				task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					if runs.Add(1) == 1 && retry {
						return jobdb.NewTaskDataOrPanic("prior diagnostic", jobdb.NewArtifactFromBytes("prior.txt", []byte("prior"))), errors.New("retry")
					}
					entered <- c
					select {
					case <-release:
					case <-c.Context().Done():
						return nil, c.Context().Err()
					}
					return jobdb.NewTaskDataOrPanic("finished", jobdb.NewArtifactFromBytes("output.txt", []byte("output"))), nil
				}}
				prior := executionTestTask{"prior", func(_ wf.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
					priorRuns.Add(1)
					return jobdb.NewTaskDataOrPanic("prior result"), nil
				}}
				key := submitExecutionTest(t, rt, w)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
				require.NoError(t, err)
				inspect := func(state wf.TaskState) wf.TaskStartEvent {
					t.Helper()
					before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
					require.NoError(t, err)
					jobBefore, err := rt.GetJob(ctx, key)
					require.NoError(t, err)
					o := &unfinishedObserver{}
					_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, Observer: o})
					var miss wf.ReplayCacheMissError
					require.ErrorAs(t, err, &miss)
					require.Equal(t, wf.ReplayCacheMissTaskResultMissing, miss.Reason)
					require.NotEmpty(t, o.starts)
					active := o.starts[len(o.starts)-1]
					require.True(t, active.Unfinished)
					require.Equal(t, "echo", active.TaskType)
					require.Equal(t, state, active.State)
					require.Equal(t, active.InvocationID, miss.InvocationID)
					require.Equal(t, active.AttemptNumber, miss.Attempt)
					require.Equal(t, state, miss.State)
					require.Equal(t, active.Input.GetDataOrPanic(), miss.Input.GetDataOrPanic())
					from := `"job"`
					if mode == "later" {
						from = `"prior result"`
					}
					require.JSONEq(t, fmt.Sprintf(`{"from":%q,"resolved":true}`, from), string(active.Input.GetDataOrPanic()))
					arts, err := active.Input.GetArtifacts()
					require.NoError(t, err)
					require.Len(t, arts, 1)
					require.Equal(t, "input.txt", arts[0].Name())
					for _, end := range o.ends {
						require.NotEqual(t, active.InvocationID, end.InvocationID)
					}
					require.Empty(t, o.jobEnds)
					after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
					require.NoError(t, err)
					require.Equal(t, before, after, "inspection must not write history or upload artifacts")
					jobAfter, err := rt.GetJob(ctx, key)
					require.NoError(t, err)
					require.Equal(t, jobBefore.Status, jobAfter.Status)
					require.Equal(t, jobBefore.ExecutionState, jobAfter.ExecutionState)
					return active
				}
				var queued wf.TaskStartEvent
				if external {
					out, err := runExecutionTest(t, rt, key, w)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunSuspended, out.Status)
					queued = inspect(wf.TaskStateReady)
					require.EqualValues(t, 1, builds.Load(), "stored wait input bypasses construction")
					require.Zero(t, runs.Load())
				}
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task, prior}, WorkerID: "live", LeaseDuration: time.Minute})
				require.NoError(t, err)
				type result struct {
					out wf.JobRunOutcome
					err error
				}
				done := make(chan result, 1)
				go func() { out, err := run.Run(nil); done <- result{out, err} }()
				var taskContext wf.TaskContext
				select {
				case taskContext = <-entered:
				case <-ctx.Done():
					t.Fatal("task did not reach barrier")
				}
				active := inspect(wf.TaskStateRunning)
				again := inspect(wf.TaskStateRunning)
				require.Equal(t, active.InvocationID, again.InvocationID)
				require.Equal(t, fmt.Sprintf("%s/attempt:%d", taskContext.InvocationID, taskContext.AttemptNumber), active.InvocationID)
				if external && !retry {
					require.Equal(t, queued.InvocationID, active.InvocationID)
				}
				if !external {
					require.True(t, active.At.IsZero(), "deferred start time is unknown")
				}
				wantRuns := int32(1)
				if retry {
					wantRuns = 2
				}
				require.Equal(t, wantRuns, runs.Load())
				wantBuilds := int32(3)
				if external || retry {
					wantBuilds = 1
				}
				require.Equal(t, wantBuilds, builds.Load())
				if mode == "later" {
					require.EqualValues(t, 1, priorBuilds.Load())
					require.EqualValues(t, 1, priorRuns.Load())
				}
				unblock()
				select {
				case result := <-done:
					require.NoError(t, result.err)
					require.Equal(t, wf.JobRunCompleted, result.out.Status)
				case <-ctx.Done():
					t.Fatal("task did not finish")
				}
				o := &unfinishedObserver{}
				_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, Observer: o})
				require.NoError(t, err)
				last := o.ends[len(o.ends)-1]
				require.Equal(t, active.InvocationID, last.InvocationID)
				require.Equal(t, active.AttemptNumber, last.AttemptNumber)
				require.JSONEq(t, `"finished"`, string(last.Output.GetDataOrPanic()))
				require.False(t, o.starts[len(o.starts)-1].Unfinished)
				require.Equal(t, wantBuilds, builds.Load(), "completed replay must skip builders")
				require.Equal(t, wantRuns, runs.Load())
			})
		}
	}
}

func TestExecutionUnfinishedBuilderErrorsAreReadOnly(t *testing.T) {
	rt := alternateRuntime(t, "sqlite")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var reconstruct func(wf.ExecutionContext, context.Context) (jobdb.TaskData, error)
	w := wf.ExecutionWorker{JobType: "builder-guards", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoTask(invocation("work"), func(ctx context.Context) (jobdb.TaskData, error) {
			if reconstruct != nil {
				return reconstruct(c, ctx)
			}
			return in, nil
		})
	}}
	task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		close(entered)
		select {
		case <-release:
		case <-c.Context().Done():
			return nil, c.Context().Err()
		}
		return in, nil
	}}
	key := submitExecutionTest(t, rt, w)
	run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "live", LeaseDuration: time.Minute})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, err := run.Run(nil); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("task did not start")
	}
	engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
	require.NoError(t, err)
	before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	sentinel := errors.New("cannot reconstruct")
	for _, tc := range []struct {
		name  string
		build func(wf.ExecutionContext, context.Context) (jobdb.TaskData, error)
		want  error
	}{
		{"failure", func(wf.ExecutionContext, context.Context) (jobdb.TaskData, error) { return nil, sentinel }, sentinel},
		{"submit", func(c wf.ExecutionContext, ctx context.Context) (jobdb.TaskData, error) {
			_, err := c.SubmitJob(ctx, jobdb.SubmitJob{})
			return nil, err
		}, wf.ErrReplayShouldNeverMutate},
		{"configuration", func(c wf.ExecutionContext, ctx context.Context) (jobdb.TaskData, error) {
			_, err := c.GetConfiguration(ctx)
			return nil, err
		}, wf.ErrReplayShouldNeverMutate},
		{"secrets", func(c wf.ExecutionContext, ctx context.Context) (jobdb.TaskData, error) {
			_, err := c.ResolveSecrets(ctx, []string{"secret"})
			return nil, err
		}, wf.ErrReplayShouldNeverMutate},
		{"yield", func(c wf.ExecutionContext, ctx context.Context) (jobdb.TaskData, error) {
			return nil, c.Yield(ctx, wf.YieldRequest{})
		}, wf.ErrReplayShouldNeverMutate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reconstruct = tc.build
			o := &unfinishedObserver{}
			_, err := engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, Observer: o})
			require.ErrorIs(t, err, tc.want)
			require.Empty(t, o.starts)
			require.Empty(t, o.ends)
			require.Empty(t, o.jobEnds)
		})
	}
	t.Run("caller cancellation", func(t *testing.T) {
		inspectionCtx, cancelInspection := context.WithCancel(ctx)
		exited := make(chan struct{})
		reconstruct = func(_ wf.ExecutionContext, ctx context.Context) (jobdb.TaskData, error) {
			defer close(exited)
			cancelInspection()
			<-ctx.Done()
			return nil, ctx.Err()
		}
		_, err := engine.ReplayJobRun(inspectionCtx, wf.ReplayRunRequest{JobKey: key})
		require.ErrorIs(t, err, context.Canceled)
		<-exited
	})
	after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	require.Equal(t, before, after)
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("task did not finish")
	}
}

func TestExecutionUnfinishedWaitingAndCancellation(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, cancelled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancelled=%v", backend, cancelled), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var runs atomic.Int32
				w := wf.ExecutionWorker{JobType: "waiting-inspection", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(invocation("wait"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				}}
				task := executionTestTask{"echo", func(c wf.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
					runs.Add(1)
					return nil, c.AwaitDuration(jobdb.Duration(time.Hour))
				}}
				key := submitExecutionTest(t, rt, w)
				ctx := context.Background()
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "waiting", LeaseDuration: time.Minute, AwaitThreshold: time.Millisecond})
				require.NoError(t, err)
				out, err := run.Run(nil)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				state := wf.TaskStateWaiting
				if cancelled {
					require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key}))
					state = wf.TaskStateCancelled
				}
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
				require.NoError(t, err)
				o := &unfinishedObserver{}
				_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, Observer: o})
				var miss wf.ReplayCacheMissError
				require.ErrorAs(t, err, &miss)
				require.Equal(t, state, miss.State)
				require.Len(t, o.starts, 1)
				require.Equal(t, state, o.starts[0].State)
				require.JSONEq(t, `"job"`, string(o.starts[0].Input.GetDataOrPanic()))
				require.Empty(t, o.ends)
				require.EqualValues(t, 1, runs.Load())
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		}
	}
}
