package workflow

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

type infrastructureData struct {
	TaskData
	dataErr, artifactsErr error
	artifacts             []Artifact
}

func (d infrastructureData) GetData() (Data, error) {
	if d.dataErr != nil {
		return nil, d.dataErr
	}
	return d.TaskData.GetData()
}
func (d infrastructureData) GetArtifacts() ([]Artifact, error) { return d.artifacts, d.artifactsErr }

type infrastructureRead struct{ err error }

func (r infrastructureRead) Read([]byte) (int, error) { return 0, r.err }
func (r infrastructureRead) Close() error             { return nil }

func infrastructureRunner(t *testing.T, execute func(ExecutionContext, JobData) (JobData, error)) (*workerRunner, *fakeExecutionLease, *runnerTestRuntime) {
	t.Helper()
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "job"}
	worker := ExecutionWorker{JobType: "infra", Execute: execute}
	seedJobStartForTest(t, rt, key, worker.Name(), jobdb.NewTaskDataOrPanic("input"), RunPolicy{})
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: worker.Name()}}
	return newWorkerRunner(rt, mustWorkSetForRunnerTest(t, worker), lease, workerRunnerOptions{}), lease, rt
}
func TestInfrastructureIgnoredDeferredFailuresWinOverSuccess(t *testing.T) {
	for _, mode := range []string{"data", "artifacts", "raw-data", "raw-artifacts", "open", "read", "eof"} {
		t.Run(mode, func(t *testing.T) {
			source := errors.New("persisted source unavailable")
			other := JobKey{TenantId: "tenant", JobId: "prerequisite"}
			classified := jobdb.ClassifyExecutionInfrastructure(source, jobdb.ExecutionInfrastructureDurableDecode, "decode_result", &other, nil)
			var runner *workerRunner
			var atObservation int
			runner, lease, rt := infrastructureRunner(t, func(c ExecutionContext, in JobData) (JobData, error) {
				d := infrastructureData{TaskData: in}
				if mode == "data" {
					d.dataErr = classified
				}
				if mode == "raw-data" {
					d.dataErr = source
				}
				if mode == "artifacts" {
					d.artifactsErr = classified
				}
				if mode == "raw-artifacts" {
					d.artifactsErr = source
				}
				if mode == "open" || mode == "read" || mode == "eof" {
					art := jobdb.NewArtifact("persisted", func() (io.ReadCloser, int64, error) {
						if mode == "open" {
							return nil, 0, source
						}
						readErr := source
						if mode == "eof" {
							readErr = io.EOF
						}
						return infrastructureRead{readErr}, 0, nil
					}, nil)
					d.artifacts = []Artifact{art}
				}
				wrapped := &guardedData{TaskData: d, key: &other, guard: &runner.guard}
				if mode == "data" || mode == "raw-data" {
					_, _ = wrapped.GetData()
				} else {
					arts, _ := wrapped.GetArtifacts()
					if len(arts) > 0 {
						s, err := arts[0].Open()
						if err == nil {
							_, _ = s.Read(make([]byte, 1))
							_ = s.Close()
						}
					}
				}
				rt := runner.runtime.(*guardedRuntime).WorkflowRuntime.(*runnerTestRuntime)
				rt.mu.Lock()
				atObservation = len(rt.chapters[runner.jobKey])
				rt.mu.Unlock()
				// Ignore the observed failure and attempt a new dispatch and late success.
				if mode != "eof" {
					_, err := c.DoTask(TaskCall{Site: "late", TaskType: "late", ContractVersion: 1, Command: []byte(`{}`)}, func(context.Context) (TaskData, error) { t.Error("late dispatch admitted"); return in, nil })
					require.True(t, jobdb.IsExecutionInfrastructureError(err))
				}
				return in, nil
			})
			_, err := runner.DoJob(celltest.Context(context.Background()))
			_, stops, complete, reschedule := lease.snapshot()
			require.Equal(t, 1, stops)
			if mode == "eof" {
				require.NoError(t, err)
				require.Len(t, complete, 1)
				return
			}
			require.True(t, jobdb.IsExecutionInfrastructureError(err), "%v", err)
			require.ErrorIs(t, err, source)
			require.Empty(t, complete)
			require.Empty(t, reschedule)
			require.Len(t, rt.chapters[runner.jobKey], atObservation, "no record after observation")
			var typed *jobdb.ExecutionInfrastructureError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, &other, typed.JobKey)
		})
	}
}
func TestInfrastructureConsumerFailureSkipsCatchAndRetry(t *testing.T) {
	source := errors.New("consumer classified source")
	failure := jobdb.ClassifyExecutionInfrastructure(source, jobdb.ExecutionInfrastructureRuntimeIO, "consumer", nil, nil)
	var calls, catches atomic.Int32
	runner, lease, _ := infrastructureRunner(t, func(c ExecutionContext, in JobData) (JobData, error) {
		return c.DoScope("scope", ScopeOptions{Retry: RetryPolicy{MaximumAttempts: 3}, Catch: []CatchRule{{ID: "catch", Match: func(error) bool { catches.Add(1); return true }, Handle: func(ExecutionContext, error) (CatchResult, error) { return CatchResult{Disposition: CatchHandled}, nil }}}}, func(ExecutionContext) (JobData, error) { calls.Add(1); return nil, failure })
	})
	_, err := runner.DoJob(celltest.Context(context.Background()))
	require.ErrorIs(t, err, source)
	require.EqualValues(t, 1, calls.Load())
	require.Zero(t, catches.Load())
	_, _, complete, reschedule := lease.snapshot()
	require.Empty(t, complete)
	require.Empty(t, reschedule)
}
func TestInfrastructureRunnerJoinsRenewalBeforeReturn(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	failure := jobdb.ClassifyExecutionInfrastructure(errors.New("stored corruption"), jobdb.ExecutionInfrastructureDurableDecode, "decode", nil, nil)
	runner, base, _ := infrastructureRunner(t, func(ExecutionContext, JobData) (JobData, error) {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Error("renewal did not start")
		}
		return nil, failure
	})
	lease := newSessionTestLease()
	lease.fakeExecutionLease = base
	var calls atomic.Int32
	lease.renew = func(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
		if calls.Add(1) == 1 {
			next := *lease
			next.expiry = time.Now().Add(120 * time.Millisecond)
			return &next, nil
		}
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil, ctx.Err()
	}
	runner.lease = lease
	done := make(chan error, 1)
	go func() { _, err := runner.DoJob(celltest.Context(context.Background())); done <- err }()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("failure did not cancel renewal")
	}
	select {
	case <-done:
		t.Fatal("runner returned before renewal exited")
	default:
	}
	close(release)
	select {
	case err := <-done:
		require.ErrorIs(t, err, failure)
	case <-time.After(3 * time.Second):
		t.Fatal("runner failed to join renewal")
	}
	_, _, complete, reschedule := base.snapshot()
	require.Empty(t, complete)
	require.Empty(t, reschedule)
}
func TestInfrastructureLatchDoesNotWaitForInflightAppend(t *testing.T) {
	rt := newRunnerTestRuntime()
	entered, release := make(chan struct{}), make(chan struct{})
	rt.putChapterHook = func(PutChapterRequest) error { close(entered); <-release; return nil }
	g := &infrastructureGuard{}
	r := &guardedRuntime{WorkflowRuntime: rt, guard: g}
	done := make(chan error, 1)
	go func() { done <- r.PutChapter(context.Background(), PutChapterRequest{}) }()
	<-entered
	failure := &jobdb.ExecutionInfrastructureError{Reason: jobdb.ExecutionInfrastructureRuntimeIO, Err: errors.New("other operation")}
	observed := make(chan struct{})
	go func() { g.observe(failure); close(observed) }()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("in-flight I/O blocked latch")
	}
	require.ErrorIs(t, r.PutChapter(context.Background(), PutChapterRequest{}), failure)
	close(release)
	require.NoError(t, <-done)
	require.Contains(t, rt.chapters[JobKey{}], int64(0), "an already admitted operation may commit")
}

type infrastructureLegacyLease struct {
	*fakeExecutionLease
	cancel                      context.CancelFunc
	done                        chan struct{}
	entered, cancelled, release chan struct{}
}

func (l *infrastructureLegacyLease) KeepAlive(ctx context.Context) error {
	ctx, l.cancel = context.WithCancel(ctx)
	go func() { defer close(l.done); close(l.entered); <-ctx.Done(); close(l.cancelled); <-l.release }()
	return nil
}
func (l *infrastructureLegacyLease) StopKeepAlive() { l.cancel(); <-l.done }
func TestInfrastructureRunnerJoinsLegacyRenewal(t *testing.T) {
	l := &infrastructureLegacyLease{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	failure := &jobdb.ExecutionInfrastructureError{Reason: jobdb.ExecutionInfrastructureDurableDecode, Err: errors.New("corrupt input")}
	runner, base, _ := infrastructureRunner(t, func(ExecutionContext, JobData) (JobData, error) { <-l.entered; return nil, failure })
	l.fakeExecutionLease = base
	runner.lease = l
	done := make(chan error, 1)
	go func() { _, err := runner.DoJob(celltest.Context(context.Background())); done <- err }()
	select {
	case <-l.cancelled:
	case <-time.After(time.Second):
		t.Fatal("failure did not cancel legacy renewal")
	}
	select {
	case <-done:
		t.Fatal("runner returned before legacy renewal exited")
	default:
	}
	close(l.release)
	select {
	case err := <-done:
		require.ErrorIs(t, err, failure)
	case <-time.After(time.Second):
		t.Fatal("runner did not join legacy renewal")
	}
	_, _, complete, reschedule := base.snapshot()
	require.Empty(t, complete)
	require.Empty(t, reschedule)
}
