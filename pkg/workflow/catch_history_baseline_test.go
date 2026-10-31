package workflow_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Independent clients share only committed storage, never engine state.
func catchHistoryRuntimes(t *testing.T, backend string) (jobdb.WorkflowRuntime, jobdb.WorkflowRuntime) {
	t.Helper()
	cfg := sqlite.Config{DBPath: filepath.Join(t.TempDir(), "catch.db")}
	store, err := sqlite.NewFromConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	if backend == "remote" {
		server := httptest.NewServer(remote.NewServer(store))
		t.Cleanup(server.Close)
		recording, err := remote.New(server.URL, server.Client())
		require.NoError(t, err)
		inspection, err := remote.New(server.URL, server.Client())
		require.NoError(t, err)
		return recording, inspection
	}
	inspection, err := sqlite.NewFromConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = inspection.Close(context.Background()) })
	return store, inspection
}

type catchBaselineObserver struct {
	mu       sync.Mutex
	trace    []string
	failures int
}

func (o *catchBaselineObserver) OnJobStart(wf.JobStartEvent)   { o.add("job-start", false) }
func (o *catchBaselineObserver) OnTaskStart(wf.TaskStartEvent) { o.add("task-start", false) }
func (o *catchBaselineObserver) OnTaskEnd(e wf.TaskEndEvent)   { o.add("task-end", e.Err != nil) }
func (o *catchBaselineObserver) OnJobEnd(wf.JobEndEvent)       { o.add("job-end", false) }
func (o *catchBaselineObserver) add(s string, failed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.trace = append(o.trace, s)
	if failed {
		o.failures++
	}
}

// This baseline deliberately does not reference ReadCatchHistory. Run it before
// adding that API to preserve behavioral evidence, rather than a compile failure.
func TestCatchHistoryBaseline(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			call := invocation("caught")
			call.Catch = []wf.CatchRule{{ID: "recover", Match: func(error) bool { return true }, Handle: func(c wf.ExecutionContext, _ error) (wf.CatchResult, error) {
				_, err := c.DoTask(invocation("durable-handler-work"), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic("nested"), nil })
				if err != nil {
					return wf.CatchResult{}, err
				}
				close(entered)
				<-release
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("recovered")}, nil
			}}}
			w := wf.ExecutionWorker{JobType: "catch-baseline", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				raw, _ := in.GetData()
				if string(raw) == `"job"` {
					return nil, errors.New("original")
				}
				return in, nil
			}}
			key := submitExecutionTest(t, recording, w)
			run, err := wf.GetJobForRun(context.Background(), recording, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "baseline", LeaseDuration: time.Minute})
			require.NoError(t, err)
			observer := &catchBaselineObserver{}
			done := make(chan error, 1)
			go func() { _, err := run.Run(observer); done <- err }()
			select {
			case <-entered:
			case <-time.After(20 * time.Second):
				t.Fatal("handler gate not reached")
			}
			before, err := jobdb.GetJobRun(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			selected, accepted := 0, 0
			for _, e := range before.Execution {
				if e.Record.Kind == "catch" && e.Record.Rule == "recover" {
					selected++
				}
				if e.Record.Kind == "handled" {
					accepted++
				}
			}
			require.Equal(t, 1, selected)
			require.Zero(t, accepted)
			unblock()
			require.NoError(t, <-done)
			after, err := jobdb.GetJobRun(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			accepted = 0
			for _, e := range after.Execution {
				if e.Record.Kind == "handled" {
					accepted++
				}
			}
			require.Equal(t, 1, accepted)
			observer.mu.Lock()
			defer observer.mu.Unlock()
			require.Equal(t, 1, observer.failures, "task-end stays failed despite accepted handling")
			t.Logf("before: selection=%d accepted=0; after: accepted=%d; lifecycle=%v; task-end-failures=%d", selected, accepted, observer.trace, observer.failures)
			t.Log("negative behavioral baseline: task-end failure cannot identify a successful catch or its rule; lifecycle provides no scope/catch event")
		})
	}
}

func TestCatchHistoryBaselineCompactScopes(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			w := wf.ExecutionWorker{JobType: "scope-baseline", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				for _, site := range []string{"first", "second"} {
					_, err := c.DoScope(site, wf.ScopeOptions{Catch: []wf.CatchRule{{ID: "recover", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
						return wf.CatchResult{Disposition: wf.CatchHandled, Output: in}, nil
					}}}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("same failure") })
					if err != nil {
						return nil, err
					}
				}
				return in, nil
			}}
			key := submitExecutionTest(t, recording, w)
			run, err := wf.GetJobForRun(context.Background(), recording, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, WorkerID: "baseline", LeaseDuration: time.Minute})
			require.NoError(t, err)
			o := &catchBaselineObserver{}
			out, err := run.Run(o)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			info, err := jobdb.GetJobRun(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			ordinals := map[int64]int{}
			handled := 0
			for _, e := range info.Execution {
				if e.Record.Kind == "handled" {
					handled++
					ordinals[e.Ordinal]++
				}
			}
			require.Equal(t, 2, handled)
			require.Len(t, ordinals, 1, "both handled episodes share a compact chapter")
			o.mu.Lock()
			defer o.mu.Unlock()
			require.Equal(t, []string{"job-start", "job-end"}, o.trace)
			t.Logf("negative behavioral baseline: two caught scopes share chapter %v; observer trace %v contains neither catching boundary", ordinals, o.trace)
		})
	}
}
