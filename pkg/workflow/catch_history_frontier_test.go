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

type catchFrozenRuntime struct {
	jobdb.WorkflowRuntime
	chapters []jobdb.Chapter
	err      error
}

func (r catchFrozenRuntime) ListChapters(context.Context, jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	return append([]jobdb.Chapter(nil), r.chapters...), r.err
}

func TestCatchHistoryFrontier(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			entered0, entered1 := make(chan struct{}), make(chan struct{})
			release0, release1 := make(chan struct{}), make(chan struct{})
			unblock0, unblock1 := sync.OnceFunc(func() { close(release0) }), sync.OnceFunc(func() { close(release1) })
			defer unblock0()
			defer unblock1()
			var callbacks atomic.Int32
			call := invocation("frontier")
			call.Catch = []wf.CatchRule{{ID: "recovery/branch", Match: func(error) bool { callbacks.Add(1); return true }, Handle: func(c wf.ExecutionContext, _ error) (wf.CatchResult, error) {
				callbacks.Add(1)
				close(entered0)
				<-release0
				_, err := c.DoTask(invocation("nested"), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic("nested"), nil })
				if err != nil {
					return wf.CatchResult{}, err
				}
				close(entered1)
				<-release1
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("recovered")}, nil
			}}}
			task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				raw, _ := in.GetData()
				if string(raw) == `"job"` {
					return nil, errors.New("original")
				}
				return in, nil
			}}
			w := wf.ExecutionWorker{JobType: "frontier", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, recording, w)
			run, err := wf.GetJobForRun(context.Background(), recording, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "frontier", LeaseDuration: time.Minute})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { _, err := run.Run(nil); done <- err }()
			defer func() {
				unblock0()
				unblock1()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("recording did not stop")
				}
			}()
			select {
			case <-entered0:
			case <-time.After(10 * time.Second):
				t.Fatal("initial gate missing")
			}
			first := readCatches(t, inspection, key)
			a := catchAt(t, first, "frontier")
			require.Equal(t, wf.CatchSelectionUnknown, a.Selection)
			require.Equal(t, wf.CatchHandlingUnknown, a.Handling)
			require.Nil(t, a.Accepted)
			require.True(t, catchGap(a, wf.CatchGapSelection, wf.CatchGapFrontier))
			unblock0()
			select {
			case <-entered1:
			case <-time.After(10 * time.Second):
				t.Fatal("nested gate missing")
			}
			prefix, err := inspection.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			pending := readCatches(t, inspection, key)
			b := catchAt(t, pending, "frontier")
			require.Equal(t, a.Boundary, b.Boundary)
			require.Equal(t, wf.CatchSelectionSelected, b.Selection)
			require.Equal(t, "recovery/branch", b.RuleID)
			require.Equal(t, wf.CatchHandlingPending, b.Handling)
			require.Nil(t, b.Accepted)
			require.Equal(t, pending, readCatches(t, catchFrozenRuntime{WorkflowRuntime: inspection, chapters: prefix}, key))
			require.EqualValues(t, 2, callbacks.Load())
			// Concurrent append/read: inspect only committed prefixes, without blocking
			// execution or requiring an application engine in the inspector.
			inspected := make(chan error, 1)
			go func() {
				for i := 0; i < 20; i++ {
					h, err := wf.ReadCatchHistory(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key})
					if err != nil {
						inspected <- err
						return
					}
					for _, row := range h.Attempts {
						if row.Handling == wf.CatchHandlingAccepted && row.Accepted == nil {
							inspected <- fmt.Errorf("synthetic acceptance")
							return
						}
					}
				}
				inspected <- nil
			}()
			unblock1()
			require.NoError(t, <-inspected)
			// Wait for execution without consuming done: the deferred cleanup owns it.
			// The final read is synchronized by a separate done relay below.
			var runErr error
			select {
			case runErr = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("run did not finish")
			}
			done <- runErr
			require.NoError(t, runErr)
			final := readCatches(t, inspection, key)
			c := catchAt(t, final, "frontier")
			require.Equal(t, b.Boundary, c.Boundary)
			require.Equal(t, wf.CatchHandlingAccepted, c.Handling)
			require.Nil(t, c.Accepted.Failure)
			// The summary is now completed, but frozen old evidence must stay pending.
			require.Equal(t, pending, readCatches(t, catchFrozenRuntime{WorkflowRuntime: inspection, chapters: prefix}, key))
			require.EqualValues(t, 2, callbacks.Load())
		})
	}
}

type catchRejectRuntime struct {
	jobdb.WorkflowRuntime
	rejected atomic.Bool
}

func (r *catchRejectRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	if body, ok := req.Chapter.Body.(jobdb.ExecutionChapter); ok {
		rec, err := jobdb.DecodeExecutionRecord(body.Record)
		if err != nil {
			return err
		}
		// The root result is the next durable boundary after a successful task catch.
		if rec.Kind == "result" && rec.ID == `/scope:"$job"#1/attempt:1` {
			r.rejected.Store(true)
			return errors.New("injected write rejection")
		}
	}
	return r.WorkflowRuntime.PutChapter(ctx, req)
}
func TestCatchHistoryFailedWrite(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			rt := &catchRejectRuntime{WorkflowRuntime: recording}
			call := invocation("write-failure")
			call.Catch = []wf.CatchRule{{ID: "recover", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("ok")}, nil
			}}}
			task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) { return nil, errors.New("original") }}
			w := wf.ExecutionWorker{JobType: "write-failure", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			_, err := runExecutionTest(t, rt, key, w, task)
			require.ErrorContains(t, err, "injected write rejection")
			require.True(t, rt.rejected.Load())
			h := readCatches(t, inspection, key)
			a := catchAt(t, h, "write-failure")
			require.Nil(t, a.Accepted)
			require.NotEqual(t, wf.CatchHandlingAccepted, a.Handling)
		})
	}
}
