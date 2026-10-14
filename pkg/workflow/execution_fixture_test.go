package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func executionFixtureWorker(builds *atomic.Int32) wf.ExecutionWorker {
	return wf.ExecutionWorker{JobType: "execution-fixture", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("sequence", wf.ScopeOptions{TotalTimeout: 2 * time.Second}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
			call := invocation("compile")
			call.Policy.Retry.MaximumAttempts = 2
			call.Catch = []wf.CatchRule{{ID: "retry", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchContinue}, nil
			}}}
			return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
				builds.Add(1)
				return jobdb.NewTaskDataOrPanic("frozen"), nil
			})
		})
	}}
}
func TestExecutionVersionOneFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/execution-v1.json")
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	require.NoError(t, json.Unmarshal(raw, &records))
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds atomic.Int32
			w := executionFixtureWorker(&builds)
			key := submitExecutionTest(t, rt, w)
			lease, err := rt.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "fixture", Routes: []jobdb.Route{{JobType: w.Name()}}, LeaseDuration: time.Second})
			require.NoError(t, err)
			token := ""
			if transport, ok := lease.(interface{ LeaseToken() string }); ok {
				token = transport.LeaseToken()
			}
			for i, record := range records {
				raw, err := json.Marshal(record)
				require.NoError(t, err)
				ordinal := int64(i + 1)
				require.NoError(t, rt.PutChapter(context.Background(), jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: key, Ordinal: ordinal}, Chapter: jobdb.Chapter{Ordinal: ordinal, TaskType: "__execution__", CreatedAt: record.At, Body: jobdb.ExecutionChapter{Record: raw}}}))
			}
			final := jobdb.Chapter{Ordinal: int64(len(records) + 1), TaskType: w.Name(), CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"frozen"`)}}}}
			require.NoError(t, lease.Complete(context.Background(), jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
			task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
				t.Error("fixture replay executed worker")
				return nil, errors.New("unexpected worker")
			}}
			out, err := replayExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			data, err := out.GetData()
			require.NoError(t, err)
			require.JSONEq(t, `"frozen"`, string(data))
			require.EqualValues(t, 0, builds.Load())
		})
	}
}

// New clients can continue v1 histories, but must not silently insert v2
// records or enable v2 dispatch semantics in those histories.
func TestExecutionVersionOneContinuation(t *testing.T) {
	raw, err := os.ReadFile("testdata/execution-v1.json")
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	require.NoError(t, json.Unmarshal(raw, &records))
	header := records[0]
	require.Equal(t, "header", header.Kind)
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend)
			var builds atomic.Int32
			w := executionFixtureWorker(&builds)
			key := submitExecutionTest(t, rt, w)
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "fixture", Routes: []jobdb.Route{{JobType: w.Name()}}, LeaseDuration: time.Second})
			require.NoError(t, err)
			require.NotNil(t, lease)
			token := ""
			if transport, ok := lease.(interface{ LeaseToken() string }); ok {
				token = transport.LeaseToken()
			}
			raw, err := json.Marshal(header)
			require.NoError(t, err)
			require.NoError(t, rt.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "__execution__", CreatedAt: header.At, Body: jobdb.ExecutionChapter{Record: raw}}}))
			require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: w.Name()}}))
			complete := func(key jobdb.JobKey) {
				out, err := runExecutionTest(t, rt, key, w)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
				require.NoError(t, err)
				pending, err := engine.GetWaitingTask(ctx, key)
				require.NoError(t, err)
				require.NoError(t, pending.Finish(ctx, jobdb.NewTaskDataOrPanic("frozen")))
				out, err = runExecutionTest(t, rt, key, w)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				_, err = replayExecutionTest(t, rt, key, w)
				require.NoError(t, err)
				for _, record := range executionRecords(t, rt, key) {
					require.EqualValues(t, 1, record.Version)
				}
			}
			complete(key)
			var scopeID string
			for _, record := range executionRecords(t, rt, key) {
				if record.Kind == "scope" && record.ID != "$job" {
					scopeID = record.ID
				}
			}
			require.NotEmpty(t, scopeID)
			restart, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: scopeID}}})
			require.NoError(t, err)
			complete(restart.JobKey)
		})
	}
}
