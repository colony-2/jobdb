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
