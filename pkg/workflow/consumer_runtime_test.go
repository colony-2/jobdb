package workflow_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// This fixture has no attachment method, including through embedding. Optional
// ports must be supplied explicitly instead of relying on a full runtime bridge.
type consumerExecutionRuntime struct{ jobdb.ConsumerRuntime }

var _ wf.WorkflowRuntime = consumerExecutionRuntime{}
var _ func(context.Context, jobdb.JobRunReader, jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error) = wf.GetJobRun
var _ func(context.Context, jobdb.JobRunReader, jobdb.GetJobRunRequest) (wf.CatchHistory, error) = wf.ReadCatchHistory
var _ func(*wf.EngineBuilder, jobdb.ConsumerRuntime) *wf.EngineBuilder = (*wf.EngineBuilder).WithRuntime

func TestConsumerRuntimeExecutionWorkerAndReplay(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			runtimeKind := backend
			if backend == "remote" {
				runtimeKind = "sqlite"
			}
			raw := alternateRuntime(t, runtimeKind)
			if backend == "remote" {
				server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(raw)))
				t.Cleanup(server.Close)
				client, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				raw = client
			}
			rt := consumerExecutionRuntime{raw}
			_, hasMutation := reflect.TypeOf(rt).MethodByName("AddGoalCandidate")
			require.False(t, hasMutation)
			var builds, runs atomic.Int32
			task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
			worker := wf.ExecutionWorker{JobType: "consumer", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(invocation("retained"), func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}}
			ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 15*time.Second)
			defer cancel()
			handle, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "consumer", Cell: "test", CellRepo: "example.test/test", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("durable")}})
			require.NoError(t, err)
			engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(worker, task).BuildEngine()
			require.NoError(t, err)
			done := make(chan struct{})
			go func() { defer close(done); engine.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("worker failed to stop")
				}
			})
			require.Eventually(t, func() bool {
				info, e := rt.GetJob(ctx, handle.JobKey)
				return e == nil && info.Status == jobdb.JobStatusCompleted
			}, 10*time.Second, 10*time.Millisecond)
			// Stop the polling worker before comparing durable history and replaying.
			cancel()
			<-done
			replayCtx := celltest.Context(context.Background())
			before, err := rt.ListChapters(replayCtx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			value, err := engine.ReplayJobRun(replayCtx, wf.ReplayRunRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			data, err := value.GetData()
			require.NoError(t, err)
			require.JSONEq(t, `"durable"`, string(data))
			require.EqualValues(t, 1, builds.Load())
			require.EqualValues(t, 1, runs.Load())
			after, err := rt.ListChapters(replayCtx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			require.Equal(t, before, after)
			detail, err := wf.GetJobRun(replayCtx, struct{ jobdb.JobRunReader }{rt}, jobdb.GetJobRunRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			require.Equal(t, handle.GoalID, detail.Job.GoalID)
		})
	}
}
