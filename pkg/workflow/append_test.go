package workflow_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestStartOrAppendConformance(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		runtimetest.RunStartOrAppendConformance(t, runtimetest.Harness{Name: backend, New: func(tb testing.TB) runtimetest.Fixture {
			return runtimetest.Fixture{Runtime: alternateRuntime(tb.(*testing.T), backend)}
		}})
	}
}
func TestAppendInputReplayAndArtifacts(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			ctx := celltest.Context(context.Background())
			source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "source", Data: jobdb.NewTaskDataOrPanic("source", jobdb.NewArtifactFromBytes("report.txt", []byte("report")))}})
			require.NoError(t, err)
			ref := jobdb.ArtifactKey{JobId: source.JobKey.JobId, TaskOrdinal: 0, Name: "report.txt", SizeBytes: 6}
			q := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "artifacts", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("start", jobdb.NewArtifactFromBytes("report.txt", []byte("initial")))}, Append: json.RawMessage(`null`), AppendArtifacts: []jobdb.ArtifactKey{ref}}}
			a, err := rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			q.Job.Append = json.RawMessage(`{"n":2}`)
			q.Job.Job.Data = jobdb.NewTaskDataOrPanic("ignored")
			b, err := rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			require.Equal(t, a, b)
			worker := wf.ExecutionWorker{JobType: "batch", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				require.JSONEq(t, `{"start":"start","appends":[null,{"n":2}]}`, string(in.GetDataOrPanic()))
				arts, err := in.GetArtifacts()
				require.NoError(t, err)
				require.Len(t, arts, 3)
				names := []string{}
				for _, art := range arts {
					names = append(names, art.Name())
					raw, err := art.Bytes(c.Context())
					require.NoError(t, err)
					if art.Name() == "start/report.txt" {
						require.Equal(t, "initial", string(raw))
					} else {
						require.Equal(t, "report", string(raw))
					}
				}
				require.ElementsMatch(t, []string{"start/report.txt", "appends/0/report.txt", "appends/1/report.txt"}, names)
				return in, nil
			}}
			result, err := runExecutionTest(t, rt, a.JobKey, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, result.Status)
			_, err = replayExecutionTest(t, rt, a.JobKey, worker)
			require.NoError(t, err)
		})
	}
}
func TestAppendRemoteToy(t *testing.T) {
	rt := alternateRuntime(t, "toy")
	server := httptest.NewServer(celltest.Handler(remote.NewServer(rt)))
	defer server.Close()
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	runtimetest.RunStartOrAppendConformance(t, runtimetest.Harness{Name: "remote-toy", New: func(testing.TB) runtimetest.Fixture { return runtimetest.Fixture{Runtime: client} }})
}

func TestAppendRestartOutsideChain(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			ctx := celltest.Context(context.Background())
			_, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
			require.NoError(t, err)
			q := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "restart", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("start")}, Append: json.RawMessage(`1`)}}
			a, err := rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			_, err = rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			restart, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: a.JobKey, LastStepToKeep: 1}})
			require.NoError(t, err)
			info, err := rt.GetJob(ctx, restart.JobKey)
			require.NoError(t, err)
			require.Empty(t, info.AppendKey)
			b, err := rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			require.Equal(t, a, b)
			worker := wf.ExecutionWorker{JobType: "batch", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				require.JSONEq(t, `{"start":"start","appends":[1,1]}`, string(in.GetDataOrPanic()))
				return in, nil
			}}
			outcome, err := runExecutionTest(t, rt, restart.JobKey, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, outcome.Status)
		})
	}
}
