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
	for _, backend := range []string{"toy", "sqlite", "remote", "remote-toy"} {
		runtimetest.RunStartOrAppendConformance(t, runtimetest.Harness{Name: backend, New: func(tb testing.TB) runtimetest.Fixture {
			return runtimetest.Fixture{Runtime: appendRuntime(tb.(*testing.T), backend)}
		}})
	}
}
func TestAppendInputReplayAndArtifacts(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote", "remote-toy"} {
		t.Run(backend, func(t *testing.T) {
			rt := appendRuntime(t, backend)
			ctx := celltest.Context(context.Background())
			source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "source", Data: jobdb.NewTaskDataOrPanic("source", jobdb.NewArtifactFromBytes("report.txt", []byte("report")))}})
			require.NoError(t, err)
			source2, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "source", Data: jobdb.NewTaskDataOrPanic("source2", jobdb.NewArtifactFromBytes("report.txt", []byte("second")))}})
			require.NoError(t, err)
			ref2 := jobdb.ArtifactKey{JobId: source2.JobKey.JobId, TaskOrdinal: 0, Name: "report.txt", SizeBytes: 6}
			ref := jobdb.ArtifactKey{JobId: source.JobKey.JobId, TaskOrdinal: 0, Name: "report.txt", SizeBytes: 6}
			q := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "artifacts", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("start", jobdb.NewArtifactFromBytes("report.txt", []byte("initial")))}, Append: json.RawMessage(`null`), AppendArtifacts: []jobdb.ArtifactKey{ref}}}
			a, err := rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			q.Job.Append = json.RawMessage(`{"n":2}`)
			q.Job.AppendArtifacts = []jobdb.ArtifactKey{ref2}
			q.Job.Job.Data = jobdb.NewTaskDataOrPanic("ignored")
			b, err := rt.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			require.Equal(t, a, b)
			checkAppendCollection(t, rt, a.JobKey, false)
			restart, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: a.JobKey, LastStepToKeep: 1}})
			require.NoError(t, err)
			checkAppendCollection(t, rt, restart.JobKey, false)
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
					key, err := art.ArtifactKey()
					require.NoError(t, err)
					if art.Name() == "start/report.txt" {
						require.Equal(t, jobdb.ArtifactKey{JobId: c.GetJobKey().JobId, TaskOrdinal: 0, Name: "report.txt", SizeBytes: 7}, key)
						require.Equal(t, "initial", string(raw))
					} else if art.Name() == "appends/0/report.txt" {
						require.Equal(t, ref, key)
						require.Equal(t, "report", string(raw))
					} else {
						require.Equal(t, ref2, key)
						require.Equal(t, "second", string(raw))
					}
				}
				require.ElementsMatch(t, []string{"start/report.txt", "appends/0/report.txt", "appends/1/report.txt"}, names)
				return jobdb.NewTaskDataOrPanic(map[string]string{"result": "actual"}, jobdb.NewArtifactFromBytes("result.bin", []byte{0, 1, 255, 42})), nil
			}}
			result, err := runExecutionTest(t, rt, a.JobKey, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, result.Status)
			checkAppendCollection(t, rt, a.JobKey, true)
			before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: a.JobKey})
			require.NoError(t, err)
			_, err = replayExecutionTest(t, rt, a.JobKey, worker)
			require.NoError(t, err)
			after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: a.JobKey})
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, err = runExecutionTest(t, rt, restart.JobKey, worker)
			require.NoError(t, err)
			checkAppendCollection(t, rt, restart.JobKey, true)
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
	for _, backend := range []string{"toy", "sqlite", "remote", "remote-toy"} {
		t.Run(backend, func(t *testing.T) {
			rt := appendRuntime(t, backend)
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
			_, err = wf.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: restart.JobKey})
			require.NoError(t, err)
		})
	}
}

func appendRuntime(t *testing.T, backend string) jobdb.WorkflowRuntime {
	t.Helper()
	if backend != "remote-toy" {
		return alternateRuntime(t, backend)
	}
	server := httptest.NewServer(celltest.Handler(remote.NewServer(alternateRuntime(t, "toy"))))
	t.Cleanup(server.Close)
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	return client
}

// Exercise each public collection entry point and all include-option combinations.
func checkAppendCollection(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, completed bool) {
	t.Helper()
	ctx := celltest.Context(context.Background())
	engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
	require.NoError(t, err)
	readers := []func(context.Context, jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error){
		func(ctx context.Context, q jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error) {
			return jobdb.GetJobRun(ctx, rt, q)
		},
		func(ctx context.Context, q jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error) {
			return wf.GetJobRun(ctx, rt, q)
		},
		engine.GetJobRun,
	}
	for flags := 0; flags < 16; flags++ {
		q := jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: flags&1 != 0, IncludeOutputs: flags&2 != 0, IncludeArtifacts: flags&4 != 0, IncludeAttemptInputs: flags&8 != 0}
		var previous jobdb.GetJobRunResponse
		for i, read := range readers {
			run, err := read(ctx, q)
			require.NoError(t, err)
			if i > 0 {
				require.Equal(t, previous, run)
			}
			previous = run
			if flags == 0 || q.IncludeInputs {
				require.Equal(t, `"start"`, string(run.Start.Input.Data))
			} else {
				require.Nil(t, run.Start.Input)
			}
			require.Len(t, run.Attempts, 1)
			if completed {
				require.Equal(t, jobdb.JobStatusCompleted, run.Job.Status)
				require.Equal(t, jobdb.TaskOutcomeStatusSucceeded, run.Attempts[0].Outcome.Status)
				require.Empty(t, run.Attempts[0].Tasks)
				output, err := run.GetOutput(engine, key.TenantId)
				require.NoError(t, err)
				require.JSONEq(t, `{"result":"actual"}`, string(output.GetDataOrPanic()))
				arts, err := output.GetArtifacts()
				require.NoError(t, err)
				if flags == 0 || q.IncludeArtifacts {
					require.Len(t, arts, 1)
					raw, err := arts[0].Bytes(ctx)
					require.NoError(t, err)
					require.Equal(t, []byte{0, 1, 255, 42}, raw)
				} else {
					require.Empty(t, arts)
				}
			} else {
				require.Empty(t, run.Attempts[0].Outcome.Status)
				require.Len(t, run.Attempts[0].Tasks, 1)
				require.Empty(t, run.Attempts[0].Tasks[0].Attempts[0].Outcome.Status)
				_, err := run.GetOutput(engine, key.TenantId)
				require.ErrorIs(t, err, jobdb.ErrJobNotComplete)
			}
		}
	}
}
