package remote

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestToyGoalAdmissionHTTPConformance(t *testing.T) {
	application := NewServer(toy.New(toy.WithProjects(celltest.UnrestrictedProjects{})))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
	}))
	defer server.Close()
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	runtimetest.RunGoalAdmissionConformance(t, rt)
}

func TestSQLiteGoalAdmissionHTTPConformance(t *testing.T) {
	embedded, err := sqlite.StartEmbeddedRuntime(context.Background(), sqlite.WithProjects(celltest.UnrestrictedProjects{}))
	require.NoError(t, err)
	defer embedded.Shutdown()
	application := NewServer(embedded.Runtime)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
	}))
	defer server.Close()
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	runtimetest.RunGoalAdmissionConformance(t, rt)
}

func TestToyTerminalFailureHTTPConformance(t *testing.T) {
	application := NewServer(toy.New(toy.WithProjects(celltest.UnrestrictedProjects{})))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
	}))
	defer server.Close()
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	runtimetest.RunTerminalFailureConformance(t, rt)
}

func TestSQLiteTerminalFailureHTTPConformance(t *testing.T) {
	embedded, err := sqlite.StartEmbeddedRuntime(context.Background(), sqlite.WithProjects(celltest.UnrestrictedProjects{}))
	require.NoError(t, err)
	defer embedded.Shutdown()
	application := NewServer(embedded.Runtime)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
	}))
	defer server.Close()
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	runtimetest.RunTerminalFailureConformance(t, rt)
}

func TestMigratedSQLiteFailureHTTPInspection(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	fixture, err := os.ReadFile(filepath.Join("..", "sqlite", "testdata", "goal-runtime-11.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(fixture))
	require.NoError(t, err)
	var goalID string
	require.NoError(t, db.QueryRow(`SELECT goal_id FROM jobdb_jobs WHERE job_id='source'`).Scan(&goalID))
	require.NoError(t, db.Close())
	_, err = sqlite.MigrateGoals(ctx, path)
	require.NoError(t, err)
	backend, err := sqlite.NewFromConfig(ctx, sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: path})
	require.NoError(t, err)
	defer backend.Close(ctx)
	application := NewServer(backend)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
	}))
	defer server.Close()
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	goal, err := rt.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: goalID})
	require.NoError(t, err)
	key := jobdb.JobKey{TenantId: "tenant", JobId: goal.FailureJobID}
	info, err := rt.GetJob(ctx, key)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobRecordMigratedGoalFailure, info.RecordKind)
	require.Equal(t, goal.Failure, info.TerminalFailure)
	require.Nil(t, info.NextRoute)
	data, err := info.Data.GetData()
	require.Nil(t, data)
	require.ErrorIs(t, err, jobdb.ErrGoalFailed)
	require.ErrorIs(t, err, jobdb.ErrJobFailed)
	run, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeOutputs: true, IncludeArtifacts: true})
	require.NoError(t, err)
	require.Empty(t, run.Attempts)
	require.Empty(t, run.Execution)
	require.Zero(t, run.Start)
	output, err := run.GetOutput(nil, "tenant")
	require.Nil(t, output)
	require.ErrorIs(t, err, jobdb.ErrGoalFailed)
	chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	require.Empty(t, chapters)
	list, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{key}})
	require.NoError(t, err)
	require.Len(t, list.Jobs, 1)
	require.Equal(t, info.TerminalFailure, list.Jobs[0].TerminalFailure)
	_, err = rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{JobID: "must-reject", PriorJobKey: key, Cell: goal.Cell, CellRepo: goal.CellRepo}})
	require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
}

func TestSQLiteTerminalFailureHTTPLostResponseReplay(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	cfg := sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "lost-response.db")}
	backend, err := sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer backend.Close(ctx)
	origin, err := celltest.Submit(backend, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "tenant", JobID: "origin", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil),
	}})
	require.NoError(t, err)
	startServer := func(backend jobdb.WorkflowRuntime) *httptest.Server {
		application := NewServer(backend)
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
		}))
	}
	server := startServer(backend)
	defer server.Close()
	var dropped atomic.Bool
	lost := errors.New("committed failure response was lost")
	transport := server.Client().Transport
	client := &http.Client{Transport: diagnosticTransport(func(req *http.Request) (*http.Response, error) {
		resp, e := transport.RoundTrip(req)
		if e == nil && req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/fail") && dropped.CompareAndSwap(false, true) {
			// Wait for the real committed HTTP response before losing it locally.
			_, e = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if e != nil {
				return nil, e
			}
			return nil, lost
		}
		return resp, e
	})}
	rt, err := New(server.URL, client)
	require.NoError(t, err)
	req := jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: origin.GoalID}, Message: " \t保存 exact\n"}
	_, err = rt.FailGoal(ctx, req)
	require.ErrorIs(t, err, lost)
	require.True(t, dropped.Load())
	saved, err := backend.GetGoal(ctx, req.GoalKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalFailed, saved.State)
	require.NotEmpty(t, saved.FailureJobID)
	_, err = backend.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{saved.FailureJobID}})
	require.NoError(t, err)
	server.Close()
	require.NoError(t, backend.Close(ctx))
	backend, err = sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer backend.Close(ctx)
	server = startServer(backend)
	defer server.Close()
	rt, err = New(server.URL, server.Client())
	require.NoError(t, err)
	replay, err := rt.FailGoal(ctx, req)
	require.NoError(t, err)
	require.Equal(t, saved, replay)
	list, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{jobdb.GoalFailureJobType}, IncludeHidden: true})
	require.NoError(t, err)
	require.Len(t, list.Jobs, 1)
	require.Equal(t, saved.FailureJobID, list.Jobs[0].JobKey.JobId)
	visible, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{jobdb.GoalFailureJobType}})
	require.NoError(t, err)
	require.Empty(t, visible.Jobs)
}
