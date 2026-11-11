package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type selectionTransport func(*http.Request) (*http.Response, error)

func (f selectionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBundledPrimaryGoalSelection(t *testing.T) {
	for _, name := range []string{"toy", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var backend jobdb.WorkflowRuntime = toy.New()
			if name == "sqlite" {
				r, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(t.TempDir(), "selection.db")})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, r.Close(ctx)) })
				backend = r
			}
			server := httptest.NewServer(localCellAdministration(remote.NewServer(backend)))
			defer server.Close()
			var lose atomic.Bool
			var header, value string
			client := &http.Client{Transport: selectionTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/primary-goal") && header != "" {
					r.Header.Set(header, value)
				}
				response, err := server.Client().Transport.RoundTrip(r)
				if err == nil && strings.HasSuffix(r.URL.Path, "/primary-goal") && lose.CompareAndSwap(true, false) {
					response.Body.Close()
					return nil, errors.New("response discarded after commit")
				}
				return response, err
			})}
			rt, err := remote.New(server.URL, client)
			require.NoError(t, err)
			source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "selected", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic("fresh")}}
			expected := source
			expected.JobKey.JobId = "selected"
			h, err := rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, expected, h)
			require.NotEmpty(t, h.CellID)
			assertCounts := func(n int) {
				jobs, e := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
				require.NoError(t, e)
				require.Len(t, jobs.Jobs, n)
				goals, e := rt.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant"})
				require.NoError(t, e)
				require.Len(t, goals.Goals, 1)
				require.Equal(t, int64(n), goals.Goals[0].CandidateCount)
			}
			info, err := rt.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			require.Equal(t, h.GoalID, info.GoalID)
			require.Empty(t, info.OtherGoalIDs)
			replay, err := rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, h, replay)
			assertCounts(2)

			lost := req
			lost.Job.JobID = "lost"
			lose.Store(true)
			_, err = rt.SubmitJob(ctx, lost)
			require.ErrorContains(t, err, "response discarded after commit")
			// Independent visibility proves commitment before replay.
			committed, err := rt.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: "lost"})
			require.NoError(t, err)
			require.Equal(t, source.GoalID, committed.GoalID)
			require.Empty(t, committed.OtherGoalIDs)
			assertCounts(3)
			lostExpected := h
			lostExpected.JobKey.JobId = "lost"
			replay, err = rt.SubmitJob(ctx, lost)
			require.NoError(t, err)
			require.Equal(t, lostExpected, replay)
			assertCounts(3)
			for _, ingress := range []struct{ header, value string }{{"X-JobDB-Lease-Token", "opaque"}, {"Authorization", "Bearer jdb-m2.opaque.signature"}} {
				header, value = ingress.header, ingress.value
				denied := req
				denied.Job.JobID = "denied"
				_, err = rt.SubmitJob(ctx, denied)
				require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
				header, value = "", ""
				_, err = rt.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: "denied"})
				require.ErrorIs(t, err, jobdb.ErrJobNotFound)
				assertCounts(3)
			}
			_, err = rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: h.GoalID}, Message: "closed"})
			require.NoError(t, err)
			for _, retained := range []jobdb.SubmitJobRequest{req, lost} {
				replay, err = rt.SubmitJob(ctx, retained)
				require.NoError(t, err)
				want := h
				want.JobKey.JobId = retained.Job.JobID
				require.Equal(t, want, replay)
				assertCounts(3)
			}

		})
	}
}

func TestBundledServePathsPrimaryGoalSelection(t *testing.T) {
	original := serveHTTPFunc
	defer func() { serveHTTPFunc = original }()
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			called := false
			serveHTTPFunc = func(ctx context.Context, _ string, handler http.Handler, cleanup func(context.Context) error) error {
				called = true
				if cleanup != nil {
					defer cleanup(ctx)
				}
				server := httptest.NewServer(handler)
				defer server.Close()
				rt, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
				require.NoError(t, err)
				h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "selected", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
				require.NoError(t, err)
				expected := source
				expected.JobKey.JobId = "selected"
				require.Equal(t, expected, h)
				return nil
			}
			cmd := newRootCmd()
			cmd.SetArgs([]string{backend, "--db", filepath.Join(t.TempDir(), "serve.db")})
			require.NoError(t, cmd.ExecuteContext(context.Background()))
			require.True(t, called)
		})
	}
}
