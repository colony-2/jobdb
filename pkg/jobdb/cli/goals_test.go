package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestGoalsCommands(t *testing.T) {
	rt := toy.New()
	ctx := celltest.Context(context.Background())
	submit := func() jobdb.JobHandle {
		h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		return h
	}
	a, b := submit(), submit()
	var requests atomic.Int64
	handler := celltest.Handler(remote.NewServer(rt))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); handler.ServeHTTP(w, r) }))
	defer server.Close()
	run := func(args ...string) []byte {
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetArgs(append([]string{"goals", "--tenant", "tenant", "--url", server.URL}, args...))
		require.NoError(t, root.ExecuteContext(ctx))
		return out.Bytes()
	}
	// The removed command must fail before reaching the runtime.
	root := newRootCmd()
	root.SetOut(&bytes.Buffer{})
	root.SetArgs([]string{"goals", "--tenant", "tenant", "--url", server.URL, "add-candidate", a.GoalID, b.JobKey.JobId})
	require.ErrorContains(t, root.ExecuteContext(ctx), "unknown command")
	require.Zero(t, requests.Load())
	require.NotContains(t, string(run("--help")), "add-candidate")
	unchanged, err := rt.GetJob(ctx, b.JobKey)
	require.NoError(t, err)
	require.Equal(t, b.GoalID, unchanged.GoalID)
	require.Empty(t, unchanged.OtherGoalIDs)
	var info jobdb.GoalInfo
	require.NoError(t, json.Unmarshal(run("fail", a.GoalID, "--message", "stopped after review"), &info))
	require.Equal(t, jobdb.GoalFailed, info.State)
	require.Equal(t, []string{a.JobKey.JobId}, info.Failure.NonTerminalJobIDs)
	require.NoError(t, json.Unmarshal(run("get", a.GoalID), &info))
	run("candidates", a.GoalID)
	var page jobdb.ListGoalsResponse
	require.NoError(t, json.Unmarshal(run("list", "--state", "FAILED", "--page-size", "1"), &page))
	require.Len(t, page.Goals, 1)
	require.Equal(t, a.GoalID, page.Goals[0].GoalKey.GoalID)
	var waits jobdb.ListDependencyWaitsResponse
	require.NoError(t, json.Unmarshal(run("waits", "--target-goal", b.GoalID), &waits))
	require.Empty(t, waits.Waits)

	state, err := rt.GetJob(ctx, b.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusReady, state.Status)
}
