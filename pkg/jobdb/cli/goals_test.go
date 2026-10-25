package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"testing"
)

func TestGoalsCommands(t *testing.T) {
	rt := toy.New()
	ctx := context.Background()
	submit := func() jobdb.JobHandle {
		h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		return h
	}
	a, b := submit(), submit()
	server := httptest.NewServer(remote.NewServer(rt))
	defer server.Close()
	run := func(args ...string) []byte {
		root := newRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetArgs(append([]string{"goals", "--tenant", "tenant", "--url", server.URL}, args...))
		require.NoError(t, root.ExecuteContext(ctx))
		return out.Bytes()
	}
	var candidate jobdb.GoalCandidateInfo
	require.NoError(t, json.Unmarshal(run("add-candidate", a.GoalID, b.JobKey.JobId), &candidate))
	require.Equal(t, b.JobKey.JobId, candidate.JobID)
	var info jobdb.GoalInfo
	require.NoError(t, json.Unmarshal(run("fail", a.GoalID, "--message", "stopped after review"), &info))
	require.Equal(t, jobdb.GoalFailed, info.State)
	require.Len(t, info.Failure.NonTerminalJobIDs, 2)
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
