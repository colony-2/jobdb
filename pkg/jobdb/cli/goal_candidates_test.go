package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestGoalCandidatesPreservesTerminalFailure(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
	h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		Cell: "test", CellRepo: "example.test/test", TenantId: "c2", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil),
	}})
	require.NoError(t, err)
	goal, err := rt.FailGoal(ctx, jobdb.FailGoalRequest{
		GoalKey: jobdb.GoalKey{TenantId: "c2", GoalID: h.GoalID}, Message: "stopped after review",
	})
	require.NoError(t, err)
	stored, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, GoalIDs: []string{h.GoalID}})
	require.NoError(t, err)
	require.Len(t, stored.Jobs, 2)
	want := make(map[string]jobdb.JobSummary)
	for _, row := range stored.Jobs {
		want[row.JobKey.JobId] = row
	}
	require.Equal(t, jobdb.JobRecordGoalFailure, want[goal.FailureJobID].RecordKind)
	require.Equal(t, goal.Failure, want[goal.FailureJobID].TerminalFailure)
	require.NotNil(t, goal.Failure)
	require.Nil(t, want[h.JobKey.JobId].TerminalFailure)

	server := httptest.NewServer(celltest.Handler(remote.NewServer(rt)))
	defer server.Close()
	for _, selector := range []string{"--project", "--tenant"} {
		t.Run(selector, func(t *testing.T) {
			seen := make(map[string]bool)
			token := ""
			for pageNumber := 0; ; pageNumber++ {
				require.Less(t, pageNumber, len(want), "pagination must terminate without repeating rows")
				cmd := newRootCmd()
				var out bytes.Buffer
				cmd.SetOut(&out)
				cmd.SetArgs([]string{"goals", selector, "c2", "--url", server.URL, "candidates", h.GoalID, "--page-size", "1", "--page-token", token})
				require.NoError(t, cmd.ExecuteContext(ctx))
				var page struct {
					Jobs []struct {
						JobKey struct {
							ProjectID string `json:"projectId"`
							JobID     string `json:"jobId"`
						} `json:"jobKey"`
						RecordKind      jobdb.JobRecordKind `json:"recordKind"`
						TerminalFailure *jobdb.GoalFailure  `json:"terminalFailure"`
					} `json:"jobs"`
					NextPageToken string `json:"nextPageToken"`
				}
				require.NoError(t, json.Unmarshal(out.Bytes(), &page))
				require.Len(t, page.Jobs, 1)
				row := page.Jobs[0]
				require.Equal(t, "c2", row.JobKey.ProjectID)
				require.NotContains(t, out.String(), `"tenantId"`)
				expected, ok := want[row.JobKey.JobID]
				require.True(t, ok, "unexpected job %q", row.JobKey.JobID)
				require.False(t, seen[row.JobKey.JobID], "job repeated across pages")
				require.Equal(t, expected.RecordKind, row.RecordKind)
				require.Equal(t, expected.TerminalFailure, row.TerminalFailure)
				seen[row.JobKey.JobID] = true
				if page.NextPageToken == "" {
					break
				}
				token = page.NextPageToken
			}
			require.Len(t, seen, len(want))
		})
	}
}
