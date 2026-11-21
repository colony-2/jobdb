package remote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestGoalAttachmentRouteRemoved(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			var rt jobdb.WorkflowRuntime
			if backend == "toy" {
				rt = toyruntime.New(toyruntime.WithProjects(celltest.UnrestrictedProjects{}))
			} else {
				embedded, err := sqliteruntime.StartEmbeddedRuntime(ctx, sqliteruntime.WithProjects(celltest.UnrestrictedProjects{}))
				require.NoError(t, err)
				t.Cleanup(embedded.Shutdown)
				rt = embedded.Runtime
			}
			a, b := goalSubmit(t, rt, "a"), goalSubmit(t, rt, "b")
			beforeGoal, err := rt.GetGoal(ctx, goalKey(a))
			require.NoError(t, err)
			beforeJobs, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true})
			require.NoError(t, err)
			server := httptest.NewServer(celltest.Handler(NewServer(rt)))
			defer server.Close()
			req, err := http.NewRequest(http.MethodPut, server.URL+"/v1/tenants/tenant/goals/"+a.GoalID+"/candidates/"+b.JobKey.JobId, nil)
			require.NoError(t, err)
			response, err := server.Client().Do(req)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, http.StatusNotFound, response.StatusCode)
			afterGoal, err := rt.GetGoal(ctx, goalKey(a))
			require.NoError(t, err)
			require.Equal(t, beforeGoal, afterGoal)
			afterJobs, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true})
			require.NoError(t, err)
			require.Equal(t, beforeJobs, afterJobs)
			members, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{a.GoalID}, IncludeHidden: true})
			require.NoError(t, err)
			require.Len(t, members.Jobs, 1)
			require.Equal(t, a.JobKey, members.Jobs[0].JobKey)
			other, err := rt.GetJob(ctx, b.JobKey)
			require.NoError(t, err)
			require.Equal(t, b.GoalID, other.GoalID)
			require.Empty(t, other.OtherGoalIDs)
		})
	}
}
