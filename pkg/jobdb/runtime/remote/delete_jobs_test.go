package remote_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestDeleteJobsHTTPValidation(t *testing.T) {
	ctx := context.Background()
	rt := toy.New()
	_, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	server := httptest.NewServer(remote.NewServer(rt))
	defer server.Close()
	for _, body := range []string{
		`{}`, `null`, `{"all":true,"jobIds":["job"]}`, `{"jobIds":[""]}`,
		`{"createdBefore":"yesterday"}`, `{"createdBefore":"9999-01-01T00:00:00Z"}`,
		`{"all":true,"dryrun":true}`, `{"jobIds":["job"],"metadataPredicate":[]}`,
		`{"metadataPredicates":[{"path":[],"values":[]}]}`,
		`{"metadataPredicates":[]}`, `{"jobIds":[]}`,
	} {
		t.Run(body, func(t *testing.T) {
			resp, err := server.Client().Post(server.URL+"/v1/tenants/tenant/jobs/delete", "application/json", strings.NewReader(body))
			require.NoError(t, err)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}
	listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
	require.NoError(t, err)
	require.Len(t, listed.Jobs, 1)
}

// Wrapping only WorkflowRuntime intentionally omits the optional deletion API.
type withoutJobDeletion struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
}

func TestDeleteJobsUnsupportedBackend(t *testing.T) {
	base := toy.New()
	server := httptest.NewServer(remote.NewServer(withoutJobDeletion{base, base}))
	defer server.Close()
	rt, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	_, err = rt.DeleteJobs(context.Background(), jobdb.DeleteJobsRequest{TenantID: "tenant", All: true})
	require.ErrorIs(t, err, jobdb.ErrJobDeletionUnsupported)
}
