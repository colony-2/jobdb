package remote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestClearCrashConcernHTTPValidation(t *testing.T) {
	r := toy.New()
	_, err := celltest.Submit(r, celltest.Context(context.Background()), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "tenant", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1),
	}})
	require.NoError(t, err)
	server := httptest.NewServer(celltest.Handler(NewServer(r)))
	defer server.Close()
	url := server.URL + "/v1/tenants/tenant/jobs/job/clear-crash-concern"
	for _, body := range []string{"", `{`, `[]`, `null`, `{"unexpected":true}`, `{"reason":123}`} {
		t.Run(body, func(t *testing.T) {
			resp, err := server.Client().Post(url, "application/json", strings.NewReader(body))
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
	resp, err := server.Client().Post(url, "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestDelegationCannotClearCrashConcern(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	path := "/v1/tenants/tenant/jobs/" + l.Job().JobKey.JobId + "/clear-crash-concern"
	status, body := delegationHTTP(t, f, http.MethodPost, path, l.LeaseToken(), map[string]string{"reason": "clear"})
	require.Equal(t, http.StatusForbidden, status)
	require.ErrorIs(t, responseError("clear", status, body, nil), jobdb.ErrLeasePermissionDenied)
}
