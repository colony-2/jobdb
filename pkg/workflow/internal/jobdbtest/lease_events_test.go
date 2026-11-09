package jobdbtest

import (
	"context"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"testing"
)

type namespaceHistoryProbe struct {
	jobdb.WorkflowRuntime
	req jobdb.ListLeaseEventsRequest
	err error
}

func (r *namespaceHistoryProbe) ListLeaseEvents(_ context.Context, req jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	r.req = req
	return jobdb.ListLeaseEventsResponse{Events: []jobdb.LeaseEvent{{EventID: "actual", JobKey: jobdb.JobKey{TenantId: req.TenantID, JobId: "job"}}}, NextPageToken: "next"}, r.err
}
func TestMandatoryHistoryForwarding(t *testing.T) {
	backend := &namespaceHistoryProbe{}
	var port jobdb.WorkflowRuntime = newTenantNamespacedRuntime(backend, "scope/")
	for _, tenant := range []string{"tenant", "other"} {
		req := jobdb.ListLeaseEventsRequest{TenantID: tenant, JobIDs: []string{"b", "a", "b"}, PageToken: "page", PageSize: 7}
		for _, failure := range []error{nil, errors.New("backend failure")} {
			backend.err = failure
			out, err := port.ListLeaseEvents(context.Background(), req)
			if failure == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.Equal(t, "scope/"+tenant, backend.req.TenantID)
			require.Equal(t, []string{"a", "b"}, backend.req.JobIDs)
			require.Equal(t, "page", backend.req.PageToken)
			require.Equal(t, 7, backend.req.PageSize)
			require.Equal(t, "actual", out.Events[0].EventID)
			require.Equal(t, tenant, out.Events[0].JobKey.TenantId)
			require.Equal(t, "next", out.NextPageToken)
		}
	}
}
