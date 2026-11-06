package remote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestCellLookupDelegationBoundary(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			f := newDelegationFixture(t, backend, nil)
			ctx := context.Background()
			b, err := f.owner.CreateCell(ctx, jobdb.CreateCellRequest{Name: "B", Repo: "example.test/b"})
			require.NoError(t, err)
			l := f.create(t, jobdb.CreateDelegationRequest{})
			// The isolated ingress has no administrative CellAuthority middleware.
			handler, err := NewDelegationServerWithOptions(f.backend, ServerOptions{LeaseTokenSigningKey: f.signer.key})
			require.NoError(t, err)
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			client, err := New(server.URL, server.Client())
			require.NoError(t, err)
			capability, err := ExportLease(l)
			require.NoError(t, err)
			isolated, err := client.ImportLease(ctx, capability)
			require.NoError(t, err)
			got, err := isolated.LookupCell(ctx, "B")
			require.NoError(t, err)
			require.Equal(t, b.Resolved(), got)
			_, err = isolated.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: got.Cell, CellRepo: got.CellRepo, JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			path := delegationBase(l) + "/cells/lookup"
			for _, wrong := range []string{
				strings.Replace(path, "/tenants/tenant/", "/tenants/other/", 1),
				strings.Replace(path, "/jobs/"+l.Job().JobKey.JobId+"/", "/jobs/other/", 1),
				strings.Replace(path, "/leases/"+l.LeaseID()+"/", "/leases/other/", 1),
			} {
				status, _ := delegationHTTP(t, f, http.MethodPost, wrong, l.LeaseToken(), map[string]string{"cell": "B"})
				require.Equal(t, http.StatusUnauthorized, status)
				status, raw := delegationHTTP(t, f, http.MethodPost, wrong, f.owner.(*remoteExecutionLease).LeaseToken(), map[string]string{"cell": "B"})
				require.ErrorIs(t, responseError("lookup", status, raw, jobdb.ErrExecutionLeaseLost), jobdb.ErrExecutionLeaseLost)
			}
			// A valid signature is required; a known live route alone is insufficient.
			status, _ := delegationHTTP(t, f, http.MethodPost, path, l.LeaseToken()+"forged", map[string]string{"cell": "B"})
			require.Equal(t, http.StatusUnauthorized, status)
			status, raw := delegationHTTP(t, f, http.MethodPost, path, f.owner.(*remoteExecutionLease).LeaseToken()+"forged", map[string]string{"cell": "B"})
			require.ErrorIs(t, responseError("lookup", status, raw, jobdb.ErrExecutionLeaseLost), jobdb.ErrExecutionLeaseLost)
			for _, body := range []any{map[string]any{"cell": nil}, map[string]any{"cell": 1}, map[string]any{}, map[string]any{"cell": "B", "tenantId": "other"}} {
				status, raw := delegationHTTP(t, f, http.MethodPost, path, l.LeaseToken(), body)
				require.Equal(t, http.StatusBadRequest, status)
				require.ErrorIs(t, responseError("lookup", status, raw, jobdb.ErrExecutionLeaseLost), jobdb.ErrInvalidCell)
			}
			deadline := time.Now().Add(100 * time.Millisecond)
			expiring := f.create(t, jobdb.CreateDelegationRequest{NotAfter: &deadline})
			time.Sleep(time.Until(deadline) + time.Millisecond)
			_, err = expiring.LookupCell(ctx, "B")
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			// Expire stored authority while leaving the signed owner token valid.
			require.NoError(t, f.backend.(interface {
				KeepAliveLeaseByID(context.Context, jobdb.JobKey, string, string, time.Duration) error
			}).KeepAliveLeaseByID(celltest.Context(ctx), l.Job().JobKey, l.LeaseID(), l.workerID, time.Nanosecond))
			for _, lease := range []jobdb.ExecutionLease{f.owner, isolated} {
				_, err = lease.LookupCell(ctx, "B")
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			}
			next, err := f.client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: l.Job().JobKey, WorkerID: "next", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, next)
			require.NotEqual(t, l.LeaseID(), next.LeaseID())
			for _, lease := range []jobdb.ExecutionLease{f.owner, isolated} {
				_, err = lease.LookupCell(ctx, "B")
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			}
			got, err = next.LookupCell(ctx, "B")
			require.NoError(t, err)
			require.Equal(t, b.Resolved(), got)
		})
	}
}
