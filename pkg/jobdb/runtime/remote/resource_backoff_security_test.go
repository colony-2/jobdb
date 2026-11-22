package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func resourceHTTP(t *testing.T, f *delegationFixture, path, token string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest("POST", f.server.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-JobDB-Lease-Token", token)
	req.Header.Set("Content-Type", "application/json")
	response, err := f.server.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
	return response.StatusCode, raw
}
func resourceRequest(t *testing.T, f *delegationFixture) jobdb.ResourceBackoffRequest {
	t.Helper()
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(celltest.Context(context.Background()), f.backend, f.owner, "resource/attempt:1", 1, jobdb.DefaultResourceBackoffPolicy())
	require.NoError(t, err)
	return req
}
func expiredResourceOwner(t *testing.T, f *delegationFixture, key jobdb.JobKey, worker string) string {
	t.Helper()
	h := f.owner.Job()
	token, err := f.signer.mintWithPrincipal(key, f.owner.LeaseID(), worker, "", time.Now().Add(-time.Second), time.Minute, nil, jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo})
	require.NoError(t, err)
	return token
}
func TestResourceBackoffHTTPReceiptSecurity(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			f := newDelegationFixture(t, backend, nil)
			req := resourceRequest(t, f)
			raw, _ := json.Marshal(req)
			path := delegationBase(f.owner) + "/resource-backoff"
			code, body := resourceHTTP(t, f, path, f.owner.(*remoteExecutionLease).LeaseToken(), raw)
			require.Equal(t, 200, code, string(body))
			var first jobdb.ResourceBackoffReceipt
			require.NoError(t, json.Unmarshal(body, &first))
			require.EqualValues(t, 1, first.Count)
			expired := expiredResourceOwner(t, f, f.owner.Job().JobKey, f.owner.(*remoteExecutionLease).workerID)
			t.Run("B16_owner_expired_exact", func(t *testing.T) {
				code, again := resourceHTTP(t, f, path, expired, raw)
				require.Equal(t, 200, code, string(again))
				require.JSONEq(t, string(body), string(again))
			})
			t.Run("B16a_rejections", func(t *testing.T) {
				bad := req
				bad.RequestID = "fresh"
				fresh, _ := json.Marshal(bad)
				for _, item := range []struct {
					path, token string
					body        []byte
				}{{path, expired, fresh}, {path, expired, []byte(strings.Replace(string(raw), `"growth":2`, `"growth":3`, 1))}, {path, expired + "x", raw}, {path, expiredResourceOwner(t, f, f.owner.Job().JobKey, "other-worker"), raw}, {strings.Replace(path, "/jobs/", "/jobs/other", 1), expired, raw}} {
					status, response := resourceHTTP(t, f, item.path, item.token, item.body)
					require.NotEqual(t, 200, status, string(response))
					require.NotContains(t, string(response), `"wakeAt"`)
				}
				for _, changed := range []string{"tenant", "lease", "schema", "principal", "cell"} {
					h := f.owner.Job()
					key, lease, schema := h.JobKey, f.owner.LeaseID(), ""
					cell := jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo}
					var principal *string
					switch changed {
					case "tenant":
						key.TenantId = "another-tenant"
					case "lease":
						lease = "another-lease"
					case "schema":
						schema = "another-schema"
					case "principal":
						v := "another-principal"
						principal = &v
					case "cell":
						cell.CellID = "another-cell"
					}
					token, err := f.signer.mintWithPrincipal(key, lease, f.owner.(*remoteExecutionLease).workerID, schema, time.Now().Add(-time.Second), time.Minute, principal, cell)
					require.NoError(t, err)
					status, response := resourceHTTP(t, f, path, token, raw)
					require.NotEqual(t, 200, status, "%s: %s", changed, response)
					require.NotContains(t, string(response), `"wakeAt"`, changed)
				}
			})
			t.Run("B16b_wrong_audience", func(t *testing.T) {
				other := *f.signer
				other.audience = "wrong"
				h := f.owner.Job()
				token, err := other.mintWithPrincipal(h.JobKey, f.owner.LeaseID(), f.owner.(*remoteExecutionLease).workerID, "", time.Now().Add(-time.Second), time.Minute, nil, jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo})
				require.NoError(t, err)
				status, response := resourceHTTP(t, f, path, token, raw)
				require.NotEqual(t, 200, status, string(response))
			})
			t.Run("B17a_after_cancellation", func(t *testing.T) {
				require.NoError(t, f.backend.CancelJob(celltest.Context(context.Background()), jobdb.CancelJobRequest{JobKey: f.owner.Job().JobKey}))
				code, again := resourceHTTP(t, f, path, expired, raw)
				require.Equal(t, 200, code, string(again))
				require.JSONEq(t, string(body), string(again))
			})
		})
	}
}
func TestResourceBackoffHTTPDelegationBinding(t *testing.T) {
	f := newDelegationFixture(t, "sqlite", nil)
	req := resourceRequest(t, f)
	raw, _ := json.Marshal(req)
	path := delegationBase(f.owner) + "/resource-backoff"
	until := time.Now().Add(time.Second)
	delegated := f.create(t, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{ResourceBackoff: true}, NotAfter: &until})
	other := f.create(t, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{ResourceBackoff: true}})
	token := delegated.LeaseToken()
	code, first := resourceHTTP(t, f, path, token, raw)
	require.Equal(t, 200, code, string(first))
	time.Sleep(time.Until(until) + time.Millisecond)
	code, again := resourceHTTP(t, f, path, token, raw)
	require.Equal(t, 200, code, string(again))
	require.JSONEq(t, string(first), string(again))
	code, body := resourceHTTP(t, f, path, other.LeaseToken(), raw)
	require.NotEqual(t, 200, code, string(body))
	require.NotContains(t, string(body), `"wakeAt"`)
	// An owner or different delegation is a different authenticated request body.
	code, body = resourceHTTP(t, f, path, f.owner.(*remoteExecutionLease).LeaseToken(), raw)
	require.NotEqual(t, 200, code, string(body))
	req.RequestID = "fresh"
	raw, _ = json.Marshal(req)
	code, body = resourceHTTP(t, f, path, token, raw)
	require.NotEqual(t, 200, code, string(body))
}
func TestResourceBackoffHTTPStrictBodyB03d(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	req := resourceRequest(t, f)
	raw, _ := json.Marshal(req)
	path := delegationBase(f.owner) + "/resource-backoff"
	token := f.owner.(*remoteExecutionLease).LeaseToken()
	for _, body := range []string{strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1), strings.Replace(string(raw), `"growth":2`, `"growth":2,"other":1`, 1), strings.Replace(string(raw), `"growth":2`, `"growth":null`, 1), strings.Replace(string(raw), `"jitter":0.2`, `"jitter":0.2,"crashAllowance":null`, 1), strings.Replace(string(raw), `"policy":{"version":1`, `"policy":{"version":2`, 1), strings.Replace(string(raw), `"jitter":0.2`, `"jitter":0.2,"crashAllowance":9223372036854775808`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"Version":1`, 1)} {
		status, response := resourceHTTP(t, f, path, token, []byte(body))
		require.Equal(t, 400, status, string(response))
	}
	status, response := resourceHTTP(t, f, path, token, raw)
	require.Equal(t, 200, status, string(response))
}
func TestResourceBackoffHTTPStableAuthorityAdvertisement(t *testing.T) {
	f := newDelegationFixture(t, "sqlite", nil)
	require.Equal(t, 0, f.owner.ExecutionState().ResourceBackoff.DurabilityVersion)
	ctx := celltest.Context(context.Background())
	info, err := f.client.GetJob(ctx, f.owner.Job().JobKey)
	require.NoError(t, err)
	require.Equal(t, 0, info.ExecutionState.ResourceBackoff.DurabilityVersion)
	list, err := f.client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{f.owner.Job().JobKey.TenantId}})
	require.NoError(t, err)
	require.NotEmpty(t, list.Jobs)
	for _, job := range list.Jobs {
		require.Equal(t, 0, job.ExecutionState.ResourceBackoff.DurabilityVersion)
	}
	key := bytes.Repeat([]byte{7}, 32)
	handler, err := NewServerWithOptions(f.backend, ServerOptions{LeaseTokenSigningKey: key})
	require.NoError(t, err)
	server := httptest.NewServer(celltest.Handler(handler))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	lease := secretsRemoteLease(t, client, "another-tenant")
	require.Equal(t, 1, lease.ExecutionState().ResourceBackoff.DurabilityVersion)
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(celltest.Context(context.Background()), f.backend, lease, "resource/attempt:1", 1, jobdb.DefaultResourceBackoffPolicy())
	require.NoError(t, err)
	receipt, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(context.Background(), req)
	require.NoError(t, err)
	// A fresh server with the same authority replays the exact retained receipt.
	handler, err = NewServerWithOptions(f.backend, ServerOptions{LeaseTokenSigningKey: key})
	require.NoError(t, err)
	other := httptest.NewServer(celltest.Handler(handler))
	defer other.Close()
	copyFixture := *f
	copyFixture.server = other
	raw, _ := json.Marshal(req)
	code, body := resourceHTTP(t, &copyFixture, fmt.Sprintf("/v1/tenants/%s/jobs/%s/leases/%s/resource-backoff", lease.Job().JobKey.TenantId, lease.Job().JobKey.JobId, lease.LeaseID()), lease.(*remoteExecutionLease).LeaseToken(), raw)
	require.Equal(t, 200, code, string(body))
	var again jobdb.ResourceBackoffReceipt
	require.NoError(t, json.Unmarshal(body, &again))
	require.Equal(t, receipt, again)
}
