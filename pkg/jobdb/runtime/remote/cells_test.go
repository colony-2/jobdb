package remote

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestCellHTTPPatchValidation(t *testing.T) {
	rt := toy.New()
	ctx := celltest.Context(context.Background())
	c, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: "root", Repo: "github.com/org/root"})
	require.NoError(t, err)
	handler := celltest.Handler(NewServer(rt))
	path := "/v1/tenants/t/cells/" + c.ID
	for _, tc := range []struct {
		name, body, etag string
		status           int
	}{
		{"missing precondition", `{"description":"new"}`, "", 428},
		{"weak precondition", `{"description":"new"}`, `W/"x"`, 400},
		{"stale precondition", `{"description":"new"}`, `"stale"`, 412},
		{"null metadata", `{"metadata":null}`, fmt.Sprintf("%q", c.Revision), 400},
		{"unknown field", `{"unknown":1}`, fmt.Sprintf("%q", c.Revision), 400},
		{"empty patch", `{}`, fmt.Sprintf("%q", c.Revision), 400},
		{"atomic root rename", `{"name":"renamed","repo":"github.com/org/other"}`, fmt.Sprintf("%q", c.Revision), 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPatch, path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			if tc.etag != "" {
				req.Header.Set("If-Match", tc.etag)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			got, err := rt.GetCell(ctx, jobdb.CellKey{TenantID: "t", CellID: c.ID})
			require.NoError(t, err)
			require.Equal(t, c, got)
		})
	}
	req := httptest.NewRequest(http.MethodPatch, path, strings.NewReader(`{"metadata":{"nested":[null,true,{"n":2}]}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", fmt.Sprintf("%q", c.Revision))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.NotEmpty(t, w.Header().Get("ETag"))
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
}

func TestCellHTTPRequiresAuthority(t *testing.T) {
	handler := NewServer(toy.New())
	for _, method := range []string{http.MethodGet, http.MethodPatch} {
		req := httptest.NewRequest(method, "/v1/tenants/t/cells/missing", strings.NewReader(`{"description":"new"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		require.Equal(t, 403, w.Code, w.Body.String())
	}
}

func TestRootCellDelegationCeilings(t *testing.T) {
	backend := toy.New()
	server := httptest.NewServer(celltest.Handler(NewServer(backend)))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	ctx := context.Background()
	root, err := client.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: "root", Repo: "github.com/org/root"})
	require.NoError(t, err)
	h, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "t", Cell: "root", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	owner, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	require.Equal(t, root.ID, owner.Job().CellID)
	policy := jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true}, CellNames: &jobdb.DelegationNames{Names: []string{"allowed"}}}}
	delegated, err := client.CreateDelegation(ctx, owner, jobdb.CreateDelegationRequest{Permissions: policy})
	require.NoError(t, err)
	inherited := jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true}}}
	child, err := client.CreateDelegation(ctx, delegated, jobdb.CreateDelegationRequest{Permissions: inherited})
	require.NoError(t, err)
	inherited.Submit.CellNames = &jobdb.DelegationNames{Any: true}
	_, err = client.CreateDelegation(ctx, delegated, jobdb.CreateDelegationRequest{Permissions: inherited})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	submit := func(name string) jobdb.SubmitJobRequest {
		return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "t", Cell: name, CellRepo: "github.com/org/" + name, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}}
	}
	_, err = child.SubmitJob(ctx, submit("denied"))
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	created, err := child.SubmitJob(ctx, submit("allowed"))
	require.NoError(t, err)
	require.NotEqual(t, root.ID, created.CellID)
	_, err = child.ResolveCell(ctx, "root")
	require.Error(t, err)
	require.NoError(t, client.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
	_, err = owner.SubmitJob(ctx, submit("lost-lease"))
	require.Error(t, err)
	list, err := client.ListCells(ctx, jobdb.ListCellsRequest{TenantID: "t", Name: "lost-lease"})
	require.NoError(t, err)
	require.Empty(t, list.Cells)
	capability, err := ExportLease(owner)
	require.NoError(t, err)
	raw, err := capability.Encode()
	require.NoError(t, err)
	_, err = DecodeLeaseCapability([]byte(strings.Replace(string(raw), `"version":2`, `"version":1`, 1)))
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}
