package remote

import (
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
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestCellValidationRouteScope(t *testing.T) {
	for _, path := range []string{
		"/cells",
		"/v1/tenants/t/cells-other",
		"/v1/tenants/cells-example/jobs/query",
		"/v1/tenants/cells/jobs/query",
		"/v1/tenants/t/jobs/cells-example/lease",
		"/v1/tenants/t/jobs/cells/lease",
		"/v1/tenants/t/jobs/cells/leases/l/jobs",
		"/v1/tenants/t/jobs/j/leases/cells/jobs",
		"/v1/tenants/t%2Fcells/jobs/query",
		"/v1/tenants/t/jobs/j%2Fcells/lease",
	} {
		t.Run(path, func(t *testing.T) {
			const body = `{"optional":null}`
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			w := httptest.NewRecorder()
			require.True(t, prepareCellRequest(w, req), w.Body.String())
			got, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, body, string(got))
		})
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1/tenants/cells-example/cells"},
		{http.MethodPatch, "/v1/tenants/cells/cells/id"},
		{http.MethodPost, "/v1/tenants/cells/jobs/cells/leases/l/cells/resolve"},
		{http.MethodPost, "/v1/tenants/cells/jobs/cells/leases/l/cells"},
		{http.MethodPost, "/v1/tenants/t%2Fcells/cells"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"name":null}`))
			w := httptest.NewRecorder()
			require.False(t, prepareCellRequest(w, req))
			require.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestCellNamesDoNotInterceptJobRPCs(t *testing.T) {
	for _, tc := range []struct{ tenant, job string }{
		{"registry-example", "example"},
		{"cells-example", "example"},
		{"cells", "example"},
		{"registry-example", "cells-example"},
		{"registry-example", "cells"},
	} {
		t.Run(tc.tenant+"/"+tc.job, func(t *testing.T) {
			server := httptest.NewServer(celltest.Handler(NewServer(toy.New(toy.WithProjects(celltest.UnrestrictedProjects{})))))
			defer server.Close()
			client, err := New(server.URL, server.Client())
			require.NoError(t, err)
			ctx := context.Background()
			cell, err := client.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tc.tenant, Name: "example", Repo: "github.com/acme/example"})
			require.NoError(t, err)
			listed, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tc.tenant}})
			require.NoError(t, err)
			require.Empty(t, listed.Jobs)
			pending, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tc.tenant}, JobTasks: []jobdb.JobTaskFilter{{JobType: "job", TaskType: "input"}}})
			require.NoError(t, err)
			require.Empty(t, pending.Jobs)
			h, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tc.tenant, JobID: tc.job, Cell: cell.Name, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			waits, err := client.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: tc.tenant, ConsumerJobID: tc.job})
			require.NoError(t, err)
			require.Empty(t, waits.Waits)
			lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
			require.NoError(t, err)
			require.NotNil(t, lease)
			resolved, err := lease.ResolveCell(ctx, cell.Name)
			require.NoError(t, err)
			require.Equal(t, cell.ID, resolved.CellID)
		})
	}
}

func TestCellHTTPPatchValidation(t *testing.T) {
	rt := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
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
	handler := NewServer(toy.New(toy.WithProjects(celltest.UnrestrictedProjects{})))
	for _, method := range []string{http.MethodGet, http.MethodPatch} {
		req := httptest.NewRequest(method, "/v1/tenants/t/cells/missing", strings.NewReader(`{"description":"new"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		require.Equal(t, 403, w.Code, w.Body.String())
	}
}

func TestCellArchiveRoutesRemoved(t *testing.T) {
	backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
	ctx := celltest.Context(context.Background())
	handler := celltest.Handler(NewServer(backend))
	const scope = "test"
	first, err := backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: scope, Name: "one", Repo: "example.test/one"})
	require.NoError(t, err)
	second, err := backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: scope, Name: "two", Repo: "example.test/two"})
	require.NoError(t, err)
	key := jobdb.CellKey{TenantID: scope, CellID: first.ID}
	for _, family := range []string{"tenants", "projects"} {
		t.Run(family, func(t *testing.T) {
			base := "/v1/" + family + "/" + scope + "/cells"
			for _, action := range []string{"archive", "unarchive"} {
				request := httptest.NewRequest(http.MethodPost, base+"/"+first.ID+"/"+action, nil)
				request.Header.Set("If-Match", fmt.Sprintf("%q", first.Revision))
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
				stored, err := backend.GetCell(ctx, key)
				require.NoError(t, err)
				require.Equal(t, first, stored)
			}
			list := func(path string) (string, string) {
				t.Helper()
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				var page struct {
					Cells []map[string]json.RawMessage `json:"cells"`
					Next  string                       `json:"nextPageToken"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
				require.Len(t, page.Cells, 1)
				require.NotContains(t, page.Cells[0], "archivedAt")
				require.JSONEq(t, `"ACTIVE"`, string(page.Cells[0]["state"]))
				var id string
				require.NoError(t, json.Unmarshal(page.Cells[0]["id"], &id))
				return id, page.Next
			}
			query := base + "?state=ACTIVE&pageSize=1"
			firstID, next := list(query)
			require.NotEmpty(t, next)
			secondID, next := list(query + "&pageToken=" + next)
			require.Empty(t, next)
			require.ElementsMatch(t, []string{first.ID, second.ID}, []string{firstID, secondID})
		})
	}
}

func TestCellRetirementKeepsHistory(t *testing.T) {
	backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
	ctx := celltest.Context(context.Background())
	handler := NewServer(backend)
	create := jobdb.CreateCellRequest{TenantID: "test", Name: "one", Repo: "example.test/one"}
	cell, err := backend.CreateCell(ctx, create)
	require.NoError(t, err)
	submit := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: create.TenantID, Cell: create.Name, CellRepo: create.Repo, JobType: "work",
		JobID: "history", Data: jobdb.NewTaskDataOrPanic("in"),
	}}
	historical, err := backend.SubmitJob(ctx, submit)
	require.NoError(t, err)
	retire := func(etag string, authorized bool) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1/tenants/test/cells/"+cell.ID+"/retire", nil)
		if etag != "" {
			request.Header.Set("If-Match", etag)
		}
		response := httptest.NewRecorder()
		if authorized {
			celltest.Handler(handler).ServeHTTP(response, request)
		} else {
			handler.ServeHTTP(response, request)
		}
		return response
	}
	etag := fmt.Sprintf("%q", cell.Revision)
	for _, failure := range []struct {
		etag       string
		authorized bool
		want       *jobdb.CellError
	}{
		{etag, false, jobdb.ErrCellForbidden},
		{"", true, jobdb.ErrCellPreconditionRequired},
		{`"stale"`, true, jobdb.ErrCellPrecondition},
		{etag, true, jobdb.ErrCellBusy},
	} {
		response := retire(failure.etag, failure.authorized)
		require.Equal(t, failure.want.Status, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), failure.want.Code)
	}
	goalComplete(t, backend, historical, true)
	response := retire(etag, true)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var retired jobdb.Cell
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &retired))
	require.Equal(t, cell.ID, retired.ID)
	require.Equal(t, jobdb.CellRetired, retired.State)
	require.NotContains(t, response.Body.String(), `"archivedAt"`)
	_, err = backend.GetChapter(ctx, jobdb.ChapterRef{JobKey: historical.JobKey, Ordinal: 1})
	require.NoError(t, err)
	submit.Job.JobID = "new"
	_, err = backend.SubmitJob(ctx, submit)
	require.ErrorIs(t, err, jobdb.ErrCellRetired)
}

func TestCellDelegationCeilings(t *testing.T) {
	backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
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
	policy := jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true}, CellNames: &jobdb.DelegationNames{Names: []string{"root"}}}}
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
	created, err := child.SubmitJob(ctx, submit("root"))
	require.NoError(t, err)
	require.Equal(t, root.ID, created.CellID)
	_, err = child.ResolveCell(ctx, "denied")
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

func TestDelegatedCellCreation(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			f := newDelegationFixture(t, backend, nil)
			ctx := context.Background()
			request := func(name string) jobdb.CreateCellRequest {
				return jobdb.CreateCellRequest{Name: name, Repo: "github.com/org/" + name, RequestID: name}
			}
			without := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			_, err := without.CreateCell(ctx, request("denied"))
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			all := jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Any: true}}
			_, err = f.client.CreateDelegation(ctx, without, jobdb.CreateDelegationRequest{Permissions: all})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			broad := f.create(t, jobdb.CreateDelegationRequest{Permissions: all})
			_, err = broad.CreateCell(ctx, request("unlisted"))
			require.NoError(t, err, "unrestricted creation permits any new name")
			policy := jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Names: []string{"allowed"}}}
			narrow, err := f.client.CreateDelegation(ctx, broad, jobdb.CreateDelegationRequest{Permissions: policy})
			require.NoError(t, err)
			_, err = f.client.CreateDelegation(ctx, narrow, jobdb.CreateDelegationRequest{Permissions: all})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			// The server must reject widening even if a client skips SDK checks.
			forged := appendTestRestriction(t, narrow.(*remoteExecutionLease).LeaseToken(), all, nil)
			status, _ := delegationHTTP(t, f, http.MethodPost, delegationBase(narrow)+"/cells", forged, request("denied"))
			require.NotEqual(t, http.StatusCreated, status)
			_, err = narrow.CreateCell(ctx, request("denied"))
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			created, err := narrow.CreateCell(ctx, request("allowed"))
			require.NoError(t, err)
			require.Equal(t, "tenant", created.TenantID)
			again, err := narrow.CreateCell(ctx, request("allowed"))
			require.NoError(t, err)
			require.Equal(t, created, again)
			// Dropping the action when delegating again cannot be undone.
			empty, err := f.client.CreateDelegation(ctx, narrow, jobdb.CreateDelegationRequest{})
			require.NoError(t, err)
			_, err = empty.CreateCell(ctx, request("allowed"))
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			_, err = f.client.CreateDelegation(ctx, empty, jobdb.CreateDelegationRequest{Permissions: policy})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			// Creating a cell does not grant jobs.submit to the delegation.
			_, err = narrow.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: created.Name, JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			_, err = f.client.CreateDelegation(ctx, f.owner, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{CreateCells: &jobdb.DelegationNames{Any: true, Names: []string{"invalid"}}}})
			require.Error(t, err)

			path := delegationBase(broad) + "/cells"
			for _, path := range []string{
				strings.Replace(path, "/tenants/tenant/", "/tenants/other/", 1),
				strings.Replace(path, "/leases/"+broad.LeaseID(), "/leases/other", 1),
			} {
				status, _ := delegationHTTP(t, f, http.MethodPost, path, broad.LeaseToken(), request("denied"))
				require.Equal(t, http.StatusUnauthorized, status)
			}
			status, _ = delegationHTTP(t, f, http.MethodPost, path, broad.LeaseToken(), map[string]any{"name": "denied", "repo": "github.com/org/denied", "metadata": nil})
			require.Equal(t, http.StatusBadRequest, status)
			status, _ = delegationHTTP(t, f, http.MethodPost, path, "invalid", request("denied"))
			require.NotEqual(t, http.StatusCreated, status)
			deadline := time.Now().Add(100 * time.Millisecond)
			expiring := f.create(t, jobdb.CreateDelegationRequest{Permissions: all, NotAfter: &deadline})
			time.Sleep(time.Until(deadline) + 10*time.Millisecond)
			_, err = expiring.CreateCell(ctx, request("denied"))
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			require.NoError(t, f.client.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: f.owner.Job().JobKey}))
			_, err = broad.CreateCell(ctx, request("denied"))
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			list, err := f.client.ListCells(ctx, jobdb.ListCellsRequest{TenantID: "tenant", Name: "denied"})
			require.NoError(t, err)
			require.Empty(t, list.Cells)
		})
	}
}

func TestCellSubmissionAllOrListed(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			f := newDelegationFixture(t, backend, nil)
			ctx := context.Background()
			for _, name := range []string{"c2j", "root"} {
				_, err := f.owner.CreateCell(ctx, jobdb.CreateCellRequest{Name: name, Repo: "github.com/colony-2/" + name})
				require.NoError(t, err)
			}
			policy := func(names *jobdb.DelegationNames) jobdb.DelegationPermissions {
				return jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{
					JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true}, CellNames: names,
				}}
			}
			all := f.create(t, jobdb.CreateDelegationRequest{Permissions: policy(nil)})
			listed, err := f.client.CreateDelegation(ctx, all, jobdb.CreateDelegationRequest{Permissions: policy(&jobdb.DelegationNames{Names: []string{"c2j"}})})
			require.NoError(t, err)
			inherited, err := f.client.CreateDelegation(ctx, listed, jobdb.CreateDelegationRequest{Permissions: policy(nil)})
			require.NoError(t, err)
			none := f.create(t, jobdb.CreateDelegationRequest{})
			empty := f.create(t, jobdb.CreateDelegationRequest{Permissions: policy(&jobdb.DelegationNames{})})
			for _, tc := range []struct {
				name    string
				lease   jobdb.ExecutionLease
				allowed map[string]bool
			}{
				{"owner", f.owner, map[string]bool{"test": true, "c2j": true, "root": true}},
				{"all", all, map[string]bool{"test": true, "c2j": true, "root": true}},
				{"listed", listed, map[string]bool{"c2j": true}},
				{"inherited", inherited, map[string]bool{"c2j": true}},
				{"no action", none, nil},
				{"empty list", empty, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for _, name := range []string{"test", "c2j", "root"} {
						resolved, err := tc.lease.ResolveCell(ctx, name)
						if tc.allowed[name] {
							require.NoError(t, err)
							require.Equal(t, name, resolved.Cell)
						} else {
							require.ErrorContains(t, err, "lease_permission_denied")
						}
						h, err := tc.lease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
							TenantId: "tenant", Cell: name, JobType: "child", Data: jobdb.NewTaskDataOrPanic(1),
						}})
						if tc.allowed[name] {
							require.NoError(t, err)
							require.Equal(t, name, h.Cell)
						} else {
							require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
						}
					}
				})
			}
			_, err = f.client.CreateDelegation(ctx, listed, jobdb.CreateDelegationRequest{Permissions: policy(&jobdb.DelegationNames{Any: true})})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
		})
	}
}
