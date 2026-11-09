package remote

import (
	"context"
	"errors"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaseEventHTTPAuthorizationAndValidation(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var rt jobdb.WorkflowRuntime = toy.New()
			ctx := celltest.Context(context.Background())
			if kind == "sqlite" {
				embedded, e := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: t.TempDir() + "/jobdb.db"})
				require.NoError(t, e)
				defer embedded.Close(ctx)
				rt = embedded
			}
			h, e := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, e)
			l, e := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
			require.NoError(t, e)
			require.NoError(t, l.KeepAlive(ctx))
			broad, _ := jobdb.CellAuthorityFromContext(ctx)
			var authority atomic.Value
			authority.Store(broad)
			handler := NewServer(rt)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r.WithContext(jobdb.WithCellAuthority(r.Context(), authority.Load().(jobdb.CellAuthority))))
			}))
			defer server.Close()
			client, e := New(server.URL, server.Client())
			require.NoError(t, e)
			q := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 1}
			page, e := client.ListLeaseEvents(ctx, q)
			require.NoError(t, e)
			require.NotEmpty(t, page.NextPageToken)
			denied := jobdb.CellAuthority{Principal: broad.Principal, TenantID: "tenant", Permissions: map[string]bool{"jobs.list": true}, Cells: map[string]bool{}}
			authority.Store(denied)
			q.PageToken = page.NextPageToken
			_, e = client.ListLeaseEvents(ctx, q)
			require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventPageToken)
			q.PageToken = ""
			page, e = client.ListLeaseEvents(ctx, q)
			require.NoError(t, e)
			require.Empty(t, page.Events)
			q.CellNames = []string{"test"}
			_, e = client.ListLeaseEvents(ctx, q)
			require.ErrorIs(t, e, jobdb.ErrCellNotFound)
			q.CellNames = nil
			q.TenantID = "other"
			_, e = client.ListLeaseEvents(ctx, q)
			require.ErrorIs(t, e, jobdb.ErrCellForbidden)
			denied.Permissions = map[string]bool{}
			authority.Store(denied)
			q.TenantID = "tenant"
			_, e = client.ListLeaseEvents(ctx, q)
			require.ErrorIs(t, e, jobdb.ErrCellForbidden)
			authority.Store(broad)
			for _, field := range []string{"cellNames", "cellIds", "jobIds", "goalIds", "workerIds", "leaseIds", "eventTypes", "jobTypes", "routes"} {
				for _, value := range []string{"null", "[]", "[ ]"} {
					request, e := http.NewRequest(http.MethodPost, server.URL+"/v1/tenants/tenant/lease-events/query", strings.NewReader(fmt.Sprintf(`{"%s":%s}`, field, value)))
					require.NoError(t, e)
					request.Header.Set("Content-Type", "application/json")
					resp, e := server.Client().Do(request)
					require.NoError(t, e)
					require.Equal(t, 400, resp.StatusCode)
					resp.Body.Close()
				}
			}
			for _, body := range []string{`{"tenantId":"other"}`, `{"unknown":1}`, `{"eventTypes":["lease_expired"]}`, `{"pageSize":0}`, `{"pageSize":201}`, `{"occurredAfter":"bad"}`, `{"routes":[{"jobType":"job","unknown":1}]}`, `{"cellIds":["id"],"cellNames":["test"]}`} {
				req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant/lease-events/query", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				celltest.Handler(handler).ServeHTTP(w, req)
				require.Equal(t, 400, w.Code, body)
				require.Contains(t, w.Body.String(), "invalid_request")
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant/lease-events/query", strings.NewReader(`{}`))
			req.Header.Set("X-JobDB-Lease-Token", "jdb-m2.delegated")
			w := httptest.NewRecorder()
			celltest.Handler(handler).ServeHTTP(w, req)
			require.Equal(t, 403, w.Code)
			require.Contains(t, w.Body.String(), "lease_permission_denied")
			// Unknown uncatalogued tenants read empty without creating a catalog key.
			page, e = client.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "empty-tenant"})
			require.NoError(t, e)
			require.Empty(t, page.Events)
			require.Empty(t, page.NextPageToken)
			_, e = client.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "empty-tenant", CellNames: []string{"test"}})
			require.ErrorIs(t, e, jobdb.ErrCellNotFound)
		})
	}
}

type failedLeaseEventsRuntime struct {
	calls int
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
}

func (r *failedLeaseEventsRuntime) ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	r.calls++
	return jobdb.ListLeaseEventsResponse{}, errors.New("injected backend failure")
}
func TestLeaseEventBackendFailureIsNotEmpty(t *testing.T) {
	backend := &failedLeaseEventsRuntime{}
	server := httptest.NewServer(celltest.Handler(NewServer(backend)))
	defer server.Close()
	client, e := New(server.URL, server.Client())
	require.NoError(t, e)
	_, e = client.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.Error(t, e)
	require.Equal(t, 1, backend.calls)
}

func TestLeaseEventHTTPRejectsBeforeBackendRead(t *testing.T) {
	backend := &failedLeaseEventsRuntime{}
	server := httptest.NewServer(NewServer(backend))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	_, err = client.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	require.Zero(t, backend.calls)
	// A tenant-bound authority cannot read another tenant, even through an adapter
	// whose backend would otherwise accept the request.
	handler := NewServer(backend)
	authority, _ := jobdb.CellAuthorityFromContext(celltest.Context(context.Background()))
	authority.TenantID = "tenant"
	scoped := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(jobdb.WithCellAuthority(r.Context(), authority)))
	}))
	defer scoped.Close()
	client, err = New(scoped.URL, scoped.Client())
	require.NoError(t, err)
	_, err = client.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "other"})
	require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	require.Zero(t, backend.calls)
}

func TestLeaseEventDelegationActionsDoNotRecord(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			f := newDelegationFixture(t, kind, nil)
			ctx := celltest.Context(context.Background())
			q := jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{f.owner.Job().JobKey.JobId}}
			before, e := f.client.ListLeaseEvents(ctx, q)
			require.NoError(t, e)
			require.Len(t, before.Events, 1)
			delegated := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			_, e = f.client.CreateDelegation(ctx, delegated, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			require.NoError(t, e)
			capability, e := ExportLease(delegated)
			require.NoError(t, e)
			imported, e := f.client.ImportLease(ctx, capability)
			require.NoError(t, e)
			_, e = imported.Renew(ctx)
			require.NoError(t, e)
			require.NoError(t, delegated.KeepAlive(ctx))
			f.validity(t)
			after, e := f.client.ListLeaseEvents(ctx, q)
			require.NoError(t, e)
			require.Equal(t, before, after)
		})
	}
}
