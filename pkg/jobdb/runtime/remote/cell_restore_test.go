package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

type restoreProjectGate struct {
	celltest.UnrestrictedProjects
	gate atomic.Pointer[func(context.Context, string) error]
}

func (projects *restoreProjectGate) set(gate func(context.Context, string) error) {
	projects.gate.Store(&gate)
}

func (projects *restoreProjectGate) AdmitWrite(ctx context.Context, tenant string) error {
	if gate := projects.gate.Load(); gate != nil && *gate != nil {
		return (*gate)(ctx, tenant)
	}
	return nil
}

func TestCellRestoreConformance(test *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, surface := range []string{"direct", "tenants", "projects"} {
			test.Run(backend+"/"+surface, func(test *testing.T) {
				projects := &restoreProjectGate{}
				fixture := runtimetest.CellRestoreFixture{WithAuthority: jobdb.WithCellAuthority, SetProjectAdmission: projects.set}
				var runtime jobdb.WorkflowRuntime
				if backend == "toy" {
					runtime = toy.New(toy.WithProjects(projects))
				} else {
					dir := test.TempDir()
					open := func() *sqlite.Runtime {
						store, err := sqlite.NewFromConfig(context.Background(), sqlite.Config{DBPath: filepath.Join(dir, "jobdb.db"), BlobDir: filepath.Join(dir, "blobs"), Projects: projects})
						require.NoError(test, err)
						return store
					}
					store := open()
					test.Cleanup(func() { require.NoError(test, store.Close(context.Background())) })
					runtime = store
					if surface == "direct" {
						fixture.Reopen = func() jobdb.ConsumerRuntime {
							require.NoError(test, store.Close(context.Background()))
							store = open()
							return store
						}
					}
				}
				fixture.Runtime = runtime
				if surface != "direct" {
					handler := NewServer(runtime)
					server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
						var authority jobdb.CellAuthority
						if err := json.Unmarshal([]byte(request.Header.Get("X-Test-Restore-Authority")), &authority); err != nil {
							http.Error(response, "invalid test authority fixture", 500)
							return
						}
						handler.ServeHTTP(response, request.WithContext(jobdb.WithCellAuthority(request.Context(), authority)))
					}))
					defer server.Close()
					client := server.Client()
					next := client.Transport
					client.Transport = restoreRoundTripper(func(req *http.Request) (*http.Response, error) {
						authority, _ := jobdb.CellAuthorityFromContext(req.Context())
						raw, err := json.Marshal(authority)
						if err != nil {
							return nil, err
						}
						req = req.Clone(req.Context())
						req.Header.Set("X-Test-Restore-Authority", string(raw))
						return next.RoundTrip(req)
					})
					newClient := New
					if surface == "projects" {
						newClient = NewProjectRuntime
					}
					var err error
					fixture.Runtime, err = newClient(server.URL, client)
					require.NoError(test, err)
				}
				runtimetest.RunCellRestoreConformance(test, fixture)
			})
		}
	}
}

type restoreRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip restoreRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

// A WorkflowRuntime and NewServer caller compiled before restore must still
// work without implementing or forwarding the new optional interface.
type restoreLegacyRuntime struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
}

var _ jobdb.WorkflowRuntime = restoreLegacyRuntime{}

func TestCellRestoreHTTPContract(test *testing.T) {
	for _, scope := range []string{"tenants", "projects"} {
		test.Run(scope, func(test *testing.T) {
			ctx := celltest.Context(context.Background())
			backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
			cell, err := backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "restore", Repo: "example.com/org/restore"})
			require.NoError(test, err)
			key := jobdb.CellKey{TenantID: cell.TenantID, CellID: cell.ID}
			retired, err := backend.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", cell.Revision)})
			require.NoError(test, err)
			path := "/v1/" + scope + "/tenant/cells/" + cell.ID + "/restore"
			etag := fmt.Sprintf("%q", retired.Revision)
			restoreCtx := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{
				Principal: "restorer", TenantID: key.TenantID,
				Permissions: map[string]bool{"cells.get": true, "cells.restore": true},
				Cells:       map[string]bool{key.CellID: true},
			})
			req := func(handler http.Handler, body io.Reader, tags ...string) *httptest.ResponseRecorder {
				request := httptest.NewRequestWithContext(restoreCtx, http.MethodPost, path, body)
				for _, tag := range tags {
					request.Header.Add("If-Match", tag)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				return response
			}
			embedded := NewServer(backend)
			for _, check := range []struct {
				body io.Reader
				tags []string
			}{
				{nil, []string{etag, etag}},
				{nil, []string{""}},
				{strings.NewReader(`{}`), []string{etag}},
			} {
				response := req(embedded, check.body, check.tags...)
				require.Equal(test, 400, response.Code, response.Body.String())
			}
			legacy := restoreLegacyRuntime{WorkflowRuntime: backend, ConfigurationAdmin: backend}
			require.NotImplements(test, (*jobdb.CellRestoreRuntime)(nil), legacy)
			unsupported := req(NewServer(legacy), nil, etag)
			require.Equal(test, 501, unsupported.Code)
			require.JSONEq(test, `{"code":"cell_restore_unsupported","message":"cell_restore_unsupported"}`, unsupported.Body.String())
			stillRetired, err := backend.GetCell(ctx, key)
			require.NoError(test, err)
			require.Equal(test, retired, stillRetired)
			_, err = backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "worker", Repo: "example.com/org/worker"})
			require.NoError(test, err)
			job, err := backend.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "worker", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(test, err)
			server := httptest.NewServer(celltest.Handler(embedded))
			defer server.Close()
			client, err := New(server.URL, server.Client())
			require.NoError(test, err)
			lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}})
			require.NoError(test, err)
			require.NotNil(test, lease)
			token := lease.(interface{ LeaseToken() string }).LeaseToken()
			require.NotEmpty(test, token)
			unauthorized := httptest.NewRequest(http.MethodPost, path, nil)
			unauthorized.Header.Set("If-Match", etag)
			unauthorized.Header.Set("X-JobDB-Lease-Token", token)
			denied := httptest.NewRecorder()
			embedded.ServeHTTP(denied, unauthorized)
			require.Equal(test, 403, denied.Code, denied.Body.String())

			ok := req(embedded, nil, etag)
			require.Equal(test, 200, ok.Code, ok.Body.String())
			var result map[string]any
			require.NoError(test, json.Unmarshal(ok.Body.Bytes(), &result))
			require.NotContains(test, result, "retiredAt")
			require.Equal(test, cell.ID, result["id"])
			require.Equal(test, "ACTIVE", result["state"])
			newETag := ok.Header().Get("ETag")
			require.Equal(test, fmt.Sprintf("%q", result["revision"]), newETag)
			require.NotEqual(test, etag, newETag)
			scopeField := "tenantId"
			if scope == "projects" {
				scopeField = "projectId"
				require.NotContains(test, result, "tenantId")
			}
			require.Equal(test, "tenant", result[scopeField])
			current := req(embedded, nil, newETag)
			require.Equal(test, 200, current.Code)
			require.Equal(test, newETag, current.Header().Get("ETag"))
			require.JSONEq(test, ok.Body.String(), current.Body.String())
		})
	}
}

func TestCellRestoreClientDoesNotFallback(test *testing.T) {
	for _, scope := range []string{"tenants", "projects"} {
		for _, status := range []int{404, 501} {
			test.Run(fmt.Sprintf("%s/%d", scope, status), func(test *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
					calls.Add(1)
					if request.Method != http.MethodPost || request.URL.Path != "/v1/"+scope+"/tenant/cells/cell/restore" || request.ContentLength != 0 || request.Header.Get("If-Match") != `"current"` {
						test.Errorf("unexpected request %s %s (length %d; If-Match %q)", request.Method, request.URL.Path, request.ContentLength, request.Header.Get("If-Match"))
					}
					if status == 404 {
						http.NotFound(response, request)
						return
					}
					writeCellError(response, jobdb.ErrCellRestoreUnsupported)
				}))
				defer server.Close()
				newClient := New
				if scope == "projects" {
					newClient = NewProjectRuntime
				}
				client, err := newClient(server.URL, server.Client())
				require.NoError(test, err)
				var restore jobdb.CellRestoreRuntime = client
				_, err = restore.RestoreCell(context.Background(), jobdb.RestoreCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: "cell"}, IfMatch: `"current"`})
				if status == 501 {
					require.ErrorIs(test, err, jobdb.ErrCellRestoreUnsupported)
				} else {
					require.ErrorContains(test, err, "HTTP 404")
					require.NotErrorIs(test, err, jobdb.ErrCellNotFound)
				}
				require.EqualValues(test, 1, calls.Load())
			})
		}
	}
}
