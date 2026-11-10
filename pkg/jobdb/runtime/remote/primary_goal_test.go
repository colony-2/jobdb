package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

// A legacy server can accept arbitrary JSON on ordinary submission. The
// selected client must not probe support by attempting a mutating ordinary call.
func TestPrimaryGoalLegacyServerNeverReceivesOrdinarySubmit(t *testing.T) {
	var mutations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			mutations.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	_, err = rt.SubmitJob(context.Background(), req)
	require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
	require.Zero(t, mutations.Load())
}

func TestPrimaryGoalOrdinaryLeaseEncodingRejectsSelection(t *testing.T) {
	_, err := submitJobRequestToAPI(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal"}})
	require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
}

func TestPrimaryGoalHTTPConformance(t *testing.T) {
	for _, name := range []string{"toy", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			var backend jobdb.WorkflowRuntime = toy.New()
			if name == "sqlite" {
				embedded, err := sqlite.StartEmbeddedRuntime(context.Background())
				require.NoError(t, err)
				defer embedded.Shutdown()
				backend = embedded.Runtime
			}
			application := NewServer(backend)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			runtimetest.RunPrimaryGoalSelectionConformance(t, rt)
		})
	}
}

func TestPrimaryGoalRoutingChangesAndRedirects(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var ordinary atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/capabilities/primary-goal-selection" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"supported":true}`))
					return
				}
				if strings.HasSuffix(r.URL.Path, "/primary-goal") {
					w.Header().Set("Location", "/v1/tenants/tenant/jobs/dest")
					w.WriteHeader(status)
					return
				}
				ordinary.Add(1)
				w.WriteHeader(200)
			}))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			_, err = rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
			require.Zero(t, ordinary.Load())
		})
	}
}

func TestPrimaryGoalHTTPAuthorityAndLostResponse(t *testing.T) {
	for _, name := range []string{"toy", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			var backend jobdb.WorkflowRuntime = toy.New()
			if name == "sqlite" {
				embedded, err := sqlite.StartEmbeddedRuntime(context.Background())
				require.NoError(t, err)
				defer embedded.Shutdown()
				backend = embedded.Runtime
			}
			ctx := runtimetest.PrimaryGoalContext(context.Background())
			source, err := celltest.Submit(backend, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			var denied, lose atomic.Bool
			handler := NewServer(backend)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authorized := jobdb.WithPrimaryGoalSelectionAuthorizer(celltest.Context(r.Context()), func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error {
					if denied.Load() {
						return jobdb.ErrPrimaryGoalSelectionForbidden
					}
					return nil
				})
				if strings.HasSuffix(r.URL.Path, "/primary-goal") && lose.CompareAndSwap(true, false) {
					recorder := httptest.NewRecorder()
					handler.ServeHTTP(recorder, r.WithContext(authorized))
					require.Equal(t, 200, recorder.Code)
					conn, _, e := w.(http.Hijacker).Hijack()
					if e == nil {
						_ = conn.Close()
					}
					return
				}
				handler.ServeHTTP(w, r.WithContext(authorized))
			}))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: source.GoalID, Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic("fresh")}}
			denied.Store(true)
			_, err = rt.SubmitJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionForbidden)
			denied.Store(false)
			lose.Store(true)
			_, err = rt.SubmitJob(ctx, req)
			require.Error(t, err)
			_, err = backend.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: source.GoalID}, Message: "closed"})
			require.NoError(t, err)
			expected, err := backend.SubmitJob(ctx, req)
			require.NoError(t, err)
			replay, err := rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, expected, replay)
			denied.Store(true)
			_, err = rt.SubmitJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionForbidden)
		})
	}
}

type noPrimaryGoalRuntime struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
	calls atomic.Int32
}

func (r *noPrimaryGoalRuntime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	r.calls.Add(1)
	return r.WorkflowRuntime.SubmitJob(ctx, req)
}
func TestPrimaryGoalUnsupportedBackendAndProtectedIngress(t *testing.T) {
	backend := toy.New()
	unsupported := &noPrimaryGoalRuntime{WorkflowRuntime: backend, ConfigurationAdmin: backend}
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	ordinary := req
	ordinary.Job.GoalID = ""
	body, err := submitJobRequestToAPI(context.Background(), ordinary)
	require.NoError(t, err)
	raw, err := json.Marshal(runtimeapi.SelectedSubmitJobRequest{GoalId: "goal", Submission: body})
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		runtime jobdb.WorkflowRuntime
		hook    bool
		token   string
		status  int
	}{
		{name: "unsupported-backend", runtime: unsupported, hook: true, status: 501},
		{name: "submit-only", runtime: backend, status: 403},
		{name: "lease-token", runtime: backend, hook: true, token: "opaque-lease-token", status: 501},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := NewServer(tc.runtime)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := celltest.Context(r.Context())
				if tc.hook {
					ctx = runtimetest.PrimaryGoalContext(r.Context())
				}
				handler.ServeHTTP(w, r.WithContext(ctx))
			}))
			defer server.Close()
			request, err := http.NewRequest(http.MethodPut, server.URL+"/v1/tenants/tenant/jobs/dest/primary-goal", bytes.NewReader(raw))
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/json")
			if tc.token != "" {
				request.Header.Set("X-JobDB-Lease-Token", tc.token)
			}
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, tc.status, response.StatusCode)
			if tc.name == "unsupported-backend" {
				rt, err := New(server.URL, server.Client())
				require.NoError(t, err)
				_, err = rt.SubmitJob(context.Background(), req)
				require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
			}
		})
	}
	require.Zero(t, unsupported.calls.Load())
}

func TestPrimaryGoalIncompleteSuccessIsAmbiguous(t *testing.T) {
	var destination string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"supported":true}`))
			return
		}
		destination = r.URL.Path
		_, _ = w.Write([]byte(`{"jobKey":{"tenantId":"tenant","jobId":"wrong"},"goalId":"goal","cell":"work","cellId":"stable","cellRepo":"example.test/work"}`))
	}))
	defer server.Close()
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	_, err = rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.ErrorIs(t, err, jobdb.ErrSubmissionReceiptUnavailable)
	require.Equal(t, "/v1/tenants/tenant/jobs/dest/primary-goal", destination)
}
