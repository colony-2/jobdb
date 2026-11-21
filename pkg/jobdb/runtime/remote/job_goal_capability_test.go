package remote

import (
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type unknownGoalRuntime struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
}

type selectionOnlyGoalRuntime struct{ unknownGoalRuntime }

func (selectionOnlyGoalRuntime) SupportsPrimaryGoalSelection() bool { return true }

type explicitGoalRuntime struct {
	unknownGoalRuntime
	capabilities jobdb.JobGoalCapabilities
}

func (r explicitGoalRuntime) JobGoalCapabilities() jobdb.JobGoalCapabilities {
	return r.capabilities
}

func TestJobGoalCapabilityHTTPForwarding(t *testing.T) {
	for bits := range 8 {
		t.Run(strconv.Itoa(bits), func(t *testing.T) {
			want := jobdb.JobGoalCapabilities{
				ImmutableMembership:    bits&1 != 0,
				AtomicAdmission:        bits&2 != 0,
				TerminalFailureRecords: bits&4 != 0,
			}
			// Every other runtime method would panic: discovery is static and
			// must not inspect goals, active counts or receipt eligibility.
			server := httptest.NewServer(NewServer(explicitGoalRuntime{capabilities: want}))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			got, err := rt.GetJobGoalCapabilities(context.Background())
			require.NoError(t, err)
			require.Equal(t, want, got)
			response, err := server.Client().Get(server.URL + "/v1/capabilities/job-goals")
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
			var wire map[string]bool
			require.NoError(t, json.NewDecoder(response.Body).Decode(&wire))
			require.Equal(t, map[string]bool{
				"immutableMembership":    want.ImmutableMembership,
				"atomicAdmission":        want.AtomicAdmission,
				"terminalFailureRecords": want.TerminalFailureRecords,
			}, wire)
		})
	}
}

func TestJobGoalCapabilityHTTPDoesNotInferSelectionSupport(t *testing.T) {
	embedded, err := sqlite.StartEmbeddedRuntime(context.Background())
	require.NoError(t, err)
	defer embedded.Shutdown()
	for name, backend := range map[string]jobdb.WorkflowRuntime{
		"unknown":        unknownGoalRuntime{},
		"selection-only": selectionOnlyGoalRuntime{},
		"toy":            toy.New(),
		"sqlite":         embedded.Runtime,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(NewServer(backend))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			got, err := rt.GetJobGoalCapabilities(context.Background())
			require.NoError(t, err)
			want := jobdb.JobGoalCapabilities{}
			if name == "toy" || name == "sqlite" {
				want = jobdb.JobGoalCapabilities{ImmutableMembership: true, AtomicAdmission: true, TerminalFailureRecords: true}
			}
			require.Equal(t, want, got)
		})
	}
}

func TestJobGoalCapabilityHTTPUnknownIsUnsupported(t *testing.T) {
	for _, status := range []int{404, 405, 501, 301, 302, 303, 307, 308} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/v1/capabilities/job-goals", r.URL.Path)
				w.Header().Set("Location", "/legacy")
				w.WriteHeader(status)
			}))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			got, err := rt.GetJobGoalCapabilities(context.Background())
			require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
			require.Equal(t, jobdb.JobGoalCapabilities{}, got)
			require.EqualValues(t, 1, requests.Load())
		})
	}
}

func TestJobGoalCapabilityHTTPMissingAndFailedResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   jobdb.JobGoalCapabilities
		fails  bool
	}{
		{"empty", 200, `{}`, jobdb.JobGoalCapabilities{}, false},
		{"selection-is-not-admission", 200, `{"supported":true}`, jobdb.JobGoalCapabilities{}, false},
		{"partial", 200, `{"atomicAdmission":true}`, jobdb.JobGoalCapabilities{AtomicAdmission: true}, false},
		{"false", 200, `{"immutableMembership":false,"atomicAdmission":false,"terminalFailureRecords":false}`, jobdb.JobGoalCapabilities{}, false},
		{"malformed", 200, `{`, jobdb.JobGoalCapabilities{}, true},
		{"wrong-type", 200, `{"atomicAdmission":"true"}`, jobdb.JobGoalCapabilities{}, true},
		{"denied", 403, `{"message":"denied"}`, jobdb.JobGoalCapabilities{}, true},
		{"unavailable", 503, `{"message":"storage unavailable"}`, jobdb.JobGoalCapabilities{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			got, err := rt.GetJobGoalCapabilities(context.Background())
			if tc.fails {
				require.Error(t, err)
				require.NotErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

type operationGoalRuntime struct {
	explicitGoalRuntime
	calls atomic.Int32
}

func (*operationGoalRuntime) SupportsPrimaryGoalSelection() bool { return true }
func (r *operationGoalRuntime) SubmitJob(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	r.calls.Add(1)
	return jobdb.JobHandle{}, jobdb.ErrGoalConflict
}
func (r *operationGoalRuntime) SubmitRestartJob(context.Context, jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	r.calls.Add(1)
	return jobdb.JobHandle{}, jobdb.ErrGoalConflict
}
func (r *operationGoalRuntime) FailGoal(context.Context, jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	r.calls.Add(1)
	return jobdb.GoalInfo{}, jobdb.ErrGoalConflict
}

func TestJobGoalOperationCapabilityEnforcement(t *testing.T) {
	for bits := range 8 {
		t.Run(strconv.Itoa(bits), func(t *testing.T) {
			caps := jobdb.JobGoalCapabilities{ImmutableMembership: bits&1 != 0, AtomicAdmission: bits&2 != 0, TerminalFailureRecords: bits&4 != 0}
			backend := &operationGoalRuntime{explicitGoalRuntime: explicitGoalRuntime{capabilities: caps}}
			application := NewServer(backend)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				application.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
			}))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
			_, err = rt.SubmitJob(context.Background(), req)
			expected := jobdb.ErrJobGoalSemanticsUnsupported
			admitted := caps.ImmutableMembership && caps.AtomicAdmission
			if admitted {
				expected = jobdb.ErrGoalConflict
			}
			require.ErrorIs(t, err, expected)
			_, err = rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{RequestID: "restart", Job: jobdb.SubmitRestartJob{PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "prior"}, JobID: "dest", Cell: "work", ShareGoal: true}})
			require.ErrorIs(t, err, expected)
			failure := jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: "goal"}, Message: "reason"}
			_, err = rt.FailGoal(context.Background(), failure)
			expected = jobdb.ErrJobGoalSemanticsUnsupported
			if caps.TerminalFailureRecords {
				expected = jobdb.ErrGoalConflict
			}
			require.ErrorIs(t, err, expected)
			wantCalls := int32(0)
			if admitted {
				wantCalls += 2
			}
			if caps.TerminalFailureRecords {
				wantCalls++
			}
			require.Equal(t, wantCalls, backend.calls.Load())
			// Bypass client discovery with actual wire requests. The server must gate
			// each operation itself; a selection-only client cannot bypass admission.
			for _, wire := range []struct {
				path, body string
				allowed    bool
			}{
				{"/v1/tenants/tenant/jobs/dest/primary-goal", `{"goalId":"goal","submission":{"job":{"cell":"work","cellRepo":"example.test/work","jobType":"work"}}}`, admitted},
				{"/v1/tenants/tenant/jobs/dest/restart", `{"job":{"priorJobKey":{"tenantId":"tenant","jobId":"prior"},"lastStepToKeep":0,"cell":"work","shareGoal":true}}`, admitted},
				{"/v1/tenants/tenant/goals/goal/fail", `{"message":"reason"}`, caps.TerminalFailureRecords},
			} {
				// Unsupported cases prove rejection without calling any runtime method.
				if wire.allowed {
					continue
				}
				method := http.MethodPut
				if strings.HasSuffix(wire.path, "/fail") {
					method = http.MethodPost
				}
				request, err := http.NewRequest(method, server.URL+wire.path, strings.NewReader(wire.body))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				response, err := server.Client().Do(request)
				require.NoError(t, err)
				raw, err := io.ReadAll(response.Body)
				response.Body.Close()
				require.NoError(t, err)
				require.Equal(t, http.StatusNotImplemented, response.StatusCode, string(raw))
				require.Contains(t, string(raw), "job_goal_semantics_unsupported")
			}
			require.Equal(t, wantCalls, backend.calls.Load())
		})
	}
}
