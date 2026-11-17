package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/stretchr/testify/require"
)

func groupedPolicy() jobdb.DelegationPermissions {
	return jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{
		JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true},
	}}
}
func groupedDelegate(t *testing.T, f *delegationFixture) *remoteExecutionLease {
	t.Helper()
	return f.create(t, jobdb.CreateDelegationRequest{Permissions: groupedPolicy()})
}

type groupedStoredState struct {
	Schemas  map[string]jobdb.ListJobSchemasResponse
	Jobs     []jobdb.JobSummary
	Chapters map[jobdb.JobKey][]jobdb.Chapter
	Goals    map[string]jobdb.ListGoalsResponse
}

func groupedState(t *testing.T, f *delegationFixture) groupedStoredState {
	t.Helper()
	ctx := celltest.Context(context.Background())
	page, err := f.backend.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant", "foreign"}, IncludeHidden: true})
	require.NoError(t, err)
	require.Empty(t, page.NextPageToken)
	state := groupedStoredState{Schemas: map[string]jobdb.ListJobSchemasResponse{}, Jobs: page.Jobs, Chapters: map[jobdb.JobKey][]jobdb.Chapter{}, Goals: map[string]jobdb.ListGoalsResponse{}}
	for _, j := range page.Jobs {
		chapters, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: j.JobKey})
		require.NoError(t, err)
		state.Chapters[j.JobKey] = chapters
	}
	for _, tenant := range []string{"tenant", "foreign"} {
		goals, err := f.backend.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: tenant})
		require.NoError(t, err)
		state.Goals[tenant] = goals
		schemas, err := f.backend.(jobdb.JobSchemaRegistry).ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: tenant})
		require.NoError(t, err)
		state.Schemas[tenant] = schemas
	}
	return state
}

// Count actual dispatch while retaining real backend semantics and validation.
type groupedDispatchProbe struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
	leaseOperationRuntime
	calls int
}

func (p *groupedDispatchProbe) ValidateExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, lease, worker string) (jobdb.LeaseValidity, error) {
	return p.WorkflowRuntime.(interface {
		ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
	}).ValidateExecutionLeaseByID(ctx, key, lease, worker)
}
func (p *groupedDispatchProbe) StartOrAppendJobWithLeaseByID(ctx context.Context, key jobdb.JobKey, lease, worker string, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	p.calls++
	return p.WorkflowRuntime.(interface {
		StartOrAppendJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error)
	}).StartOrAppendJobWithLeaseByID(ctx, key, lease, worker, q)
}
func groupedProbe(t *testing.T, f *delegationFixture) *groupedDispatchProbe {
	t.Helper()
	p := &groupedDispatchProbe{WorkflowRuntime: f.backend, ConfigurationAdmin: f.backend.(jobdb.ConfigurationAdmin), leaseOperationRuntime: runtimeLeaseOps(f.backend)}
	h, err := newServer(p, f.signer)
	require.NoError(t, err)
	f.server = httptest.NewServer(celltest.Handler(h))
	t.Cleanup(f.server.Close)
	return p
}
func seedGrouped(t *testing.T, f *delegationFixture, state string) (jobdb.StartOrAppendJobRequest, jobdb.JobHandle) {
	t.Helper()
	q := groupedRequest()
	q.Job.MaxInputs = 2
	var h jobdb.JobHandle
	n := map[string]int{"create": 0, "join": 1, "successor": 2}[state]
	for i := 0; i < n; i++ {
		var err error
		h, err = f.client.StartOrAppendJob(celltest.Context(context.Background()), q)
		require.NoError(t, err)
	}
	return q, h
}

func assertGroupedHandle(t *testing.T, f *delegationFixture, h jobdb.JobHandle) jobdb.JobSummary {
	t.Helper()
	for _, j := range groupedJobs(t, f.backend) {
		if j.JobKey == h.JobKey {
			require.Equal(t, jobdb.JobHandle{JobKey: j.JobKey, GoalID: j.GoalID, Cell: j.Cell, CellID: j.CellID, CellRepo: j.CellRepo}, h)
			require.NotEmpty(t, h.JobKey.JobId)
			require.NotEmpty(t, h.GoalID)
			require.NotEmpty(t, h.CellID)
			require.Equal(t, "tenant", h.JobKey.TenantId)
			require.Equal(t, "test", h.Cell)
			require.Equal(t, "example.test/test", h.CellRepo)
			require.Empty(t, j.ParentJobID)
			return j
		}
	}
	t.Fatal("returned handle absent from storage")
	return jobdb.JobSummary{}
}
func TestDelegatedGroupedCreateJoinSuccessor(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			f := newDelegationFixture(t, backend, nil)
			broad := groupedDelegate(t, f)
			narrow := groupedPolicy()
			narrow.Submit.CellNames = &jobdb.DelegationNames{Names: []string{"test"}}
			narrow.Submit.JobTypes = jobdb.DelegationNames{Names: []string{"batch"}}
			deadline := time.Now().UTC().Add(time.Hour)
			l, err := f.client.CreateDelegation(ctx, broad, jobdb.CreateDelegationRequest{Permissions: narrow, NotAfter: &deadline})
			require.NoError(t, err)
			delegate := l.(*remoteExecutionLease)
			f.staleSnapshot(t, delegate)
			before := f.validity(t)
			schema, err := f.client.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)})
			require.NoError(t, err)
			other, err := f.client.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{}}`)})
			require.NoError(t, err)
			q := groupedRequest()
			q.Job.MaxInputs = 2
			q.Job.Job.TenantId = "foreign" // The authenticated lease, not this field, binds tenancy.
			q.Job.Job.Schema = &jobdb.JobSchemaSelector{Hash: schema.SchemaHash}
			q.Job.Job.Metadata = json.RawMessage(`{"creator":true}`)
			q.Job.AppendArtifacts = []jobdb.ArtifactKey{{JobId: "source", TaskOrdinal: 1, Name: "first", SizeBytes: 5}}
			h, err := delegate.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			initial := assertGroupedHandle(t, f, h)
			require.Equal(t, schema.SchemaHash, initial.SchemaHash)
			require.JSONEq(t, string(q.Job.Job.Metadata), string(initial.Metadata))
			q.Job.Job.Schema = &jobdb.JobSchemaSelector{Hash: other.SchemaHash}
			q.Job.Job.Data = jobdb.NewTaskDataOrPanic("ignored join template")
			q.Job.Job.CellRepo = "example.test/forged"
			q.Job.Job.Metadata = json.RawMessage(`{"creator":false}`)
			q.Job.Append = json.RawMessage(`{"n":2}`)
			q.Job.AppendArtifacts = []jobdb.ArtifactKey{{JobId: "source", TaskOrdinal: 2, Name: "second", SizeBytes: 6}}
			joined, err := delegate.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			require.Equal(t, h, joined)
			stored := assertGroupedHandle(t, f, joined)
			require.Equal(t, initial.SchemaHash, stored.SchemaHash)
			require.Equal(t, initial.Metadata, stored.Metadata)
			chapters, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
			require.NoError(t, err)
			require.Len(t, chapters, 2)
			first := chapters[0].Body.(jobdb.JobStartChapter)
			require.Equal(t, `"creator"`, string(first.Input.Data))
			require.JSONEq(t, `{"n":1}`, string(first.Append.Data))
			require.Equal(t, "first", first.AppendArtifacts[0].Name)
			second := chapters[1].Body.(jobdb.AdditionalInputChapter)
			require.JSONEq(t, `{"n":2}`, string(second.Input.Data))
			require.Equal(t, q.Job.AppendArtifacts, second.Artifacts)
			q.Job.Job.CellRepo = ""
			next, err := delegate.StartOrAppendJob(ctx, q)
			require.NoError(t, err)
			require.NotEqual(t, h.JobKey, next.JobKey)
			successor := assertGroupedHandle(t, f, next)
			require.Equal(t, []string{h.JobKey.JobId}, successor.WaitFor)
			require.Equal(t, other.SchemaHash, successor.SchemaHash)
			require.Equal(t, before, f.validity(t))
			require.Zero(t, f.renews.Load())
		})
	}
}

func TestDelegatedGroupedAuthorizationBeforeMutation(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, state := range []string{"create", "join", "successor"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				f := newDelegationFixture(t, backend, nil)
				q, h := seedGrouped(t, f, state)
				probe := groupedProbe(t, f)
				denials := []string{"no-submit", "empty-ids", "exact-id", "cell", "type", "inline-schema", "narrow-cell", "narrow-type", "narrow-ids", "tenant-path", "job-path", "lease-path", "job-id", "goal-id", "dependencies", "prerequisites", "malformed", "registry", "widen-cell", "widen-type", "widen-ids"}
				if state != "join" {
					denials = append(denials, "repository")
				}
				for _, denial := range denials {
					t.Run(denial, func(t *testing.T) {
						policy := groupedPolicy()
						body, err := appendToAPI(ctx, q)
						require.NoError(t, err)
						switch denial {
						case "no-submit":
							policy.Submit = nil
						case "empty-ids":
							policy.Submit.JobIDs = jobdb.DelegationNames{}
						case "exact-id":
							id := h.JobKey.JobId
							if id == "" {
								id = "prospective-id"
							}
							policy.Submit.JobIDs = jobdb.DelegationNames{Names: []string{id}}
						case "cell":
							body.Job.Cell = "forbidden"
						case "type":
							body.Job.JobType = "forbidden"
						case "inline-schema":
							raw := json.RawMessage(`{}`)
							body.Job.Schema = &runtimeapi.JobSchemaSelector{Schema: &raw}
						case "job-id":
							id := "chosen"
							body.Job.JobId = &id
						case "goal-id":
							id := "chosen"
							body.Job.GoalId = &id
						case "dependencies":
							v := []runtimeapi.Dependency{}
							body.Job.Dependencies = &v
						case "prerequisites":
							v := []runtimeapi.JobPrerequisite{}
							body.Job.Prerequisites = &v
						case "repository":
							repo := "example.test/forged"
							body.Job.CellRepo = &repo
						}
						// Broad parent; every request must honor the narrower child's selectors.
						parent := groupedDelegate(t, f)
						switch denial {
						case "cell", "narrow-cell", "widen-cell":
							policy.Submit.CellNames = &jobdb.DelegationNames{Names: []string{"test"}}
							if denial == "narrow-cell" || denial == "widen-cell" {
								policy.Submit.CellNames.Names = []string{"other"}
							}
						case "type":
							policy.Submit.JobTypes = jobdb.DelegationNames{Names: []string{"batch"}}
						case "narrow-type", "widen-type":
							policy.Submit.JobTypes = jobdb.DelegationNames{Names: []string{"other"}}
						case "narrow-ids", "widen-ids":
							policy.Submit.JobIDs = jobdb.DelegationNames{Names: []string{h.JobKey.JobId + "restricted"}}
						}
						l, err := f.client.CreateDelegation(ctx, parent, jobdb.CreateDelegationRequest{Permissions: policy})
						require.NoError(t, err)
						token := l.(*remoteExecutionLease).LeaseToken()
						path := delegationBase(l) + "/jobs/start-or-append"
						var requestBody any = body
						want := http.StatusForbidden
						code := "lease_permission_denied"
						switch denial {
						case "tenant-path":
							path = strings.Replace(path, "/tenant/", "/foreign/", 1)
							want = 401
							code = "invalid_delegation_credential"
						case "job-path":
							path = strings.Replace(path, "/jobs/"+l.Job().JobKey.JobId+"/", "/jobs/foreign/", 1)
							want = 401
							code = "invalid_delegation_credential"
						case "lease-path":
							path = strings.Replace(path, l.LeaseID(), "foreign", 1)
							want = 401
							code = "invalid_delegation_credential"
						case "job-id", "goal-id", "dependencies", "prerequisites":
							want = 400
							code = "invalid_append_request"
						case "repository":
							want = 409
							code = "cell_repo_mismatch"
						case "malformed":
							requestBody = map[string]any{"job": 42}
							want = 400
							code = "invalid_delegation_request"
						case "widen-cell", "widen-type", "widen-ids":
							token = appendTestRestriction(t, token, groupedPolicy(), nil)
							want = 401
							code = "invalid_delegation_credential"
						case "registry":
							path = "/v1/tenants/tenant/schemas"
							requestBody = map[string]any{"schema": map[string]any{}}
						}
						before := groupedState(t, f)
						calls := probe.calls
						status, raw := delegationHTTP(t, f, "POST", path, token, requestBody)
						require.Equal(t, want, status, string(raw))
						require.Contains(t, string(raw), code)
						if want == 403 {
							require.ErrorIs(t, responseError("grouped", status, raw, nil), jobdb.ErrLeasePermissionDenied)
						}
						if denial != "repository" {
							require.Equal(t, calls, probe.calls, "rejection dispatched grouped mutation")
						}
						if want == 403 && denial != "registry" {
							clientCalls := 0
							client, err := New(f.server.URL, &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
								clientCalls++
								require.Equal(t, "POST", r.Method)
								require.Equal(t, path, r.URL.Path)
								require.True(t, token == r.Header.Get("X-JobDB-Lease-Token"), "submission credentials changed")
								return f.server.Client().Transport.RoundTrip(r)
							})})
							require.NoError(t, err)
							lease := l.(*remoteExecutionLease)
							lease.runtime = client
							req, err := appendFromAPI(&body, "tenant")
							require.NoError(t, err)
							_, err = lease.StartOrAppendJob(ctx, req)
							require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
							require.NotErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
							require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
							require.Equal(t, 1, clientCalls)
							require.Equal(t, calls, probe.calls)
						}
						require.Equal(t, before, groupedState(t, f))
					})
				}
			})
		}
	}
}

func TestDelegatedGroupedLiveAuthority(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, state := range []string{"create", "join", "successor"} {
			for _, failure := range []string{"cancel", "complete", "expire", "replace", "worker", "owner-cell", "owner-repo", "deadline", "attenuated-deadline"} {
				t.Run(backend+"/"+state+"/"+failure, func(t *testing.T) {
					ctx := celltest.Context(context.Background())
					f := newDelegationFixture(t, backend, nil)
					q, _ := seedGrouped(t, f, state)
					owner := f.owner.(*remoteExecutionLease)
					if failure == "worker" || failure == "owner-cell" || failure == "owner-repo" {
						worker, cell := owner.workerID, owner.cell
						if failure == "worker" {
							worker = "wrong-worker"
						} else if failure == "owner-repo" {
							cell.CellRepo = "example.test/wrong-owner"
						} else {
							cell.CellID = "wrong-cell"
						}
						token, err := f.signer.mintWithPrincipal(owner.jobKey, owner.leaseID, worker, "", owner.LeaseExpiry(), time.Minute, nil, cell)
						require.NoError(t, err)
						owner.leaseToken = token
					}
					l := groupedDelegate(t, f)
					switch failure {
					case "cancel":
						require.NoError(t, f.backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: owner.jobKey}))
					case "complete":
						final := jobdb.Chapter{Ordinal: 1, TaskType: owner.Route().JobType, CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}
						require.NoError(t, owner.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
					case "expire", "replace":
						_, err := f.backend.(leaseRenewalRuntime).KeepAliveLeaseByIDWithExpiry(ctx, owner.jobKey, owner.leaseID, owner.workerID, time.Nanosecond)
						require.NoError(t, err)
						if failure == "replace" {
							next, err := f.backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: owner.jobKey, WorkerID: "replacement", Routes: []jobdb.Route{owner.Route()}, LeaseDuration: time.Minute})
							require.NoError(t, err)
							require.NotNil(t, next)
							require.NotEqual(t, owner.leaseID, next.LeaseID())
						}
					case "deadline", "attenuated-deadline":
						past := time.Now().UTC().Add(-time.Second)
						if failure == "attenuated-deadline" {
							narrow := groupedPolicy()
							narrow.Submit.JobTypes = jobdb.DelegationNames{Names: []string{"batch"}}
							child, err := f.client.CreateDelegation(ctx, l, jobdb.CreateDelegationRequest{Permissions: narrow})
							require.NoError(t, err)
							l = child.(*remoteExecutionLease)
							l.leaseToken = appendTestRestriction(t, l.LeaseToken(), narrow, &past)
						} else {
							l.leaseToken = appendTestRestriction(t, l.LeaseToken(), groupedPolicy(), &past)
						}
					}
					probe := groupedProbe(t, f)
					calls := 0
					token := l.LeaseToken()
					client, err := New(f.server.URL, &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
						calls++
						require.Equal(t, "POST", r.Method)
						require.Equal(t, delegationBase(l)+"/jobs/start-or-append", r.URL.Path)
						require.True(t, token == r.Header.Get("X-JobDB-Lease-Token"), "submission credentials changed")
						resp, err := f.server.Client().Transport.RoundTrip(r)
						if err == nil && strings.Contains(failure, "deadline") {
							raw, readErr := io.ReadAll(resp.Body)
							require.NoError(t, readErr)
							require.NoError(t, resp.Body.Close())
							require.Equal(t, 403, resp.StatusCode)
							require.Contains(t, string(raw), "delegation_deadline_reached")
							resp.Body = io.NopCloser(bytes.NewReader(raw))
						}
						return resp, err
					})})
					require.NoError(t, err)
					l.runtime = client
					before := groupedState(t, f)
					_, err = l.StartOrAppendJob(ctx, q)
					if strings.Contains(failure, "deadline") {
						require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
						require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
						require.NotErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
					} else {
						require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
						require.NotErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
					}
					require.NotErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
					require.Equal(t, 1, calls)
					require.Zero(t, probe.calls)
					require.Equal(t, before, groupedState(t, f))
				})
			}
		}
	}
}

func TestDelegatedGroupedOnlyIngress(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			f := newDelegationFixture(t, backend, nil)
			l := groupedDelegate(t, f)
			handler, err := NewDelegationServerWithOptions(f.backend, ServerOptions{LeaseTokenSigningKey: f.signer.key})
			require.NoError(t, err)
			ingress := httptest.NewServer(celltest.Handler(handler))
			defer ingress.Close()
			client, err := New(ingress.URL, ingress.Client())
			require.NoError(t, err)
			l.runtime = client
			for _, state := range []string{"create", "join", "successor"} {
				q := groupedRequest()
				q.Job.MaxInputs = 2
				before := groupedState(t, f)
				body, err := appendToAPI(celltest.Context(context.Background()), q)
				require.NoError(t, err)
				for _, token := range []string{"", f.owner.(*remoteExecutionLease).LeaseToken()} {
					raw, err := json.Marshal(body)
					require.NoError(t, err)
					req, err := http.NewRequest("POST", ingress.URL+delegationBase(l)+"/jobs/start-or-append", strings.NewReader(string(raw)))
					require.NoError(t, err)
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("X-JobDB-Lease-Token", token)
					resp, err := ingress.Client().Do(req)
					require.NoError(t, err)
					resp.Body.Close()
					require.Equal(t, 401, resp.StatusCode, state)
					require.Equal(t, before, groupedState(t, f))
				}
				h, err := l.StartOrAppendJob(celltest.Context(context.Background()), q)
				require.NoError(t, err)
				assertGroupedHandle(t, f, h)
			}
		})
	}
}
