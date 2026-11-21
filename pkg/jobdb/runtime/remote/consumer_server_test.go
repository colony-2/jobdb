package remote

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// A consumer decorator must forward the optional ports it actually supports.
// No full runtime or concrete backend is embedded, so attachment cannot leak.
type consumerServerPorts interface {
	jobdb.ConfigurationAdmin
	jobdb.JobSchemaRegistry
	jobdb.JobDeletionRuntime
	jobdb.PrimaryGoalSelectionCapability
	jobdb.JobGoalCapability
	leaseOperationRuntime
	leaseRenewalRuntime
	StartOrAppendJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error)
	RenewExecutionLeaseByID(context.Context, jobdb.JobKey, string, string, time.Duration) (jobdb.RenewableExecutionLease, error)
	ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
	ConfigurationLimits() jobdb.ConfigurationLimits
	SecretResolutionLimits() jobdb.SecretLimits
	ResolveCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
	LookupCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
	CreateCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.CreateCellRequest) (jobdb.Cell, error)
}
type consumerServerRuntime struct {
	jobdb.ConsumerRuntime
	consumerServerPorts
}

func consumerFixture(t *testing.T, rt jobdb.ConsumerRuntime) consumerServerRuntime {
	t.Helper()
	ports, ok := rt.(consumerServerPorts)
	require.True(t, ok, "backend must supply the forwarded optional capabilities")
	r := consumerServerRuntime{rt, ports}
	_, found := reflect.TypeOf(r).MethodByName("AddGoalCandidate")
	require.False(t, found)
	return r
}
func consumerBackend(t *testing.T, kind string) jobdb.WorkflowRuntime {
	t.Helper()
	if kind == "toy" {
		return toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
	}
	return secretsSQLite(t)
}
func consumerClient(t *testing.T, handler http.Handler) *Runtime {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
	}))
	t.Cleanup(server.Close)
	rt, err := New(server.URL, server.Client())
	require.NoError(t, err)
	return rt
}

func TestConsumerServerRegisteredRoutes(t *testing.T) {
	legacy := chi.NewRouter()
	runtimeapi.HandlerFromMux(runtimeapi.NewStrictHandler(&proxyServer{}, nil), legacy)
	consumer := chi.NewRouter()
	runtimeapi.ConsumerHandlerFromMux(runtimeapi.NewConsumerStrictHandler(&proxyServer{}, nil), consumer)
	inventory := func(r chi.Routes) map[string]bool {
		routes := map[string]bool{}
		require.NoError(t, chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
			routes[method+" "+route] = true
			return nil
		}))
		return routes
	}
	oldRoutes, newRoutes := inventory(legacy), inventory(consumer)
	retired := "PUT /v1/tenants/{tenantId}/goals/{goalId}/candidates/{jobId}"
	require.False(t, oldRoutes[retired])
	require.False(t, newRoutes[retired])
	delete(oldRoutes, retired)
	require.Equal(t, oldRoutes, newRoutes)
	for _, typ := range []reflect.Type{reflect.TypeFor[runtimeapi.ConsumerStrictServerInterface](), reflect.TypeFor[runtimeapi.ConsumerServerInterface](), reflect.TypeFor[*proxyServer]()} {
		_, found := typ.MethodByName("AddGoalCandidate")
		require.False(t, found, typ.String())
	}
	require.Equal(t, reflect.TypeFor[runtimeapi.StrictServerInterface]().NumMethod(), reflect.TypeFor[runtimeapi.ConsumerStrictServerInterface]().NumMethod())
}

func TestConsumerServerNoAttachmentAndLegacyCompatibility(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			backend := consumerBackend(t, kind)
			a, b := goalSubmit(t, backend, "a"), goalSubmit(t, backend, "b")
			ctx := celltest.Context(context.Background())
			before, err := backend.GetGoal(ctx, goalKey(a))
			require.NoError(t, err)
			opts := ServerOptions{LeaseTokenSigningKey: []byte(strings.Repeat("k", 32))}
			configured, err := NewConsumerServerWithOptions(consumerFixture(t, backend), opts)
			require.NoError(t, err)
			for _, handler := range []http.Handler{NewConsumerServer(consumerFixture(t, backend)), configured} {
				req := httptest.NewRequest(http.MethodPut, "/v1/tenants/tenant/goals/"+a.GoalID+"/candidates/"+b.JobKey.JobId, nil)
				recorder := httptest.NewRecorder()
				celltest.Handler(handler).ServeHTTP(recorder, req)
				require.Equal(t, http.StatusNotFound, recorder.Code)
				after, err := backend.GetGoal(ctx, goalKey(a))
				require.NoError(t, err)
				require.Equal(t, before, after)
				info, err := backend.GetJob(ctx, b.JobKey)
				require.NoError(t, err)
				require.Empty(t, info.OtherGoalIDs)
			}
			// Both original constructors also retire attachment.
			legacy, err := NewServerWithOptions(backend, opts)
			require.NoError(t, err)
			for _, handler := range []http.Handler{NewServer(backend), legacy} {
				req := httptest.NewRequest(http.MethodPut, "/v1/tenants/tenant/goals/"+a.GoalID+"/candidates/"+b.JobKey.JobId, nil)
				recorder := httptest.NewRecorder()
				celltest.Handler(handler).ServeHTTP(recorder, req)
				require.Equal(t, http.StatusNotFound, recorder.Code)
			}
			_, err = NewConsumerServerWithOptions(consumerFixture(t, backend), ServerOptions{})
			require.Error(t, err)
			_, err = NewConsumerServerWithOptions(struct{ jobdb.ConsumerRuntime }{backend}, opts)
			require.ErrorContains(t, err, "configuration administration")
		})
	}
}

func TestConsumerServerConformance(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			t.Run("selection", func(t *testing.T) {
				retained := consumerFixture(t, consumerBackend(t, kind))
				require.True(t, retained.SupportsPrimaryGoalSelection())
				runtimetest.RunPrimaryGoalSelectionConformance(t, consumerClient(t, NewConsumerServer(retained)))
			})
			t.Run("boundaries", func(t *testing.T) {
				runtimetest.RunPrimaryGoalSelectionBoundaries(t, consumerFixture(t, consumerBackend(t, kind)))
			})
			t.Run("configuration", func(t *testing.T) {
				runtimetest.RunConfigurationConformance(t, consumerClient(t, NewConsumerServer(consumerFixture(t, consumerBackend(t, kind)))))
			})
			t.Run("parent-identity", func(t *testing.T) {
				for _, constructor := range []string{"default", "options"} {
					t.Run(constructor, func(t *testing.T) {
						runtimetest.RunParentIdentityConformance(t, runtimetest.Harness{
							Name: "retained-http",
							New: func(testing.TB) runtimetest.Fixture {
								retained := consumerFixture(t, consumerBackend(t, kind))
								handler := NewConsumerServer(retained)
								if constructor == "options" {
									var err error
									handler, err = NewConsumerServerWithOptions(retained, ServerOptions{LeaseTokenSigningKey: []byte(strings.Repeat("k", 32))})
									require.NoError(t, err)
								}
								return runtimetest.Fixture{Runtime: consumerClient(t, handler)}
							},
						})
					})
				}
			})
		})
	}
}

// A signed token alone cannot authorize child submission through a retained
// decorator. Preserve main's live-validation boundary before provider dispatch.
func TestConsumerServerParentLeaseValidation(t *testing.T) {
	for _, constructor := range []string{"default", "options"} {
		t.Run(constructor, func(t *testing.T) {
			for _, invalidation := range []string{"cancel", "complete", "expire", "replace", "missing-worker", "missing-validator"} {
				t.Run(invalidation, func(t *testing.T) {
					validator := invalidation != "missing-validator"
					f := newDelegationFixture(t, "toy", nil)
					ctx := celltest.Context(context.Background())
					owner := f.owner.(*remoteExecutionLease)
					probe := &submissionDispatchProbe{
						WorkflowRuntime: f.backend, ConfigurationAdmin: f.backend.(jobdb.ConfigurationAdmin),
						leaseOperationRuntime: runtimeLeaseOps(f.backend),
						validate:              consumerFixture(t, f.backend).ValidateExecutionLeaseByID,
					}
					// Hide every legacy method while forwarding real submission operations.
					type retainedSubmission struct {
						jobdb.ConsumerRuntime
						jobdb.ConfigurationAdmin
						leaseOperationRuntime
					}
					base := retainedSubmission{f.backend, probe.ConfigurationAdmin, probe}
					var retained jobdb.ConsumerRuntime = base
					if validator {
						type leaseValidator interface {
							ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
						}
						retained = struct {
							retainedSubmission
							leaseValidator
						}{base, probe}
					}
					_, mutation := reflect.TypeOf(retained).MethodByName("AddGoalCandidate")
					require.False(t, mutation)
					// Inject the signer into the default constructor's shared builder so
					// a valid signed token missing its worker can also be exercised.
					handler, err := newRuntimeServer(retained, f.signer)
					if constructor == "options" {
						handler, err = NewConsumerServerWithOptions(retained, ServerOptions{LeaseTokenSigningKey: f.signer.key})
					}
					require.NoError(t, err)
					server := httptest.NewServer(celltest.Handler(handler))
					t.Cleanup(server.Close)
					f.server = server
					body, err := submitJobRequestToAPI(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
					require.NoError(t, err)
					restart, err := submitRestartJobRequestToAPI(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: owner.jobKey, Cell: "test", CellRepo: "example.test/test"}})
					require.NoError(t, err)
					operations := []struct {
						method, suffix string
						body           any
					}{
						{"POST", "/jobs", body}, {"PUT", "/jobs/child", body},
						{"POST", "/jobs/restarts", restart}, {"PUT", "/jobs/child/restart", restart},
					}
					if validator {
						for _, op := range operations {
							status, raw := delegationHTTP(t, f, op.method, delegationBase(owner)+op.suffix, owner.LeaseToken(), op.body)
							require.Equal(t, http.StatusOK, status, string(raw))
						}
						require.Equal(t, 4, probe.calls)
						require.Equal(t, owner.jobKey, probe.parent)
						require.Equal(t, owner.workerID, probe.worker)
					}
					switch invalidation {
					case "cancel":
						require.NoError(t, f.backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: owner.jobKey}))
					case "complete":
						final := jobdb.Chapter{Ordinal: 1, TaskType: owner.Route().JobType, CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}
						require.NoError(t, owner.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
					case "expire", "replace":
						_, err = f.backend.(leaseRenewalRuntime).KeepAliveLeaseByIDWithExpiry(ctx, owner.jobKey, owner.leaseID, owner.workerID, time.Nanosecond)
						require.NoError(t, err)
						if invalidation == "replace" {
							later, err := f.backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: owner.jobKey, WorkerID: "new-worker", Routes: []jobdb.Route{owner.Route()}, LeaseDuration: time.Minute})
							require.NoError(t, err)
							require.NotNil(t, later)
						}
					}
					calls := probe.calls
					token := owner.LeaseToken()
					if invalidation == "missing-worker" {
						h := owner.Job()
						token, err = f.signer.mintWithPrincipal(owner.jobKey, owner.leaseID, "", "", owner.LeaseExpiry(), time.Minute, nil, jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo})
						require.NoError(t, err)
					}
					for _, op := range operations {
						status, raw := delegationHTTP(t, f, op.method, delegationBase(owner)+op.suffix, token, op.body)
						want := http.StatusNotImplemented
						if validator {
							want = http.StatusConflict
						}
						require.Equal(t, want, status, string(raw))
					}
					require.Equal(t, calls, probe.calls, "invalid authority reached provider submission")
				})
			}
		})
	}
}

func TestConsumerServerRetainedLifecycle(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			backend := consumerBackend(t, kind)
			client := consumerClient(t, NewConsumerServer(consumerFixture(t, backend)))
			ctx := celltest.Context(context.Background())
			a := goalSubmit(t, client, "source")
			dependent := goalSubmit(t, client, "dependent", jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess})
			require.Nil(t, goalLease(t, client, dependent))
			goalComplete(t, client, a, false)
			req := jobdb.SubmitRestartJobRequest{RequestID: "retained-restart", Job: jobdb.SubmitRestartJob{PriorJobKey: a.JobKey, Cell: "test", ShareGoal: true}}
			restart, err := client.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, a.GoalID, restart.GoalID)
			retry, err := client.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, restart, retry)
			goalComplete(t, client, restart, true)
			retry, err = client.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, restart, retry)
			wait, err := client.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: dependent.JobKey, WaitID: "submission"})
			require.NoError(t, err)
			require.Equal(t, jobdb.DependencySatisfied, wait.State)
			require.Equal(t, restart.JobKey.JobId, wait.Dependencies[0].SatisfiedByJobID)
			failed := goalSubmit(t, client, "failed")
			result, err := client.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(failed), Message: "owner stopped"})
			require.NoError(t, err)
			require.Equal(t, jobdb.GoalFailed, result.State)
			info, err := client.GetGoal(ctx, goalKey(failed))
			require.NoError(t, err)
			require.Equal(t, result, info)
			require.NoError(t, client.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: dependent.JobKey, Reason: "reviewed"}))
			lease := goalLease(t, client, dependent)
			require.NotNil(t, lease)
			require.NoError(t, lease.KeepAlive(ctx))
			renewed, err := lease.(jobdb.RenewableExecutionLease).Renew(ctx)
			require.NoError(t, err)
			require.Equal(t, lease.LeaseID(), renewed.LeaseID())
			cell, err := renewed.LookupCell(ctx, "test")
			require.NoError(t, err)
			require.Equal(t, dependent.CellID, cell.CellID)
			_, err = renewed.CreateCell(ctx, jobdb.CreateCellRequest{Name: "child-cell", Repo: "example.test/child"})
			require.NoError(t, err)
			resolved, err := renewed.ResolveCell(ctx, "child-cell")
			require.NoError(t, err)
			require.Equal(t, "example.test/child", resolved.CellRepo)
			delegator := struct {
				jobdb.ConsumerRuntime
				jobdb.LeaseDelegator
			}{client, client}
			_, mutation := reflect.TypeOf(delegator).MethodByName("AddGoalCandidate")
			require.False(t, mutation)
			delegated, err := delegator.CreateDelegation(ctx, renewed, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Read: &jobdb.DelegationChapters{Ordinals: []int64{0}}}})
			require.NoError(t, err)
			_, err = client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: dependent.JobKey, LeaseID: delegated.LeaseID(), LeaseToken: leaseTokenForTest(delegated)})
			require.NoError(t, err)
			_, err = delegated.GetConfiguration(ctx)
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			_, err = client.GetConfiguration(ctx, jobdb.GetConfigurationRequest{JobKey: dependent.JobKey, LeaseID: renewed.LeaseID()})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.ErrorIs(t, client.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: dependent.JobKey, Reason: "live"}), jobdb.ErrConflict)
			require.NoError(t, client.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: dependent.JobKey, Reason: "stop"}))
			require.ErrorIs(t, renewed.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
			_, err = delegated.ResolveSecrets(ctx, []string{"token"})
			require.Error(t, err)
			schema, err := client.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)})
			require.NoError(t, err)
			stored, err := client.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: schema.SchemaHash})
			require.NoError(t, err)
			require.Equal(t, schema, stored)
			events, err := jobdb.ListLeaseEvents(ctx, struct{ jobdb.LeaseEventReader }{client}, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{dependent.JobKey.JobId}})
			require.NoError(t, err)
			require.NotEmpty(t, events.Events)
		})
	}
}

func TestConsumerServerSecretsAndArtifacts(t *testing.T) {
	backend := secretsSQLite(t)
	retained := struct {
		consumerServerRuntime
		devSecretAdmin
	}{consumerFixture(t, backend), backend}
	server := httptest.NewServer(celltest.Handler(NewConsumerServer(retained)))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	ctx := celltest.Context(context.Background())
	status, raw := secretHTTP(t, server.URL, "/dev/v1/tenants/tenant/secrets/set", "", `{"name":"token","value":"retained-secret"}`)
	require.Equal(t, 204, status, raw)
	lease := secretsRemoteLease(t, client, "tenant")
	result, err := lease.ResolveSecrets(ctx, []string{"token"})
	require.NoError(t, err)
	require.Equal(t, "retained-secret", result.Secrets["token"].Value)
	artifact := jobdb.NewArtifactFromBytes("result.txt", []byte("retained artifact"))
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "job", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
	require.NoError(t, client.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: leaseTokenForTest(lease), Ref: jobdb.ChapterRef{JobKey: lease.Job().JobKey, Ordinal: 1}, Chapter: chapter, ArtifactUploads: []jobdb.ArtifactUpload{{Name: artifact.Name(), Size: artifact.Size(), Open: artifact.Open}}}))
	stored, err := client.GetChapter(ctx, jobdb.ChapterRef{JobKey: lease.Job().JobKey, Ordinal: 1})
	require.NoError(t, err)
	require.Len(t, stored.Artifacts, 1)
	reader, err := client.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: lease.Job().JobKey, Ordinal: 1, Name: artifact.Name()})
	require.NoError(t, err)
	stream, err := reader.Open()
	require.NoError(t, err)
	defer stream.Close()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "retained artifact", string(body))
	chapter.Ordinal = 2
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "succeeded", Chapter: &chapter}))
}

// Start-or-append was added after the saved preparation. Exercise both retained
// routes through a decorator that forwards its real lease submission capability.
func TestConsumerServerStartOrAppend(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		for _, constructor := range []string{"default", "options"} {
			t.Run(kind+"/"+constructor, func(t *testing.T) {
				backend := consumerBackend(t, kind)
				retained := consumerFixture(t, backend)
				handler := NewConsumerServer(retained)
				if constructor == "options" {
					var err error
					handler, err = NewConsumerServerWithOptions(retained, ServerOptions{LeaseTokenSigningKey: []byte(strings.Repeat("k", 32))})
					require.NoError(t, err)
				}
				client := consumerClient(t, handler)
				ctx := celltest.Context(context.Background())
				owner := secretsRemoteLease(t, client, "tenant")
				for _, mode := range []string{"ordinary", "lease"} {
					t.Run(mode, func(t *testing.T) {
						submit := client.StartOrAppendJob
						if mode == "lease" {
							submit = owner.StartOrAppendJob
						}
						req := groupedRequest()
						req.Job.AppendKey = mode
						created, err := submit(ctx, req)
						require.NoError(t, err)
						req.Job.Append = json.RawMessage(`{"n":2}`)
						joined, err := submit(ctx, req)
						require.NoError(t, err)
						require.Equal(t, created.JobKey, joined.JobKey)
						chapters, err := backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: created.JobKey})
						require.NoError(t, err)
						require.Len(t, chapters, 2)
						require.JSONEq(t, `{"n":1}`, string(chapters[0].Body.(jobdb.JobStartChapter).Append.Data))
						require.JSONEq(t, `{"n":2}`, string(chapters[1].Body.(jobdb.AdditionalInputChapter).Input.Data))
					})
				}
				require.NoError(t, backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: owner.Job().JobKey}))
				req := groupedRequest()
				req.Job.AppendKey = "rejected"
				_, err := owner.StartOrAppendJob(ctx, req)
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				page, err := backend.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{"batch"}})
				require.NoError(t, err)
				require.Len(t, page.Jobs, 2, "cancelled parent must not create a third grouped job")
				for _, job := range page.Jobs {
					require.Empty(t, job.ParentJobID, "grouped submission keeps its existing root identity")
				}
			})
		}
	}
}
