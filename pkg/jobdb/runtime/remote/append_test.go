package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

type appendTransportFunc func(*http.Request) (*http.Response, error)

func (f appendTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func groupedRequest() jobdb.StartOrAppendJobRequest {
	return jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "errors", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("creator")}, Append: json.RawMessage(`{"n":1}`)}}
}
func groupedJobs(t *testing.T, rt jobdb.WorkflowRuntime) []jobdb.JobSummary {
	t.Helper()
	page, err := rt.ListJobs(celltest.Context(context.Background()), jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{"batch"}})
	require.NoError(t, err)
	require.Empty(t, page.NextPageToken)
	return page.Jobs
}
func groupedSubmitter(t *testing.T, f *delegationFixture, client *Runtime, mode string) (func(context.Context, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error), string) {
	t.Helper()
	if mode == "ordinary" {
		return client.StartOrAppendJob, ""
	}
	lease := f.owner.(*remoteExecutionLease)
	if mode == "delegated" {
		lease = groupedDelegate(t, f)
	}
	lease.runtime = client
	return lease.StartOrAppendJob, lease.LeaseToken()
}

func TestGroupedColdSchemaExplicitRetry(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, mode := range []string{"ordinary", "owner", "delegated"} {
			for _, state := range []string{"create", "successor"} {
				t.Run(fmt.Sprintf("%s/%s/%s", backend, mode, state), func(t *testing.T) {
					ctx := celltest.Context(context.Background())
					f := newDelegationFixture(t, backend, nil)
					schema := json.RawMessage(`{"chapterShape":{"type":"object"}}`)
					hash, _, err := jobdb.JobSchemaHash(schema)
					require.NoError(t, err)
					q, _ := seedGrouped(t, f, state)
					before := groupedState(t, f)
					q.Job.Job.Schema = &jobdb.JobSchemaSelector{Hash: hash}
					calls := 0
					var paths []string
					var tokens []string
					var status int
					var body []byte
					observed := &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
						paths = append(paths, r.Method+" "+r.URL.Path)
						resp, err := f.server.Client().Transport.RoundTrip(r)
						if err == nil && strings.HasSuffix(r.URL.Path, "/start-or-append") {
							calls++
							tokens = append(tokens, r.Header.Get("X-JobDB-Lease-Token"))
							status = resp.StatusCode
							body, err = io.ReadAll(resp.Body)
							resp.Body.Close()
							resp.Body = io.NopCloser(bytes.NewReader(body))
						}
						return resp, err
					})}
					client, err := New(f.server.URL, observed)
					require.NoError(t, err)
					submit, credential := groupedSubmitter(t, f, client, mode)
					_, err = submit(ctx, q)
					require.ErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
					require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
					require.Equal(t, http.StatusNotFound, status)
					var response struct {
						Code string `json:"code"`
					}
					require.NoError(t, json.Unmarshal(body, &response))
					require.Equal(t, "job_schema_not_found", response.Code)
					require.Equal(t, 1, calls)
					require.Equal(t, before, groupedState(t, f))
					require.Len(t, paths, 1)
					require.True(t, f.validity(t).ExpiresAt.After(time.Now()))
					require.NoError(t, f.owner.KeepAlive(ctx))
					registered, err := f.client.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: schema})
					require.NoError(t, err)
					require.Equal(t, hash, registered.SchemaHash)
					h, err := submit(ctx, q) // One caller-controlled retry after definite rejection.
					require.NoError(t, err)
					require.Equal(t, 2, calls)
					if mode == "delegated" {
						require.Len(t, paths, 2)
						require.Equal(t, "POST "+delegationBase(f.owner)+"/jobs/start-or-append", paths[0])
						require.Equal(t, paths[0], paths[1])
					}
					if mode == "delegated" {
						require.True(t, tokens[0] == tokens[1], "explicit retry changed credentials")
					}
					require.Equal(t, mode == "delegated", isDelegationToken(tokens[0]))
					require.True(t, credential == tokens[0], "submission credentials changed")
					jobs := groupedJobs(t, f.backend)
					wantJobs := 1
					if state == "successor" {
						wantJobs = 2
					}
					require.Len(t, jobs, wantJobs)
					assertGroupedHandle(t, f, h)
					chapters, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
					require.NoError(t, err)
					require.Len(t, chapters, 1)
					start := chapters[0].Body.(jobdb.JobStartChapter)
					require.Equal(t, `"creator"`, string(start.Input.Data))
					require.JSONEq(t, `{"n":1}`, string(start.Append.Data))
					require.True(t, f.validity(t).ExpiresAt.After(time.Now()))
				})
			}
		}

	}
}

type wrappedGroupedSchemaRuntime struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
}

func (r wrappedGroupedSchemaRuntime) StartOrAppendJob(context.Context, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, fmt.Errorf("schema lookup: %w", jobdb.ErrJobSchemaNotFound)
}
func (r wrappedGroupedSchemaRuntime) StartOrAppendJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, fmt.Errorf("leased schema lookup: %w", jobdb.ErrJobSchemaNotFound)
}
func (r wrappedGroupedSchemaRuntime) ValidateExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, id, worker string) (jobdb.LeaseValidity, error) {
	return r.WorkflowRuntime.(interface {
		ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
	}).ValidateExecutionLeaseByID(ctx, key, id, worker)
}
func TestGroupedWrappedSchemaError(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, mode := range []string{"ordinary", "owner", "delegated"} {
			t.Run(fmt.Sprintf("%s/%s", backend, mode), func(t *testing.T) {
				f := newDelegationFixture(t, backend, nil)
				handler, err := newServer(wrappedGroupedSchemaRuntime{f.backend, f.backend.(jobdb.ConfigurationAdmin)}, f.signer)
				require.NoError(t, err)
				server := httptest.NewServer(celltest.Handler(handler))
				defer server.Close()
				client, err := New(server.URL, server.Client())
				require.NoError(t, err)
				submit, _ := groupedSubmitter(t, f, client, mode)
				_, err = submit(celltest.Context(context.Background()), groupedRequest())
				require.ErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
				require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				require.Empty(t, groupedJobs(t, f.backend))
			})
		}
	}
}

func TestGroupedGenuineLeaseLoss(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, failure := range []string{"missing", "expired", "stale"} {
			t.Run(backend+"/"+failure, func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				f := newDelegationFixture(t, backend, nil)
				owner := f.owner.(*remoteExecutionLease)
				q := groupedRequest()
				registered, err := f.client.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`)})
				require.NoError(t, err)
				q.Job.Job.Schema = &jobdb.JobSchemaSelector{Hash: registered.SchemaHash}
				switch failure {
				case "missing":
					owner.leaseID = "missing"
					token, err := f.signer.mintWithPrincipal(owner.jobKey, owner.leaseID, owner.workerID, "", time.Now().Add(time.Minute), time.Minute, nil, jobdb.ResolvedCell{Cell: owner.Job().Cell, CellID: owner.Job().CellID, CellRepo: owner.Job().CellRepo})
					require.NoError(t, err)
					owner.leaseToken = token
				case "expired":
					require.NoError(t, owner.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}))
					short, err := f.client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: owner.jobKey, WorkerID: "short", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: 100 * time.Millisecond})
					require.NoError(t, err)
					require.NotNil(t, short)
					owner = short.(*remoteExecutionLease)
					// Let both the actual storage lease and its credential expire.
					time.Sleep(time.Until(owner.LeaseExpiry()) + 20*time.Millisecond)
				case "stale":
					require.NoError(t, owner.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}))
					next, err := f.client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: owner.jobKey, WorkerID: "replacement", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					require.NotNil(t, next)
					require.NotEqual(t, owner.leaseID, next.LeaseID())
				}
				probe := groupedProbe(t, f)
				client, err := New(f.server.URL, f.server.Client())
				require.NoError(t, err)
				owner.runtime = client
				before := groupedState(t, f)
				_, err = owner.StartOrAppendJob(ctx, q)
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				require.Zero(t, probe.calls)
				require.Equal(t, before, groupedState(t, f))
				require.NotErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
				require.Empty(t, groupedJobs(t, f.backend))
			})
		}
	}
}

func TestGroupedUnknown404DoesNotImplySchema(t *testing.T) {
	for _, body := range []string{"job schema not found", `{"code":`, `{"code":"other","message":"job schema not found"}`} {
		for _, sentinel := range []error{nil, jobdb.ErrExecutionLeaseLost} {
			err := responseError("append", http.StatusNotFound, []byte(body), sentinel)
			require.Error(t, err)
			require.NotErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
			if sentinel != nil {
				require.ErrorIs(t, err, sentinel)
			}
		}
	}
}

func TestGroupedTransportFailureAfterAcceptance(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, mode := range []string{"ordinary", "owner", "delegated"} {
			for _, existing := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/existing=%t", backend, mode, existing), func(t *testing.T) {
					ctx := celltest.Context(context.Background())
					f := newDelegationFixture(t, backend, nil)
					q := groupedRequest()
					source := []byte("accepted artifact bytes")
					require.NoError(t, f.client.PutChapter(ctx, jobdb.PutChapterRequest{
						LeaseID: f.owner.LeaseID(), LeaseToken: f.owner.(*remoteExecutionLease).LeaseToken(),
						Ref: jobdb.ChapterRef{JobKey: f.owner.Job().JobKey, Ordinal: 1}, Chapter: delegationChapter(1),
						ArtifactUploads: []jobdb.ArtifactUpload{{Name: "piece", Size: int64(len(source)), Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(source)), nil }}},
					}))
					q.Job.AppendArtifacts = []jobdb.ArtifactKey{{JobId: f.owner.Job().JobKey.JobId, TaskOrdinal: 1, Name: "piece", SizeBytes: int64(len(source))}}
					if existing {
						_, err := f.client.StartOrAppendJob(ctx, q)
						require.NoError(t, err)
					}
					lost := errors.New("response lost after acceptance")
					paths := []string{}
					var tokens []string
					client, err := New(f.server.URL, &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
						paths = append(paths, r.Method+" "+r.URL.Path)
						tokens = append(tokens, r.Header.Get("X-JobDB-Lease-Token"))
						resp, err := f.server.Client().Transport.RoundTrip(r)
						if err != nil {
							return nil, err
						}
						defer resp.Body.Close()
						_, err = io.Copy(io.Discard, resp.Body)
						require.NoError(t, err)
						require.Equal(t, http.StatusOK, resp.StatusCode)
						return nil, lost
					})})
					require.NoError(t, err)
					submit, credential := groupedSubmitter(t, f, client, mode)
					_, err = submit(ctx, q)
					require.ErrorIs(t, err, lost)
					var infrastructure *jobdb.ExecutionInfrastructureError
					require.ErrorAs(t, err, &infrastructure)
					require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, infrastructure.Reason)
					require.NotErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
					require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
					require.Len(t, paths, 1)
					expected := "/v1/tenants/tenant/jobs/start-or-append"
					if mode != "ordinary" {
						expected = delegationBase(f.owner) + "/jobs/start-or-append"
					}
					require.Equal(t, "POST "+expected, paths[0])
					require.Equal(t, mode == "delegated", isDelegationToken(tokens[0]))
					require.True(t, credential == tokens[0], "submission credentials changed")
					// An independent healthy client inspects acceptance without resubmission.
					jobs := groupedJobs(t, f.client)
					require.Len(t, jobs, 1)
					chapters, err := f.client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: jobs[0].JobKey})
					require.NoError(t, err)
					if existing {
						require.Len(t, chapters, 2)
						joined := chapters[1].Body.(jobdb.AdditionalInputChapter)
						require.JSONEq(t, string(q.Job.Append), string(joined.Input.Data))
						require.Equal(t, q.Job.AppendArtifacts, joined.Artifacts)
					} else {
						require.Len(t, chapters, 1)
						start := chapters[0].Body.(jobdb.JobStartChapter)
						require.JSONEq(t, string(q.Job.Append), string(start.Append.Data))
						require.Equal(t, q.Job.AppendArtifacts, start.AppendArtifacts)
					}
					reader, err := f.client.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: f.owner.Job().JobKey, Ordinal: 1, Name: "piece"})
					require.NoError(t, err)
					rc, err := reader.Open()
					require.NoError(t, err)
					actual, err := io.ReadAll(rc)
					require.NoError(t, err)
					require.NoError(t, rc.Close())
					require.Equal(t, source, actual)
				})
			}
		}
	}
}

// Exercise the existing error boundary without requiring a backend-specific way
// to populate four billion queue generations.
type exhaustedGroupedRuntime struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
}

func (r exhaustedGroupedRuntime) StartOrAppendJob(context.Context, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, fmt.Errorf("%w: append generation exhausted (maximum 4294967295)", jobdb.ErrInvalidAppendRequest)
}
func TestGroupedGenerationExhaustionHTTP(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	server := httptest.NewServer(celltest.Handler(NewServer(exhaustedGroupedRuntime{f.backend, f.backend.(jobdb.ConfigurationAdmin)})))
	defer server.Close()
	var status int
	var body []byte
	client, err := New(server.URL, &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
		resp, err := server.Client().Transport.RoundTrip(r)
		if err == nil {
			status = resp.StatusCode
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(body))
		}
		return resp, err
	})})
	require.NoError(t, err)
	_, err = client.StartOrAppendJob(celltest.Context(context.Background()), groupedRequest())
	require.ErrorIs(t, err, jobdb.ErrInvalidAppendRequest)
	require.Contains(t, err.Error(), "append generation exhausted (maximum 4294967295)")
	require.Equal(t, http.StatusBadRequest, status)
	var response struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(body, &response))
	require.Equal(t, "invalid_append_request", response.Code)
}

func TestGroupedAppendBusyHTTP(test *testing.T) {
	recorder := httptest.NewRecorder()
	require.True(test, writeAppendError(recorder, jobdb.ErrAppendBusy))
	require.Equal(test, http.StatusConflict, recorder.Code)
	require.Equal(test, "application/json", recorder.Header().Get("Content-Type"))
	var response struct {
		Code string `json:"code"`
	}
	require.NoError(test, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(test, "append_busy", response.Code)
	require.ErrorIs(test, appendResponseError(recorder.Body.Bytes()), jobdb.ErrAppendBusy)
}
