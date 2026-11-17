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
func groupedSubmitter(t *testing.T, f *delegationFixture, client *Runtime, leased bool) func(context.Context, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	t.Helper()
	if !leased {
		return client.StartOrAppendJob
	}
	owner := f.owner.(*remoteExecutionLease)
	owner.runtime = client
	return owner.StartOrAppendJob
}

func TestGroupedColdSchemaExplicitRetry(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, leased := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/leased=%t", backend, leased), func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				f := newDelegationFixture(t, backend, nil)
				schema := json.RawMessage(`{"chapterShape":{"type":"object"}}`)
				hash, _, err := jobdb.JobSchemaHash(schema)
				require.NoError(t, err)
				q := groupedRequest()
				q.Job.Job.Schema = &jobdb.JobSchemaSelector{Hash: hash}
				calls := 0
				var status int
				var body []byte
				observed := &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
					resp, err := f.server.Client().Transport.RoundTrip(r)
					if err == nil && strings.HasSuffix(r.URL.Path, "/start-or-append") {
						calls++
						status = resp.StatusCode
						body, err = io.ReadAll(resp.Body)
						resp.Body.Close()
						resp.Body = io.NopCloser(bytes.NewReader(body))
					}
					return resp, err
				})}
				client, err := New(f.server.URL, observed)
				require.NoError(t, err)
				submit := groupedSubmitter(t, f, client, leased)
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
				require.Empty(t, groupedJobs(t, f.backend))
				require.True(t, f.validity(t).ExpiresAt.After(time.Now()))
				require.NoError(t, f.owner.KeepAlive(ctx))
				registered, err := client.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: schema})
				require.NoError(t, err)
				require.Equal(t, hash, registered.SchemaHash)
				h, err := submit(ctx, q) // One caller-controlled retry after definite rejection.
				require.NoError(t, err)
				require.Equal(t, 2, calls)
				jobs := groupedJobs(t, f.backend)
				require.Len(t, jobs, 1)
				require.Equal(t, h.JobKey, jobs[0].JobKey)
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
		for _, leased := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/leased=%t", backend, leased), func(t *testing.T) {
				f := newDelegationFixture(t, backend, nil)
				handler, err := newServer(wrappedGroupedSchemaRuntime{f.backend, f.backend.(jobdb.ConfigurationAdmin)}, f.signer)
				require.NoError(t, err)
				server := httptest.NewServer(celltest.Handler(handler))
				defer server.Close()
				client, err := New(server.URL, server.Client())
				require.NoError(t, err)
				_, err = groupedSubmitter(t, f, client, leased)(celltest.Context(context.Background()), groupedRequest())
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
				_, err = owner.StartOrAppendJob(ctx, q)
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
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
		for _, leased := range []bool{false, true} {
			for _, existing := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/leased=%t/existing=%t", backend, leased, existing), func(t *testing.T) {
					ctx := celltest.Context(context.Background())
					f := newDelegationFixture(t, backend, nil)
					q := groupedRequest()
					if existing {
						_, err := f.client.StartOrAppendJob(ctx, q)
						require.NoError(t, err)
					}
					lost := errors.New("response lost after acceptance")
					paths := []string{}
					client, err := New(f.server.URL, &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
						paths = append(paths, r.URL.Path)
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
					_, err = groupedSubmitter(t, f, client, leased)(ctx, q)
					require.ErrorIs(t, err, lost)
					require.NotErrorIs(t, err, jobdb.ErrJobSchemaNotFound)
					require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
					require.Len(t, paths, 1)
					expected := "/v1/tenants/tenant/jobs/start-or-append"
					if leased {
						expected = delegationBase(f.owner) + "/jobs/start-or-append"
					}
					require.Equal(t, expected, paths[0])
					// An independent healthy client inspects acceptance without resubmission.
					jobs := groupedJobs(t, f.client)
					require.Len(t, jobs, 1)
					chapters, err := f.client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: jobs[0].JobKey})
					require.NoError(t, err)
					if existing {
						require.Len(t, chapters, 2)
						require.IsType(t, jobdb.AdditionalInputChapter{}, chapters[1].Body)
					} else {
						require.Len(t, chapters, 1)
					}
				})
			}
		}
	}
}
