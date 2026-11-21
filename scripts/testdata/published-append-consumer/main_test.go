package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
)

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func localRuntime(t *testing.T) (*sqlite.Runtime, context.Context) {
	t.Helper()
	ctx := celltest.Context(context.Background())
	r, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(t.TempDir(), "consumer.db")})
	check(t, err)
	t.Cleanup(func() { check(t, r.Close(ctx)) })
	_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	check(t, err)
	return r, ctx
}
func appendRequest() jobdb.StartOrAppendJobRequest {
	return jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "consumer", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("input", jobdb.NewArtifactFromBytes("saved", []byte("artifact")))}, Append: json.RawMessage(`{"n":1}`)}}
}
func TestPublishedCancelledReads(t *testing.T) {
	for _, additional := range []bool{false, true} {
		t.Run(fmt.Sprint(additional), func(t *testing.T) {
			r, ctx := localRuntime(t)
			q := appendRequest()
			h, err := r.StartOrAppendJob(ctx, q)
			check(t, err)
			if additional {
				_, err = r.StartOrAppendJob(ctx, q)
				check(t, err)
			}
			reason := " \nconsumer cancellation\t"
			check(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey, Reason: reason}))
			before, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
			check(t, err)
			server := httptest.NewServer(celltest.Handler(remote.NewServer(r)))
			defer server.Close()
			client, err := remote.New(server.URL, server.Client())
			check(t, err)
			for _, reader := range []jobdb.WorkflowRuntime{r, client} {
				info, err := reader.GetJob(ctx, h.JobKey)
				check(t, err)
				if info.Status != jobdb.JobStatusCancelled || info.CompletionDetail != reason || info.CompletionStatus != "cancelled" {
					t.Fatalf("cancelled info: %+v", info)
				}
				_, err = info.Data.GetData()
				check(t, err)
				artifacts, err := info.Data.GetArtifacts()
				check(t, err)
				if !additional {
					if len(artifacts) != 1 {
						t.Fatal("missing retained artifact")
					}
					stream, err := artifacts[0].Open()
					check(t, err)
					got, err := io.ReadAll(stream)
					check(t, err)
					check(t, stream.Close())
					if string(got) != "artifact" {
						t.Fatal("artifact changed")
					}
				}
				run, err := jobdb.GetJobRun(ctx, reader, jobdb.GetJobRunRequest{JobKey: h.JobKey})
				check(t, err)
				_, err = run.GetOutput(nil, h.JobKey.TenantId)
				var cancelled *jobdb.JobCancelledError
				if !errors.Is(err, jobdb.ErrJobCancelled) || !errors.As(err, &cancelled) || cancelled.Detail != reason {
					t.Fatalf("cancelled output: %v", err)
				}
			}
			after, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
			check(t, err)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("reads mutated history")
			}
		})
	}
}

type rejectingRuntime struct {
	*sqlite.Runtime
	failure                     error
	ordinary, leased, submitted int
}

func (r *rejectingRuntime) StartOrAppendJob(context.Context, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	r.ordinary++
	return jobdb.JobHandle{}, r.failure
}
func (r *rejectingRuntime) StartOrAppendJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	r.leased++
	return jobdb.JobHandle{}, r.failure
}
func (r *rejectingRuntime) SubmitJob(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	r.submitted++
	return jobdb.JobHandle{}, errors.New("unexpected submit")
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestPublishedSemanticAppend(t *testing.T) {
	detailed := &jobdb.ChapterApplicationPayloadError{LogicalID: "consumer", RecordKind: "start", LimitBytes: 1, ActualBytes: 3, ExcessBytes: 2, Ordinal: 2, Contributors: []jobdb.PayloadContribution{{Location: "input", Bytes: 3}}}
	for _, mode := range []string{"ordinary", "owner", "delegated"} {
		for _, failure := range []error{jobdb.ErrLeasePermissionDenied, jobdb.ErrChapterApplicationPayloadTooLarge, detailed} {
			for _, wrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%v/wrapped=%t", mode, failure, wrapped), func(t *testing.T) {
					r, ctx := localRuntime(t)
					q := appendRequest()
					parent, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "parent", Data: jobdb.NewTaskDataOrPanic("parent")}})
					check(t, err)
					probe := &rejectingRuntime{Runtime: r, failure: failure}
					if wrapped {
						probe.failure = fmt.Errorf("private-wrapper-marker: %w", failure)
					}
					server := httptest.NewServer(celltest.Handler(remote.NewServer(probe)))
					defer server.Close()
					requests := 0
					status := 0
					var raw []byte
					client, err := remote.New(server.URL, &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
						resp, err := server.Client().Transport.RoundTrip(req)
						if err != nil {
							return nil, err
						}
						if strings.HasSuffix(req.URL.Path, "/start-or-append") {
							requests++
							status = resp.StatusCode
							raw, err = io.ReadAll(resp.Body)
							resp.Body.Close()
							resp.Body = io.NopCloser(strings.NewReader(string(raw)))
						}
						return resp, err
					})})
					check(t, err)
					submit := client.StartOrAppendJob
					if mode != "ordinary" {
						lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "consumer", Routes: []jobdb.Route{{JobType: "parent"}}, LeaseDuration: time.Minute})
						check(t, err)
						if lease == nil {
							t.Fatal("missing owner lease")
						}
						if mode == "delegated" {
							lease, err = client.CreateDelegation(ctx, lease, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true}}}})
							check(t, err)
						}
						submit = lease.StartOrAppendJob
					}
					before, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true})
					check(t, err)
					_, err = submit(ctx, q)
					sentinel := jobdb.ErrChapterApplicationPayloadTooLarge
					wantStatus := 413
					code := jobdb.ChapterApplicationPayloadTooLargeCode
					if errors.Is(failure, jobdb.ErrLeasePermissionDenied) {
						sentinel = jobdb.ErrLeasePermissionDenied
						wantStatus = 403
						code = "lease_permission_denied"
					}
					if !errors.Is(err, sentinel) || jobdb.IsExecutionInfrastructureError(err) || errors.Is(err, jobdb.ErrExecutionLeaseLost) {
						t.Fatalf("semantic identity: %v", err)
					}
					if requests != 1 || probe.ordinary+probe.leased != 1 || probe.submitted != 0 || status != wantStatus {
						t.Fatalf("dispatch/status: %d %+v %d", requests, probe, status)
					}
					if (mode == "ordinary" && probe.ordinary != 1) || (mode != "ordinary" && probe.leased != 1) {
						t.Fatal("wrong append route")
					}
					var envelope struct {
						Code, Message string
						Details       *jobdb.ChapterApplicationPayloadError
					}
					check(t, json.Unmarshal(raw, &envelope))
					if envelope.Code != code || strings.Contains(string(raw), "private-wrapper-marker") {
						t.Fatalf("unsafe/wrong envelope: %s", raw)
					}
					var got *jobdb.ChapterApplicationPayloadError
					if failure == detailed {
						if !errors.As(err, &got) || !reflect.DeepEqual(got, detailed) || !reflect.DeepEqual(envelope.Details, detailed) || envelope.Message != detailed.Error() {
							t.Fatalf("detailed error: %v %s", err, raw)
						}
					} else if errors.As(err, &got) || envelope.Details != nil || envelope.Message != sentinel.Error() {
						t.Fatalf("sentinel details/message: %s", raw)
					}
					after, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true})
					check(t, err)
					if !reflect.DeepEqual(before, after) {
						t.Fatal("rejection changed stored jobs")
					}
				})
			}
		}
	}
}

func TestPublishedPayloadRejection(t *testing.T) {
	for _, mode := range []string{"direct", "http"} {
		for _, state := range []string{"create", "join", "successor"} {
			t.Run(mode+"/"+state, func(t *testing.T) {
				r, ctx := localRuntime(t)
				q := appendRequest()
				q.Job.MaxInputs = 2
				for i := 0; i < map[string]int{"create": 0, "join": 1, "successor": 2}[state]; i++ {
					_, err := r.StartOrAppendJob(ctx, q)
					check(t, err)
				}
				var runtime jobdb.WorkflowRuntime = r
				if mode == "http" {
					server := httptest.NewServer(celltest.Handler(remote.NewServer(r)))
					defer server.Close()
					client, err := remote.New(server.URL, server.Client())
					check(t, err)
					runtime = client
				}
				before, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true})
				check(t, err)
				chapters := map[jobdb.JobKey][]jobdb.Chapter{}
				for _, j := range before.Jobs {
					chapters[j.JobKey], err = r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: j.JobKey})
					check(t, err)
				}
				q.Job.Append = json.RawMessage(`"` + strings.Repeat("x", int(jobdb.DefaultMaxChapterApplicationPayloadBytes)) + `"`)
				_, err = runtime.StartOrAppendJob(ctx, q)
				var detail *jobdb.ChapterApplicationPayloadError
				if !errors.Is(err, jobdb.ErrChapterApplicationPayloadTooLarge) || !errors.As(err, &detail) || detail.ActualBytes <= detail.LimitBytes || jobdb.IsExecutionInfrastructureError(err) {
					t.Fatalf("real payload rejection: %v", err)
				}
				after, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, IncludeHidden: true})
				check(t, err)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("payload rejection changed jobs")
				}
				for key, want := range chapters {
					got, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
					check(t, err)
					if !reflect.DeepEqual(want, got) {
						t.Fatal("payload rejection changed history")
					}
				}
			})
		}
	}
}
