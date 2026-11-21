package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

type semanticAppendProbe struct {
	*groupedDispatchProbe
	failure                     error
	ordinary, leased, submitted int
}

func (p *semanticAppendProbe) StartOrAppendJob(context.Context, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	p.ordinary++
	return jobdb.JobHandle{}, p.failure
}
func (p *semanticAppendProbe) StartOrAppendJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	p.leased++
	return jobdb.JobHandle{}, p.failure
}
func (p *semanticAppendProbe) SubmitJob(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	p.submitted++
	return jobdb.JobHandle{}, errors.New("unexpected fallback submission")
}

func TestGroupedSemanticRejections(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "diagnostic-job"}
	remainder := 7
	detailed := &jobdb.ChapterApplicationPayloadError{
		JobKey: &key, LogicalID: "invocation", RecordKind: "start", Attempt: 2,
		DiagnosticsTruncated: true, LimitBytes: 100, ActualBytes: 201, ExcessBytes: 101,
		Ordinal: 3, TaskType: "batch", OmittedFields: 2, OmittedBytes: 5,
		Contributors: []jobdb.PayloadContribution{{Location: "input", Bytes: 201,
			JSON: &jobdb.JSONPayloadStats{
				Status: "complete", RootKind: "object", Members: 2, Elements: 3,
				Scalars: 4, MaxDepth: 5, LargestStringBytes: 6, RootElements: 3,
				RootRemainderBytes: &remainder,
				RootMembers:        []jobdb.JSONPayloadField{{Path: "$.value", Kind: "string", Bytes: 99, Elements: 2}},
				Largest:            []jobdb.JSONPayloadField{{Path: "$.value", Kind: "string", Bytes: 99, Elements: 2}},
			},
		}},
	}
	for _, backend := range []string{"toy", "sqlite"} {
		for _, mode := range []string{"ordinary", "owner", "delegated"} {
			for _, state := range []string{"create", "join", "successor"} {
				for _, rejection := range []struct {
					name string
					err  error
				}{
					{"permission", jobdb.ErrLeasePermissionDenied},
					{"payload-sentinel", jobdb.ErrChapterApplicationPayloadTooLarge},
					{"payload-detail", detailed},
				} {
					failure := rejection.err
					for _, wrapped := range []bool{false, true} {
						name := fmt.Sprintf("%s/%s/%s/%s/wrapped=%t", backend, mode, state, rejection.name, wrapped)
						t.Run(name, func(t *testing.T) {
							ctx := celltest.Context(context.Background())
							f := newDelegationFixture(t, backend, nil)
							q, _ := seedGrouped(t, f, state)
							p := &semanticAppendProbe{groupedDispatchProbe: &groupedDispatchProbe{WorkflowRuntime: f.backend, ConfigurationAdmin: f.backend.(jobdb.ConfigurationAdmin), leaseOperationRuntime: runtimeLeaseOps(f.backend)}, failure: failure}
							if wrapped {
								p.failure = fmt.Errorf("secret-wrapper-marker: %w", failure)
							}
							handler, err := newServer(p, f.signer)
							require.NoError(t, err)
							server := httptest.NewServer(celltest.Handler(handler))
							defer server.Close()
							calls := 0
							var status int
							var raw []byte
							client, err := New(server.URL, &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
								calls++
								resp, err := server.Client().Transport.RoundTrip(r)
								if err != nil {
									return nil, err
								}
								status = resp.StatusCode
								raw, err = io.ReadAll(resp.Body)
								resp.Body.Close()
								resp.Body = io.NopCloser(strings.NewReader(string(raw)))
								return resp, err
							})})
							require.NoError(t, err)
							submit, _ := groupedSubmitter(t, f, client, mode)
							before := groupedState(t, f)
							_, err = submit(ctx, q)
							sentinel := jobdb.ErrChapterApplicationPayloadTooLarge
							code := jobdb.ChapterApplicationPayloadTooLargeCode
							wantStatus := 413
							if errors.Is(failure, jobdb.ErrLeasePermissionDenied) {
								sentinel = jobdb.ErrLeasePermissionDenied
								code = "lease_permission_denied"
								wantStatus = 403
							}
							require.ErrorIs(t, err, sentinel)
							require.NotErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
							require.False(t, jobdb.IsExecutionInfrastructureError(err))
							require.Equal(t, wantStatus, status)
							require.Equal(t, 1, calls)
							require.Zero(t, p.submitted)
							if mode == "ordinary" {
								require.Equal(t, 1, p.ordinary)
								require.Zero(t, p.leased)
							} else {
								require.Zero(t, p.ordinary)
								require.Equal(t, 1, p.leased)
							}
							require.Equal(t, before, groupedState(t, f))
							var envelope struct {
								Code, Message string
								Details       *jobdb.ChapterApplicationPayloadError
							}
							require.NoError(t, json.Unmarshal(raw, &envelope))
							require.Equal(t, code, envelope.Code)
							require.NotContains(t, string(raw), "secret-wrapper-marker")
							var got *jobdb.ChapterApplicationPayloadError
							if failure == detailed {
								require.ErrorAs(t, err, &got)
								require.Equal(t, detailed, got)
								require.Equal(t, detailed, envelope.Details)
								require.Equal(t, detailed.Error(), envelope.Message)
							} else {
								require.False(t, errors.As(err, &got))
								require.Nil(t, envelope.Details)
								require.Equal(t, sentinel.Error(), envelope.Message)
							}
						})
					}
				}
			}
		}
	}
}

func TestPayloadSizeSentinelEnvelope(t *testing.T) {
	for _, body := range []string{`{"code":"chapter_application_payload_too_large"}`, `{"code":"chapter_application_payload_too_large","details":null}`} {
		err := payloadSizeResponseError([]byte(body))
		require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
		var detail *jobdb.ChapterApplicationPayloadError
		require.False(t, errors.As(err, &detail))
	}
	for _, body := range []string{`{`, `{"code":"other"}`, `{"code":"chapter_application_payload_too_large","details":"invalid"}`} {
		require.Nil(t, payloadSizeResponseError([]byte(body)))
	}
}

func TestGroupedRealPayloadRejectionAtomicity(t *testing.T) {
	for _, mode := range []string{"direct", "ordinary", "owner", "delegated"} {
		for _, state := range []string{"create", "join", "successor"} {
			t.Run(mode+"/"+state, func(t *testing.T) {
				f := newDelegationFixture(t, "sqlite", nil)
				q, _ := seedGrouped(t, f, state)
				submit := f.backend.StartOrAppendJob
				if mode != "direct" {
					submit, _ = groupedSubmitter(t, f, f.client, mode)
				}
				q.Job.Append = json.RawMessage(`"` + strings.Repeat("x", int(jobdb.DefaultMaxChapterApplicationPayloadBytes)) + `"`)
				before := groupedState(t, f)
				_, err := submit(celltest.Context(context.Background()), q)
				require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
				require.False(t, jobdb.IsExecutionInfrastructureError(err))
				var detail *jobdb.ChapterApplicationPayloadError
				require.ErrorAs(t, err, &detail)
				require.Greater(t, detail.ActualBytes, detail.LimitBytes)
				require.Equal(t, before, groupedState(t, f))
			})
		}
	}
}

type lostAppendBody struct{ cause error }

func (b lostAppendBody) Read([]byte) (int, error) { return 0, b.cause }
func (b lostAppendBody) Close() error             { return nil }

func TestGroupedTransportUncertainty(t *testing.T) {
	for _, mode := range []string{"ordinary", "owner", "delegated"} {
		for _, state := range []string{"create", "join", "successor"} {
			for _, failure := range []string{"before-dispatch", "interrupted-body"} {
				t.Run(mode+"/"+state+"/"+failure, func(t *testing.T) {
					f := newDelegationFixture(t, "sqlite", nil)
					q, _ := seedGrouped(t, f, state)
					before := groupedState(t, f)
					cause := errors.New("private-local-transport-cause")
					calls := 0
					client, err := New(f.server.URL, &http.Client{Transport: appendTransportFunc(func(r *http.Request) (*http.Response, error) {
						calls++
						if failure == "before-dispatch" {
							return nil, cause
						}
						resp, err := f.server.Client().Transport.RoundTrip(r)
						if err != nil {
							return nil, err
						}
						require.Equal(t, http.StatusOK, resp.StatusCode)
						resp.Body.Close()
						resp.Body = lostAppendBody{cause}
						return resp, nil
					})})
					require.NoError(t, err)
					submit, _ := groupedSubmitter(t, f, client, mode)
					_, err = submit(celltest.Context(context.Background()), q)
					require.ErrorIs(t, err, cause)
					var infrastructure *jobdb.ExecutionInfrastructureError
					require.ErrorAs(t, err, &infrastructure)
					require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, infrastructure.Reason)
					require.Equal(t, 1, calls)
					require.NotErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
					require.NotErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
					after := groupedState(t, f)
					if failure == "before-dispatch" {
						require.Equal(t, before, after)
					} else {
						require.NotEqual(t, before, after, "accepted write must remain observable through independent reads")
						want := 1
						if state == "successor" {
							want = 2
						}
						require.Len(t, groupedJobs(t, f.backend), want)
					}
				})
			}
		}
	}
}
