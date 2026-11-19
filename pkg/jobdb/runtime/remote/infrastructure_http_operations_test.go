package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestInfrastructureHTTPOperationSource(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	origin := jobdb.JobKey{TenantId: "tenant", JobId: "prerequisite"}
	cause := errors.New("transport unavailable")
	zero := int64(0)
	for _, ordinal := range []int64{0, 7} {
		for _, operation := range []string{"put-chapter", "dependency-wait", "complete", "reschedule", "open-artifact", "complete-task"} {
			for _, mode := range []string{"legacy", "legacy-json", "malformed-typed", "malformed-json", "transport", "server-origin", "server-unknown", "server-determinism", "transport-origin", "forbidden", "unauthorized", "lease-lost", "conflict", "not-found", "invalid-request"} {
				t.Run(fmt.Sprintf("%s/%d/%s", operation, ordinal, mode), func(t *testing.T) {
					serverKey, serverOrdinal := &origin, &zero
					if mode == "server-unknown" {
						serverKey, serverOrdinal = nil, nil
					}
					reason := jobdb.ExecutionInfrastructureDurableDecode
					if mode == "server-determinism" {
						reason = jobdb.ExecutionInfrastructureDeterminism
						serverOrdinal = nil
					}
					failure := jobdb.ClassifyExecutionInfrastructure(cause, reason, "source_read", serverKey, serverOrdinal)
					recorder := httptest.NewRecorder()
					require.True(t, writeInfrastructureError(recorder, failure))
					calls := 0
					client, err := New("http://example.test", &http.Client{Transport: infrastructureTransport(func(*http.Request) (*http.Response, error) {
						calls++
						if mode == "transport" {
							return nil, cause
						}
						if mode == "transport-origin" {
							return nil, failure
						}
						status, body, contentType := 500, recorder.Body.String(), "application/json"
						switch mode {
						case "legacy":
							status, body, contentType = 503, "unavailable", "text/plain"
						case "legacy-json":
							status, body = 503, "unavailable"
						case "malformed-typed":
							body = `{"code":"execution_infrastructure_error","infrastructure":{"reason":"invalid"}}`
						case "malformed-json":
							body = "{"
						case "forbidden":
							status = 403 // A typed-looking body cannot override authorization.
						case "unauthorized":
							status, body = 401, `{"code":"invalid_delegation_credential","message":"expired"}`
						case "lease-lost":
							status, body = 409, `{"code":"execution_lease_lost","message":"lease lost"}`
						case "conflict":
							status, body = 409, `{"code":"conflict","message":"`+jobdb.ErrConflict.Error()+`"}`
						case "not-found":
							status, body = 404, `{"code":"not_found","message":"missing"}`
						case "invalid-request":
							status, body = 400, `{"code":"invalid_request","message":"invalid"}`
						}
						return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(body))}, nil
					})})
					require.NoError(t, err)
					lease := &remoteExecutionLease{runtime: client, jobKey: key, leaseID: "lease", leaseToken: "token"}
					chapter := delegationChapter(ordinal)
					switch operation {
					case "put-chapter":
						err = client.PutChapter(context.Background(), jobdb.PutChapterRequest{LeaseID: "lease", LeaseToken: "token", Ref: jobdb.ChapterRef{JobKey: key, Ordinal: ordinal}, Chapter: chapter})
					case "dependency-wait":
						_, err = client.GetDependencyWait(context.Background(), jobdb.DependencyWaitKey{JobKey: key, WaitID: "wait"})
					case "complete":
						err = lease.Complete(context.Background(), jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter})
					case "open-artifact":
						_, err = client.OpenArtifact(context.Background(), jobdb.ArtifactRef{JobKey: key, Ordinal: ordinal, Name: "artifact"})
					case "complete-task":
						err = client.CompleteTaskIfWaiting(context.Background(), jobdb.CompleteTaskIfWaitingRequest{JobKey: key, Route: jobdb.Route{JobType: "job", TaskType: "task"}, InputOrdinal: ordinal, OutputOrdinal: ordinal, Data: jobdb.NewTaskDataOrPanic("output")})
					case "reschedule":
						err = lease.Reschedule(context.Background(), jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}})
					}
					require.Equal(t, 1, calls, "reach the boundary exactly once")
					require.Error(t, err)
					switch mode {
					case "forbidden", "unauthorized", "lease-lost", "conflict", "not-found", "invalid-request":
						require.False(t, jobdb.IsExecutionInfrastructureError(err), "semantic/authorization handling: %v", err)
						if mode == "forbidden" {
							require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
						}
						if mode == "unauthorized" || mode == "lease-lost" {
							require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
						}
						if mode == "conflict" {
							require.ErrorIs(t, err, jobdb.ErrConflict)
						}
						return
					}
					var detail *jobdb.ExecutionInfrastructureError
					require.ErrorAs(t, err, &detail)
					if strings.HasPrefix(mode, "server-") || mode == "transport-origin" {
						require.Equal(t, serverKey, detail.JobKey)
						require.Equal(t, serverOrdinal, detail.Ordinal)
						require.Equal(t, reason, detail.Reason)
						require.Equal(t, "source_read", detail.Operation)
						if mode == "server-determinism" {
							require.ErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
						}
					} else {
						require.Equal(t, &key, detail.JobKey)
						var position *int64
						if operation != "dependency-wait" && operation != "reschedule" {
							position = &ordinal
						}
						require.Equal(t, position, detail.Ordinal, "physical source/attempted append ordinal")
						require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, detail.Reason)
						require.NotEmpty(t, detail.Operation)
					}
					if strings.HasPrefix(mode, "transport") {
						require.ErrorIs(t, err, cause)
					}
				})
			}
		}
	}
}

type infrastructureBodyFailure struct{ err error }

func (r infrastructureBodyFailure) Read([]byte) (int, error) { return 0, r.err }

func TestInfrastructureHTTPArtifactBodySource(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	cause := errors.New("response stream interrupted")
	for _, failure := range []error{cause, jobdb.ClassifyExecutionInfrastructure(cause, jobdb.ExecutionInfrastructureDurableDecode, "other_source", nil, nil)} {
		client, err := New("http://example.test", &http.Client{Transport: infrastructureTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(infrastructureBodyFailure{failure})}, nil
		})})
		require.NoError(t, err)
		_, err = client.OpenArtifact(context.Background(), jobdb.ArtifactRef{JobKey: key, Ordinal: 7, Name: "artifact"})
		require.ErrorIs(t, err, cause)
		var detail *jobdb.ExecutionInfrastructureError
		require.ErrorAs(t, err, &detail)
		if failure == cause {
			require.Equal(t, &key, detail.JobKey)
			position := int64(7)
			require.Equal(t, &position, detail.Ordinal)
			require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, detail.Reason)
		} else {
			require.Nil(t, detail.JobKey)
			require.Nil(t, detail.Ordinal)
			require.Equal(t, "other_source", detail.Operation)
		}
	}
}
