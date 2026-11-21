package remote

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type infrastructureRuntimeFixture struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
	failure error
}

func (r infrastructureRuntimeFixture) GetJob(context.Context, jobdb.JobKey) (jobdb.JobInfo, error) {
	return jobdb.JobInfo{}, r.failure
}
func TestInfrastructureHTTPRoundTrip(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "prerequisite"}
	zero := int64(0)
	for _, reason := range []jobdb.ExecutionInfrastructureReason{jobdb.ExecutionInfrastructureDeterminism, jobdb.ExecutionInfrastructureDurableDecode, jobdb.ExecutionInfrastructureRuntimeIO} {
		for _, ordinal := range []*int64{nil, &zero} {
			t.Run(string(reason)+"/ordinal", func(t *testing.T) {
				backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
				cause := errors.New("payload password capability stack secret")
				if reason == jobdb.ExecutionInfrastructureDeterminism {
					cause = errors.Join(cause, jobdb.ErrWorkflowNotDeterministic)
				}
				failure := &jobdb.ExecutionInfrastructureError{Reason: reason, Operation: strings.Repeat("é", 100), JobKey: &key, Ordinal: ordinal, Err: cause}
				recorder := httptest.NewRecorder()
				require.True(t, writeInfrastructureError(recorder, failure))
				require.LessOrEqual(t, recorder.Body.Len(), 16<<10)
				require.NotContains(t, recorder.Body.String(), "password")
				require.Contains(t, recorder.Body.String(), `"causeTruncated":true`)
				app := NewServer(infrastructureRuntimeFixture{WorkflowRuntime: backend, ConfigurationAdmin: backend, failure: failure})
				server := httptest.NewServer(celltest.Handler(app))
				defer server.Close()
				client, err := New(server.URL, server.Client())
				require.NoError(t, err)
				_, err = client.GetJob(context.Background(), jobdb.JobKey{TenantId: "tenant", JobId: "target"})
				var detail *jobdb.ExecutionInfrastructureError
				require.ErrorAs(t, err, &detail)
				require.Equal(t, reason, detail.Reason)
				require.Equal(t, &key, detail.JobKey)
				require.Equal(t, ordinal, detail.Ordinal)
				require.LessOrEqual(t, len(detail.Operation), 128)
				var remoteCause *jobdb.RemoteExecutionCause
				require.ErrorAs(t, err, &remoteCause)
				require.Equal(t, reason == jobdb.ExecutionInfrastructureDeterminism, errors.Is(err, jobdb.ErrWorkflowNotDeterministic))
			})
		}
	}
}

type infrastructureTransport func(*http.Request) (*http.Response, error)

func (f infrastructureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestInfrastructureHTTPLegacyMalformedTransportAndDenial(t *testing.T) {
	for _, mode := range []string{"transport", "legacy", "malformed", "forbidden", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			cause := errors.New("transport cause")
			client, err := New("http://example.test", &http.Client{Transport: infrastructureTransport(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				if mode == "transport" {
					return nil, cause
				}
				status := 200
				body := "{"
				if mode == "legacy" {
					status = 500
					body = "sensitive backend input"
				}
				if mode == "forbidden" {
					status = 403
					body = "denied"
				}
				if mode == "conflict" {
					status = 409
					body = jobdb.ErrConflict.Error()
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})})
			require.NoError(t, err)
			_, err = client.GetJob(context.Background(), jobdb.JobKey{TenantId: "tenant", JobId: "job"})
			require.Error(t, err)
			require.EqualValues(t, 1, calls.Load())
			if mode == "forbidden" || mode == "conflict" {
				require.False(t, jobdb.IsExecutionInfrastructureError(err))
				return
			}
			require.True(t, jobdb.IsExecutionInfrastructureError(err))
			require.NotContains(t, err.Error(), "sensitive")
			if mode == "transport" {
				require.ErrorIs(t, err, cause)
				var urlErr *url.Error
				require.ErrorAs(t, err, &urlErr)
			}
		})
	}
}
