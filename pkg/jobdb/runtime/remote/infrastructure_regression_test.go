package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInfrastructureRegressionHTTPTransportRetainsKnownSource(t *testing.T) {
	cause := errors.New("transport unavailable")
	client, err := New("http://example.test", &http.Client{Transport: infrastructureTransport(func(*http.Request) (*http.Response, error) { return nil, cause })})
	if err != nil {
		t.Fatal(err)
	}
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	ordinal := int64(7)
	_, err = client.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: key, Ordinal: ordinal})
	var detail *jobdb.ExecutionInfrastructureError
	if !errors.As(err, &detail) {
		t.Fatalf("not typed: %v", err)
	}
	if detail.JobKey == nil || detail.Ordinal == nil || *detail.JobKey != key || *detail.Ordinal != ordinal {
		t.Fatalf("known get-chapter identity lost: key=%v ordinal=%v operation=%s", detail.JobKey, detail.Ordinal, detail.Operation)
	}
}
func TestInfrastructureRegressionMalformedTypedHTTPEnvelope(t *testing.T) {
	body := `{"code":"execution_infrastructure_error","infrastructure":{"reason":"not_a_reason","operation":"` + strings.Repeat("x", 200) + `","cause":{"code":"unknown","message":"test"}}}`
	client, err := New("http://example.test", &http.Client{Transport: infrastructureTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetJob(context.Background(), jobdb.JobKey{TenantId: "tenant", JobId: "job"})
	var detail *jobdb.ExecutionInfrastructureError
	if !errors.As(err, &detail) {
		t.Fatalf("not typed: %v", err)
	}
	if detail.Reason != jobdb.ExecutionInfrastructureRuntimeIO || len(detail.Operation) > 128 {
		t.Fatalf("malformed envelope accepted: reason=%q operationBytes=%d", detail.Reason, len(detail.Operation))
	}
}

func TestInfrastructureMalformedEnvelopeFieldsUseSafeFallback(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	recorder := httptest.NewRecorder()
	require.True(t, writeInfrastructureError(recorder, jobdb.ClassifyExecutionInfrastructure(errors.New("source"), jobdb.ExecutionInfrastructureDurableDecode, "decode", &key, nil)))
	for _, field := range []string{"reason", "operation", "identity", "ordinal", "cause-message", "cause-code", "message", "missing-cause", "null-truncated"} {
		t.Run(field, func(t *testing.T) {
			var value map[string]any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &value))
			detail := value["infrastructure"].(map[string]any)
			switch field {
			case "reason":
				detail["reason"] = "future"
			case "operation":
				detail["operation"] = strings.Repeat("é", 65)
			case "identity":
				detail["jobId"] = nil
			case "ordinal":
				detail["ordinal"] = -1
			case "cause-message":
				detail["cause"].(map[string]any)["message"] = strings.Repeat("s", 4097)
			case "cause-code":
				detail["cause"].(map[string]any)["code"] = strings.Repeat("s", 129)
			case "message":
				value["message"] = strings.Repeat("s", 4097)
			case "missing-cause":
				delete(detail, "cause")
			case "null-truncated":
				detail["causeTruncated"] = nil
			}
			raw, err := json.Marshal(value)
			require.NoError(t, err)
			result := responseError("get_job", 500, raw, nil)
			var classified *jobdb.ExecutionInfrastructureError
			require.ErrorAs(t, result, &classified)
			require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, classified.Reason)
			require.Equal(t, "get_job", classified.Operation)
			require.NotContains(t, result.Error(), strings.Repeat("s", 100))
		})
	}
	// A typed envelope cannot turn an authorization status into infrastructure.
	require.ErrorIs(t, responseError("get_job", 403, recorder.Body.Bytes(), nil), jobdb.ErrLeasePermissionDenied)
}

func TestInfrastructureHTTPFallbackRetainsKnownSource(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	origin := jobdb.JobKey{TenantId: "tenant", JobId: "prerequisite"}
	ordinal := int64(7)
	for _, operation := range []struct {
		name     string
		read     func(*Runtime) error
		position *int64
	}{
		{"chapter", func(c *Runtime) error {
			_, err := c.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: key, Ordinal: ordinal})
			return err
		}, &ordinal},
		{"chapters", func(c *Runtime) error {
			_, err := c.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			return err
		}, nil},
		{"job", func(c *Runtime) error { _, err := c.GetJob(context.Background(), key); return err }, nil},
		{"delegated-chapter", func(c *Runtime) error {
			_, err := c.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: key, Ordinal: ordinal, LeaseID: "lease", LeaseToken: "token"})
			return err
		}, &ordinal},
		{"delegated-chapters", func(c *Runtime) error {
			_, err := c.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key, LeaseID: "lease", LeaseToken: "token"})
			return err
		}, nil},
	} {
		for _, response := range []struct {
			name   string
			status int
			body   string
		}{
			{"legacy", 503, "unavailable"},
			{"malformed-typed", 500, `{"code":"execution_infrastructure_error","infrastructure":{"reason":"invalid"}}`},
			{"empty-success", 200, "null"},
		} {
			// Empty JSON objects have endpoint-specific success/decode semantics. Keep
			// the original GetChapter control, without changing those other contracts.
			if response.status == 200 && operation.name != "chapter" {
				continue
			}
			t.Run(operation.name+"/"+response.name, func(t *testing.T) {
				calls := 0
				client, err := New("http://example.test", &http.Client{Transport: infrastructureTransport(func(*http.Request) (*http.Response, error) {
					calls++
					return &http.Response{StatusCode: response.status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(response.body))}, nil
				})})
				require.NoError(t, err)
				err = operation.read(client)
				var detail *jobdb.ExecutionInfrastructureError
				require.ErrorAs(t, err, &detail)
				require.Equal(t, &key, detail.JobKey)
				require.Equal(t, operation.position, detail.Ordinal)
				if response.status == 200 {
					require.Equal(t, jobdb.ExecutionInfrastructureDurableDecode, detail.Reason)
				} else {
					require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, detail.Reason)
				}
				require.Equal(t, 1, calls)
			})
		}
	}

	// A valid server origin, including an explicitly unknown identity, wins over
	// local request context. It can describe a failed prerequisite instead.
	for _, known := range []bool{true, false} {
		t.Run(fmt.Sprintf("valid-origin-%t", known), func(t *testing.T) {
			var source *jobdb.JobKey
			var position *int64
			if known {
				source, position = &origin, new(int64)
			}
			recorder := httptest.NewRecorder()
			require.True(t, writeInfrastructureError(recorder, jobdb.ClassifyExecutionInfrastructure(errors.New("source"), jobdb.ExecutionInfrastructureDurableDecode, "decode_source", source, position)))
			client, err := New("http://example.test", &http.Client{Transport: infrastructureTransport(func(*http.Request) (*http.Response, error) { return recorder.Result(), nil })})
			require.NoError(t, err)
			_, err = client.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: key, Ordinal: ordinal})
			var detail *jobdb.ExecutionInfrastructureError
			require.ErrorAs(t, err, &detail)
			require.Equal(t, source, detail.JobKey)
			require.Equal(t, position, detail.Ordinal)
			require.Equal(t, "decode_source", detail.Operation)
			require.Equal(t, jobdb.ExecutionInfrastructureDurableDecode, detail.Reason)
		})
	}
}
