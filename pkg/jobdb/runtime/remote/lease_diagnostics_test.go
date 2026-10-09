package remote

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/stretchr/testify/require"
)

const diagnosticSecret = "private-lease-token"

type diagnosticTransport func(*http.Request) (*http.Response, error)

func (f diagnosticTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type diagnosticBody struct {
	err          error
	read, closed bool
}

func (b *diagnosticBody) Read([]byte) (int, error) { b.read = true; return 0, b.err }
func (b *diagnosticBody) Close() error             { b.closed = true; return nil }

type observedDiagnosticBody struct {
	io.ReadCloser
	once    sync.Once
	reading chan struct{}
}

func (b *observedDiagnosticBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	return b.ReadCloser.Read(p)
}

func diagnosticLease(t *testing.T, target string, transport http.RoundTripper) *remoteExecutionLease {
	t.Helper()
	r, err := New(target, &http.Client{Transport: transport})
	require.NoError(t, err)
	return &remoteExecutionLease{runtime: r, jobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, leaseID: "lease", leaseToken: diagnosticSecret, workerID: "worker", expiresAt: time.Now().Add(time.Hour)}
}

func callDiagnosticLease(ctx context.Context, l *remoteExecutionLease, operation string) error {
	switch operation {
	case "import":
		_, err := l.runtime.ImportLease(ctx, LeaseCapability{wire: leaseCapabilityWire{Version: 1, TenantID: "tenant", JobID: "job", LeaseID: "lease", Token: diagnosticSecret}})
		return err
	case "renew":
		_, err := l.Renew(ctx)
		return err
	default:
		return l.KeepAlive(ctx)
	}
}

func assertSafeLeaseDiagnostic(t *testing.T, err error) *LeaseTransportError {
	t.Helper()
	var diagnostic *LeaseTransportError
	require.ErrorAs(t, err, &diagnostic)
	for _, value := range []any{diagnostic, *diagnostic, &jobdb.LeaseRenewalError{Err: fmt.Errorf("unsafe wrapper %s: %w", diagnosticSecret, diagnostic)}} {
		for _, format := range []string{"%s", "%v", "%+v", "%#v"} {
			formatted := fmt.Sprintf(format, value)
			for _, secret := range []string{diagnosticSecret, "url-password", "query-secret", "fragment-secret", "path-secret", "HTTP status 0"} {
				require.NotContains(t, formatted, secret)
			}
		}
	}
	for _, value := range []any{diagnostic, *diagnostic, &jobdb.LeaseRenewalError{Err: diagnostic}} {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		for _, secret := range []string{diagnosticSecret, "url-password", "query-secret", "fragment-secret", "path-secret"} {
			require.NotContains(t, string(raw), secret)
		}
	}
	return diagnostic
}

// Exercise the same failures through import, direct renewal, and legacy
// keepalive. This prevents their error contracts from diverging again.
func TestLeaseDiagnosticsTransportFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cause    error
		category LeaseFailureCategory
	}{
		{"dns", &net.DNSError{Err: diagnosticSecret, Name: "query-secret", IsNotFound: true}, LeaseFailureDNS},
		{"refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, LeaseFailureRefused},
		{"network_unreachable", syscall.ENETUNREACH, LeaseFailureUnreachable},
		{"host_unreachable", syscall.EHOSTUNREACH, LeaseFailureUnreachable},
		{"dial_timeout", &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}, LeaseFailureTimeout},
		{"dns_timeout", &net.DNSError{Err: diagnosticSecret, IsTimeout: true}, LeaseFailureTimeout},
		{"cancelled", context.Canceled, LeaseFailureCancelled},
		{"deadline", context.DeadlineExceeded, LeaseFailureTimeout},
		{"certificate", &tls.CertificateVerificationError{Err: errors.New(diagnosticSecret)}, LeaseFailureTLS},
		{"unknown_ca", x509.UnknownAuthorityError{}, LeaseFailureTLS},
		{"invalid_certificate", x509.CertificateInvalidError{Cert: &x509.Certificate{}, Reason: x509.Expired}, LeaseFailureTLS},
		{"hostname", x509.HostnameError{Certificate: &x509.Certificate{}, Host: diagnosticSecret}, LeaseFailureTLS},
		{"tls_record", tls.RecordHeaderError{Msg: diagnosticSecret}, LeaseFailureTLS},
		{"tls_alert", tls.AlertError(40), LeaseFailureTLS},
		{"unknown", errors.New("transport leaked " + diagnosticSecret), LeaseFailureUnknown},
		{"ambiguous_eof", io.ErrUnexpectedEOF, LeaseFailureUnknown},
	} {
		for _, operation := range []string{"import", "renew", "keep_alive"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				calls := 0
				lease := diagnosticLease(t, "https://user:url-password@jobdb.example:9047/path-secret?token=query-secret&unknown=query-secret#fragment-secret", diagnosticTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					require.Equal(t, diagnosticSecret, r.Header.Get("X-JobDB-Lease-Token"))
					return nil, fmt.Errorf("nested %s: %w", diagnosticSecret, tc.cause)
				}))
				err := callDiagnosticLease(context.Background(), lease, operation)
				d := assertSafeLeaseDiagnostic(t, err)
				require.ErrorIs(t, err, tc.cause)
				require.False(t, errors.Is(err, jobdb.ErrExecutionLeaseLost))
				require.Equal(t, tc.category, d.Category)
				require.Equal(t, LeasePhaseExchange, d.Phase)
				require.Equal(t, "https://jobdb.example:9047", d.Destination)
				require.False(t, d.ResponseReceived)
				require.Zero(t, d.StatusCode)
				wantOperation := operation
				if operation == "import" {
					wantOperation = "renew"
				}
				require.Equal(t, wantOperation, d.Operation)
				require.Equal(t, 1, calls, "renewal must not retry or acquire work")
			})
		}
	}
}

func validDiagnosticSnapshot() runtimeapi.ExecutionLease {
	expiry, worker := time.Now().Add(time.Hour), "worker"
	return runtimeapi.ExecutionLease{Job: runtimeapi.JobHandle{JobKey: runtimeapi.JobKey{TenantId: "tenant", JobId: "job"}}, LeaseId: "lease", LeaseToken: "replacement", WorkerId: &worker, ExpiresAt: &expiry, ClientPayloadRevision: "0"}
}

func TestLeaseDiagnosticsResponses(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		readError   error
		contentType string
		phase       LeaseFailurePhase
		category    LeaseFailureCategory
	}{
		{"malformed", `{"lease":`, nil, "application/json", LeasePhaseDecode, LeaseFailureDecode},
		{"wrong_type", `{"leaseToken":123}`, nil, "application/json", LeasePhaseDecode, LeaseFailureDecode},
		{"trailing_data", `{} {}`, nil, "application/json", LeasePhaseDecode, LeaseFailureDecode},
		{"missing_content_type", `{}`, nil, "", LeasePhaseDecode, LeaseFailureDecode},
		{"wrong_content_type", diagnosticSecret, nil, "text/html", LeasePhaseDecode, LeaseFailureDecode},
		{"truncated", "", io.ErrUnexpectedEOF, "application/json", LeasePhaseRead, LeaseFailureRead},
		{"read_error", "", errors.New(diagnosticSecret), "application/json", LeasePhaseRead, LeaseFailureRead},
		{"body_timeout", "", context.DeadlineExceeded, "application/json", LeasePhaseRead, LeaseFailureTimeout},
		{"body_cancelled", "", context.Canceled, "application/json", LeasePhaseRead, LeaseFailureCancelled},
	} {
		for _, operation := range []string{"import", "renew", "keep_alive"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				failedBody := &diagnosticBody{err: tc.readError}
				lease := diagnosticLease(t, "https://jobdb.example", diagnosticTransport(func(r *http.Request) (*http.Response, error) {
					var body io.ReadCloser = io.NopCloser(strings.NewReader(tc.body))
					if tc.readError != nil {
						body = failedBody
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{tc.contentType}}, Body: body, Request: r}, nil
				}))
				err := callDiagnosticLease(context.Background(), lease, operation)
				d := assertSafeLeaseDiagnostic(t, err)
				require.Equal(t, 200, d.StatusCode)
				require.True(t, d.ResponseReceived)
				require.Equal(t, tc.phase, d.Phase)
				require.Equal(t, tc.category, d.Category)
				require.False(t, errors.Is(err, jobdb.ErrExecutionLeaseLost))
				require.False(t, errors.Is(err, jobdb.ErrLeaseRenewalUnsupported))
				if tc.readError != nil {
					require.ErrorIs(t, err, tc.readError)
					require.True(t, failedBody.closed)
				}
				if tc.name == "malformed" {
					var syntax *json.SyntaxError
					require.ErrorAs(t, err, &syntax)
				}
				if tc.name == "wrong_type" {
					var fieldType *json.UnmarshalTypeError
					require.ErrorAs(t, err, &fieldType)
				}
				require.Equal(t, diagnosticSecret, lease.LeaseToken(), "failed keepalive must not replace credentials")
			})
		}
	}
}

func TestLeaseDiagnosticsRejectionDoesNotDependOnBody(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 409, 429, 500, 503} {
		for _, operation := range []string{"import", "renew", "keep_alive"} {
			t.Run(fmt.Sprintf("%d/%s", status, operation), func(t *testing.T) {
				body := &diagnosticBody{err: errors.New("reflected " + diagnosticSecret)}
				lease := diagnosticLease(t, "https://jobdb.example", diagnosticTransport(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: body}, nil
				}))
				err := callDiagnosticLease(context.Background(), lease, operation)
				d := assertSafeLeaseDiagnostic(t, err)
				require.Equal(t, status, d.StatusCode)
				require.True(t, d.ResponseReceived)
				require.Equal(t, LeaseFailureHTTP, d.Category)
				require.Equal(t, LeasePhaseExchange, d.Phase)
				require.Equal(t, status < 429, errors.Is(err, jobdb.ErrExecutionLeaseLost))
				require.False(t, body.read, "status must survive even a stalled or unreadable rejection body")
				require.True(t, body.closed)
			})
		}
	}
}

func TestLeaseDiagnosticsSnapshotFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*runtimeapi.ExecutionLease)
		lost bool
	}{
		{"revision", func(s *runtimeapi.ExecutionLease) { s.ClientPayloadRevision = diagnosticSecret }, false},
		{"negative_revision", func(s *runtimeapi.ExecutionLease) { s.ClientPayloadRevision = "-1" }, false},
		{"execution_state", func(s *runtimeapi.ExecutionLease) {
			invalid := diagnosticSecret
			s.ExecutionState.RunPolicy = &runtimeapi.RunPolicy{InvocationTimeout: &invalid}
		}, false},
		{"lease_id", func(s *runtimeapi.ExecutionLease) { s.LeaseId = diagnosticSecret }, true},
		{"job_id", func(s *runtimeapi.ExecutionLease) { s.Job.JobKey.JobId = diagnosticSecret }, true},
		{"tenant_id", func(s *runtimeapi.ExecutionLease) { s.Job.JobKey.TenantId = diagnosticSecret }, true},
		{"missing_worker", func(s *runtimeapi.ExecutionLease) { s.WorkerId = nil }, true},
		{"empty_worker", func(s *runtimeapi.ExecutionLease) { s.WorkerId = new(string) }, true},
		{"missing_expiry", func(s *runtimeapi.ExecutionLease) { s.ExpiresAt = nil }, true},
		{"expired", func(s *runtimeapi.ExecutionLease) { expiry := time.Now().Add(-time.Hour); s.ExpiresAt = &expiry }, true},
		{"missing_token", func(s *runtimeapi.ExecutionLease) { s.LeaseToken = "" }, true},
	} {
		for _, operation := range []string{"import", "renew", "keep_alive"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				snapshot := validDiagnosticSnapshot()
				tc.edit(&snapshot)
				raw, err := json.Marshal(runtimeapi.KeepAliveLeaseResponse{LeaseToken: "replacement", Lease: &snapshot})
				require.NoError(t, err)
				lease := diagnosticLease(t, "https://jobdb.example", diagnosticTransport(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
				}))
				err = callDiagnosticLease(context.Background(), lease, operation)
				d := assertSafeLeaseDiagnostic(t, err)
				require.Equal(t, LeasePhaseSnapshot, d.Phase)
				require.Equal(t, LeaseFailureSnapshot, d.Category)
				require.Equal(t, 200, d.StatusCode)
				require.True(t, d.ResponseReceived)
				require.NotNil(t, errors.Unwrap(d), "conversion cause must survive")
				require.Equal(t, tc.lost, errors.Is(err, jobdb.ErrExecutionLeaseLost))
				if tc.name == "revision" {
					require.Contains(t, errors.Unwrap(d).Error(), diagnosticSecret)
				}
				require.Equal(t, diagnosticSecret, lease.LeaseToken())
			})
		}
	}
}

func TestLeaseDiagnosticsWorkerBinding(t *testing.T) {
	for _, operation := range []string{"renew", "keep_alive"} {
		t.Run(operation, func(t *testing.T) {
			snapshot := validDiagnosticSnapshot()
			other := "other-worker"
			snapshot.WorkerId = &other
			raw, err := json.Marshal(runtimeapi.KeepAliveLeaseResponse{Lease: &snapshot, LeaseToken: "replacement"})
			require.NoError(t, err)
			lease := diagnosticLease(t, "https://jobdb.example", diagnosticTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
			}))
			err = callDiagnosticLease(context.Background(), lease, operation)
			d := assertSafeLeaseDiagnostic(t, err)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.Equal(t, LeasePhaseSnapshot, d.Phase)
			require.Equal(t, diagnosticSecret, lease.LeaseToken())
		})
	}
}

func TestLeaseDiagnosticsEncodedDestinationAndHeaders(t *testing.T) {
	for _, tc := range []struct{ target, destination string }{
		{"http://user:%75rl-password@127.0.0.1:9047/%70ath-secret?token=%71uery-secret&unknown=%71uery-secret#fragment-secret", "http://127.0.0.1:9047"},
		{"http://user:%75rl-password@[::1]:9047/%70ath-secret?unknown=%71uery-secret#fragment-secret", "http://[::1]:9047"},
	} {
		t.Run(tc.destination, func(t *testing.T) {
			lease := diagnosticLease(t, tc.target, diagnosticTransport(func(r *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("reflected request: %s headers: %v", r.URL, r.Header)
			}))
			lease.runtime.raw.RequestEditors = append(lease.runtime.raw.RequestEditors, func(_ context.Context, r *http.Request) error {
				r.Header.Set("Authorization", "Bearer authorization-secret")
				return nil
			})
			_, err := lease.Renew(context.Background())
			d := assertSafeLeaseDiagnostic(t, err)
			require.Equal(t, tc.destination, d.Destination)
			raw, marshalErr := json.Marshal(d)
			require.NoError(t, marshalErr)
			for _, forbidden := range []string{"%75rl-password", "%70ath-secret", "%71uery-secret", "authorization-secret", "X-Jobdb-Lease-Token"} {
				require.NotContains(t, err.Error(), forbidden)
				require.NotContains(t, string(raw), forbidden)
			}
		})
	}
}

func TestLeaseRenewalErrorOnlyDisplaysSafeCauses(t *testing.T) {
	unsafe := &jobdb.LeaseRenewalError{Err: errors.New(diagnosticSecret)}
	require.NotContains(t, unsafe.Error(), diagnosticSecret)
	require.NotContains(t, fmt.Sprintf("%#v", unsafe), diagnosticSecret)
	for _, cause := range []error{nil, context.Canceled, jobdb.ErrExecutionLeaseLost} {
		err := &jobdb.LeaseRenewalError{Err: cause}
		require.Equal(t, "execution lease renewal failed", err.Error())
		require.Equal(t, cause, errors.Unwrap(err))
	}
}

func TestLeaseDiagnosticsRequestAndRedirectFailures(t *testing.T) {
	t.Run("request_build", func(t *testing.T) {
		lease := diagnosticLease(t, "http://url-password:bad%escape", diagnosticTransport(func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid request reached transport")
			return nil, nil
		}))
		_, err := lease.Renew(context.Background())
		d := assertSafeLeaseDiagnostic(t, err)
		require.Equal(t, LeasePhaseRequest, d.Phase)
		require.Empty(t, d.Destination)
		require.False(t, d.ResponseReceived)
	})
	t.Run("redirect", func(t *testing.T) {
		cause := errors.New("redirect to " + diagnosticSecret)
		lease := diagnosticLease(t, "https://jobdb.example", diagnosticTransport(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 307, Header: http.Header{"Location": []string{"https://user:url-password@other.example/path-secret?token=query-secret#fragment-secret"}}, Body: io.NopCloser(strings.NewReader(diagnosticSecret)), Request: r}, nil
		}))
		lease.runtime.raw.Client.(*http.Client).CheckRedirect = func(*http.Request, []*http.Request) error { return cause }
		_, err := lease.Renew(context.Background())
		d := assertSafeLeaseDiagnostic(t, err)
		require.ErrorIs(t, err, cause)
		require.Equal(t, LeasePhaseExchange, d.Phase)
		require.Equal(t, LeaseFailureUnknown, d.Category)
		require.True(t, d.ResponseReceived)
		require.Equal(t, 307, d.StatusCode)
		require.Equal(t, "https://jobdb.example", d.Destination)
	})
}

func TestLeaseDiagnosticsRealHTTPFailures(t *testing.T) {
	t.Run("tls_verification", func(t *testing.T) {
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("untrusted TLS reached handler") }))
		server.Config.ErrorLog = log.New(io.Discard, "", 0)
		server.StartTLS()
		defer server.Close()
		transport := &http.Transport{Proxy: nil}
		defer transport.CloseIdleConnections()
		lease := diagnosticLease(t, server.URL, transport)
		_, err := lease.Renew(context.Background())
		d := assertSafeLeaseDiagnostic(t, err)
		require.Equal(t, LeaseFailureTLS, d.Category)
		var verification *tls.CertificateVerificationError
		require.ErrorAs(t, err, &verification)
	})
	for _, phase := range []string{"headers", "body"} {
		for _, stop := range []string{"cancel", "deadline"} {
			t.Run(phase+"/"+stop, func(t *testing.T) {
				entered := make(chan struct{})
				reading := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if phase == "body" {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
					}
					close(entered)
					<-r.Context().Done()
				}))
				defer server.Close()
				lease := diagnosticLease(t, server.URL, diagnosticTransport(func(r *http.Request) (*http.Response, error) {
					resp, err := server.Client().Transport.RoundTrip(r)
					if resp != nil {
						resp.Body = &observedDiagnosticBody{ReadCloser: resp.Body, reading: reading}
					}
					return resp, err
				}))
				var ctx context.Context
				var cancel context.CancelFunc
				if stop == "deadline" {
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
				} else {
					ctx, cancel = context.WithCancel(context.Background())
				}
				defer cancel()
				result := make(chan error, 1)
				go func() { _, err := lease.Renew(ctx); result <- err }()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("request did not reach server")
				}
				if phase == "body" {
					select {
					case <-reading:
					case <-time.After(3 * time.Second):
						t.Fatal("response body was not read")
					}
				}
				if stop == "cancel" {
					cancel()
				}
				var err error
				select {
				case err = <-result:
				case <-time.After(3 * time.Second):
					t.Fatal("renewal did not stop")
				}
				d := assertSafeLeaseDiagnostic(t, err)
				if stop == "deadline" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Equal(t, LeaseFailureTimeout, d.Category)
				} else {
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, LeaseFailureCancelled, d.Category)
				}
				if phase == "body" {
					require.Equal(t, LeasePhaseRead, d.Phase)
					require.Equal(t, 200, d.StatusCode)
					require.True(t, d.ResponseReceived)
				} else {
					require.Equal(t, LeasePhaseExchange, d.Phase)
					require.False(t, d.ResponseReceived)
				}
			})
		}
	}
}

func TestLeaseDiagnosticsLegacyAndSuccessfulResponses(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			response := runtimeapi.KeepAliveLeaseResponse{LeaseToken: "replacement"}
			if full {
				snapshot := validDiagnosticSnapshot()
				response.Lease = &snapshot
			}
			raw, err := json.Marshal(response)
			require.NoError(t, err)
			lease := diagnosticLease(t, "https://jobdb.example", diagnosticTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
			}))
			next, err := lease.Renew(context.Background())
			if full {
				require.NoError(t, err)
				require.Equal(t, "replacement", next.(*remoteExecutionLease).LeaseToken())
			} else {
				require.ErrorIs(t, err, jobdb.ErrLeaseRenewalUnsupported)
				d := assertSafeLeaseDiagnostic(t, err)
				require.Equal(t, LeaseFailureUnsupported, d.Category)
				require.Equal(t, LeasePhaseSnapshot, d.Phase)
				require.Equal(t, 200, d.StatusCode)
			}
			require.NoError(t, lease.KeepAlive(context.Background()))
			require.Equal(t, "replacement", lease.LeaseToken())
		})
	}
}
