package projectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Exercise both generated request methods and their response parsers over HTTP.
// A documented 403 must retain its typed envelope rather than only its raw body.
func TestProjectAppendPermissionDeniedResponses(t *testing.T) {
	for _, leased := range []bool{false, true} {
		name := "ordinary"
		path := "/v1/projects/c2/jobs/start-or-append"
		if leased {
			name = "leased"
			path = "/v1/projects/c2/jobs/parent/leases/lease/jobs/start-or-append"
		}
		for _, malformed := range []bool{false, true} {
			variant := "typed"
			payload := `{"code":"lease_permission_denied","message":"lease permission denied"}`
			if malformed {
				variant = "malformed"
				payload = `{"code":`
			}
			t.Run(name+"/"+variant, func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Method != http.MethodPost || r.URL.Path != path {
						t.Errorf("request = %s %s, want POST %s", r.Method, r.URL.Path, path)
					}
					if leased && r.Header.Get("X-JobDB-Lease-Token") != "token" {
						t.Error("missing lease token")
					}
					w.Header().Set("Content-Type", "application/json; charset=utf-8")
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(payload))
				}))
				defer server.Close()
				client, err := NewClientWithResponses(server.URL, WithHTTPClient(server.Client()))
				if err != nil {
					t.Fatal(err)
				}
				var status int
				var raw []byte
				var denial *ErrorResponse
				body := strings.NewReader(`{"appendKey":"key","job":{"cell":"test","jobType":"batch","data":null},"append":null}`)
				if leased {
					response, callErr := client.ProjectstartOrAppendJobWithLeaseWithBodyWithResponse(context.Background(), "c2", "parent", "lease", &ProjectstartOrAppendJobWithLeaseParams{XJobDBLeaseToken: "token"}, "application/json", body)
					err = callErr
					if response != nil {
						status, raw, denial = response.StatusCode(), response.Body, response.JSON403
					}
				} else {
					response, callErr := client.ProjectstartOrAppendJobWithBodyWithResponse(context.Background(), "c2", "application/json", body)
					err = callErr
					if response != nil {
						status, raw, denial = response.StatusCode(), response.Body, response.JSON403
					}
				}
				if calls.Load() != 1 {
					t.Fatalf("requests = %d, want one", calls.Load())
				}
				if malformed {
					if err == nil {
						t.Fatal("malformed JSON403 must fail decoding")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if status != http.StatusForbidden || string(raw) != payload {
					t.Fatalf("response = %d %q", status, raw)
				}
				if denial == nil || denial.Code != ErrorCodeLeasePermissionDenied || denial.Message != "lease permission denied" || denial.Details != nil {
					t.Fatalf("typed JSON403 = %+v", denial)
				}
			})
		}
	}
}
