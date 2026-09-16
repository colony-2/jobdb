package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthcheckTargetURLDefaultsFromListenEnvironment(t *testing.T) {
	t.Setenv(listenAddrEnvVar, "0.0.0.0:8080")

	got, err := healthcheckTargetURL(nil)
	if err != nil {
		t.Fatalf("healthcheckTargetURL returned error: %v", err)
	}
	if want := "http://127.0.0.1:8080/healthz"; got != want {
		t.Fatalf("healthcheckTargetURL = %q, want %q", got, want)
	}
}

func TestHealthcheckTargetURLSupportsIPv6Wildcard(t *testing.T) {
	t.Setenv(listenAddrEnvVar, "[::]:8080")

	got, err := healthcheckTargetURL(nil)
	if err != nil {
		t.Fatalf("healthcheckTargetURL returned error: %v", err)
	}
	if want := "http://127.0.0.1:8080/healthz"; got != want {
		t.Fatalf("healthcheckTargetURL = %q, want %q", got, want)
	}
}

func TestHealthcheckTargetURLAcceptsBaseURL(t *testing.T) {
	got, err := healthcheckTargetURL([]string{"https://jobdb.example.test/runtime/"})
	if err != nil {
		t.Fatalf("healthcheckTargetURL returned error: %v", err)
	}
	if want := "https://jobdb.example.test/runtime/healthz"; got != want {
		t.Fatalf("healthcheckTargetURL = %q, want %q", got, want)
	}
}

func TestHealthcheckTargetURLRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "unsupported scheme", args: []string{"ftp://jobdb.example.test"}},
		{name: "missing scheme", args: []string{"jobdb.example.test:8080"}},
		{name: "query", args: []string{"https://jobdb.example.test?x=1"}},
		{name: "too many", args: []string{"https://one.example.test", "https://two.example.test"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := healthcheckTargetURL(tt.args); err == nil {
				t.Fatal("healthcheckTargetURL returned nil error")
			}
		})
	}
}

func TestRunHealthcheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("request path = %q, want /healthz", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := runHealthcheck(context.Background(), server.Client(), server.URL+"/healthz"); err != nil {
		t.Fatalf("runHealthcheck returned error: %v", err)
	}
}

func TestRunHealthcheckRejectsUnhealthyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	if err := runHealthcheck(context.Background(), server.Client(), server.URL+"/healthz"); err == nil {
		t.Fatal("runHealthcheck returned nil error")
	}
}
