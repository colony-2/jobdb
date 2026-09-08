package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestRootCommandDefaultsToSQLite(t *testing.T) {
	origServeHTTP := serveHTTPFunc
	defer func() {
		serveHTTPFunc = origServeHTTP
	}()

	called := 0
	var gotListenAddr string
	serveHTTPFunc = func(ctx context.Context, listenAddr string, _ http.Handler, cleanup func(context.Context) error) error {
		called++
		gotListenAddr = listenAddr
		if cleanup != nil {
			if err := cleanup(ctx); err != nil {
				t.Fatalf("cleanup returned error: %v", err)
			}
		}
		return nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--listen", "127.0.0.1:9999",
		"--db", filepath.Join(t.TempDir(), "jobdb.db"),
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("ExecuteContext returned error: %v", err)
	}
	if called != 1 {
		t.Fatalf("serveHTTPFunc called %d times, want 1", called)
	}
	if gotListenAddr != "127.0.0.1:9999" {
		t.Fatalf("listen address = %q, want custom root flag value", gotListenAddr)
	}
}

func TestResolveRequiredStringPrefersFlag(t *testing.T) {
	t.Setenv(postgresDSNEnvVar, "postgres://env")

	got, err := resolveRequiredString("postgres://flag", postgresDSNEnvVar, "postgres DSN")
	if err != nil {
		t.Fatalf("resolveRequiredString returned error: %v", err)
	}
	if got != "postgres://flag" {
		t.Fatalf("resolveRequiredString = %q, want flag value", got)
	}
}

func TestResolveRequiredStringFallsBackToEnv(t *testing.T) {
	t.Setenv(postgresDSNEnvVar, "postgres://env")

	got, err := resolveRequiredString("", postgresDSNEnvVar, "postgres DSN")
	if err != nil {
		t.Fatalf("resolveRequiredString returned error: %v", err)
	}
	if got != "postgres://env" {
		t.Fatalf("resolveRequiredString = %q, want env value", got)
	}
}

func TestResolveRequiredStringRequiresValue(t *testing.T) {
	t.Setenv(postgresDSNEnvVar, "")

	_, err := resolveRequiredString("", postgresDSNEnvVar, "postgres DSN")
	if err == nil {
		t.Fatal("resolveRequiredString returned nil error, want failure")
	}
	if got, want := err.Error(), "postgres DSN is required via --postgres-dsn or "+postgresDSNEnvVar; got != want {
		t.Fatalf("resolveRequiredString error = %q, want %q", got, want)
	}
}

func TestSQLiteConfigFromFlagsUsesBlobStoreURI(t *testing.T) {
	cfg := sqliteConfigFromFlags("jobdb.db", "", "local.blobs", "s3://jobdb-artifacts?region=us-east-1")

	if cfg.BlobStoreURI != "s3://jobdb-artifacts?region=us-east-1" {
		t.Fatalf("BlobStoreURI = %q, want configured URL", cfg.BlobStoreURI)
	}
	if cfg.BlobDir != "local.blobs" {
		t.Fatalf("BlobDir = %q, want legacy flag preserved", cfg.BlobDir)
	}
}

func TestWithHealthCheck(t *testing.T) {
	called := false
	handler := withHealthCheck(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got, want := recorder.Body.String(), "ok\n"; got != want {
		t.Fatalf("GET /healthz body = %q, want %q", got, want)
	}
	if called {
		t.Fatal("GET /healthz reached runtime handler")
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/example", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("runtime request status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if !called {
		t.Fatal("runtime request did not reach runtime handler")
	}
}
