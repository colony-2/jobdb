package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServeConfigDefaultsAndRequiredValues(t *testing.T) {
	wantKey := validServeEnv(t)

	cfg, err := serveConfigFromEnv()
	if err != nil {
		t.Fatalf("serveConfigFromEnv returned error: %v", err)
	}
	if got, want := cfg.ListenAddr, defaultServeListenAddr; got != want {
		t.Fatalf("ListenAddr = %q, want %q", got, want)
	}
	if got, want := cfg.PostgresDSN, "postgres://jobdb@example/jobdb"; got != want {
		t.Fatalf("PostgresDSN = %q, want %q", got, want)
	}
	if got, want := cfg.BlobStoreURI, "s3://jobdb-artifacts?region=us-east-1"; got != want {
		t.Fatalf("BlobStoreURI = %q, want %q", got, want)
	}
	if got, want := cfg.MaxInlineArtifactBytes, int64(4096); got != want {
		t.Fatalf("MaxInlineArtifactBytes = %d, want %d", got, want)
	}
	if !bytes.Equal(cfg.LeaseTokenSigningKey, wantKey) {
		t.Fatal("LeaseTokenSigningKey does not match decoded configuration")
	}
}

func TestServeConfigAcceptsRemoteBlobSchemes(t *testing.T) {
	validServeEnv(t)

	for _, value := range []string{
		"s3://bucket?region=us-east-1",
		"gs://bucket",
		"azblob://container",
		"S3://bucket?region=us-east-1",
	} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(blobStoreURIEnvVar, value)
			cfg, err := serveConfigFromEnv()
			if err != nil {
				t.Fatalf("serveConfigFromEnv returned error: %v", err)
			}
			if !strings.Contains(cfg.BlobStoreURI, "://") {
				t.Fatalf("BlobStoreURI = %q, want normalized URI", cfg.BlobStoreURI)
			}
		})
	}
}

func TestServeConfigRejectsNonRemoteBlobStores(t *testing.T) {
	validServeEnv(t)

	for _, value := range []string{
		"",
		"bucket-without-a-scheme",
		"blobfs:///tmp/jobdb",
		"file:///tmp/jobdb",
		"mem://artifacts",
		"https://example.com/bucket",
		"s3:///missing-bucket",
		"gs://",
		"azblob:///missing-container",
		"s3://bucket#fragment",
	} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(blobStoreURIEnvVar, value)
			if _, err := serveConfigFromEnv(); err == nil {
				t.Fatalf("serveConfigFromEnv accepted %q", value)
			}
		})
	}
}

func TestServeConfigRequiresPostgres(t *testing.T) {
	validServeEnv(t)
	t.Setenv(postgresDSNEnvVar, "")

	if _, err := serveConfigFromEnv(); err == nil || !strings.Contains(err.Error(), postgresDSNEnvVar) {
		t.Fatalf("serveConfigFromEnv error = %v, want missing %s", err, postgresDSNEnvVar)
	}
}

func TestServeConfigSupportsSigningKeyFile(t *testing.T) {
	wantKey := validServeEnv(t)
	t.Setenv(leaseTokenSigningKeyEnvVar, "")
	keyFile := filepath.Join(t.TempDir(), "lease-token-key")
	if err := os.WriteFile(keyFile, []byte(base64.StdEncoding.EncodeToString(wantKey)+"\n"), 0o600); err != nil {
		t.Fatalf("write signing key: %v", err)
	}
	t.Setenv(leaseTokenSigningKeyFileEnvVar, keyFile)

	cfg, err := serveConfigFromEnv()
	if err != nil {
		t.Fatalf("serveConfigFromEnv returned error: %v", err)
	}
	if !bytes.Equal(cfg.LeaseTokenSigningKey, wantKey) {
		t.Fatal("LeaseTokenSigningKey does not match file contents")
	}
}

func TestServeConfigValidatesSigningKeyWithoutLeakingIt(t *testing.T) {
	validServeEnv(t)

	t.Run("both sources", func(t *testing.T) {
		t.Setenv(leaseTokenSigningKeyFileEnvVar, "/run/secrets/jobdb-key")
		if _, err := serveConfigFromEnv(); err == nil {
			t.Fatal("serveConfigFromEnv accepted both signing key sources")
		}
	})

	t.Run("missing", func(t *testing.T) {
		t.Setenv(leaseTokenSigningKeyEnvVar, "")
		if _, err := serveConfigFromEnv(); err == nil {
			t.Fatal("serveConfigFromEnv accepted a missing signing key")
		}
	})

	for name, value := range map[string]string{
		"malformed": "not-base64-secret-value",
		"too short": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, minimumServeSigningKeyBytes-1)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(leaseTokenSigningKeyEnvVar, value)
			_, err := serveConfigFromEnv()
			if err == nil {
				t.Fatal("serveConfigFromEnv accepted an invalid signing key")
			}
			if strings.Contains(err.Error(), value) {
				t.Fatalf("error leaked configured key: %v", err)
			}
		})
	}
}

func TestServeConfigValidatesOverrides(t *testing.T) {
	validServeEnv(t)
	t.Setenv(listenAddrEnvVar, "[::]:18080")
	t.Setenv(maxInlineArtifactBytesEnvVar, "8192")

	cfg, err := serveConfigFromEnv()
	if err != nil {
		t.Fatalf("serveConfigFromEnv returned error: %v", err)
	}
	if got, want := cfg.ListenAddr, "[::]:18080"; got != want {
		t.Fatalf("ListenAddr = %q, want %q", got, want)
	}
	if got, want := cfg.MaxInlineArtifactBytes, int64(8192); got != want {
		t.Fatalf("MaxInlineArtifactBytes = %d, want %d", got, want)
	}

	for _, value := range []string{"8080", "localhost", "localhost:0", "localhost:65536"} {
		t.Run("listen "+value, func(t *testing.T) {
			t.Setenv(listenAddrEnvVar, value)
			if _, err := serveConfigFromEnv(); err == nil {
				t.Fatalf("serveConfigFromEnv accepted listen address %q", value)
			}
		})
	}
	for _, value := range []string{"0", "-1", "1.5", "many"} {
		t.Run("inline "+value, func(t *testing.T) {
			t.Setenv(maxInlineArtifactBytesEnvVar, value)
			if _, err := serveConfigFromEnv(); err == nil {
				t.Fatalf("serveConfigFromEnv accepted inline threshold %q", value)
			}
		})
	}
}

func TestServeConfigRejectsLocalBackendEnvironment(t *testing.T) {
	validServeEnv(t)

	for _, name := range prohibitedServeEnvVars {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "configured")
			_, err := serveConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("serveConfigFromEnv error = %v, want rejection of %s", err, name)
			}
		})
	}
}

func TestServeCommandRejectsInheritedFlags(t *testing.T) {
	validServeEnv(t)
	origRunDirectServer := runDirectServerFunc
	defer func() { runDirectServerFunc = origRunDirectServer }()
	runDirectServerFunc = func(context.Context, directServerConfig) error {
		t.Fatal("runDirectServerFunc called after a flag was supplied")
		return nil
	}

	for _, args := range [][]string{
		{"--listen", "127.0.0.1:9999", "serve"},
		{"serve", "--listen", "127.0.0.1:9999"},
		{"--db", "jobdb.db", "serve"},
		{"serve", "--db", "jobdb.db"},
		{"--sqlite-dsn", "file:jobdb.db", "serve"},
		{"serve", "--blob-dir", "/tmp/blobs"},
		{"serve", "--blob-store-uri", "s3://another-bucket"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cmd := newRootCmd()
			cmd.SetArgs(args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			err := cmd.ExecuteContext(context.Background())
			if err == nil || !strings.Contains(err.Error(), "does not accept flags") {
				t.Fatalf("ExecuteContext error = %v, want serve flag rejection", err)
			}
		})
	}
}

func TestServeCommandPassesValidatedConfigToDirectRuntime(t *testing.T) {
	wantKey := validServeEnv(t)
	origRunDirectServer := runDirectServerFunc
	defer func() { runDirectServerFunc = origRunDirectServer }()

	called := 0
	runDirectServerFunc = func(_ context.Context, cfg directServerConfig) error {
		called++
		if got, want := cfg.ListenAddr, defaultServeListenAddr; got != want {
			t.Errorf("ListenAddr = %q, want %q", got, want)
		}
		if got, want := cfg.MaxInlineArtifactBytes, defaultServeMaxInlineBytes; got != want {
			t.Errorf("MaxInlineArtifactBytes = %d, want %d", got, want)
		}
		if !bytes.Equal(cfg.LeaseTokenSigningKey, wantKey) {
			t.Error("LeaseTokenSigningKey does not match decoded configuration")
		}
		return nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"serve"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("ExecuteContext returned error: %v", err)
	}
	if called != 1 {
		t.Fatalf("runDirectServerFunc called %d times, want 1", called)
	}
}

func TestServeCommandRejectsPositionalArguments(t *testing.T) {
	validServeEnv(t)
	cmd := newRootCmd()
	cmd.SetArgs([]string{"serve", "sqlite"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.ExecuteContext(context.Background()); err == nil {
		t.Fatal("ExecuteContext accepted a serve positional argument")
	}
}

func TestDirectCommandRetainsFlagConfiguration(t *testing.T) {
	origRunDirectServer := runDirectServerFunc
	defer func() { runDirectServerFunc = origRunDirectServer }()

	called := 0
	runDirectServerFunc = func(_ context.Context, cfg directServerConfig) error {
		called++
		if got, want := cfg.ListenAddr, "127.0.0.1:9999"; got != want {
			t.Errorf("ListenAddr = %q, want %q", got, want)
		}
		if got, want := cfg.PostgresDSN, "postgres://flag"; got != want {
			t.Errorf("PostgresDSN = %q, want %q", got, want)
		}
		if got, want := cfg.BlobStoreURI, "blobfs:///tmp/artifacts"; got != want {
			t.Errorf("BlobStoreURI = %q, want %q", got, want)
		}
		if len(cfg.LeaseTokenSigningKey) != 0 {
			t.Error("direct command unexpectedly configured a lease token signing key")
		}
		return nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--listen", "127.0.0.1:9999",
		"--blob-store-uri", "blobfs:///tmp/artifacts",
		"direct", "--postgres-dsn", "postgres://flag",
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("ExecuteContext returned error: %v", err)
	}
	if called != 1 {
		t.Fatalf("runDirectServerFunc called %d times, want 1", called)
	}
}

func validServeEnv(t *testing.T) []byte {
	t.Helper()
	key := bytes.Repeat([]byte{11}, minimumServeSigningKeyBytes)
	t.Setenv(postgresDSNEnvVar, "postgres://jobdb@example/jobdb")
	t.Setenv(blobStoreURIEnvVar, "s3://jobdb-artifacts?region=us-east-1")
	t.Setenv(leaseTokenSigningKeyEnvVar, base64.StdEncoding.EncodeToString(key))
	t.Setenv(leaseTokenSigningKeyFileEnvVar, "")
	t.Setenv(listenAddrEnvVar, "")
	t.Setenv(maxInlineArtifactBytesEnvVar, "")
	for _, name := range prohibitedServeEnvVars {
		t.Setenv(name, "")
	}
	return key
}
