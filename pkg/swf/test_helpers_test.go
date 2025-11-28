package swf_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata/strata-go/pkg/client"
	"github.com/colony-2/strata/strata-go/pkg/client/core"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/fergusstrange/embedded-postgres"
)

// startEmbeddedPostgres launches a temporary embedded Postgres instance with isolated paths.
func startEmbeddedPostgres(t *testing.T) (string, func()) {
	t.Helper()
	pgPort := uint32(20000 + (time.Now().UnixNano() % 1000))
	tmpDir := t.TempDir()
	runtimePath := filepath.Join(tmpDir, "runtime")
	dataPath := filepath.Join(tmpDir, "data")
	cachePath := filepath.Join(tmpDir, "cache")
	_ = os.MkdirAll(runtimePath, 0o755)
	_ = os.MkdirAll(dataPath, 0o755)
	_ = os.MkdirAll(cachePath, 0o755)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(pgPort).
			RuntimePath(runtimePath).
			DataPath(dataPath).
			CachePath(cachePath),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("failed to start embedded postgres: %v", err)
	}
	stop := func() { _ = postgres.Stop() }
	return fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", pgPort), stop
}

// installPGWF runs the pgwf schema installer against the provided DSN.
func installPGWF(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	return impl.InstallPGWF(ctx, db)
}

// startStrata either uses an existing STRATA_BASE_URL or starts an embedded daemon if available.
type strataHandle struct {
	BaseURL  string
	APIKey   string
	Shutdown func()
}

func startStrata(t *testing.T) (string, *strataHandle) {
	t.Helper()
	if base := os.Getenv("STRATA_BASE_URL"); base != "" {
		apiKey := os.Getenv("STRATA_API_KEY")
		return base, &strataHandle{BaseURL: base, APIKey: apiKey, Shutdown: func() {}}
	}

	s, err := impl.StartEmbeddedStrata()
	if err != nil {
		t.Fatalf("failed to start embedded strata: %v", err)
	}
	return s.BaseURL, &strataHandle{BaseURL: s.BaseURL, APIKey: s.APIKey, Shutdown: s.Shutdown}
}

func waitForStrataReady(t *testing.T, baseURL string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("strata not ready at %s", baseURL)
}

// waitForChapterValue polls Strata for a chapter and decodes "n" from its payload.
func waitForChapterValue(t *testing.T, client *strataclient.Client, key story.Key, ordinal int64, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		chap, err := client.Chapter(context.Background(), key, ordinal)
		if err == nil {
			return decodeNumber(t, chap.Body())
		}
		if !errors.Is(err, core.ErrNotFound) {
			t.Fatalf("unexpected error fetching chapter %d: %v", ordinal, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for chapter %d", ordinal)
	return 0
}

func decodeNumber(t *testing.T, body []byte) int {
	t.Helper()
	var env struct {
		PayloadKind string          `json:"payload_kind"`
		Payload     json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("failed to decode chapter body: %v", err)
	}
	if env.PayloadKind != "App" {
		t.Fatalf("unexpected payload kind %q", env.PayloadKind)
	}

	var payload map[string]int
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	return payload["n"]
}

// randPort helper to avoid collisions in some legacy tests.
func randPort(base uint32) uint32 {
	return base + uint32(rand.Intn(1000))
}
