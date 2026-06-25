package workflow_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	directtest "github.com/colony-2/jobdb/pkg/internal/directtestsupport"
	"github.com/colony-2/jobdb/pkg/jobdb"
	directruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/direct"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/workflow"
)

// startEmbeddedPostgres launches a temporary embedded Postgres instance with isolated paths.
func startEmbeddedPostgres(t *testing.T) (string, func()) {
	t.Helper()
	dsn, stop, err := directtest.StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("failed to start embedded postgres: %v", err)
	}
	return dsn, stop
}

// installPGWF runs the pgwf schema installer against the provided DSN.
func installPGWF(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	return directtest.InstallPGWF(ctx, db)
}

type chapterBlobHandle struct {
	BlobStoreURI string
	Shutdown     func()
}

func startChapterBlobStore(t *testing.T) (string, *chapterBlobHandle) {
	t.Helper()
	blobDir := t.TempDir()
	uri := fmt.Sprintf("blobfs://%s", filepath.ToSlash(blobDir))
	return uri, &chapterBlobHandle{BlobStoreURI: uri, Shutdown: func() {}}
}

type testChapterReader interface {
	GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error)
}

type testArtifactOpener interface {
	OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error)
}

// waitForChapterValue polls JobDB for a chapter and decodes "n" from its payload.
func waitForChapterValue(t *testing.T, source any, jobKey jobdb.JobKey, ordinal int64, timeout time.Duration) int {
	t.Helper()
	reader := mustChapterReader(t, source)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		chap, err := reader.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: jobKey, Ordinal: ordinal})
		if err == nil {
			return decodeNumber(t, chapterPayloadBytes(t, chap))
		}
		if !errors.Is(err, jobdb.ErrChapterNotFound) {
			t.Fatalf("unexpected error fetching chapter %d: %v", ordinal, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for chapter %d", ordinal)
	return 0
}

func chapterPayloadBytes(t *testing.T, chap jobdb.Chapter) []byte {
	t.Helper()
	switch body := chap.Body.(type) {
	case jobdb.JobStartChapter:
		return body.Input.Data
	case jobdb.TaskAttemptOutcomeChapter:
		return outcomePayloadBytes(t, body.Outcome)
	case jobdb.JobAttemptOutcomeChapter:
		return outcomePayloadBytes(t, body.Outcome)
	case jobdb.RestartExtraChapter:
		return body.Output.Data
	default:
		t.Fatalf("unsupported chapter body %T", chap.Body)
		return nil
	}
}

func outcomePayloadBytes(t *testing.T, outcome jobdb.ChapterOutcome) []byte {
	t.Helper()
	switch out := outcome.(type) {
	case jobdb.ApplicationOutputOutcome:
		return out.Output.Data
	default:
		t.Fatalf("unsupported chapter outcome %T", outcome)
		return nil
	}
}

func mustChapterReader(t *testing.T, source any) testChapterReader {
	t.Helper()
	reader, ok := source.(testChapterReader)
	if !ok {
		t.Fatalf("source %T cannot read chapters", source)
	}
	return reader
}

func mustArtifactOpener(t *testing.T, source any) testArtifactOpener {
	t.Helper()
	opener, ok := source.(testArtifactOpener)
	if !ok {
		t.Fatalf("source %T cannot open artifacts", source)
	}
	return opener
}

func readStoredArtifact(t *testing.T, source any, jobKey jobdb.JobKey, ordinal int64, art jobdb.StoredArtifact) []byte {
	t.Helper()
	reader, err := mustArtifactOpener(t, source).OpenArtifact(context.Background(), jobdb.ArtifactRef{
		JobKey:  jobKey,
		Ordinal: ordinal,
		Name:    art.Name,
		Digest:  art.Digest,
	})
	if err != nil {
		t.Fatalf("open artifact %s: %v", art.Name, err)
	}
	rc, err := reader.Open()
	if err != nil {
		t.Fatalf("open artifact reader %s: %v", art.Name, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read artifact %s: %v", art.Name, err)
	}
	return data
}

func decodeNumber(t *testing.T, body []byte) int {
	t.Helper()
	var payload map[string]int
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	return payload["n"]
}

// randPort helper to avoid collisions in some legacy tests.
func randPort(base uint32) uint32 {
	return base
}

type directTestEngine struct {
	workflow.Engine
	runtime jobdb.WorkflowRuntime
}

func (e *directTestEngine) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	return e.runtime.GetChapter(ctx, ref)
}

func (e *directTestEngine) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	return e.runtime.OpenArtifact(ctx, ref)
}

func buildDirectEngine(t *testing.T, postgresDSN, blobStoreURI string, configure func(*workflow.EngineBuilder)) workflow.Engine {
	t.Helper()
	runtime, err := directruntime.NewFromConfig(directruntime.Config{
		PostgresDSN:  postgresDSN,
		BlobStoreURI: blobStoreURI,
	})
	if err != nil {
		t.Fatalf("create direct runtime: %v", err)
	}
	builder := workflow.NewEngineBuilder()
	builder.WithRuntime(runtime)
	if configure != nil {
		configure(builder)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	return &directTestEngine{Engine: engine, runtime: runtime}
}

func buildToyEngine(t *testing.T, configure func(*workflow.EngineBuilder), opts ...toyruntime.Option) (workflow.Engine, context.CancelFunc) {
	t.Helper()
	runtime := toyruntime.New(opts...)
	builder := workflow.NewEngineBuilder().WithRuntime(runtime)
	if configure != nil {
		configure(builder)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		t.Fatalf("build toy engine: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go engine.Run(ctx)
	return engine, cancel
}
