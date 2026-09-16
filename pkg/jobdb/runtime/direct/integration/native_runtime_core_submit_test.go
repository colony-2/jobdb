package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeRuntimeCoreSubmitsAndReconcilesJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{
			BlobStoreURI: "blobfs://" + t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = chapters.Close(ctx) }()
		schemas, err := schemapostgres.NewSQLDB(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := runtimecore.NewRuntime(runtimecore.Config{
			Scheduler: runtimeadapter.Scheduler{DB: db}, Chapters: chapters,
			Schemas: schemas,
		})
		if err != nil {
			t.Fatal(err)
		}
		artifact := jobdb.NewArtifactFromBytes("input.txt", []byte("hello"))
		data, err := jobdb.NewTaskData(map[string]any{"item": 1}, artifact)
		if err != nil {
			t.Fatal(err)
		}
		req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "job", JobType: "collect", Data: data,
			Metadata: json.RawMessage(`{"source":"api"}`),
			RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{
				InitialInterval: jobdb.Duration(250 * time.Millisecond), MaximumAttempts: 3,
			}},
		}, WorkerID: "submitter"}
		handle, err := runtime.SubmitJob(ctx, req)
		if err != nil || handle.JobKey.JobId != "job" {
			t.Fatalf("submit job = %+v, %v", handle, err)
		}
		first, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 0})
		if err != nil || first.TaskType != "collect" || first.InputHash == "" ||
			len(first.Artifacts) != 1 || first.Artifacts[0].Name != "input.txt" {
			t.Fatalf("first chapter = %+v, %v", first, err)
		}
		artifactKey, err := artifact.ArtifactKey()
		if err != nil || artifactKey.JobId != "job" || artifactKey.TaskOrdinal != 0 {
			t.Fatalf("submitted artifact key = %+v, %v", artifactKey, err)
		}
		reader, err := runtime.OpenArtifact(ctx, jobdb.ArtifactRef{
			JobKey: handle.JobKey, Ordinal: 0, Name: "input.txt",
			Digest: first.Artifacts[0].Digest,
		})
		if err != nil {
			t.Fatal(err)
		}
		stream, err := reader.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil || string(content) != "hello" {
			t.Fatalf("submitted artifact = %q, %v", content, err)
		}
		stored, err := pgjobdb.GetJob(ctx, db, "tenant", "job")
		if err != nil || stored.JobType != "collect" || stored.SchemaHash != "" ||
			string(stored.AppMetadata) != `{"source": "api"}` {
			t.Fatalf("native job = %+v, %v", stored, err)
		}
		handle, err = runtime.SubmitJob(ctx, req)
		if err != nil || handle.JobKey.JobId != "job" {
			t.Fatalf("repeat submit = %+v, %v", handle, err)
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "job", "worker",
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatal(err)
		}
		handle, err = runtime.SubmitJob(ctx, req)
		if err != nil || handle.JobKey.JobId != "job" {
			t.Fatalf("repeat archived submit = %+v, %v", handle, err)
		}
		changed, err := jobdb.NewTaskData(map[string]any{"item": 2})
		if err != nil {
			t.Fatal(err)
		}
		req.Job.Data = changed
		if _, err := runtime.SubmitJob(ctx, req); !errors.Is(err, jobdb.ErrExistingJobMismatch) {
			t.Fatalf("changed explicit job input = %v", err)
		}
	})
}
