package integration_test

import (
	"bytes"
	"context"
	"database/sql"
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

func TestNativeRuntimeCorePutChapterRequiresCurrentLease(t *testing.T) {
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
		data, err := jobdb.NewTaskData(map[string]any{"item": 1})
		if err != nil {
			t.Fatal(err)
		}
		handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "job", JobType: "collect", Data: data,
		}})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker",
			pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}},
			pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil {
			t.Fatalf("lease job = %+v, %v", lease, err)
		}
		chapter := jobdb.Chapter{
			Ordinal: 1, TaskType: "collect", CreatedAt: time.Now().UTC(),
			Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
				Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
			}},
		}
		request := jobdb.PutChapterRequest{
			LeaseID: lease.LeaseID,
			Ref:     jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 1},
			Chapter: chapter,
			ArtifactUploads: []jobdb.ArtifactUpload{{Name: "result.txt", Size: 6,
				Open: func() (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader([]byte("result"))), nil
				},
			}},
		}
		if err := runtime.PutChapter(ctx, request); err != nil {
			t.Fatalf("append chapter: %v", err)
		}
		stored, err := runtime.GetChapter(ctx, request.Ref)
		if err != nil || len(stored.Artifacts) != 1 || stored.Artifacts[0].Name != "result.txt" {
			t.Fatalf("stored chapter = %+v, %v", stored, err)
		}
		if err := runtime.PutChapter(ctx, request); !errors.Is(err, jobdb.ErrConflict) {
			t.Fatalf("duplicate chapter = %v", err)
		}
		if err := pgjobdb.RescheduleJob(ctx, db, lease.Identity(), pgjobdb.RescheduleRequest{
			RouteJobType: "collect", WorkKind: pgjobdb.WorkKindJob,
		}); err != nil {
			t.Fatal(err)
		}
		request.Ref.Ordinal = 2
		request.Chapter.Ordinal = 2
		if err := runtime.PutChapter(ctx, request); !errors.Is(err, jobdb.ErrExecutionLeaseLost) {
			t.Fatalf("stale lease chapter = %v", err)
		}
	})
}
