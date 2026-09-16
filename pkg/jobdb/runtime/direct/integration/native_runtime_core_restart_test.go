package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeRuntimeCoreRestartsFromChapterPrefix(t *testing.T) {
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
		source, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "source", JobType: "collect", Data: data,
		}})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "source", "worker",
			pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}},
			pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil {
			t.Fatalf("lease source = %+v, %v", lease, err)
		}
		if err := runtime.PutChapter(ctx, jobdb.PutChapterRequest{
			LeaseID: lease.LeaseID,
			Ref:     jobdb.ChapterRef{JobKey: source.JobKey, Ordinal: 1},
			Chapter: jobdb.Chapter{
				Ordinal: 1, TaskType: "collect", CreatedAt: time.Now().UTC(),
				Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
					Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"done":true}`)},
				}},
			},
		}); err != nil {
			t.Fatal(err)
		}
		extra, err := jobdb.NewTaskData(map[string]any{"cached": true})
		if err != nil {
			t.Fatal(err)
		}
		req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{
			PriorJobKey: source.JobKey, LastStepToKeep: 0,
			JobID: "restart", ExtraTaskOutput: extra,
		}}
		handle, err := runtime.SubmitRestartJob(ctx, req)
		if err != nil || handle.JobKey.JobId != "restart" {
			t.Fatalf("restart job = %+v, %v", handle, err)
		}
		first, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 0})
		if err != nil || first.InputHash == "" {
			t.Fatalf("cloned first chapter = %+v, %v", first, err)
		}
		restartExtra, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 1})
		if err != nil || restartExtra.TaskType != "__restart_extra__" {
			t.Fatalf("restart extra chapter = %+v, %v", restartExtra, err)
		}
		if _, err := runtime.SubmitRestartJob(ctx, req); err != nil {
			t.Fatalf("repeat restart: %v", err)
		}
		plain := req
		plain.Job.JobID = "restart-plain"
		plain.Job.ExtraTaskOutput = nil
		plainHandle, err := runtime.SubmitRestartJob(ctx, plain)
		if err != nil {
			t.Fatalf("restart without extra output: %v", err)
		}
		plainChapters, err := runtime.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: plainHandle.JobKey})
		if err != nil || len(plainChapters) != 1 {
			t.Fatalf("plain restart chapters = %+v, %v", plainChapters, err)
		}
		changed, err := jobdb.NewTaskData(map[string]any{"cached": false})
		if err != nil {
			t.Fatal(err)
		}
		req.Job.ExtraTaskOutput = changed
		if _, err := runtime.SubmitRestartJob(ctx, req); !errors.Is(err, jobdb.ErrExistingJobMismatch) {
			t.Fatalf("changed restart extra = %v", err)
		}
	})
}
