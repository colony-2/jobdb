package integration_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
)

func TestNativeRuntimeCoreCancelsJob(t *testing.T) {
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
			TenantId: "tenant", JobID: "cancelled", JobType: "collect", Data: data,
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.CancelJob(ctx, jobdb.CancelJobRequest{
			JobKey: handle.JobKey, Reason: "no longer needed",
		}); err != nil {
			t.Fatal(err)
		}
		info, err := runtime.GetJob(ctx, handle.JobKey)
		if err != nil || info.Status != jobdb.JobStatusCancelled {
			t.Fatalf("cancelled job = %+v, %v", info, err)
		}
	})
}
