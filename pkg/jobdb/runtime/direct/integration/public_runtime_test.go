package integration_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	directruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/direct"
)

func TestPublicRuntimeInitializesFreshDatabase(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		db.SetMaxOpenConns(1)
		cfg := directruntime.Config{BlobStoreURI: "blobfs://" + t.TempDir()}
		runtime, err := directruntime.NewSQLDB(ctx, db, cfg)
		if err != nil {
			t.Fatalf("initialize runtime: %v", err)
		}
		var schema string
		if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil || schema != "public" {
			t.Fatalf("installer leaked search path: %q, %v", schema, err)
		}
		input, err := jobdb.NewTaskData(map[string]any{"item": 1})
		if err != nil {
			t.Fatal(err)
		}
		handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "fresh", JobType: "collect", Data: input,
		}})
		if err != nil || handle.JobKey.JobId != "fresh" {
			t.Fatalf("submit through public runtime = %+v, %v", handle, err)
		}
		run, err := runtime.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: handle.JobKey})
		if err != nil || run.Job.JobKey != handle.JobKey {
			t.Fatalf("get job run = %+v, %v", run, err)
		}
		if err := runtime.Close(ctx); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
		reopened, err := directruntime.NewSQLDB(ctx, db, cfg)
		if err != nil {
			t.Fatalf("reopen runtime: %v", err)
		}
		defer func() { _ = reopened.Close(ctx) }()
		info, err := reopened.GetJob(ctx, handle.JobKey)
		if err != nil || info.Status != jobdb.JobStatusReady {
			t.Fatalf("read reopened job = %+v, %v", info, err)
		}
	})
}
