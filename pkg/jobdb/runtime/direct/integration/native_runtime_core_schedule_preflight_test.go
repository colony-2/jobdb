package integration_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
)

func TestNativeRuntimeCoreSchedulePreflight(t *testing.T) {
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
			Scheduler: runtimeadapter.Scheduler{DB: db}, Chapters: chapters, Schemas: schemas,
		})
		if err != nil {
			t.Fatal(err)
		}
		input, err := jobdb.NewTaskData(map[string]any{"item": 1})
		if err != nil {
			t.Fatal(err)
		}
		info, err := runtime.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{
			TenantId: "tenant", ScheduleId: "hourly",
			Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Hour},
			Target:  jobdb.ScheduleTarget{JobType: "collect", Data: input},
		})
		if err != nil || info.NextJobKey == nil {
			t.Fatalf("upsert = %+v, %v", info, err)
		}
		initial, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: *info.NextJobKey, WorkerID: "worker", Capabilities: []string{"collect"},
		})
		if err != nil || initial == nil {
			job, jobErr := runtime.GetJob(ctx, *info.NextJobKey)
			t.Fatalf("initial schedule lease = %+v, %v; job = %+v, %v", initial, err, job, jobErr)
		}
		runs, err := runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
		if err != nil || len(runs.Runs) != 2 {
			t.Fatalf("preflight should submit next run = %+v, %v", runs, err)
		}
		paused, err := runtime.PauseSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey})
		if err != nil || paused.State != jobdb.ScheduleStatePaused {
			t.Fatalf("pause = %+v, %v", paused, err)
		}
		manual, err := runtime.TriggerSchedule(ctx, jobdb.TriggerScheduleRequest{
			ScheduleKey: info.ScheduleKey, RequestID: "blocked", RequestTime: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
		blocked, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: manual.JobKey, WorkerID: "worker", Capabilities: []string{"collect"},
		})
		if err != nil || blocked != nil {
			t.Fatalf("paused occurrence lease = %+v, %v", blocked, err)
		}
		job, err := runtime.GetJob(ctx, manual.JobKey)
		if err != nil || job.Status != jobdb.JobStatusCancelled {
			t.Fatalf("preflight cancellation = %+v, %v", job, err)
		}
		runs, err = runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, run := range runs.Runs {
			if run.JobKey == manual.JobKey {
				found = run.ReasonCode == "schedule_paused"
			}
		}
		if !found {
			t.Fatalf("missing schedule_paused run reason: %+v", runs)
		}
	})
}
