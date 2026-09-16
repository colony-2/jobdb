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
)

func TestNativeRuntimeCoreScheduleLifecycle(t *testing.T) {
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
		artifact := jobdb.NewArtifactFromBytes("input.txt", []byte("snapshot"))
		input, err := jobdb.NewTaskData(map[string]any{"item": 1}, artifact)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		request := jobdb.UpsertScheduleRequest{
			TenantId: "tenant", ScheduleId: "daily", RequestTime: now,
			Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Minute},
			Target:  jobdb.ScheduleTarget{JobType: "collect", Data: input},
		}
		info, err := runtime.UpsertSchedule(ctx, request)
		if err != nil || info.Generation != 1 || info.NextJobKey == nil {
			t.Fatalf("upsert schedule = %+v, %v", info, err)
		}
		got, err := runtime.GetSchedule(ctx, info.ScheduleKey)
		if err != nil || got.SpecHash != info.SpecHash || got.NextFireAt == nil {
			t.Fatalf("get schedule = %+v, %v", got, err)
		}
		listed, err := runtime.ListSchedules(ctx, jobdb.ListSchedulesRequest{TenantId: "tenant"})
		if err != nil || len(listed.Schedules) != 1 {
			t.Fatalf("list schedules = %+v, %v", listed, err)
		}
		runs, err := runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
		if err != nil || len(runs.Runs) != 1 || runs.Runs[0].JobKey != *info.NextJobKey {
			t.Fatalf("initial schedule run = %+v, %v", runs, err)
		}
		first, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: *info.NextJobKey, Ordinal: 0})
		if err != nil || len(first.Artifacts) != 1 || first.Artifacts[0].Name != "input.txt" {
			t.Fatalf("snapshotted target chapter = %+v, %v", first, err)
		}
		expected := int64(1)
		request.ExpectedGeneration = &expected
		updated, err := runtime.UpsertSchedule(ctx, request)
		if err != nil || updated.Generation != 2 || updated.NextJobKey == nil ||
			*updated.NextJobKey == *info.NextJobKey {
			t.Fatalf("upsert next generation = %+v, %v", updated, err)
		}
		if _, err := runtime.UpsertSchedule(ctx, request); !errors.Is(err, jobdb.ErrConflict) {
			t.Fatalf("stale schedule generation = %v", err)
		}
		manual, err := runtime.TriggerSchedule(ctx, jobdb.TriggerScheduleRequest{
			ScheduleKey: info.ScheduleKey, RequestID: "manual-one", RequestTime: now,
		})
		if err != nil {
			t.Fatalf("trigger schedule: %v", err)
		}
		manualAgain, err := runtime.TriggerSchedule(ctx, jobdb.TriggerScheduleRequest{
			ScheduleKey: info.ScheduleKey, RequestID: "manual-one", RequestTime: now,
		})
		if err != nil || manualAgain.JobKey != manual.JobKey {
			t.Fatalf("repeat trigger = %+v, %v", manualAgain, err)
		}
		paused, err := runtime.PauseSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey})
		if err != nil || paused.State != jobdb.ScheduleStatePaused || paused.Generation != 3 {
			t.Fatalf("pause schedule = %+v, %v", paused, err)
		}
		resumed, err := runtime.ResumeSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey})
		if err != nil || resumed.State != jobdb.ScheduleStateActive || resumed.Generation != 4 || resumed.NextJobKey == nil {
			t.Fatalf("resume schedule = %+v, %v", resumed, err)
		}
		archived, err := runtime.ArchiveSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey})
		if err != nil || archived.State != jobdb.ScheduleStateArchived {
			t.Fatalf("archive schedule = %+v, %v", archived, err)
		}
		if _, err := runtime.TriggerSchedule(ctx, jobdb.TriggerScheduleRequest{
			ScheduleKey: info.ScheduleKey, RequestID: "too-late",
		}); !errors.Is(err, jobdb.ErrConflict) {
			t.Fatalf("trigger archived schedule = %v", err)
		}
	})
}
