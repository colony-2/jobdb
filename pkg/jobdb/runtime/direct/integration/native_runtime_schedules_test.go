package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	"github.com/colony-2/pgjobdb"
)

func TestNativeSchedulerAdapterSchedulesAndRuns(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		adapter := runtimeadapter.Scheduler{DB: db}
		key := jobdb.ScheduleKey{TenantId: "tenant", ScheduleId: "daily"}
		schedule, err := adapter.UpsertSchedule(ctx, runtimecore.StoredScheduleMutation{
			ScheduleKey: key, State: jobdb.ScheduleStateActive,
			SpecHash: "hash", TriggerSnapshot: json.RawMessage(`{}`),
			TargetJobType: "collect", TargetSnapshot: json.RawMessage(`{}`),
			OverlapPolicy:         jobdb.ScheduleOverlapSerial,
			FailurePolicySnapshot: json.RawMessage(`{}`),
		})
		if err != nil || schedule.Generation != 1 || schedule.TargetJobType != "collect" {
			t.Fatalf("upsert schedule = %+v, %v", schedule, err)
		}
		listed, err := adapter.ListSchedules(ctx, runtimecore.ListSchedulesRequest{TenantId: "tenant"})
		if err != nil || len(listed.Schedules) != 1 || listed.Schedules[0].ScheduleKey != key {
			t.Fatalf("list schedules = %+v, %v", listed, err)
		}
		scheduledAt := time.Now().UTC().Truncate(time.Microsecond)
		_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "run-1", WorkerID: "submitter", JobType: "collect",
			Runtime: pgjobdb.RuntimeMetadata{Schedule: &pgjobdb.ScheduleOccurrence{
				ScheduleID: "daily", Generation: 1, SpecHash: "hash",
				ScheduledAt: scheduledAt, RunID: "request-1",
			}},
		})
		if err != nil {
			t.Fatalf("submit schedule run: %v", err)
		}
		runs, err := adapter.ListScheduleRuns(ctx, runtimecore.ListScheduleRunsRequest{ScheduleKey: key})
		if err != nil || len(runs.Runs) != 1 || runs.Runs[0].Schedule == nil ||
			runs.Runs[0].Schedule.RunId != "request-1" {
			t.Fatalf("list schedule runs = %+v, %v", runs, err)
		}
		paused, err := adapter.MutateSchedule(ctx, runtimecore.ScheduleStateMutation{
			ScheduleKey: key, State: jobdb.ScheduleStatePaused,
		})
		if err != nil || paused.State != jobdb.ScheduleStatePaused || paused.Generation != 2 {
			t.Fatalf("pause schedule = %+v, %v", paused, err)
		}
		got, err := adapter.GetSchedule(ctx, key)
		if err != nil || got.State != jobdb.ScheduleStatePaused {
			t.Fatalf("get paused schedule = %+v, %v", got, err)
		}
	})
}
