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

func TestNativeSchedulerAdapterReadsArchivedTask(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "job", WorkerID: "submitter", JobType: "collect",
			RunPolicy: pgjobdb.RunPolicy{Retry: pgjobdb.RetryPolicy{
				InitialIntervalMillis: 250, MaximumAttempts: 4,
			}},
			AppMetadata:  json.RawMessage(`{"source":"api"}`),
			LeasePayload: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "job", "worker",
			pgjobdb.RescheduleRequest{RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask,
				Task: &pgjobdb.TaskWork{TaskType: "download", ResumeJobType: "collect",
					InputOrdinal: 1, OutputOrdinal: 2, InputHash: "sha256:input"}}); err != nil {
			t.Fatal(err)
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "job", "worker",
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatal(err)
		}
		adapter := runtimeadapter.Scheduler{DB: db}
		key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
		row, err := adapter.GetJob(ctx, key)
		if err != nil || row.Store != jobdb.JobStoreArchived ||
			row.TaskWork == nil || row.TaskWork.InputOrdinal != 1 ||
			!row.LeasePayloadVisible || string(row.LeasePayload) != `{}` ||
			row.RunPolicy.Retry.InitialInterval.ToDuration() != 250*time.Millisecond ||
			row.Completion == nil || row.Completion.Status != "success" {
			t.Fatalf("adapted archived job = %+v, %v", row, err)
		}
		listed, err := adapter.ListJobs(ctx, runtimecore.ListJobsRequest{
			TenantIds: []string{"tenant"}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived},
			PageSize: 10,
		})
		if err != nil || len(listed.Jobs) != 1 || listed.Jobs[0].JobKey != key {
			t.Fatalf("adapted list = %+v, %v", listed, err)
		}
	})
}
