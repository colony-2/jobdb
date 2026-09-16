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
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeSchedulerAdapterMutatesTypedJob(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		adapter := runtimeadapter.Scheduler{DB: db}
		key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
		created, err := adapter.CreateJob(ctx, runtimecore.CreateJobRequest{
			JobKey: key, JobType: "collect", WorkerID: "submitter",
			RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{
				InitialInterval: jobdb.Duration(250 * time.Millisecond), MaximumAttempts: 3,
			}},
			AppMetadata: json.RawMessage(`{"source":"api"}`),
		})
		if err != nil || created.Store != jobdb.JobStoreActive ||
			created.RunPolicy.Retry.InitialInterval.ToDuration() != 250*time.Millisecond {
			t.Fatalf("create typed job = %+v, %v", created, err)
		}
		selector := pgjobdb.WorkSelector{JobTypes: []pgjobdb.JobType{"collect"}}
		lease, err := pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker",
			selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil {
			t.Fatalf("lease job = %+v, %v", lease, err)
		}
		identity := runtimecore.LeaseIdentity{JobKey: key, LeaseID: lease.LeaseID,
			WorkerID: "worker", ExpiresAt: lease.ExpiresAt}
		task := runtimecore.TaskWork{TaskType: "download", ResumeJobType: "collect",
			InputOrdinal: 1, OutputOrdinal: 2, InputHash: "sha256:input"}
		waiting, err := adapter.RescheduleLease(ctx, runtimecore.RescheduleMutation{
			Identity: identity, RouteJobType: "collect", WorkKind: runtimecore.WorkKindTask,
			TaskWork: &task, LeasePayload: json.RawMessage(`{}`),
		})
		if err != nil || waiting.TaskWork == nil || !waiting.LeasePayloadVisible {
			t.Fatalf("route task = %+v, %v", waiting, err)
		}
		taskSnapshot, err := adapter.GetWaitingTask(ctx, key)
		if err != nil || taskSnapshot.Task.InputHash != "sha256:input" {
			t.Fatalf("waiting task = %+v, %v", taskSnapshot, err)
		}
		resumed, err := adapter.CompleteTaskWork(ctx, runtimecore.CompleteTaskWorkMutation{
			JobKey: key, WorkerID: "external", Task: taskSnapshot,
			ClearLeasePayload: true,
		})
		if err != nil || resumed.WorkKind != runtimecore.WorkKindJob || resumed.LeasePayloadVisible {
			t.Fatalf("resume job = %+v, %v", resumed, err)
		}
		lease, err = pgjobdb.GetJobLease(ctx, db, "tenant", "job", "worker",
			selector, pgjobdb.GetJobLeaseOptions{})
		if err != nil || lease == nil {
			t.Fatalf("lease resumed job = %+v, %v", lease, err)
		}
		archived, err := adapter.CompleteLease(ctx, runtimecore.CompletionMutation{
			Identity: runtimecore.LeaseIdentity{JobKey: key, LeaseID: lease.LeaseID,
				WorkerID: "worker", ExpiresAt: lease.ExpiresAt},
			Status: "success", Detail: "done",
		})
		if err != nil || archived.Store != jobdb.JobStoreArchived ||
			archived.Completion == nil || archived.Completion.Detail != "done" ||
			archived.LeasePayloadVisible {
			t.Fatalf("complete job = %+v, %v", archived, err)
		}
	})
}
