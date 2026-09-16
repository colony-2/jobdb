package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	directruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/direct"
)

func TestPublicRuntimeLeaseTransport(t *testing.T) {
	withBareDatabase(t, func(ctx context.Context, db *sql.DB) {
		runtime, err := directruntime.NewSQLDB(ctx, db, directruntime.Config{
			BlobStoreURI: "blobfs://" + t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = runtime.Close(ctx) }()
		input, err := jobdb.NewTaskData(map[string]any{"item": 1})
		if err != nil {
			t.Fatal(err)
		}
		root, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "root", JobType: "collect", Data: input,
		}})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: root.JobKey, WorkerID: "worker", Capabilities: []string{"collect"},
		})
		if err != nil || lease == nil {
			t.Fatalf("lease root = %+v, %v", lease, err)
		}
		expires, err := runtime.KeepAliveLeaseByIDWithExpiry(ctx, root.JobKey,
			lease.LeaseID(), "worker", time.Minute)
		if err != nil || !expires.After(time.Now()) {
			t.Fatalf("renew lease = %s, %v", expires, err)
		}
		child, err := runtime.SubmitJobWithLeaseByID(ctx, root.JobKey,
			lease.LeaseID(), "worker", jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
				TenantId: "tenant", JobID: "child", JobType: "collect", Data: input,
			}})
		if err != nil || child.JobKey.JobId != "child" {
			t.Fatalf("submit child = %+v, %v", child, err)
		}
		if err := runtime.RescheduleJobWithLeaseByID(ctx, root.JobKey, lease.LeaseID(),
			"worker", jobdb.RescheduleExecutionRequest{NextNeed: "collect"}); err != nil {
			t.Fatalf("reschedule lease: %v", err)
		}
		if err := runtime.KeepAliveLeaseByID(ctx, root.JobKey, lease.LeaseID(),
			"worker", time.Minute); !errors.Is(err, jobdb.ErrExecutionLeaseLost) {
			t.Fatalf("renew stale lease = %v", err)
		}
		again, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: root.JobKey, WorkerID: "worker-2", Capabilities: []string{"collect"},
		})
		if err != nil || again == nil {
			t.Fatalf("lease rescheduled job = %+v, %v", again, err)
		}
		if err := runtime.CompleteJobWithLeaseByID(ctx, root.JobKey, again.LeaseID(),
			"worker-2", jobdb.CompleteExecutionRequest{
				Status: "success", Chapter: &jobdb.Chapter{
					Ordinal: 1, TaskType: "collect", CreatedAt: time.Now().UTC(),
					Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
						Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"done":true}`)},
					}},
				},
			}); err != nil {
			t.Fatalf("complete lease: %v", err)
		}
		info, err := runtime.GetJob(ctx, root.JobKey)
		if err != nil || info.Status != jobdb.JobStatusCompleted {
			t.Fatalf("completed job = %+v, %v", info, err)
		}
	})
}
