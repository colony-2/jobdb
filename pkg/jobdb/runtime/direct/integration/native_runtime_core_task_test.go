package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
)

func TestNativeRuntimeCoreCompletesWaitingTask(t *testing.T) {
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
		root, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "tenant", JobID: "task-job", JobType: "collect", Data: input,
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
		if err := lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
			NextNeed: "collect:download",
			Payload:  json.RawMessage(`{"run_policy":{"retry":{"maximum_attempts":1}},"task_wait":{"in":0,"out":1,"next":"collect","input_hash":"sha256:input"}}`),
		}); err != nil {
			t.Fatal(err)
		}
		outputArtifact := jobdb.NewArtifactFromBytes("result.txt", []byte("ready"))
		output, err := jobdb.NewTaskData(map[string]any{"ok": true}, outputArtifact)
		if err != nil {
			t.Fatal(err)
		}
		req := jobdb.CompleteTaskIfWaitingRequest{
			JobKey: root.JobKey, Capability: "collect:download", ResumeNeed: "collect",
			OutputOrdinal: 1, InputHash: "sha256:input", Data: output,
		}
		wrong := req
		wrong.InputHash = "sha256:wrong"
		if err := runtime.CompleteTaskIfWaiting(ctx, wrong); !errors.Is(err, jobdb.ErrConflict) {
			t.Fatalf("wrong input hash = %v", err)
		}
		taskLease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: root.JobKey, WorkerID: "task-worker", Capabilities: []string{"collect:download"},
		})
		if err != nil || taskLease == nil {
			t.Fatalf("lease task = %+v, %v", taskLease, err)
		}
		if err := runtime.CompleteTaskIfWaiting(ctx, req); !errors.Is(err, jobdb.ErrConflict) {
			t.Fatalf("completion during active lease = %v", err)
		}
		if err := taskLease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
			NextNeed: "collect:download", Payload: taskLease.Payload(),
		}); err != nil {
			t.Fatalf("return task to queue: %v", err)
		}
		if err := runtime.CompleteTaskIfWaiting(ctx, req); err != nil {
			t.Fatalf("complete waiting task: %v", err)
		}
		if err := runtime.CompleteTaskIfWaiting(ctx, req); !errors.Is(err, jobdb.ErrConflict) {
			t.Fatalf("repeated completion = %v", err)
		}
		chapter, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: root.JobKey, Ordinal: 1})
		if err != nil || chapter.TaskType != "download" || len(chapter.Artifacts) != 1 {
			t.Fatalf("task output chapter = %+v, %v", chapter, err)
		}
		if _, ok := chapter.Body.(jobdb.TaskAttemptOutcomeChapter); !ok {
			t.Fatalf("unexpected task output body %T", chapter.Body)
		}
		key, err := outputArtifact.ArtifactKey()
		if err != nil || key.JobId != root.JobKey.JobId || key.TaskOrdinal != 1 {
			t.Fatalf("task artifact key = %+v, %v", key, err)
		}
		resumed, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
			JobKey: root.JobKey, WorkerID: "job-worker", Capabilities: []string{"collect"},
		})
		if err != nil || resumed == nil || resumed.Capability() != "collect" {
			t.Fatalf("resumed job lease = %+v, %v", resumed, err)
		}
	})
}
