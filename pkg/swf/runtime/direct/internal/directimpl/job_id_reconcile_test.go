package directimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/runtime/direct/testsupport"
	_ "github.com/lib/pq"
)

func TestSubmitJobRecoversMissingPgwfRecordForExplicitJobID(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req := swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-submit-recover",
			JobID:    "submit-recover",
			JobType:  "manual",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	}

	handle, err := rt.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	deletePgwfJobForTest(t, ctx, rt, handle.JobKey)

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(handle.JobKey.TenantId), pgwf.JobID(handle.JobKey.JobId), pgwf.GetJobOptions{}); !errors.Is(err, pgwf.ErrJobNotFound) {
		t.Fatalf("expected pgwf row to be deleted, got %v", err)
	}

	recovered, err := rt.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("recover submit job: %v", err)
	}
	if recovered.JobKey != handle.JobKey {
		t.Fatalf("unexpected recovered handle %+v", recovered.JobKey)
	}

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(handle.JobKey.TenantId), pgwf.JobID(handle.JobKey.JobId), pgwf.GetJobOptions{}); err != nil {
		t.Fatalf("expected pgwf row after recovery: %v", err)
	}
}

func TestSubmitRestartJobRecoversMissingPgwfRecordForExplicitJobID(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sourceReq := swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-restart-recover",
			JobID:    "restart-source",
			JobType:  "manual",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	}
	sourceHandle, err := rt.SubmitJob(ctx, sourceReq)
	if err != nil {
		t.Fatalf("submit source job: %v", err)
	}

	lease, err := rt.GetJobLease(ctx, swf.GetJobLeaseRequest{
		JobKey:        sourceHandle.JobKey,
		WorkerID:      "restart-recovery-writer",
		Capabilities:  []string{"manual"},
		LeaseDuration: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("get source lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected source lease")
	}
	if err := rt.PutChapter(ctx, swf.PutChapterRequest{
		LeaseID: lease.LeaseID(),
		Ref: swf.ChapterRef{
			JobKey:  sourceHandle.JobKey,
			Ordinal: 1,
		},
		Chapter: swf.StoredChapter{
			Ordinal:     1,
			TaskType:    "manual",
			ChapterType: "Manual",
			PayloadKind: "App",
			InputHash:   "restart-recover-input",
			CreatedAt:   time.Now().UTC(),
			Data:        json.RawMessage(`{"n":2}`),
		},
	}); err != nil {
		t.Fatalf("put source chapter: %v", err)
	}
	if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
		t.Fatalf("complete source lease: %v", err)
	}

	restartReq := swf.SubmitRestartJobRequest{
		Job: swf.SubmitRestartJob{
			PriorJobKey:    sourceHandle.JobKey,
			LastStepToKeep: 0,
			JobID:          "restart-recover",
		},
		RequestTime: time.Now().UTC(),
	}

	restartHandle, err := rt.SubmitRestartJob(ctx, restartReq)
	if err != nil {
		t.Fatalf("submit restart job: %v", err)
	}

	deletePgwfJobForTest(t, ctx, rt, restartHandle.JobKey)

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(restartHandle.JobKey.TenantId), pgwf.JobID(restartHandle.JobKey.JobId), pgwf.GetJobOptions{}); !errors.Is(err, pgwf.ErrJobNotFound) {
		t.Fatalf("expected restart pgwf row to be deleted, got %v", err)
	}

	recovered, err := rt.SubmitRestartJob(ctx, restartReq)
	if err != nil {
		t.Fatalf("recover restart job: %v", err)
	}
	if recovered.JobKey != restartHandle.JobKey {
		t.Fatalf("unexpected recovered restart handle %+v", recovered.JobKey)
	}

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(restartHandle.JobKey.TenantId), pgwf.JobID(restartHandle.JobKey.JobId), pgwf.GetJobOptions{}); err != nil {
		t.Fatalf("expected restart pgwf row after recovery: %v", err)
	}
}

func newEmbeddedDirectRuntimeForTest(t *testing.T) (*Runtime, func()) {
	t.Helper()

	dsn, stopPG, err := testsupport.StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		stopPG()
		t.Fatalf("open postgres: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		stopPG()
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := testsupport.InstallPGWF(setupCtx, db); err != nil {
		cleanup()
		t.Fatalf("install pgwf: %v", err)
	}
	strata, err := testsupport.StartEmbeddedStrata()
	if err != nil {
		cleanup()
		t.Fatalf("start embedded strata: %v", err)
	}

	rt, err := NewFromConfig(dsn, strata.BaseURL, strata.APIKey)
	if err != nil {
		strata.Shutdown()
		cleanup()
		t.Fatalf("new direct runtime: %v", err)
	}

	return rt, func() {
		strata.Shutdown()
		cleanup()
	}
}

func deletePgwfJobForTest(t *testing.T, ctx context.Context, rt *Runtime, jobKey swf.JobKey) {
	t.Helper()
	if _, err := rt.udb.ExecContext(ctx, `DELETE FROM pgwf.jobs WHERE tenant_id = $1 AND job_id = $2`, jobKey.TenantId, jobKey.JobId); err != nil {
		t.Fatalf("delete pgwf job: %v", err)
	}
}
