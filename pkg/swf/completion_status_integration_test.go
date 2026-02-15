package swf_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
)

const (
	completionSuccessJobName = "completion_success_job"
	completionAppJobName     = "completion_app_error_job"
	completionSystemJobName  = "completion_system_error_job"
	completionTimeoutJobName = "completion_timeout_job"
)

type completionSuccessWorker struct{}

func (completionSuccessWorker) Name() string { return completionSuccessJobName }
func (completionSuccessWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return input, nil
}

type completionAppErrorWorker struct{}

func (completionAppErrorWorker) Name() string { return completionAppJobName }
func (completionAppErrorWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "app failed"}}
}

type completionSystemErrorWorker struct{}

func (completionSystemErrorWorker) Name() string { return completionSystemJobName }
func (completionSystemErrorWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return nil, swf.SystemError{Payload: swf.SystemErrorPayload{Message: "system failed"}}
}

type completionTimeoutWorker struct{}

func (completionTimeoutWorker) Name() string { return completionTimeoutJobName }
func (completionTimeoutWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	time.Sleep(200 * time.Millisecond)
	return input, nil
}

func TestCompletionStatusAndDetail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()
	waitForStrataReady(t, baseURL)

	engine, err := swf.NewEngineBuilder().
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		PlusWorkers(completionSuccessWorker{}).
		PlusWorkers(completionAppErrorWorker{}).
		PlusWorkers(completionSystemErrorWorker{}).
		PlusWorkers(completionTimeoutWorker{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine: %v", err)
	}

	go engine.Run(ctx)

	tenantID := "completion-status-tenant"
	successKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  completionSuccessJobName,
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
	})
	if err != nil {
		t.Fatalf("start success job: %v", err)
	}
	appKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  completionAppJobName,
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	if err != nil {
		t.Fatalf("start app error job: %v", err)
	}
	systemKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  completionSystemJobName,
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 3}),
	})
	if err != nil {
		t.Fatalf("start system error job: %v", err)
	}
	timeoutKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  completionTimeoutJobName,
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 4}),
		RunPolicy: swf.RunPolicy{
			InvocationTimeout: swf.AsDuration(50 * time.Millisecond),
		},
	})
	if err != nil {
		t.Fatalf("start timeout job: %v", err)
	}

	waitForCompletedStatus(t, ctx, engine, successKey)
	waitForCompletedStatus(t, ctx, engine, appKey)
	waitForCompletedStatus(t, ctx, engine, systemKey)
	waitForCompletedStatus(t, ctx, engine, timeoutKey)

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	assertCompletion(t, db, successKey, pgwf.CompletionStatus("success"), "")
	assertCompletion(t, db, appKey, pgwf.CompletionStatus("failed_app"), "app failed")
	assertCompletion(t, db, systemKey, pgwf.CompletionStatus("failed_system"), "system failed")
	assertCompletion(t, db, timeoutKey, pgwf.CompletionStatus("failed_timeout"), "timed out")
}

func assertCompletion(t *testing.T, db *sql.DB, key swf.JobKey, status pgwf.CompletionStatus, detailSubstring string) {
	t.Helper()

	job, err := pgwf.GetJob(context.Background(), db, pgwf.TenantID(key.TenantId), pgwf.JobID(key.JobId), pgwf.GetJobOptions{})
	if err != nil {
		t.Fatalf("get job %s: %v", key.String(), err)
	}
	if job.CompletionStatus == nil || *job.CompletionStatus != status {
		t.Fatalf("job %s expected completion status %q, got %v", key.String(), status, job.CompletionStatus)
	}
	if detailSubstring == "" {
		if job.CompletionDetail != nil && *job.CompletionDetail != "" {
			t.Fatalf("job %s expected empty completion detail, got %q", key.String(), *job.CompletionDetail)
		}
		return
	}
	if job.CompletionDetail == nil {
		t.Fatalf("job %s expected completion detail containing %q, got nil", key.String(), detailSubstring)
	}
	if !strings.Contains(*job.CompletionDetail, detailSubstring) {
		t.Fatalf("job %s expected completion detail to contain %q, got %q", key.String(), detailSubstring, *job.CompletionDetail)
	}
}
