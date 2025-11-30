package swf_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/lib/pq"
)

// TestJobsEventuallyComplete verifies that every job we start transitions to a completed
// archive state (surfaceable via CheckJobStatus) rather than getting stuck in the active view.
func TestJobsEventuallyComplete(t *testing.T) {
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

	engine, err := swf.NewEngineBuilder("job-status-tenant").
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		PlusWorkers(statusJobWorker{}, statusTaskWorker{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine: %v", err)
	}

	go engine.Run(ctx)

	jobInputs := []int{1, 2, 3}
	jobIDs := make([]swf.JobId, 0, len(jobInputs))
	for _, n := range jobInputs {
		id, err := engine.StartJob(ctx, swf.StartJob{
			JobType: statusJobName,
			Data:    &swf.SimpleTaskData{Data: swf.NewMapData(map[string]interface{}{"n": n})},
		})
		if err != nil {
			t.Fatalf("failed to start job: %v", err)
		}
		jobIDs = append(jobIDs, id)
	}

	for _, id := range jobIDs {
		waitForCompletedStatus(t, ctx, engine, id)
	}

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	jobIDStrings := make([]string, 0, len(jobIDs))
	for _, id := range jobIDs {
		jobIDStrings = append(jobIDStrings, string(id))
	}
	var archived int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pgwf.jobs_archive WHERE job_id = ANY($1)`, pq.Array(jobIDStrings)).Scan(&archived); err != nil {
		t.Fatalf("count archived jobs: %v", err)
	}
	if archived != len(jobIDs) {
		t.Fatalf("expected %d archived jobs, got %d", len(jobIDs), archived)
	}
}

func waitForCompletedStatus(t *testing.T, ctx context.Context, engine swf.SWFEngine, jobID swf.JobId) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.CheckJobStatus(ctx, jobID)
		if err == nil && status == swf.JobStatusCompleted {
			return
		}
		if err != nil && !errors.Is(err, swf.ErrJobNotFound) {
			t.Fatalf("check status for job %s: %v", jobID, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach completed status", jobID)
}

const (
	statusJobName  = "status_job"
	statusTaskName = "status_task"
)

type statusJobWorker struct{}

func (statusJobWorker) Name() string { return statusJobName }
func (statusJobWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, statusTaskName, input)
}

type statusTaskWorker struct{}

func (statusTaskWorker) Name() string { return statusTaskName }
func (statusTaskWorker) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	payload, err := input.GetData()
	if err != nil {
		return nil, err
	}
	return &swf.SimpleTaskData{Data: payload}, nil
}
