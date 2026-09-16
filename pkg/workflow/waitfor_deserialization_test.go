package workflow_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb"
)

func TestWaitForDeserialization(t *testing.T) {
	ctx := context.Background()
	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPgjobdb(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgjobdb: %v", err)
	}

	blobStoreURI, blobs := startChapterBlobStore(t)
	defer blobs.Shutdown()

	engine := buildDirectEngine(t, postgresDSN, blobStoreURI, nil)

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	// Insert a job with WaitFor dependencies
	childJobID1 := "child-job-1"
	childJobID2 := "child-job-2"
	parentJobID := "parent-job-1"

	// Insert child jobs
	_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
		TenantID: "tenant-1", JobID: pgjobdb.JobID(childJobID1),
		WorkerID: "submitter", JobType: "child-task",
	})
	if err != nil {
		t.Fatalf("insert child job 1: %v", err)
	}

	_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
		TenantID: "tenant-1", JobID: pgjobdb.JobID(childJobID2),
		WorkerID: "submitter", JobType: "child-task",
	})
	if err != nil {
		t.Fatalf("insert child job 2: %v", err)
	}

	// Insert parent job waiting for child jobs
	_, err = pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
		TenantID: "tenant-1", JobID: pgjobdb.JobID(parentJobID),
		WorkerID: "submitter", JobType: "parent-task",
		WaitFor: []pgjobdb.JobID{pgjobdb.JobID(childJobID1), pgjobdb.JobID(childJobID2)},
	})
	if err != nil {
		t.Fatalf("insert parent job: %v", err)
	}

	// Test: List jobs and verify WaitFor is deserialized correctly
	resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds: []string{"tenant-1"},
	})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}

	var parentJob *jobdb.JobSummary
	for i := range resp.Jobs {
		if resp.Jobs[i].JobKey.JobId == parentJobID {
			parentJob = &resp.Jobs[i]
			break
		}
	}

	if parentJob == nil {
		t.Fatalf("parent job not found in response")
	}

	if len(parentJob.WaitFor) != 2 {
		t.Fatalf("expected 2 WaitFor dependencies, got %d", len(parentJob.WaitFor))
	}

	expectedWaitFor := map[string]bool{
		childJobID1: false,
		childJobID2: false,
	}

	for _, jobId := range parentJob.WaitFor {
		if _, ok := expectedWaitFor[jobId]; !ok {
			t.Errorf("unexpected WaitFor JobId: %s", jobId)
		}
		expectedWaitFor[jobId] = true
	}

	for jobId, found := range expectedWaitFor {
		if !found {
			t.Errorf("expected WaitFor JobId %s not found", jobId)
		}
	}

	t.Log("WaitFor deserialization test passed successfully")
}
