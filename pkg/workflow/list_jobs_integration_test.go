package workflow_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgjobdb"
)

func TestListJobsRoutesByStatusAndOrdersWithUnion(t *testing.T) {
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

	now := time.Now().UTC().Truncate(time.Second)
	createdActiveA := now.Add(-1 * time.Minute)
	createdActiveB := now.Add(-2 * time.Minute)
	createdArchive := now.Add(-3 * time.Minute)

	insertJob := func(id, jobType string, created time.Time, taskType string, cancel bool) {
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "test-tenant", JobID: pgjobdb.JobID(id),
			WorkerID: "submitter", JobType: pgjobdb.JobType(jobType),
		})
		if err != nil {
			t.Fatalf("submit job %s: %v", id, err)
		}
		if taskType != "" {
			err = pgjobdb.RescheduleUnheldJob(ctx, db, "test-tenant", pgjobdb.JobID(id),
				"submitter", pgjobdb.RescheduleRequest{
					RouteJobType: pgjobdb.JobType(jobType), WorkKind: pgjobdb.WorkKindTask,
					Task: &pgjobdb.TaskWork{TaskType: pgjobdb.TaskType(taskType),
						ResumeJobType: pgjobdb.JobType(jobType), InputOrdinal: 0,
						OutputOrdinal: 1, InputHash: "sha256:input"},
				})
			if err != nil {
				t.Fatalf("route task %s: %v", id, err)
			}
		}
		_, err = db.ExecContext(ctx, `UPDATE pgjobdb.job_facts SET created_at = $1
			WHERE tenant_id = 'test-tenant' AND job_id = $2`, created, id)
		if err != nil {
			t.Fatalf("set creation time %s: %v", id, err)
		}
		if cancel {
			if _, err := db.ExecContext(ctx, `UPDATE pgjobdb.jobs SET cancel_requested = true
				WHERE tenant_id = 'test-tenant' AND job_id = $1`, id); err != nil {
				t.Fatalf("mark cancelled %s: %v", id, err)
			}
		}
	}
	insertArchive := func(id, jobType string, created time.Time, taskType string) {
		insertJob(id, jobType, created, taskType, false)
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "test-tenant", pgjobdb.JobID(id),
			"submitter", pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatalf("archive job %s: %v", id, err)
		}
	}

	insertJob("job-active-A", "alpha", createdActiveA, "", false)
	insertJob("job-active-B", "beta", createdActiveB, "task", true)
	insertArchive("job-archived-C", "alpha", createdArchive, "other")
	insertArchive("job-archived-D", "delta", createdArchive.Add(-time.Minute), "other")

	t.Run("completed status uses archive only", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			Statuses:  []jobdb.JobStatus{jobdb.JobStatusCompleted},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 2 {
			t.Fatalf("expected 2 archived jobs, got %d", len(resp.Jobs))
		}
		seen := map[string]bool{}
		for _, j := range resp.Jobs {
			seen[j.JobKey.JobId] = true
		}
		if !seen["job-archived-C"] || !seen["job-archived-D"] {
			t.Fatalf("missing archived jobs in response: %+v", resp.Jobs)
		}
	})

	t.Run("filters by job type on active", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			JobTypes:  []string{"alpha"},
			Stores:    []jobdb.JobStore{jobdb.JobStoreActive},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 1 {
			t.Fatalf("expected 1 active job, got %d", len(resp.Jobs))
		}
		got := resp.Jobs[0]
		if got.JobKey.JobId != "job-active-A" || got.JobType != "alpha" {
			t.Fatalf("unexpected job %+v", got)
		}
	})

	t.Run("filters by job/task tuple", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			JobTasks:  []jobdb.JobTaskFilter{{JobType: "beta", TaskType: "task"}},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 1 || resp.Jobs[0].JobKey.JobId != "job-active-B" {
			t.Fatalf("expected job-active-B from job/task filter, got %+v", resp.Jobs)
		}
	})

	t.Run("requires tenant ids", func(t *testing.T) {
		if _, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{}); err == nil {
			t.Fatal("expected ListJobs to reject empty tenant ids")
		}
	})

	t.Run("filters by job ids list", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			JobKeys: []jobdb.JobKey{
				{TenantId: "test-tenant", JobId: "job-active-A"},
				{TenantId: "test-tenant", JobId: "job-archived-D"},
			},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 2 {
			t.Fatalf("expected 2 jobs, got %+v", resp.Jobs)
		}
		want := map[string]bool{"job-active-A": true, "job-archived-D": true}
		for _, j := range resp.Jobs {
			if !want[j.JobKey.JobId] {
				t.Fatalf("unexpected job %s", j.JobKey.JobId)
			}
		}
	})

	t.Run("paginates newest first across union", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			PageSize:  2,
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 2 {
			t.Fatalf("expected 2 jobs on first page, got %d", len(resp.Jobs))
		}
		if resp.NextPageToken == "" {
			t.Fatalf("expected next page token")
		}
		if resp.Jobs[0].CreatedAt.Before(resp.Jobs[1].CreatedAt) {
			t.Fatalf("jobs not ordered by created_at desc")
		}

		resp2, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			PageSize:  2,
			PageToken: resp.NextPageToken,
		})
		if err != nil {
			t.Fatalf("ListJobs page 2: %v", err)
		}
		if len(resp2.Jobs) != 2 {
			t.Fatalf("expected 2 jobs on second page, got %d", len(resp2.Jobs))
		}
	})
}
