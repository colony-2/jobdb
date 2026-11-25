package sqlite

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestSeedFormat14(t *testing.T) {
	ctx := celltest.Context(context.Background())
	dir := os.Getenv("FORMAT14_OUT")
	require.NotEmpty(t, dir)
	path := filepath.Join(t.TempDir(), "source.db")
	runtime, err := NewFromConfig(ctx, Config{DBPath: path, Projects: celltest.UnrestrictedProjects{}})
	require.NoError(t, err)
	now := time.Date(2032, 3, 5, 9, 0, 0, 21, time.UTC)
	runtime.clock = func() time.Time { return now }
	route := jobdb.Route{JobType: "work"}
	for _, id := range []string{"never", "expiry-only", "held-acquired", "held-renewed", "cleared", "archived"} {
		now = now.Add(time.Second)
		handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: route.JobType, Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, err)
		if id == "never" {
			continue
		}
		if id == "expiry-only" {
			_, err = runtime.db.Exec(`UPDATE jobdb_jobs SET lease_expires_at_ns=? WHERE job_id=?`, now.Add(time.Hour).UnixNano(), id)
			require.NoError(t, err)
			continue
		}
		lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, Routes: []jobdb.Route{route}, WorkerID: "fixture-worker", LeaseDuration: time.Hour})
		require.NoError(t, err)
		require.NotNil(t, lease)
		if id != "held-acquired" {
			now = now.Add(time.Second)
			require.NoError(t, lease.KeepAlive(ctx))
		}
		switch id {
		case "cleared":
			require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route}))
			_, err = runtime.db.Exec(`DELETE FROM jobdb_lease_events WHERE job_id=? AND event_type='renewed'`, id)
		case "archived":
			err = lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: route.JobType, CreatedAt: now, Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`2`)}}}}})
			require.NoError(t, err)
			_, err = runtime.db.Exec(`DELETE FROM jobdb_lease_events WHERE job_id=? AND event_type='acquired'`, id)
		}
		require.NoError(t, err)
	}
	var version int
	require.NoError(t, runtime.db.QueryRow("PRAGMA user_version").Scan(&version))
	require.Equal(t, 14, version)
	pageRequest := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, PageSize: 2}
	var pages []jobdb.ListJobsResponse
	for {
		page, err := runtime.ListJobs(ctx, pageRequest)
		require.NoError(t, err)
		pages = append(pages, page)
		pageRequest.PageToken = page.NextPageToken
		if page.NextPageToken == "" {
			break
		}
	}
	require.Len(t, pages, 3)
	_, err = runtime.db.Exec("PRAGMA journal_mode=DELETE")
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "format14.db"), data, 0o644))
	data, err = json.MarshalIndent(pages, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "format14_pages.json"), append(data, '\n'), 0o644))
}
