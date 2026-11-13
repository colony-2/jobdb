package sqlite

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func runScheduleCleanup(t *testing.T, path string) ([]byte, error) {
	t.Helper()
	return exec.Command("python3", "../../../../scripts/drop-unused-sqlite-schedules.py", path).CombinedOutput()
}

func TestUnusedScheduleTableRemovalPreservesJobs(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "saved.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	var count int
	require.NoError(t, r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE 'jobdb_schedules%'").Scan(&count))
	require.Zero(t, count, "new installations must not create schedule objects")
	h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "tenant", JobID: "saved", Cell: "test", CellRepo: "example.test/test", JobType: "job",
		Data: &jobdb.SimpleTaskData{Data: []byte(`{"saved":true}`), Artifacts: []jobdb.Artifact{jobdb.NewArtifactFromBytes("input.txt", []byte("saved artifact"))}},
	}})
	require.NoError(t, err)
	ddl, err := os.ReadFile("testdata/unused-schedules.sql")
	require.NoError(t, err)
	_, err = r.db.Exec(string(ddl))
	require.NoError(t, err)
	saved := compatibilitySnapshot(t, r.db)
	require.NoError(t, r.Close(ctx))

	// An existing database opens with the unused table still present.
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, saved, compatibilitySnapshot(t, r.db))
	assertStartArtifact(t, ctx, r, h.JobKey, "input.txt", []byte("saved artifact"))
	require.NoError(t, r.Close(ctx))

	for i := 0; i < 2; i++ {
		out, err := runScheduleCleanup(t, cfg.DBPath)
		require.NoError(t, err, "%s", out)
	}
	delete(saved, "jobdb_schedules")
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	require.Equal(t, saved, compatibilitySnapshot(t, r.db))
	require.NoError(t, r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE 'jobdb_schedules%'").Scan(&count))
	require.Zero(t, count, "opening must not recreate schedule objects")
	assertStartArtifact(t, ctx, r, h.JobKey, "input.txt", []byte("saved artifact"))
	leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, Limit: 1})
	require.NoError(t, err)
	require.Len(t, leases, 1)
	require.NoError(t, leases[0].Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}))
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "job", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`2`)}}}}}))
	job, err := r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, "success", job.CompletionStatus)
	// These cell paths previously accessed schedules even for ordinary jobs.
	cell, err := r.GetCell(ctx, jobdb.CellKey{TenantID: "tenant", CellID: h.CellID})
	require.NoError(t, err)
	repo := "example.test/updated"
	_, err = r.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: h.CellID}, IfMatch: `"` + cell.Revision + `"`, Repo: &repo})
	require.NoError(t, err)
	empty, err := r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "empty", Repo: "example.test/empty"})
	require.NoError(t, err)
	_, err = r.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: empty.ID}, IfMatch: `"` + empty.Revision + `"`})
	require.NoError(t, err)
}

func TestScheduleCleanupRefusesUsedFeature(t *testing.T) {
	for _, used := range []string{"table", "metadata"} {
		t.Run(used, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			cfg := Config{DBPath: filepath.Join(t.TempDir(), "used.db")}
			r, err := NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			_, err = celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			_, err = r.db.Exec("CREATE TABLE jobdb_schedules(schedule_id TEXT)")
			require.NoError(t, err)
			if used == "table" {
				_, err = r.db.Exec("INSERT INTO jobdb_schedules VALUES ('used')")
			} else {
				_, err = r.db.Exec(`UPDATE jobdb_jobs SET metadata='{"internal":{"schedule":{"scheduleId":"used"}}}'`)
			}
			require.NoError(t, err)
			saved := compatibilitySnapshot(t, r.db)
			require.NoError(t, r.Close(ctx))
			out, err := runScheduleCleanup(t, cfg.DBPath)
			require.Error(t, err, "%s", out)
			r, err = NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			defer r.Close(ctx)
			require.Equal(t, saved, compatibilitySnapshot(t, r.db))
		})
	}
}
