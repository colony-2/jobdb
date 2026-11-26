package sqlite

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/stretchr/testify/require"
)

func TestSeedJobMetadataUpgradeFromMain(t *testing.T) {
	ctx := projectAdminContext()
	path := os.Getenv("JOBDB_METADATA_FIXTURE_PATH")
	require.True(t, filepath.IsAbs(path))
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	runtime, err := NewFromConfig(ctx, Config{DBPath: path})
	require.NoError(t, err)
	inventory, err := InspectProjectInstallation(ctx, runtime.db, projectcatalog.InstallationDigest([]byte("job metadata upgrade fixture")))
	require.NoError(t, err)
	require.NoError(t, InstallProjectCatalog(ctx, runtime.db, inventory, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}))
	now := time.Date(2026, 10, 2, 3, 4, 5, 0, time.UTC)
	runtime.clock = func() time.Time { return now }
	for _, id := range []string{"ready", "retained"} {
		handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1), Metadata: []byte(`{"application":"untouched"}`)}})
		require.NoError(t, err)
		if id == "retained" {
			lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, Routes: []jobdb.Route{{JobType: "work"}}, WorkerID: "fixture-worker", LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: now, Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`2`)}}}}}))
		}
		goal, err := runtime.GetGoal(ctx, jobdb.GoalKey{TenantId: "c2", GoalID: handle.GoalID})
		require.NoError(t, err)
		t.Logf("job=%s goal=%s state=%s", id, goal.GoalKey.GoalID, goal.State)
	}
	_, err = runtime.db.Exec("PRAGMA journal_mode=DELETE")
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))
}
