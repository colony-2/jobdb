//go:build integration

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

func TestJobExtendedMetadataSQLiteUpgradeFromMain(t *testing.T) {
	ctx := projectAdminContext()
	path := filepath.Join(t.TempDir(), "jobs.db")
	fixture, err := os.ReadFile("testdata/job-metadata-format15.db")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, fixture, 0o600))
	manifest := projectcatalog.InstallationDigest([]byte("job metadata upgrade fixture"))
	db, err := sql.Open(sqliteDriverName, path)
	require.NoError(t, err)
	defer db.Close()
	before, err := InspectProjectInstallation(ctx, db, manifest)
	require.NoError(t, err)
	require.Equal(t, 15, before.Format)
	for _, table := range before.Tables {
		require.NotContains(t, table.Name, "extended_job_metadata")
	}
	goalStates := map[string]jobdb.GoalState{"ready": jobdb.GoalOpen, "retained": jobdb.GoalSucceeded}
	assertExisting := func(reader jobdb.ConsumerRuntime, runtime *Runtime, states map[string]jobdb.GoalState) {
		t.Helper()
		page, err := reader.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 2)
		for _, summary := range page.Jobs {
			id := summary.JobKey.JobId
			require.JSONEq(t, `{"application":"untouched"}`, string(summary.Metadata))
			info, err := reader.GetJob(ctx, summary.JobKey)
			require.NoError(t, err)
			require.Equal(t, summary.GoalID, info.GoalID)
			if id == "retained" {
				require.NotNil(t, summary.ArchivedAt)
				require.Equal(t, "success", info.CompletionStatus)
				chapter, err := reader.GetChapter(ctx, jobdb.ChapterRef{JobKey: summary.JobKey, Ordinal: 1})
				require.NoError(t, err)
				require.Equal(t, "2", string(chapter.Body.(jobdb.JobAttemptOutcomeChapter).Outcome.(jobdb.ApplicationOutputOutcome).Output.Data))
			}
			goal, err := runtime.GetGoal(ctx, jobdb.GoalKey{TenantId: "c2", GoalID: summary.GoalID})
			require.NoError(t, err)
			require.Equal(t, states[id], goal.State)
			if goal.State == jobdb.GoalSucceeded {
				require.Equal(t, id, goal.SuccessfulJobID)
			}
		}
	}
	runtime, err := NewFromConfig(ctx, Config{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = runtime.Close(ctx) })
	after, err := InspectProjectInstallation(ctx, db, manifest)
	require.NoError(t, err)
	var originalTables []ProjectTableInventory
	for _, table := range after.Tables {
		if table.Name == jobMetadataTable || table.Name == jobMetadataEventsTable {
			require.Zero(t, table.Rows)
		} else {
			originalTables = append(originalTables, table)
		}
	}
	require.Len(t, after.Tables, len(before.Tables)+2)
	require.Equal(t, before.Tables, originalTables)
	require.Equal(t, before.Scopes, after.Scopes)
	assertExisting(runtime, runtime, goalStates)
	key := jobdb.JobKey{TenantId: "c2", JobId: "retained"}
	var version jobdb.MetadataVersion
	entry, err := runtime.EditJobMetadata(ctx, key, jobdb.JobMetadataEdit{Key: "exact", ExpectedVersion: &version, Set: json.RawMessage(`9007199254740993123456789`)})
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))
	runtime, err = NewFromConfig(ctx, Config{DBPath: path})
	require.NoError(t, err)
	var stored []byte
	require.NoError(t, runtime.db.QueryRow(`SELECT value FROM `+jobMetadataEventsTable+` WHERE tenant_id='c2' AND job_id='retained' AND key='exact' AND version=1`).Scan(&stored))
	require.Equal(t, []byte(entry.Value), stored)
	server := httptest.NewServer(localProjectTestHandler(remote.NewServer(runtime)))
	defer server.Close()
	project, err := remote.NewProjectRuntime(server.URL, server.Client())
	require.NoError(t, err)
	assertExisting(project, runtime, goalStates)
	info, err := project.GetJob(ctx, key)
	require.NoError(t, err)
	require.Equal(t, entry, info.ExtendedMetadata["exact"])
	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: jobdb.JobKey{TenantId: "c2", JobId: "ready"}, Routes: []jobdb.Route{{JobType: "work"}}, WorkerID: "after-upgrade", LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`3`)}}}}}))
	goalStates["ready"] = jobdb.GoalSucceeded
	assertExisting(project, runtime, goalStates)
	_, err = db.ExecContext(context.Background(), `CREATE TABLE unforeseen_participant (tenant_id TEXT)`)
	require.NoError(t, err)
	_, err = InspectProjectInstallation(ctx, db, manifest)
	require.ErrorContains(t, err, "unrecognized participant table unforeseen_participant")
}
