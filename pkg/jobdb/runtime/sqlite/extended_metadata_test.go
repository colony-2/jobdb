package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadataaudittest"
	"github.com/stretchr/testify/require"
)

func TestJobExtendedMetadataSQLiteAuditAtomicity(t *testing.T) {
	ctx := celltest.Context(context.Background())
	runtime, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "jobs.db"), Projects: celltest.UnrestrictedProjects{}})
	require.NoError(t, err)
	defer runtime.Close(ctx)
	handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	version := jobdb.MetadataVersion(0)
	edit := jobdb.JobMetadataEdit{Key: "key", ExpectedVersion: &version, Set: json.RawMessage(`null`)}
	_, err = runtime.db.Exec(`CREATE TRIGGER test_fail_metadata BEFORE INSERT ON ` + jobMetadataEventsTable + ` BEGIN SELECT RAISE(ABORT,'injected metadata audit failure'); END`)
	require.NoError(t, err)
	_, err = runtime.EditJobMetadata(ctx, handle.JobKey, edit)
	require.ErrorContains(t, err, "injected metadata audit failure")
	var count int
	require.NoError(t, runtime.db.QueryRow(`SELECT count(*) FROM `+jobMetadataTable).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, runtime.db.QueryRow(`SELECT count(*) FROM `+jobMetadataEventsTable).Scan(&count))
	require.Zero(t, count)
	_, err = runtime.db.Exec(`DROP TRIGGER test_fail_metadata`)
	require.NoError(t, err)
	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, Routes: []jobdb.Route{{JobType: "work"}}, WorkerID: "worker", LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	writer := lease.(jobdb.JobMetadataLease)
	entry, err := writer.EditJobMetadata(ctx, edit)
	require.NoError(t, err)
	require.EqualValues(t, 1, entry.Version)
	_, err = writer.EditJobMetadata(ctx, edit)
	require.ErrorIs(t, err, jobdb.ErrConflict)
	version = 1
	unchanged, err := runtime.EditJobMetadata(ctx, handle.JobKey, edit)
	require.NoError(t, err)
	require.Equal(t, entry, unchanged)
	removed, err := runtime.EditJobMetadata(ctx, handle.JobKey, jobdb.JobMetadataEdit{Key: edit.Key, ExpectedVersion: &version, Remove: true})
	require.NoError(t, err)
	require.Equal(t, jobdb.JobMetadataEntry{Version: 2}, removed)
	failure := jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "failed after edits"}}
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: failure}}}))
	_, err = writer.EditJobMetadata(ctx, jobdb.JobMetadataEdit{Key: "stale", ExpectedVersion: &version, Remove: true})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	rows, err := runtime.db.Query(`SELECT tenant_id, job_id, key, version, value FROM ` + jobMetadataEventsTable + ` ORDER BY version`)
	require.NoError(t, err)
	defer rows.Close()
	for index, want := range [][]byte{[]byte(`null`), nil} {
		require.True(t, rows.Next())
		var tenant, id, key string
		var actualVersion int
		var value []byte
		require.NoError(t, rows.Scan(&tenant, &id, &key, &actualVersion, &value))
		require.Equal(t, handle.JobKey, jobdb.JobKey{TenantId: tenant, JobId: id})
		require.Equal(t, edit.Key, key)
		require.Equal(t, index+1, actualVersion)
		require.Equal(t, want, value)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func TestJobExtendedMetadataSQLiteHTTPAudit(t *testing.T) {
	for _, surface := range []struct{ name, family string }{{"tenant-http", "tenants"}, {"project-http", "projects"}} {
		t.Run(surface.name, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			runtime, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "jobs.db"), Projects: celltest.UnrestrictedProjects{}})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runtime.Close(ctx)) })
			jobmetadataaudittest.RunHTTP(t, surface.family, runtime, func(jobdb.JobKey) []jobmetadataaudittest.Event {
				rows, err := runtime.db.Query(`SELECT tenant_id,job_id,key,version,value FROM ` + jobMetadataEventsTable)
				require.NoError(t, err)
				defer rows.Close()
				var events []jobmetadataaudittest.Event
				for rows.Next() {
					var event jobmetadataaudittest.Event
					var value []byte
					require.NoError(t, rows.Scan(&event.JobKey.TenantId, &event.JobKey.JobId, &event.Key, &event.Entry.Version, &value))
					event.Entry.Present, event.Entry.Value = value != nil, value
					events = append(events, event)
				}
				require.NoError(t, rows.Err())
				return events
			})
		})
	}
}
