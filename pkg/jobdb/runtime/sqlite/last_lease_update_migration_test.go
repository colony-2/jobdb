package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func copyFormat14(t *testing.T) (string, []byte) {
	t.Helper()
	data, err := os.ReadFile("testdata/format14.db")
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "jobs.db")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path, data
}

func TestLastLeaseUpdateFormat14Upgrade(t *testing.T) {
	ctx := celltest.Context(context.Background())
	path, original := copyFormat14(t)
	cfg := Config{DBPath: path, Projects: celltest.UnrestrictedProjects{}}
	preflight, err := CheckGoalMigration(ctx, path)
	require.NoError(t, err)
	require.Equal(t, "READY", preflight.Verdict)
	require.Equal(t, 14, preflight.SourceFormat)
	require.Equal(t, 16, preflight.TargetFormat)
	_, err = NewFromConfig(ctx, cfg)
	require.ErrorIs(t, err, ErrMigrationRequired)
	db, err := sql.Open(sqliteDriverName, path)
	require.NoError(t, err)
	_, err = New(db)
	require.ErrorIs(t, err, ErrMigrationRequired)
	rows, err := db.Query(`SELECT printf('"%w"', name) FROM pragma_table_info('jobdb_jobs') ORDER BY cid`)
	require.NoError(t, err)
	var columns []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		columns = append(columns, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	saved := compatibilitySnapshot(t, db)
	require.NoError(t, db.Close())
	unchanged, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, unchanged)
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	for _, verdict := range []string{"MIGRATED", "CURRENT"} {
		report, err := MigrateGoals(ctx, path)
		require.NoError(t, err)
		require.Equal(t, verdict, report.Verdict)
	}
	var baseline []jobdb.ListJobsResponse
	paging, err := os.ReadFile("testdata/format14_pages.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(paging, &baseline))
	now := time.Date(2032, 3, 5, 9, 0, 20, 21, time.UTC)
	var runtime *Runtime
	for range 2 {
		runtime, err = NewFromConfig(ctx, cfg)
		require.NoError(t, err)
		runtime.clock = func() time.Time { return now }
		actual := compatibilitySnapshot(t, runtime.db, columns...)
		for table, values := range saved {
			require.Equal(t, values, actual[table], table)
		}
		request := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, PageSize: 2}
		for _, want := range baseline {
			got, err := runtime.ListJobs(ctx, request)
			require.NoError(t, err)
			for _, item := range got.Jobs {
				require.Nil(t, item.LastLeaseUpdatedAt)
			}
			wantJSON, err := json.Marshal(want)
			require.NoError(t, err)
			gotJSON, err := json.Marshal(got)
			require.NoError(t, err)
			require.JSONEq(t, string(wantJSON), string(gotJSON))
			request.PageToken = want.NextPageToken
		}
		require.NoError(t, runtime.Close(ctx))
	}
	runtime, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer runtime.Close(ctx)
	runtime.clock = func() time.Time { return now }
	held := jobdb.JobKey{TenantId: "tenant", JobId: "held-renewed"}
	stored, err := runtime.loadJobRow(ctx, held)
	require.NoError(t, err)
	renewed, err := runtime.RenewExecutionLeaseByID(ctx, held, stored.leaseID.String, stored.leaseWorkerID.String, time.Minute)
	require.NoError(t, err)
	require.Equal(t, stored.leaseID.String, renewed.LeaseID())
	renewedAt := now
	now = now.Add(time.Second)
	never := jobdb.JobKey{TenantId: "tenant", JobId: "never"}
	claimed, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: never, Routes: []jobdb.Route{{JobType: "work"}}})
	require.NoError(t, err)
	require.NotNil(t, claimed)
	got, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
	require.NoError(t, err)
	require.Len(t, got.Jobs, 6)
	for _, item := range got.Jobs {
		switch item.JobKey {
		case held:
			require.Equal(t, &renewedAt, item.LastLeaseUpdatedAt)
		case never:
			require.Equal(t, &now, item.LastLeaseUpdatedAt)
		default:
			require.Nil(t, item.LastLeaseUpdatedAt)
		}
	}
}

func TestLastLeaseUpdateFormat14Rollback(t *testing.T) {
	for _, stage := range []string{"lease_update_added", "marked"} {
		t.Run(stage, func(t *testing.T) {
			path, _ := copyFormat14(t)
			db, err := sql.Open(sqliteDriverName, path)
			require.NoError(t, err)
			rows, schema := compatibilitySnapshot(t, db), schemaSnapshot(t, db)
			require.NoError(t, db.Close())
			failure := errors.New("interrupt upgrade")
			_, err = migrateGoals(context.Background(), path, func(at string) error {
				if at == stage {
					return failure
				}
				return nil
			})
			require.ErrorIs(t, err, failure)
			db, err = sql.Open(sqliteDriverName, path)
			require.NoError(t, err)
			defer db.Close()
			require.Equal(t, schema, schemaSnapshot(t, db))
			require.Equal(t, rows, compatibilitySnapshot(t, db))
			var version int
			require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
			require.Equal(t, 14, version)
		})
	}
}

func TestLastLeaseUpdateRejectsMissingColumn(t *testing.T) {
	ctx := context.Background()
	path, _ := copyFormat14(t)
	db, err := sql.Open(sqliteDriverName, path)
	require.NoError(t, err)
	_, err = db.Exec("PRAGMA user_version=15")
	require.NoError(t, err)
	schema := schemaSnapshot(t, db)
	_, err = New(db)
	require.ErrorIs(t, err, ErrMigrationRequired)
	require.NoError(t, db.Close())
	_, err = NewFromConfig(ctx, Config{DBPath: path})
	require.ErrorIs(t, err, ErrMigrationRequired)
	_, err = CheckGoalMigration(ctx, path)
	require.ErrorIs(t, err, ErrGoalMigrationBlocked)
	_, err = MigrateGoals(ctx, path)
	require.ErrorIs(t, err, ErrGoalMigrationBlocked)
	db, err = sql.Open(sqliteDriverName, path)
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, schema, schemaSnapshot(t, db))
}

func TestLastLeaseUpdateFormat14Fencing(t *testing.T) {
	ctx := context.Background()
	t.Run("exclusive writer lock", func(t *testing.T) {
		path, _ := copyFormat14(t)
		writer, err := sql.Open(sqliteDriverName, path)
		require.NoError(t, err)
		defer writer.Close()
		var exercised bool
		report, err := migrateGoals(ctx, path, func(stage string) error {
			if stage == "locked" {
				exercised = true
				_, err := writer.ExecContext(ctx, `UPDATE jobdb_jobs SET updated_at_ns=0 WHERE job_id='never'`)
				if err == nil {
					return errors.New("independent writer was not fenced")
				}
				require.ErrorContains(t, err, "database is locked")
			}
			return nil
		})
		require.True(t, exercised)
		require.NoError(t, err)
		require.Equal(t, "MIGRATED", report.Verdict)
	})
	t.Run("stale preflight", func(t *testing.T) {
		path, _ := copyFormat14(t)
		ready, err := CheckGoalMigration(ctx, path)
		require.NoError(t, err)
		require.Equal(t, "READY", ready.Verdict)
		writer, err := sql.Open(sqliteDriverName, path)
		require.NoError(t, err)
		_, err = writer.Exec(`ALTER TABLE jobdb_jobs ADD COLUMN last_lease_updated_at_ns TEXT`)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		before, err := os.ReadFile(path)
		require.NoError(t, err)
		rejected, err := MigrateGoals(ctx, path)
		require.ErrorIs(t, err, ErrGoalMigrationBlocked)
		require.True(t, hasMigrationIssue(rejected, "inconsistent_schema"))
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}
