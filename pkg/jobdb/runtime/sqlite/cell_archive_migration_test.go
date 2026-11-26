package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestCellArchiveFormat15Upgrade(t *testing.T) {
	ctx := celltest.Context(context.Background())
	path := filepath.Join(t.TempDir(), "jobs.db")
	fixture, err := os.ReadFile("testdata/job-metadata-format15.db")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, fixture, 0o600))
	config := Config{DBPath: path, Projects: celltest.UnrestrictedProjects{}}
	_, err = NewFromConfig(ctx, config)
	var required *MigrationRequiredError
	require.ErrorAs(t, err, &required)
	require.Equal(t, 15, required.SourceFormat)
	check, err := CheckGoalMigration(ctx, path)
	require.NoError(t, err)
	require.Equal(t, "READY", check.Verdict)
	require.Equal(t, 15, check.SourceFormat)
	require.Equal(t, 16, check.TargetFormat)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fixture, contents)

	snapshot := func(format, archiveColumns int) map[string][]string {
		db, err := sql.Open(sqliteDriverName, path)
		require.NoError(t, err)
		defer db.Close()
		var version, columns int
		require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
		require.Equal(t, format, version)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM pragma_table_info('jobdb_cells') WHERE name='archived_at_ns'`).Scan(&columns))
		require.Equal(t, archiveColumns, columns)
		if columns != 0 {
			var marked int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM jobdb_cells WHERE archived_at_ns IS NOT NULL`).Scan(&marked))
			require.Zero(t, marked)
		}
		return compatibilitySnapshot(t, db)
	}
	before := snapshot(15, 0)
	require.Len(t, before["jobdb_jobs"], 2)
	require.NotEmpty(t, before["jobdb_cells"])
	fault := errors.New("stop before archive upgrade commit")
	for _, checkpoint := range []string{"cell_archive_added", "marked"} {
		_, err = migrateGoals(ctx, path, func(stage string) error {
			if stage == checkpoint {
				return fault
			}
			return nil
		})
		require.ErrorIs(t, err, fault)
		require.Equal(t, before, snapshot(15, 0))
	}
	var stages []string
	result, err := migrateGoals(ctx, path, func(stage string) error {
		stages = append(stages, stage)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "MIGRATED", result.Verdict)
	require.Equal(t, []string{"locked", "cell_archive_added", "marked"}, stages)
	require.Equal(t, before, snapshot(16, 1))
	result, err = MigrateGoals(ctx, path)
	require.NoError(t, err)
	require.Equal(t, "CURRENT", result.Verdict)
	checkCellArchiveReopen(t, ctx, config, "c2")
}

func TestCellArchiveFreshSQLite(t *testing.T) {
	ctx := celltest.Context(context.Background())
	config := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db"), Projects: celltest.UnrestrictedProjects{}}
	runtime, err := NewFromConfig(ctx, config)
	require.NoError(t, err)
	defer runtime.Close(ctx)
	_, err = runtime.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "fresh", Name: "example", Repo: "example.test/archive/example"})
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))
	checkCellArchiveReopen(t, ctx, config, "fresh")
}

func checkCellArchiveReopen(t *testing.T, ctx context.Context, config Config, tenant string) {
	t.Helper()
	runtime, err := NewFromConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runtime.Close(ctx) })
	list, err := runtime.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant})
	require.NoError(t, err)
	require.Len(t, list.Cells, 1)
	cell := list.Cells[0]
	require.Nil(t, cell.ArchivedAt)
	key := jobdb.CellKey{TenantID: cell.TenantID, CellID: cell.ID}
	marked, err := runtime.ArchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", cell.Revision)})
	require.NoError(t, err)
	require.NotNil(t, marked.ArchivedAt)
	description := "changed while archived"
	updated, err := runtime.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", marked.Revision), Description: &description})
	require.NoError(t, err)
	require.Equal(t, marked.ArchivedAt, updated.ArchivedAt)
	require.NoError(t, runtime.Close(ctx))
	runtime, err = NewFromConfig(ctx, config)
	require.NoError(t, err)
	loaded, err := runtime.GetCell(ctx, key)
	require.NoError(t, err)
	require.Equal(t, updated, loaded)
	cleared, err := runtime.UnarchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", loaded.Revision)})
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))
	runtime, err = NewFromConfig(ctx, config)
	require.NoError(t, err)
	loaded, err = runtime.GetCell(ctx, key)
	require.NoError(t, err)
	require.Equal(t, cleared, loaded)
	require.Nil(t, loaded.ArchivedAt)
}
