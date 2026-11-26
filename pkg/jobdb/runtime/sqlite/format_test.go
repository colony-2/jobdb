package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOldDatabaseFormatIsRejectedWithoutConversion(t *testing.T) {
	db, err := sql.Open(sqliteDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE old_jobs (value TEXT);INSERT INTO old_jobs VALUES ('untouched')`); err != nil {
		t.Fatal(err)
	}
	if err := migrate(context.Background(), db); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("old format accepted: %v", err)
	}
	var value string
	if err := db.QueryRow(`SELECT value FROM old_jobs`).Scan(&value); err != nil || value != "untouched" {
		t.Fatalf("old data changed: %s %v", value, err)
	}
}

func TestRetainedFormatFreshAndLegacyConstructorRefusal(t *testing.T) {
	ctx := context.Background()
	for _, version := range []int{11, 12} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			path, db := migrationFixture(t, version, true, true, true)
			require.NoError(t, db.Close())
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			_, err = NewFromConfig(ctx, Config{DBPath: path, BlobStoreURI: "unsupported://must-not-open"})
			var prerequisite *MigrationRequiredError
			require.ErrorAs(t, err, &prerequisite)
			require.ErrorIs(t, err, ErrMigrationRequired)
			require.Equal(t, version, prerequisite.SourceFormat)
			require.Equal(t, path, prerequisite.Path)
			raw, err := sql.Open(sqliteDriverName, (&url.URL{Scheme: "file", Path: path}).String())
			require.NoError(t, err)
			defer raw.Close()
			_, err = New(raw, WithBlobStoreURI("unsupported://must-not-open"))
			require.ErrorAs(t, err, &prerequisite)
			require.ErrorIs(t, err, ErrMigrationRequired)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, err = os.Stat(path + ".secrets.db")
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "fresh.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	for range 3 {
		var version, n int
		require.NoError(t, r.db.QueryRow("PRAGMA user_version").Scan(&version))
		require.Equal(t, 15, version)
		require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM pragma_table_info('jobdb_cells') WHERE name='archived_at'`).Scan(&n))
		require.Zero(t, n)
		require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('jobdb_goals','jobdb_goal_candidates')`).Scan(&n))
		require.Zero(t, n)
		require.NoError(t, r.Close(ctx))
		r, err = NewFromConfig(ctx, cfg)
		require.NoError(t, err)
	}
	require.NoError(t, r.Close(ctx))
}

func TestRetainedFormatConstructorBoundary(t *testing.T) {
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "boundary.db")}
	runtime, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))
	db, err := sql.Open(sqliteDriverName, cfg.DBPath)
	require.NoError(t, err)
	runtime, err = New(db)
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))

	db, err = sql.Open(sqliteDriverName, cfg.DBPath)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`ALTER TABLE jobdb_cells ADD COLUMN archived_at INTEGER; PRAGMA user_version=16`)
	require.NoError(t, err)
	before, err := os.ReadFile(cfg.DBPath)
	require.NoError(t, err)
	_, err = New(db)
	require.ErrorContains(t, err, "unsupported database format 16")
	_, err = NewFromConfig(ctx, cfg)
	require.ErrorContains(t, err, "unsupported database format 16")
	after, err := os.ReadFile(cfg.DBPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
