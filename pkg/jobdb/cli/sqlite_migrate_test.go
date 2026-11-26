package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/stretchr/testify/require"
)

func cliMigrationFile(t *testing.T, format int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy.db")
	if format == 14 {
		data, err := os.ReadFile("../runtime/sqlite/testdata/format14.db")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, data, 0o600))
		return path
	}
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	files := []string{"testdata/goal-format-11.sql", "testdata/goal-lease-events.sql", "testdata/goal-receipts.sql"}
	if format == 13 {
		files[0] = "schema/format-13.sql"
	}
	for _, name := range files {
		raw, err := os.ReadFile(filepath.Join("..", "runtime", "sqlite", filepath.FromSlash(name)))
		require.NoError(t, err)
		if format == 13 && name == "testdata/goal-lease-events.sql" {
			// Format 13 already includes append acquisition. Keep the frozen
			// format-11 extension unchanged for the older conversion paths.
			raw = []byte(strings.Replace(string(raw), "'task_completion')", "'task_completion','append')", 1))
		}
		_, err = db.Exec(string(raw))
		require.NoError(t, err)
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA user_version=%d", format))
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return path
}

func executeMigrationCheck(t *testing.T, args ...string) (sqliteruntime.GoalMigrationReport, error, string) {
	t.Helper()
	cmd := NewRootCmd()
	cmd.SetArgs(args)
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	err := cmd.ExecuteContext(context.Background())
	var report sqliteruntime.GoalMigrationReport
	if stdout.Len() > 0 {
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &report), stdout.String())
	}
	return report, err, stdout.String()
}

func TestSQLiteCheckMigrateReadOnlyAndNoRuntime(t *testing.T) {
	t.Setenv(sqliteDSNEnvVar, "")
	for _, format := range []int{11, 12, 13, 14} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			path := cliMigrationFile(t, format)
			verdict := "CONVERTIBLE"
			if format >= 13 {
				verdict = "READY"
			}
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			for _, args := range [][]string{{"sqlite", "check-migrate", "--db", path}, {"--db", path, "sqlite", "check-migrate"}} {
				// Invalid runtime-only configuration must not be initialized by checking.
				args = append(args, "--secret-providers-file", filepath.Join(t.TempDir(), "absent.json"), "--blob-store-uri", "unavailable-provider://bucket")
				report, err, _ := executeMigrationCheck(t, args...)
				require.NoError(t, err, "%+v", report)
				require.Equal(t, verdict, report.Verdict)
				require.Equal(t, format, report.SourceFormat)
				require.Equal(t, 16, report.TargetFormat)
				require.Equal(t, path, report.Path)
				after, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
			files, err := os.ReadDir(filepath.Dir(path))
			require.NoError(t, err)
			require.Len(t, files, 1, "no chapter/configuration/secret/blob store may be initialized")
		})
	}
}

func TestSQLiteCheckMigrateExplicitPathAndDSN(t *testing.T) {
	t.Setenv(sqliteDSNEnvVar, "")
	path := cliMigrationFile(t, 11)
	_, err, _ := executeMigrationCheck(t, "sqlite", "check-migrate")
	require.ErrorContains(t, err, "explicit --db")
	_, err, _ = executeMigrationCheck(t, "sqlite", "check-migrate", "--db", path, "--sqlite-dsn", "file:other.db")
	require.ErrorContains(t, err, "overrides")
	t.Setenv(sqliteDSNEnvVar, "file:other.db")
	_, err, _ = executeMigrationCheck(t, "sqlite", "check-migrate", "--db", path)
	require.ErrorContains(t, err, sqliteDSNEnvVar)
}

func TestSQLiteCheckMigrateBlockedAndExecutionFailures(t *testing.T) {
	t.Setenv(sqliteDSNEnvVar, "")
	path := cliMigrationFile(t, 7)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	report, err, body := executeMigrationCheck(t, "sqlite", "check-migrate", "--db", path)
	require.ErrorIs(t, err, sqliteruntime.ErrGoalMigrationBlocked)
	require.Equal(t, "BLOCKED", report.Verdict)
	require.Contains(t, body, "unsupported_format")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	missing := filepath.Join(t.TempDir(), "absent.db")
	report, err, _ = executeMigrationCheck(t, "sqlite", "check-migrate", "--db", missing)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, "ERROR", report.Verdict)
	_, err = os.Stat(missing)
	require.ErrorIs(t, err, os.ErrNotExist)
	corrupt := filepath.Join(t.TempDir(), "corrupt.db")
	require.NoError(t, os.WriteFile(corrupt, []byte("invalid database"), 0600))
	report, err, body = executeMigrationCheck(t, "sqlite", "check-migrate", "--db", corrupt)
	require.Error(t, err)
	require.Equal(t, "ERROR", report.Verdict)
	require.Contains(t, body, "check_failed")
}

func TestSQLiteMigrateExplicitPathAndNoRuntime(t *testing.T) {
	t.Setenv(sqliteDSNEnvVar, "")
	for _, format := range []int{11, 12, 13, 14} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			path := cliMigrationFile(t, format)
			report, err, _ := executeMigrationCheck(t, "sqlite", "migrate", "--db", path, "--blob-store-uri", "unsupported://must-not-open", "--secret-providers-file", "/not/a/file")
			require.NoError(t, err, "%+v", report)
			require.Equal(t, "MIGRATED", report.Verdict)
			require.Equal(t, 16, report.TargetFormat)
			require.Equal(t, path, report.Path)
			current, err, _ := executeMigrationCheck(t, "sqlite", "migrate", "--db", path)
			require.NoError(t, err)
			require.Equal(t, "CURRENT", current.Verdict)
			raw, err := sql.Open("sqlite", path)
			require.NoError(t, err)
			defer raw.Close()
			var version, n int
			require.NoError(t, raw.QueryRow("PRAGMA user_version").Scan(&version))
			require.Equal(t, 16, version)
			require.NoError(t, raw.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('jobdb_resource_sequences','jobdb_resource_receipts')`).Scan(&n))
			require.Equal(t, 2, n)
			require.NoError(t, raw.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('jobdb_goals','jobdb_goal_candidates')`).Scan(&n))
			require.Zero(t, n)
		})
	}
	for _, args := range [][]string{{"sqlite", "migrate"}, {"sqlite", "migrate", "--db", cliMigrationFile(t, 11), "--sqlite-dsn", ":memory:"}, {"sqlite", "migrate", "--db", filepath.Join(t.TempDir(), "absent.db")}} {
		_, err, _ := executeMigrationCheck(t, args...)
		require.Error(t, err)
	}
}
