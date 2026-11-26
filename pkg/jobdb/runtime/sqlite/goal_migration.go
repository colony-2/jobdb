package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// MigrateGoals converts supported legacy storage in one exclusive transaction.
// All serving writers must be stopped before calling it. A saved preflight report
// is never an input: the full inspector runs again under the conversion lock.
// It opens only the selected existing database; ancillary stores stay unopened.
func MigrateGoals(ctx context.Context, path string) (GoalMigrationReport, error) {
	return migrateGoals(ctx, path, nil)
}

// The hook allows tests to interrupt actual conversion boundaries; production
// callers cannot inject callbacks or bypass locked inspection.
func migrateGoals(ctx context.Context, path string, hook func(string) error) (report GoalMigrationReport, err error) {
	report = newGoalMigrationReport()
	defer func() {
		if err != nil && !errors.Is(err, ErrGoalMigrationBlocked) {
			report.Verdict = "ERROR"
			report.issue("conversion_failed", "", "", "", "", err.Error())
		}
	}()
	if strings.TrimSpace(path) == "" {
		return report, fmt.Errorf("SQLite migration requires an explicit database path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return report, err
	}
	report.Path = abs
	info, err := os.Stat(abs)
	if err != nil {
		return report, err
	}
	if !info.Mode().IsRegular() {
		return report, fmt.Errorf("SQLite migration requires a regular database file: %s", abs)
	}
	u := url.URL{Scheme: "file", Path: abs}
	// mode=rw cannot create a missing database. _txlock=exclusive makes BeginTx
	// acquire the SQLite lock before any input observation, including in WAL.
	// locking_mode=EXCLUSIVE excludes other handles for the connection lifetime.
	u.RawQuery = url.Values{"mode": {"rw"}, "_txlock": {"exclusive"}, "_pragma": {"busy_timeout(5000)", "foreign_keys(0)", "locking_mode(EXCLUSIVE)"}}.Encode()
	db, err := sql.Open(sqliteDriverName, u.String())
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx = ctxOrBackground(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	checkpoint := func(stage string) error {
		if hook != nil {
			return hook(stage)
		}
		return ctx.Err()
	}
	if err = checkpoint("locked"); err != nil {
		return report, err
	}
	if err = inspectGoalMigration(ctx, tx, &report); err != nil {
		return report, err
	}
	if report.Verdict == "CURRENT" {
		return report, nil
	}
	switch report.SourceFormat {
	case 11, 12:
		input, loadErr := loadGoalMigrationInput(ctx, tx, &report)
		if loadErr != nil {
			return report, loadErr
		}
		if err = checkpoint("inspected"); err != nil {
			return report, err
		}
		if err = convertRetainedJobs(ctx, tx, input, &report, checkpoint); err != nil {
			return report, err
		}
		if err = checkpoint("verified"); err != nil {
			return report, err
		}
	case 13, 14, 15:
	default:
		return report, fmt.Errorf("SQLite migration requires a supported legacy format (11–15)")
	}
	if report.SourceFormat <= 13 {
		if err = convertResourceSchema(ctx, tx); err != nil {
			return report, err
		}
	}
	if report.SourceFormat < 15 {
		if err = addLeaseUpdateSchema(ctx, tx); err != nil {
			return report, err
		}
		if err = checkpoint("lease_update_added"); err != nil {
			return report, err
		}
	}
	if err = addCellArchiveSchema(ctx, tx); err != nil {
		return report, err
	}
	if err = checkpoint("cell_archive_added"); err != nil {
		return report, err
	}
	if err = markCurrentFormat(ctx, tx); err != nil {
		return report, err
	}
	if err = checkpoint("marked"); err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Verdict = "MIGRATED"
	return report, nil
}

type retainedDefinition struct{ name, kind, ddl string }

func retainedJobDefinitions(ctx context.Context) ([]retainedDefinition, error) {
	db, err := sql.Open(sqliteDriverName, ":memory:")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, retainedGoalSchemaSQL); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT name,type,sql FROM sqlite_master WHERE tbl_name='jobdb_jobs' AND sql IS NOT NULL ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END,name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []retainedDefinition
	for rows.Next() {
		var d retainedDefinition
		if err = rows.Scan(&d.name, &d.kind, &d.ddl); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func convertRetainedJobs(ctx context.Context, tx *sql.Tx, input *goalMigrationInput, report *GoalMigrationReport, checkpoint func(string) error) error {
	definitions, err := retainedJobDefinitions(ctx)
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, d := range definitions {
		known[d.name] = true
	}
	// Preserve application indexes and triggers on jobs as exact definitions.
	// Restore them after copying/closure publication, avoiding migration side effects.
	extras := []string{}
	if err = migrationRows(ctx, tx, `SELECT name,sql FROM sqlite_master WHERE tbl_name='jobdb_jobs' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name`, func(rows *sql.Rows) error {
		var name, ddl string
		if e := rows.Scan(&name, &ddl); e != nil {
			return e
		}
		if !known[name] {
			extras = append(extras, ddl)
		}
		return nil
	}); err != nil {
		return err
	}
	columns := []string{}
	if err = migrationRows(ctx, tx, `SELECT name FROM pragma_table_info('jobdb_jobs') ORDER BY cid`, func(rows *sql.Rows) error {
		var name string
		if e := rows.Scan(&name); e != nil {
			return e
		}
		columns = append(columns, `"`+strings.ReplaceAll(name, `"`, `""`)+`"`)
		return nil
	}); err != nil {
		return err
	}
	columnSQL := strings.Join(columns, ",")
	if _, err = tx.ExecContext(ctx, `CREATE TEMP TABLE retained_conversion_input AS SELECT rowid AS original_rowid,* FROM jobdb_jobs`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DROP TABLE jobdb_jobs`); err != nil {
		return err
	}
	for _, d := range definitions {
		if d.kind == "table" {
			if _, err = tx.ExecContext(ctx, d.ddl); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO jobdb_jobs(rowid,`+columnSQL+`) SELECT original_rowid,`+columnSQL+` FROM retained_conversion_input`); err != nil {
		return err
	}
	// Install guards/indexes before publishing any retained origin or closure.
	for _, d := range definitions {
		if d.kind != "table" {
			if _, err = tx.ExecContext(ctx, d.ddl); err != nil {
				return err
			}
		}
	}
	if err = checkpoint("copied"); err != nil {
		return err
	}
	store := retainedStore(ctx, tx)
	for i := range report.Goals {
		plan := &report.Goals[i]
		if err = store.MarkOrigin(jobdb.JobKey{TenantId: plan.GoalKey.TenantId, JobId: plan.OriginJobID}); err != nil {
			return err
		}
	}
	for i := range report.Goals {
		plan := &report.Goals[i]
		if plan.State == jobdb.GoalOpen {
			continue
		}
		g := input.goals[plan.GoalKey].info
		g.CreatedAt = plan.DerivedCreatedAt
		g.UpdatedAt = plan.DerivedUpdatedAt
		if plan.CreateFailureRecord {
			key, e := store.insertFailure(g, *jobdb.CloneGoalFailure(g.Failure), jobdb.JobRecordMigratedGoalFailure)
			if e != nil {
				return e
			}
			g.FailureJobID = key.JobId
			plan.ClosureJobID = key.JobId
		}
		if err = store.saveClosure(g, plan.ClosureJobID, "migrated"); err != nil {
			return err
		}
	}
	if err = checkpoint("closures"); err != nil {
		return err
	}
	// Every original job column and rowid remains exact; new sparse facts are
	// checked separately. No application/chapter/receipt/wait payload is rewritten.
	var changed int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT original_rowid,`+columnSQL+` FROM retained_conversion_input EXCEPT SELECT rowid,`+columnSQL+` FROM jobdb_jobs)`).Scan(&changed); err != nil {
		return err
	}
	if changed != 0 {
		return fmt.Errorf("SQLite conversion changed %d retained jobs", changed)
	}
	if _, err = tx.ExecContext(ctx, `DROP TABLE jobdb_goal_candidates; DROP TABLE jobdb_goals; DROP TABLE retained_conversion_input`); err != nil {
		return err
	}
	var events int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name='jobdb_lease_events'`).Scan(&events); err != nil {
		return err
	}
	if events == 0 {
		if _, err = tx.ExecContext(ctx, leaseEventSchemaSQL); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, submissionReceiptSchemaSQL); err != nil {
		return err
	}
	for _, ddl := range extras {
		if _, err = tx.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	if err = migrateAppend(ctx, tx); err != nil {
		return err
	}
	if err = validateRetainedGoalSchema(ctx, tx); err != nil {
		return err
	}
	if err = verifyRetainedGoalFacts(ctx, tx); err != nil {
		return err
	}
	beforeIssues := len(report.Issues)
	if err = checkMigrationIntegrity(ctx, tx, report); err != nil {
		return err
	}
	if len(report.Issues) != beforeIssues {
		return report.finish()
	}
	return nil
}
