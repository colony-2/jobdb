package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrMigrationRequired identifies the manual-upgrade prerequisite independently
// of the source format and selected database path.
var ErrMigrationRequired = errors.New("sqlite runtime: manual migration required")

// MigrationRequiredError reports a legacy database that requires explicit offline conversion.
// Constructors never convert, reset, or initialize ancillary stores for these inputs.
type MigrationRequiredError struct {
	SourceFormat int
	Path         string
}

func (e *MigrationRequiredError) Error() string {
	return fmt.Sprintf("sqlite runtime: database %q format %d requires manual upgrade to 13; stop every writer, back up the database, run jobdb sqlite check-migrate --db PATH, then jobdb sqlite migrate --db PATH", e.Path, e.SourceFormat)
}

func (e *MigrationRequiredError) Unwrap() error { return ErrMigrationRequired }

func legacyMigrationRequired(ctx context.Context, q goalQuery, version int) error {
	var path string
	if err := q.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
		return err
	}
	return &MigrationRequiredError{SourceFormat: version, Path: path}
}

func refuseLegacy(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == 11 || version == 12 {
		return legacyMigrationRequired(ctx, db, version)
	}
	return nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite runtime: db is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version, tables int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	switch version {
	case 11, 12:
		return legacyMigrationRequired(ctx, tx, version)
	case 0:
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil {
			return err
		}
		if tables != 0 {
			return fmt.Errorf("sqlite runtime: unsupported unversioned database; existing data was not changed")
		}
		if err = createRetainedGoalSchema(ctx, tx); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "PRAGMA user_version=13"); err != nil {
			return err
		}
	case 13:
		if err = validateRetainedGoalSchema(ctx, tx); err != nil {
			return err
		}
	default:
		return fmt.Errorf("sqlite runtime: unsupported database format %d", version)
	}
	if err = migrateAppend(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func validateLeaseEventBaseSchema(ctx context.Context, tx *sql.Tx, requireEvents bool) error {
	var appendColumns int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('jobdb_jobs') WHERE name IN ('append_key','append_gen')`).Scan(&appendColumns); err != nil {
		return err
	}
	if appendColumns != 0 && appendColumns != 2 {
		return fmt.Errorf("sqlite runtime: incomplete append columns")
	}
	expected, err := sql.Open(sqliteDriverName, ":memory:")
	if err != nil {
		return err
	}
	defer expected.Close()
	if _, err = expected.ExecContext(ctx, legacyGoalSchemaSQL+legacyLeaseEventSchemaSQL); err != nil {
		return err
	}
	rows, err := expected.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return err
	}
	defer rows.Close()
	normalize := func(s string) string {
		s = strings.Join(strings.Fields(strings.ReplaceAll(s, "IF NOT EXISTS ", "")), " ")
		s = strings.ReplaceAll(s, ", append_key TEXT", "")
		s = strings.ReplaceAll(s, ", append_gen INTEGER", "")
		s = strings.ReplaceAll(s, "'task_completion','append'", "'task_completion'")
		return s
	}
	var eventTable int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE name='jobdb_lease_events'").Scan(&eventTable); err != nil {
		return err
	}
	for rows.Next() {
		var name, want, got string
		if err = rows.Scan(&name, &want); err != nil {
			return err
		}
		// Scheduling was removed without deleting dormant historical data. Both
		// authentic older schemas and current schedule-free inputs are supported.
		if strings.HasPrefix(name, "jobdb_schedules") {
			var exists int
			if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE name=?", name).Scan(&exists); err != nil {
				return err
			}
			if exists == 0 {
				continue
			}
		}
		if strings.HasPrefix(name, "jobdb_lease_events") && !requireEvents && eventTable == 0 {
			continue
		}
		if err = tx.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE name=?", name).Scan(&got); err != nil {
			return fmt.Errorf("sqlite runtime: inconsistent schema %s: %w", name, err)
		}
		if normalize(got) != normalize(want) {
			return fmt.Errorf("sqlite runtime: inconsistent schema %s", name)
		}
	}
	return rows.Err()
}

const leaseEventSchemaSQL = `
CREATE TABLE jobdb_lease_events (
 event_id INTEGER PRIMARY KEY AUTOINCREMENT,
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('lease_acquired','lease_renewed')),
 occurred_at_ns INTEGER NOT NULL,
 cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,
 goal_id TEXT NOT NULL,job_type TEXT NOT NULL,
 route_job_type TEXT NOT NULL,route_task_type TEXT NOT NULL,
 lease_id TEXT NOT NULL,worker_id TEXT NOT NULL,lease_expires_at_ns INTEGER NOT NULL,
 previous_lease_expires_at_ns INTEGER,acquisition_source TEXT,previous_lease_id TEXT,
 CHECK ((event_type='lease_acquired' AND acquisition_source IS NOT NULL AND acquisition_source IN ('poll','targeted','task_completion','append') AND previous_lease_expires_at_ns IS NULL)
 OR (event_type='lease_renewed' AND previous_lease_expires_at_ns IS NOT NULL AND acquisition_source IS NULL AND previous_lease_id IS NULL))
);
CREATE UNIQUE INDEX jobdb_lease_events_acquired_unique ON jobdb_lease_events(tenant_id,job_id,lease_id) WHERE event_type='lease_acquired';
CREATE INDEX jobdb_lease_events_time_idx ON jobdb_lease_events(tenant_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_job_id_idx ON jobdb_lease_events(tenant_id,job_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_cell_id_idx ON jobdb_lease_events(tenant_id,cell_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_goal_id_idx ON jobdb_lease_events(tenant_id,goal_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_worker_id_idx ON jobdb_lease_events(tenant_id,worker_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_lease_id_idx ON jobdb_lease_events(tenant_id,lease_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_event_type_idx ON jobdb_lease_events(tenant_id,event_type,occurred_at_ns DESC,event_id DESC);
`

// The append columns and lease source are additive to the format-11 schema.
func migrateAppend(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('jobdb_jobs') WHERE name='append_key'").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE jobdb_jobs ADD COLUMN append_key TEXT; ALTER TABLE jobdb_jobs ADD COLUMN append_gen INTEGER;`); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS jobdb_jobs_append_idx ON jobdb_jobs(tenant_id,cell_id,job_type,append_key,append_gen) WHERE append_key IS NOT NULL;`); err != nil {
		return err
	}
	var ddl string
	if err := tx.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE name='jobdb_lease_events'").Scan(&ddl); err != nil {
		return err
	}
	if !strings.Contains(ddl, "'append'") {
		// Rebuild in place rather than renaming the source: SQLite otherwise
		// rewrites dependent views and foreign keys to the temporary table name.
		rows, err := tx.QueryContext(ctx, `SELECT name,sql FROM sqlite_master WHERE tbl_name='jobdb_lease_events' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name`)
		if err != nil {
			return err
		}
		type eventDefinition struct{ name, ddl string }
		var definitions []eventDefinition
		for rows.Next() {
			var d eventDefinition
			if err = rows.Scan(&d.name, &d.ddl); err != nil {
				rows.Close()
				return err
			}
			definitions = append(definitions, d)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		var sequence sql.NullInt64
		err = tx.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name='jobdb_lease_events'`).Scan(&sequence)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err = tx.ExecContext(ctx, `CREATE TEMP TABLE retained_conversion_events AS SELECT * FROM jobdb_lease_events; DROP TABLE jobdb_lease_events`); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, leaseEventSchemaSQL); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO jobdb_lease_events SELECT * FROM retained_conversion_events; DROP TABLE retained_conversion_events`); err != nil {
			return err
		}
		for _, d := range definitions {
			var exists int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name=?`, d.name).Scan(&exists); err != nil {
				return err
			}
			if exists == 0 {
				if _, err = tx.ExecContext(ctx, d.ddl); err != nil {
					return err
				}
			}
		}
		if sequence.Valid {
			if _, err = tx.ExecContext(ctx, `UPDATE sqlite_sequence SET seq=max(seq,?) WHERE name='jobdb_lease_events'`, sequence.Int64); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO sqlite_sequence(name,seq) SELECT 'jobdb_lease_events',? WHERE NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name='jobdb_lease_events')`, sequence.Int64); err != nil {
				return err
			}
		}
	}
	return nil
}
