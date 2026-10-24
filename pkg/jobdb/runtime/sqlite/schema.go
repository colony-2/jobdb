package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
	"time"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS jobdb_jobs (
	tenant_id TEXT NOT NULL,
	job_id TEXT NOT NULL,
 goal_id TEXT NOT NULL,
 dependency_wait_id TEXT NOT NULL DEFAULT '',
	job_type TEXT NOT NULL,
	route_job_type TEXT NOT NULL,
 route_task_type TEXT NOT NULL DEFAULT '',
	payload BLOB NOT NULL DEFAULT x'',
 client_payload BLOB,
 client_payload_revision INTEGER NOT NULL DEFAULT 0,
 initial_payload_digest TEXT NOT NULL,
	metadata BLOB,
	parent_job_id TEXT,
	wait_for BLOB NOT NULL DEFAULT x'',
	available_at_ns INTEGER NOT NULL,
	wait_deadline_ns INTEGER,
	created_at_ns INTEGER NOT NULL,
	updated_at_ns INTEGER NOT NULL,
	archived_at_ns INTEGER,
	hidden_at_ns INTEGER,
	cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
	completion_status TEXT,
	completion_detail TEXT,
	consecutive_expirations INTEGER NOT NULL DEFAULT 0,
	lease_id TEXT,
	lease_worker_id TEXT,
	lease_expires_at_ns INTEGER,
	alternate_job_type TEXT,
 alternate_task_type TEXT,
	alternate_at_ns INTEGER,
	PRIMARY KEY (tenant_id, job_id)
);

CREATE INDEX IF NOT EXISTS jobdb_jobs_poll_idx
	ON jobdb_jobs (archived_at_ns, tenant_id, route_job_type, route_task_type, available_at_ns, created_at_ns);

CREATE INDEX IF NOT EXISTS jobdb_jobs_list_idx
	ON jobdb_jobs (tenant_id, created_at_ns DESC, job_id DESC);

CREATE TABLE IF NOT EXISTS jobdb_schedules (
	tenant_id TEXT NOT NULL,
	schedule_id TEXT NOT NULL,
	state TEXT NOT NULL,
	generation INTEGER NOT NULL,
	spec_hash TEXT NOT NULL,
	trigger_json BLOB NOT NULL,
	target_json BLOB NOT NULL,
	target_job_type TEXT NOT NULL,
	overlap_policy TEXT NOT NULL,
	failure_policy_json BLOB NOT NULL,
	next_fire_at_ns INTEGER,
	next_job_id TEXT,
	created_at_ns INTEGER NOT NULL,
	updated_at_ns INTEGER NOT NULL,
	PRIMARY KEY (tenant_id, schedule_id)
);

CREATE INDEX IF NOT EXISTS jobdb_schedules_list_idx
	ON jobdb_schedules (tenant_id, state, updated_at_ns DESC, schedule_id DESC);

CREATE TABLE IF NOT EXISTS jobdb_schemas (
	tenant_id TEXT NOT NULL,
	schema_hash TEXT NOT NULL,
	schema_json BLOB NOT NULL,
	state TEXT NOT NULL,
	created_at_ns INTEGER NOT NULL,
	archived_at_ns INTEGER,
	PRIMARY KEY (tenant_id, schema_hash)
);

CREATE INDEX IF NOT EXISTS jobdb_schemas_list_idx
	ON jobdb_schemas (tenant_id, state, created_at_ns DESC, schema_hash ASC);
`

const goalSchemaSQL = `
CREATE INDEX IF NOT EXISTS jobdb_jobs_goal_idx ON jobdb_jobs(tenant_id,goal_id,job_id);
CREATE TABLE IF NOT EXISTS jobdb_goals(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,goal_id));
CREATE TABLE IF NOT EXISTS jobdb_goal_candidates(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,job_id TEXT NOT NULL,added_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,goal_id,job_id));
CREATE INDEX IF NOT EXISTS jobdb_goal_candidates_job_idx ON jobdb_goal_candidates(tenant_id,job_id,goal_id);
CREATE TABLE IF NOT EXISTS jobdb_dependency_waits(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id));
CREATE TABLE IF NOT EXISTS jobdb_dependency_targets(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,target_kind TEXT NOT NULL,target_id TEXT NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id,target_kind,target_id));
CREATE INDEX IF NOT EXISTS jobdb_dependency_targets_lookup ON jobdb_dependency_targets(tenant_id,target_kind,target_id);
CREATE TABLE IF NOT EXISTS jobdb_goal_preparations(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,request_id TEXT,goal_id TEXT NOT NULL,share_goal INTEGER NOT NULL,descriptor BLOB NOT NULL,dependencies BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id),UNIQUE(tenant_id,request_id));
`

func migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite runtime: db is required")
	}
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == 7 {
		return nil
	}
	if version != 6 {
		var tables int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil {
			return err
		}
		if version != 0 || tables != 0 {
			return fmt.Errorf("sqlite runtime: unsupported database format; create a fresh database")
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if version == 6 {
		if _, err = tx.ExecContext(ctx, "ALTER TABLE jobdb_jobs ADD COLUMN goal_id TEXT NOT NULL DEFAULT ''; ALTER TABLE jobdb_jobs ADD COLUMN dependency_wait_id TEXT NOT NULL DEFAULT '';"); err != nil {
			return err
		}
	} else {
		if _, err = tx.ExecContext(ctx, schemaSQL); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, goalSchemaSQL); err != nil {
		return err
	}
	if version == 6 {
		rows, err := tx.QueryContext(ctx, "SELECT tenant_id,job_id,created_at_ns,archived_at_ns,completion_status FROM jobdb_jobs")
		if err != nil {
			return err
		}
		type prior struct {
			tenant, id string
			created    int64
			archived   sql.NullInt64
			status     sql.NullString
		}
		var jobs []prior
		for rows.Next() {
			var j prior
			if err = rows.Scan(&j.tenant, &j.id, &j.created, &j.archived, &j.status); err != nil {
				rows.Close()
				return err
			}
			jobs = append(jobs, j)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, j := range jobs {
			if j.archived.Valid && !knownGoalMigrationOutcome(j.status.String) {
				return fmt.Errorf("job %s has no durable outcome; reconcile before migrating", j.id)
			}
			goalID := ksuid.New().String()
			created := time.Unix(0, j.created).UTC()
			g := jobdb.GoalInfo{GoalKey: jobdb.GoalKey{TenantId: j.tenant, GoalID: goalID}, State: jobdb.GoalOpen, Revision: 1, CreatedAt: created, UpdatedAt: created}
			if j.archived.Valid && (j.status.String == "success" || j.status.String == "succeeded") {
				closed := time.Unix(0, j.archived.Int64).UTC()
				g.State = jobdb.GoalSucceeded
				g.Revision = 2
				g.SuccessfulJobID = j.id
				g.ClosedAt = &closed
				g.UpdatedAt = closed
			}
			raw, _ := json.Marshal(g)
			if _, err = tx.ExecContext(ctx, "INSERT INTO jobdb_goals(tenant_id,goal_id,payload) VALUES(?,?,?)", j.tenant, goalID, raw); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "UPDATE jobdb_jobs SET goal_id=? WHERE tenant_id=? AND job_id=?", goalID, j.tenant, j.id); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, "PRAGMA user_version=7"); err != nil {
		return err
	}
	return tx.Commit()
}

func knownGoalMigrationOutcome(status string) bool {
	switch status {
	case "success", "succeeded", "failed_app", "failed_system", "failed_timeout", "cancelled":
		return true
	}
	return false
}
