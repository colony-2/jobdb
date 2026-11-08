package sqlite

import (
	"context"
	"database/sql"

	"fmt"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS jobdb_jobs (
	tenant_id TEXT NOT NULL,
	job_id TEXT NOT NULL,
 goal_id TEXT NOT NULL,
 cell_id TEXT NOT NULL,
 cell_name TEXT NOT NULL,
 cell_repo TEXT NOT NULL,
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
	PRIMARY KEY (tenant_id, job_id),
 FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id)
);

CREATE INDEX IF NOT EXISTS jobdb_jobs_poll_idx
	ON jobdb_jobs (archived_at_ns, tenant_id, route_job_type, route_task_type, available_at_ns, created_at_ns);

CREATE INDEX IF NOT EXISTS jobdb_jobs_list_idx
	ON jobdb_jobs (tenant_id, created_at_ns DESC, job_id DESC);

CREATE TABLE IF NOT EXISTS jobdb_schedules (
 pause_reason TEXT NOT NULL DEFAULT '',
	tenant_id TEXT NOT NULL,
	schedule_id TEXT NOT NULL,
	state TEXT NOT NULL,
	generation INTEGER NOT NULL,
	spec_hash TEXT NOT NULL,
	trigger_json BLOB NOT NULL,
	target_json BLOB NOT NULL,
 cell_id TEXT NOT NULL,
 cell_name TEXT NOT NULL,
 cell_repo TEXT NOT NULL,
	target_job_type TEXT NOT NULL,
	overlap_policy TEXT NOT NULL,
	failure_policy_json BLOB NOT NULL,
	next_fire_at_ns INTEGER,
	next_job_id TEXT,
	created_at_ns INTEGER NOT NULL,
	updated_at_ns INTEGER NOT NULL,
 FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id),
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
CREATE TABLE jobdb_cells(tenant_id TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,state TEXT NOT NULL CHECK(state IN ('ACTIVE','RETIRED')),revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,retired_at_ns INTEGER,PRIMARY KEY(tenant_id,cell_id),UNIQUE(tenant_id,name),UNIQUE(tenant_id,repo));
CREATE TABLE jobdb_cell_names(tenant_id TEXT NOT NULL,name TEXT NOT NULL,cell_id TEXT NOT NULL,PRIMARY KEY(tenant_id,name),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
CREATE TABLE jobdb_cell_catalog(tenant_id TEXT PRIMARY KEY,cursor_key BLOB NOT NULL);
CREATE TABLE jobdb_cell_requests(tenant_id TEXT NOT NULL,principal TEXT NOT NULL,request_id TEXT NOT NULL,descriptor TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,principal,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
CREATE INDEX jobdb_jobs_cell_idx ON jobdb_jobs(tenant_id,cell_id,job_id);

CREATE INDEX IF NOT EXISTS jobdb_jobs_goal_idx ON jobdb_jobs(tenant_id,goal_id,job_id);
CREATE TABLE IF NOT EXISTS jobdb_goals(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,state TEXT NOT NULL DEFAULT '',created_at_ns INTEGER NOT NULL DEFAULT 0,updated_at_ns INTEGER NOT NULL DEFAULT 0,cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,goal_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
CREATE TABLE IF NOT EXISTS jobdb_goal_candidates(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,job_id TEXT NOT NULL,added_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,goal_id,job_id));
CREATE INDEX IF NOT EXISTS jobdb_goal_candidates_job_idx ON jobdb_goal_candidates(tenant_id,job_id,goal_id);
CREATE TABLE IF NOT EXISTS jobdb_dependency_waits(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id));
CREATE TABLE IF NOT EXISTS jobdb_dependency_targets(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,target_kind TEXT NOT NULL,target_id TEXT NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id,target_kind,target_id));
CREATE INDEX IF NOT EXISTS jobdb_dependency_targets_lookup ON jobdb_dependency_targets(tenant_id,target_kind,target_id);
CREATE TABLE IF NOT EXISTS jobdb_goal_preparations(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,request_id TEXT,goal_id TEXT NOT NULL,share_goal INTEGER NOT NULL,descriptor BLOB NOT NULL,requested_cell TEXT NOT NULL,requested_repo TEXT NOT NULL,cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,parent_job_id TEXT NOT NULL,prior_job_id TEXT NOT NULL,dependencies BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id),UNIQUE(tenant_id,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
 CREATE INDEX jobdb_goals_cell_idx ON jobdb_goals(tenant_id,cell_id,state);
 CREATE INDEX jobdb_preparations_cell_idx ON jobdb_goal_preparations(tenant_id,cell_id);
 CREATE INDEX jobdb_schedules_cell_idx ON jobdb_schedules(tenant_id,cell_id,state);
 CREATE INDEX jobdb_goals_state_idx ON jobdb_goals(tenant_id,state,goal_id);
 CREATE INDEX jobdb_goals_created_idx ON jobdb_goals(tenant_id,created_at_ns,goal_id);
 CREATE INDEX jobdb_goals_updated_idx ON jobdb_goals(tenant_id,updated_at_ns,goal_id);
 CREATE INDEX jobdb_jobs_type_goal_idx ON jobdb_jobs(tenant_id,job_type,goal_id,job_id);
 CREATE INDEX jobdb_dependency_targets_discovery_idx ON jobdb_dependency_targets(tenant_id,target_kind,target_id,job_id,wait_id);
 CREATE INDEX jobdb_dependency_waits_state_idx ON jobdb_dependency_waits(tenant_id,job_id,state,wait_id);
`

func migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite runtime: db is required")
	}
	var version, tables int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == 12 {
		return nil
	}
	if version == 11 {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, leaseEventSchemaSQL); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "PRAGMA user_version=12"); err != nil {
			return err
		}
		return tx.Commit()
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil {
		return err
	}
	if version != 0 || tables != 0 {
		return fmt.Errorf("sqlite runtime: unsupported database format %d; cell storage requires a fresh database; reset required (no old-job migration)", version)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, schemaSQL); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, goalSchemaSQL); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, leaseEventSchemaSQL); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "PRAGMA user_version=12"); err != nil {
		return err
	}
	return tx.Commit()
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
 CHECK ((event_type='lease_acquired' AND acquisition_source IS NOT NULL AND acquisition_source IN ('poll','targeted','task_completion') AND previous_lease_expires_at_ns IS NULL)
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
