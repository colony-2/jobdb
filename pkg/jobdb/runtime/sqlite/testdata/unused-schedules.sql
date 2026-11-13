-- Schedule table from the pre-removal SQLite schema. Test fixture only.
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

CREATE INDEX jobdb_schedules_cell_idx ON jobdb_schedules(tenant_id,cell_id,state);
