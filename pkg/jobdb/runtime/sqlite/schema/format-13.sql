-- Target retained-job schema. The caller advances the format only after verification.

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
 append_key TEXT,
 append_gen INTEGER,
 record_kind TEXT NOT NULL DEFAULT 'EXECUTION' CHECK(record_kind IN ('EXECUTION','GOAL_FAILURE','MIGRATED_GOAL_FAILURE')),
 goal_origin INTEGER NOT NULL DEFAULT 0 CHECK(goal_origin IN (0,1)),
 goal_closure_state TEXT,
 goal_closed_at_ns INTEGER,
 goal_winner_job_id TEXT,
 goal_closure_origin TEXT,
 goal_failure BLOB,
 CHECK(
  (goal_closure_state IS NULL AND goal_closed_at_ns IS NULL AND goal_winner_job_id IS NULL AND goal_closure_origin IS NULL)
  OR (goal_closure_state IS NOT NULL AND goal_closure_state='SUCCEEDED' AND goal_closed_at_ns IS NOT NULL AND goal_winner_job_id IS NOT NULL
      AND goal_closure_origin IS NOT NULL AND goal_closure_origin IN ('native','migrated') AND record_kind='EXECUTION')
  OR (goal_closure_state IS NOT NULL AND goal_closure_state='FAILED' AND goal_closed_at_ns IS NOT NULL AND goal_winner_job_id IS NULL
      AND goal_closure_origin IS NOT NULL AND goal_closure_origin IN ('native','migrated')
      AND record_kind IN ('GOAL_FAILURE','MIGRATED_GOAL_FAILURE'))
 ),
 CHECK(goal_origin=0 OR record_kind='EXECUTION'),
 CHECK((record_kind='EXECUTION' AND goal_failure IS NULL)
  OR (record_kind IN ('GOAL_FAILURE','MIGRATED_GOAL_FAILURE') AND goal_failure IS NOT NULL
      AND job_type='jobdb.goal_failure' AND archived_at_ns IS NOT NULL AND completion_status IS NOT NULL
      AND completion_status='failed' AND completion_detail IS NOT NULL
      AND route_job_type='' AND route_task_type='' AND dependency_wait_id=''
      AND lease_id IS NULL AND lease_worker_id IS NULL AND lease_expires_at_ns IS NULL
      AND wait_deadline_ns IS NULL AND alternate_job_type IS NULL AND alternate_task_type IS NULL
      AND alternate_at_ns IS NULL AND cancel_requested=0 AND parent_job_id IS NULL
      AND length(payload)=0 AND length(wait_for)=0 AND client_payload IS NULL AND metadata IS NULL)),
 FOREIGN KEY(tenant_id,goal_winner_job_id) REFERENCES jobdb_jobs(tenant_id,job_id),
	PRIMARY KEY (tenant_id, job_id),
 FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id)
);

CREATE INDEX IF NOT EXISTS jobdb_jobs_poll_idx
	ON jobdb_jobs (archived_at_ns, tenant_id, route_job_type, route_task_type, available_at_ns, created_at_ns);

CREATE INDEX IF NOT EXISTS jobdb_jobs_list_idx
	ON jobdb_jobs (tenant_id, created_at_ns DESC, job_id DESC);

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

CREATE TABLE jobdb_cells(tenant_id TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,state TEXT NOT NULL CHECK(state IN ('ACTIVE','RETIRED')),revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,retired_at_ns INTEGER,PRIMARY KEY(tenant_id,cell_id),UNIQUE(tenant_id,name),UNIQUE(tenant_id,repo));
CREATE TABLE jobdb_cell_names(tenant_id TEXT NOT NULL,name TEXT NOT NULL,cell_id TEXT NOT NULL,PRIMARY KEY(tenant_id,name),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
CREATE TABLE jobdb_cell_catalog(tenant_id TEXT PRIMARY KEY,cursor_key BLOB NOT NULL);
CREATE TABLE jobdb_cell_requests(tenant_id TEXT NOT NULL,principal TEXT NOT NULL,request_id TEXT NOT NULL,descriptor TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,principal,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
CREATE INDEX jobdb_jobs_cell_idx ON jobdb_jobs(tenant_id,cell_id,job_id);

CREATE INDEX IF NOT EXISTS jobdb_jobs_goal_idx ON jobdb_jobs(tenant_id,goal_id,job_id);
CREATE TABLE IF NOT EXISTS jobdb_dependency_waits(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id));
CREATE TABLE IF NOT EXISTS jobdb_dependency_targets(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,target_kind TEXT NOT NULL,target_id TEXT NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id,target_kind,target_id));
CREATE INDEX IF NOT EXISTS jobdb_dependency_targets_lookup ON jobdb_dependency_targets(tenant_id,target_kind,target_id);
CREATE TABLE IF NOT EXISTS jobdb_goal_preparations(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,request_id TEXT,goal_id TEXT NOT NULL,share_goal INTEGER NOT NULL,descriptor BLOB NOT NULL,requested_cell TEXT NOT NULL,requested_repo TEXT NOT NULL,cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,parent_job_id TEXT NOT NULL,prior_job_id TEXT NOT NULL,dependencies BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id),UNIQUE(tenant_id,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
 CREATE INDEX jobdb_preparations_cell_idx ON jobdb_goal_preparations(tenant_id,cell_id);
 CREATE INDEX jobdb_jobs_type_goal_idx ON jobdb_jobs(tenant_id,job_type,goal_id,job_id);
 CREATE INDEX jobdb_dependency_targets_discovery_idx ON jobdb_dependency_targets(tenant_id,target_kind,target_id,job_id,wait_id);
 CREATE INDEX jobdb_dependency_waits_state_idx ON jobdb_dependency_waits(tenant_id,job_id,state,wait_id);

-- Partial uniqueness preserves inherited active overlaps while protecting origins and first closure.
CREATE UNIQUE INDEX jobdb_jobs_goal_origin_unique ON jobdb_jobs(tenant_id,goal_id) WHERE goal_origin=1;
CREATE UNIQUE INDEX jobdb_jobs_goal_closure_unique ON jobdb_jobs(tenant_id,goal_id) WHERE goal_closure_state IS NOT NULL;
CREATE INDEX jobdb_jobs_goal_active_idx ON jobdb_jobs(tenant_id,goal_id,job_id) WHERE archived_at_ns IS NULL;
CREATE INDEX jobdb_jobs_goal_origin_cell_idx ON jobdb_jobs(tenant_id,cell_id,goal_id) WHERE goal_origin=1;
CREATE INDEX jobdb_jobs_goal_origin_created_idx ON jobdb_jobs(tenant_id,created_at_ns,goal_id) WHERE goal_origin=1;
CREATE INDEX jobdb_jobs_goal_closure_state_idx ON jobdb_jobs(tenant_id,goal_closure_state,goal_id) WHERE goal_closure_state IS NOT NULL;
CREATE INDEX jobdb_jobs_goal_closure_time_idx ON jobdb_jobs(tenant_id,goal_closed_at_ns,goal_id) WHERE goal_closure_state IS NOT NULL;

CREATE TRIGGER jobdb_jobs_identity_immutable BEFORE UPDATE ON jobdb_jobs
WHEN NEW.tenant_id IS NOT OLD.tenant_id OR NEW.job_id IS NOT OLD.job_id OR NEW.goal_id IS NOT OLD.goal_id
 OR NEW.cell_id IS NOT OLD.cell_id OR NEW.cell_name IS NOT OLD.cell_name OR NEW.cell_repo IS NOT OLD.cell_repo
 OR NEW.created_at_ns IS NOT OLD.created_at_ns OR NEW.record_kind IS NOT OLD.record_kind
 OR NEW.goal_failure IS NOT OLD.goal_failure
BEGIN SELECT RAISE(ABORT,'retained job identity and protected snapshot are immutable'); END;

CREATE TRIGGER jobdb_jobs_goal_facts_immutable BEFORE UPDATE ON jobdb_jobs
WHEN (OLD.goal_origin=1 AND NEW.goal_origin IS NOT OLD.goal_origin)
 OR (OLD.goal_closure_state IS NOT NULL AND
     (NEW.goal_closure_state IS NOT OLD.goal_closure_state OR NEW.goal_closed_at_ns IS NOT OLD.goal_closed_at_ns
      OR NEW.goal_winner_job_id IS NOT OLD.goal_winner_job_id OR NEW.goal_closure_origin IS NOT OLD.goal_closure_origin))
 OR (OLD.record_kind<>'EXECUTION' AND
     (NEW.archived_at_ns IS NOT OLD.archived_at_ns OR NEW.completion_status IS NOT OLD.completion_status
      OR NEW.completion_detail IS NOT OLD.completion_detail))
BEGIN SELECT RAISE(ABORT,'retained goal facts are immutable'); END;

CREATE TRIGGER jobdb_jobs_goal_facts_retained BEFORE DELETE ON jobdb_jobs
WHEN OLD.goal_origin=1 OR OLD.goal_closure_state IS NOT NULL OR OLD.record_kind<>'EXECUTION'
BEGIN SELECT RAISE(ABORT,'goal origin and closure jobs must be retained'); END;

CREATE UNIQUE INDEX jobdb_jobs_append_idx ON jobdb_jobs(tenant_id,cell_id,job_type,append_key,append_gen) WHERE append_key IS NOT NULL;
