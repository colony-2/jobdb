CREATE TABLE IF NOT EXISTS jobdb_submission_receipts (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL,
 descriptor BLOB NOT NULL, requested_cell TEXT NOT NULL,
 goal_id TEXT NOT NULL, cell_id TEXT NOT NULL, cell_name TEXT NOT NULL, cell_repo TEXT NOT NULL,
 PRIMARY KEY(tenant_id,job_id)
);