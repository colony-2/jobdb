-- Frozen optional format-11 / required format-12 lease-event extension at f85c24f.

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
