package directimpl

import (
	"context"
	"database/sql"
	"fmt"
)

const scheduleSchemaSQL = `
CREATE TABLE IF NOT EXISTS jobdb_schedules (
	tenant_id TEXT NOT NULL,
	schedule_id TEXT NOT NULL,
	state TEXT NOT NULL,
	generation BIGINT NOT NULL,
	spec_hash TEXT NOT NULL,
	trigger_json JSONB NOT NULL,
	target_json JSONB NOT NULL,
	target_job_type TEXT NOT NULL,
	overlap_policy TEXT NOT NULL,
	failure_policy_json JSONB NOT NULL,
	next_fire_at TIMESTAMPTZ,
	next_job_id TEXT,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (tenant_id, schedule_id)
);

CREATE INDEX IF NOT EXISTS jobdb_schedules_list_idx
	ON jobdb_schedules (tenant_id, state, updated_at DESC, schedule_id DESC);
`

func migrateSchedules(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if _, err := db.ExecContext(ctx, scheduleSchemaSQL); err != nil {
		return fmt.Errorf("migrate schedules: %w", err)
	}
	return nil
}
