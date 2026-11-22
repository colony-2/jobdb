package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"strings"
)

// Kept separate from the frozen format-13 schema used by legacy conversion.
const resourceSchemaSQL = `
ALTER TABLE jobdb_jobs ADD COLUMN crash_budget_remaining INTEGER NOT NULL DEFAULT 5 CHECK(typeof(crash_budget_remaining)='integer' AND crash_budget_remaining>=0);
ALTER TABLE jobdb_jobs ADD COLUMN resource_not_before_ns INTEGER;
ALTER TABLE jobdb_jobs ADD COLUMN resource_resume BLOB;
CREATE TABLE jobdb_resource_sequences (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, invocation TEXT NOT NULL, attempt INTEGER NOT NULL, reason TEXT NOT NULL,
 count INTEGER NOT NULL CHECK(count>0), policy BLOB NOT NULL, allowance INTEGER NOT NULL CHECK(allowance>0),
 PRIMARY KEY(tenant_id,job_id,invocation,attempt,reason),
 FOREIGN KEY(tenant_id,job_id) REFERENCES jobdb_jobs(tenant_id,job_id) ON DELETE CASCADE
);
CREATE TABLE jobdb_resource_receipts (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, lease_id TEXT NOT NULL, worker_id TEXT NOT NULL, request_id TEXT NOT NULL,
 request BLOB NOT NULL, receipt BLOB NOT NULL,
 PRIMARY KEY(tenant_id,job_id,lease_id,request_id),
 FOREIGN KEY(tenant_id,job_id) REFERENCES jobdb_jobs(tenant_id,job_id) ON DELETE CASCADE
);
CREATE TRIGGER jobdb_resource_cleanup AFTER DELETE ON jobdb_jobs BEGIN
 DELETE FROM jobdb_resource_sequences WHERE tenant_id=OLD.tenant_id AND job_id=OLD.job_id;
 DELETE FROM jobdb_resource_receipts WHERE tenant_id=OLD.tenant_id AND job_id=OLD.job_id;
END;
`

func validateLegacyCrashCounts(ctx context.Context, tx *sql.Tx) error {
	var bad int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_jobs WHERE typeof(consecutive_expirations)<>'integer' OR consecutive_expirations<0`).Scan(&bad); err != nil {
		return err
	}
	if bad != 0 {
		return fmt.Errorf("SQLite migration: corrupt legacy crash counts")
	}
	return nil
}
func convertResourceSchema(ctx context.Context, tx *sql.Tx) error {
	if err := validateLegacyCrashCounts(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, resourceSchemaSQL); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE jobdb_jobs SET crash_budget_remaining=max(5-consecutive_expirations,0); PRAGMA user_version=14`)
	return err
}

func rowExecutionState(row jobRow) jobdb.ExecutionState {
	s := jobExecutionState(row.payload)
	b := row.crashBudgetRemaining
	s.CrashBudgetRemaining = &b
	s.ResourceBackoff = &jobdb.ResourceBackoffCapability{ProtocolVersion: 1, DurabilityVersion: 1}
	if len(row.resourceResume) > 0 {
		_ = json.Unmarshal(row.resourceResume, &s.ResourceResume)
	}
	return s
}
func acquiredBudget(row jobRow) int64 {
	b := row.crashBudgetRemaining
	if b > 0 && row.leaseID.Valid && !strings.HasPrefix(row.leaseWorkerID.String, "jobdb-append-") {
		b--
	}
	return b
}
