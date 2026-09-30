package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
)

func (l *executionLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	return l.runtime.RenewExecutionLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, l.duration)
}

// RenewExecutionLeaseByID renews an exact live lease and loads its execution snapshot.
func (r *Runtime) RenewExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, duration time.Duration) (jobdb.RenewableExecutionLease, error) {
	if leaseID == "" || workerID == "" {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	var lease *executionLease
	err := r.withTx(ctx, func(tx *sql.Tx) error {
		row, err := r.loadJobRowTx(ctx, tx, key)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := validateLeaseRow(row, leaseID, workerID, now); err != nil {
			return err
		}
		duration = leaseDurationOrDefault(duration)
		expires := now.Add(duration)
		if _, err := tx.ExecContext(ctx, `UPDATE jobdb_jobs SET lease_expires_at_ns = ?, updated_at_ns = ? WHERE tenant_id = ? AND job_id = ?`, timeToNS(expires), timeToNS(now), key.TenantId, key.JobId); err != nil {
			return err
		}
		lease = &executionLease{runtime: r, jobKey: key, leaseID: leaseID, workerID: workerID,
			route:   row.nextRoute,
			payload: cloneBytes(row.payload), clientPayload: cloneJSON(row.clientPayload), clientPayloadRevision: row.clientPayloadRevision,
			duration: duration, expiresAt: expires, schemaHash: jobmetadata.SchemaHashFromStoredMetadata(row.metadata)}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lease, nil
}
