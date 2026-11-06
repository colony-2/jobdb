package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (r *Runtime) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	if err := r.validate(); err != nil {
		return err
	}
	ctx = ctxOrBackground(ctx)
	return r.withTx(ctx, func(tx *sql.Tx) error {
		var archived, hidden, expires sql.NullInt64
		var cancelled bool
		var consecutive int64
		err := tx.QueryRowContext(ctx, `SELECT archived_at_ns, hidden_at_ns, cancel_requested, lease_expires_at_ns, consecutive_expirations
FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, req.JobKey.TenantId, req.JobKey.JobId).Scan(&archived, &hidden, &cancelled, &expires, &consecutive)
		if err == sql.ErrNoRows {
			return jobdb.ErrJobNotFound
		}
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if archived.Valid || cancelled || hidden.Valid || expires.Valid && expires.Int64 > timeToNS(now) {
			return fmt.Errorf("%w: cannot clear crash concern for a live, final, cancelled, or hidden job", jobdb.ErrConflict)
		}
		if consecutive == 0 {
			return nil
		}
		_, err = tx.ExecContext(ctx, `UPDATE jobdb_jobs SET consecutive_expirations=0 WHERE tenant_id=? AND job_id=?`, req.JobKey.TenantId, req.JobKey.JobId)
		return err
	})
}
