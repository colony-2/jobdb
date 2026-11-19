package sqlite

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

func (r *Runtime) ReadExecutionProvenance(ctx context.Context, q jobdb.ReadExecutionProvenanceRequest) (jobdb.ExecutionProvenance, error) {
	return runtimecore.ReadExecutionProvenance(ctx, q, func(ctx context.Context) error {
		if err := r.requirePublishedJob(ctx, q.JobKey); err != nil {
			return err
		}
		var hidden bool
		if err := r.db.QueryRowContext(ctx, `SELECT hidden_at_ns IS NOT NULL FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, q.JobKey.TenantId, q.JobKey.JobId).Scan(&hidden); err != nil {
			return err
		}
		if hidden {
			return jobdb.ErrJobNotFound
		}
		row, err := r.loadJobRow(ctx, q.JobKey)
		if err != nil {
			return err
		}
		_, err = decodeJobPayload(row.payload)
		return err
	}, r.chapterStore)
}
