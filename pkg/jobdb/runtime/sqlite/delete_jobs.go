package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

var _ jobdb.JobDeletionRuntime = (*Runtime)(nil)

func (r *Runtime) DeleteJobs(ctx context.Context, req jobdb.DeleteJobsRequest) (jobdb.DeleteJobsResponse, error) {
	if err := req.Validate(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	if err := r.validate(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	raw, err := jobdb.MetadataPredicates(req.MetadataFilter)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	predicates, err := normalizeMetadataPredicates(raw)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, fmt.Errorf("%w: %v", jobdb.ErrInvalidJobDeletion, err)
	}
	ids := stringSet(req.JobIDs)
	var out jobdb.DeleteJobsResponse
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		query := `SELECT job_id, metadata FROM jobdb_jobs WHERE tenant_id = ? AND hidden_at_ns IS NULL`
		args := []any{req.TenantID}
		if req.CreatedBefore != nil {
			query += ` AND created_at_ns < ?`
			args = append(args, req.CreatedBefore.UnixNano())
		}
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		var matches []string
		for rows.Next() {
			var id string
			var metadata []byte
			if err := rows.Scan(&id, &metadata); err != nil {
				return err
			}
			if len(ids) > 0 && !ids[id] {
				continue
			}
			match, err := metadataMatches(metadata, predicates)
			if err != nil {
				return err
			}
			if match {
				matches = append(matches, id)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		out.MatchedCount = int64(len(matches))
		if req.DryRun {
			return nil
		}
		now := time.Now().UTC().UnixNano()
		for _, id := range matches {
			_, err := tx.ExecContext(ctx, `UPDATE jobdb_jobs SET hidden_at_ns=?, updated_at_ns=?,
cancel_requested=CASE WHEN archived_at_ns IS NULL THEN 1 ELSE cancel_requested END,
completion_status=CASE WHEN archived_at_ns IS NULL THEN 'cancelled' ELSE completion_status END,
completion_detail=CASE WHEN archived_at_ns IS NULL THEN 'job deleted' ELSE completion_detail END,
archived_at_ns=COALESCE(archived_at_ns,?), lease_id=NULL, lease_worker_id=NULL, lease_expires_at_ns=NULL
WHERE tenant_id=? AND job_id=? AND hidden_at_ns IS NULL`, now, now, now, req.TenantID, id)
			if err != nil {
				return err
			}
		}
		out.DeletedCount = out.MatchedCount
		return nil
	})
	if err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	return out, nil
}
