package toyimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

var _ jobdb.JobDeletionRuntime = (*Runtime)(nil)

func (r *Runtime) DeleteJobs(ctx context.Context, req jobdb.DeleteJobsRequest) (jobdb.DeleteJobsResponse, error) {
	if err := req.Validate(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	raw, err := jobdb.MetadataPredicates(req.MetadataFilter)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	predicates, err := normalizeMetadataPredicates(raw)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, fmt.Errorf("%w: %v", jobdb.ErrInvalidJobDeletion, err)
	}
	ids := make(map[string]bool, len(req.JobIDs))
	for _, id := range req.JobIDs {
		ids[id] = true
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	var matches []*jobRecord
	for key, record := range r.engine.jobRecords {
		if key.TenantId != req.TenantID || len(ids) > 0 && !ids[key.JobId] {
			continue
		}
		record.mu.Lock()
		if record.hidden || req.CreatedBefore != nil && !record.createdAt.Before(*req.CreatedBefore) {
			record.mu.Unlock()
			continue
		}
		match, err := metadataMatches(record.metadata, predicates)
		record.mu.Unlock()
		if err != nil {
			return jobdb.DeleteJobsResponse{}, err
		}
		if match {
			matches = append(matches, record)
		}
	}
	if err := ctx.Err(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	out := jobdb.DeleteJobsResponse{MatchedCount: int64(len(matches))}
	if req.DryRun {
		return out, nil
	}
	now := time.Now().UTC()
	for _, record := range matches {
		record.mu.Lock()
		record.hidden = true
		if record.archived == nil {
			record.cancelled = true
			record.archived = &now
			record.finished = now
			record.status = jobdb.JobStatusCancelled
			record.err = context.Canceled
			record.completionStatus = "cancelled"
			record.completionDetail = "job deleted"
		}
		record.leased = false
		record.leaseID = ""
		record.leaseWorkerID = ""
		record.leaseExpiresAt = time.Time{}
		record.mu.Unlock()
	}
	out.DeletedCount = out.MatchedCount
	return out, nil
}
