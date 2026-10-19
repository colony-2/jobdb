package runtimecore

import (
	"context"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// JobDeletionScheduler optionally supports atomic tenant-scoped deletion.
// Implementations must match selectors atomically and hide matching visible
// jobs from ListJobs and ListScheduleRuns before pagination, revoke leases,
// and cancel unfinished jobs so acquisition cannot restart them. Preserve
// terminal outcomes, GetJob, dependency facts, and chapter logs for references.
// DryRun must not mutate state. Already-hidden jobs do not count as matches.
type JobDeletionScheduler interface {
	DeleteJobs(context.Context, DeleteJobsRequest) (jobdb.DeleteJobsResponse, error)
}

type DeleteJobsRequest struct {
	TenantID       string
	JobIDs         []string
	CreatedBefore  *time.Time
	MetadataEquals []jobdb.MetadataPredicate
	All            bool
	DryRun         bool
	Now            time.Time
}

func (r *Runtime) DeleteJobs(ctx context.Context, req jobdb.DeleteJobsRequest) (jobdb.DeleteJobsResponse, error) {
	if err := req.Validate(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	if err := r.validate(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	store, ok := r.scheduler.(JobDeletionScheduler)
	if !ok {
		return jobdb.DeleteJobsResponse{}, jobdb.ErrJobDeletionUnsupported
	}
	predicates, err := jobdb.MetadataPredicates(req.MetadataFilter)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	return store.DeleteJobs(ctx, DeleteJobsRequest{TenantID: req.TenantID, JobIDs: append([]string(nil), req.JobIDs...), CreatedBefore: req.CreatedBefore, MetadataEquals: predicates, All: req.All, DryRun: req.DryRun, Now: r.now()})
}
