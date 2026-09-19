package runtimecore

import (
	"context"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
)

// CancelJob marks a job cancelled in the scheduler.
func (r *Runtime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	if err := req.JobKey.Validate(); err != nil {
		return err
	}
	workerID := req.WorkerID
	if workerID == "" {
		workerID = "jobdb-core-" + ksuid.New().String()
	}
	_, err := r.scheduler.CancelJob(ctx, CancelJobMutation{
		JobKey: req.JobKey, Reason: req.Reason, WorkerID: workerID,
		Now: r.now(),
	})
	return err
}

// ListJobs reads native scheduler rows and projects the stable JobDB summary.
func (r *Runtime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	if err := req.ValidateRoutes(); err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	if err := r.validate(); err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	if len(req.TenantIds) == 0 {
		return jobdb.ListJobsResponse{}, fmt.Errorf("tenant_ids is required for ListJobs")
	}
	if req.RootOnly && len(req.ParentJobIDs) > 0 {
		return jobdb.ListJobsResponse{}, fmt.Errorf("RootOnly cannot be combined with ParentJobIDs")
	}
	predicates, err := jobdb.MetadataPredicates(req.MetadataFilter)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = jobdb.DefaultListJobsPageSize
	} else if pageSize > jobdb.MaxListJobsPageSize {
		pageSize = jobdb.MaxListJobsPageSize
	}
	stored, err := r.scheduler.ListJobs(ctx, ListJobsRequest{
		TenantIds:    append([]string(nil), req.TenantIds...),
		Statuses:     append([]jobdb.JobStatus(nil), req.Statuses...),
		Stores:       append([]jobdb.JobStore(nil), req.Stores...),
		JobTypes:     append([]string(nil), req.JobTypes...),
		JobTasks:     append([]jobdb.JobTaskFilter(nil), req.JobTasks...),
		JobKeys:      append([]jobdb.JobKey(nil), req.JobKeys...),
		ParentJobIDs: append([]string(nil), req.ParentJobIDs...),
		RootOnly:     req.RootOnly, MetadataEquals: predicates,
		CreatedAfter: req.CreatedAfter, CreatedBefore: req.CreatedBefore,
		PageSize: pageSize, PageToken: req.PageToken,
	})
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	out := jobdb.ListJobsResponse{
		Jobs:          make([]jobdb.JobSummary, 0, len(stored.Jobs)),
		NextPageToken: stored.NextPageToken,
	}
	for _, row := range stored.Jobs {
		summary, err := jobSummaryFromStored(row)
		if err != nil {
			return jobdb.ListJobsResponse{}, err
		}
		out.Jobs = append(out.Jobs, summary)
	}
	return out, nil
}

func jobSummaryFromStored(row StoredJob) (jobdb.JobSummary, error) {
	nextRoute := jobdb.Route{JobType: row.RouteJobType}
	if row.WorkKind == WorkKindTask && row.TaskWork != nil {
		nextRoute.TaskType = row.TaskWork.TaskType
	}
	summary := jobdb.JobSummary{
		JobKey: row.JobKey, Status: row.Status, JobType: row.JobType,
		ClientPayload: append([]byte(nil), row.ClientPayload...), ClientPayloadRevision: row.ClientPayloadRevision, ExecutionState: executionState(row.RunPolicy, row.TaskWork),
		NextRoute: &nextRoute, WaitFor: append([]string(nil), row.WaitForJobIDs...),
		AvailableAt: row.AvailableAt, ExpiresAt: row.ExpiresAt,
		LeaseExpiresAt: row.LeaseExpiresAt, CancelRequested: row.CancelRequested,
		CreatedAt: row.CreatedAt, ArchivedAt: row.ArchivedAt,
		Metadata:   append([]byte(nil), row.AppMetadata...),
		SchemaHash: row.SchemaHash, ParentJobID: row.ParentJobID,
	}
	if row.WorkKind == WorkKindTask {
		if row.TaskWork == nil {
			return jobdb.JobSummary{}, fmt.Errorf("task route for %s is missing coordinates", row.JobKey)
		}
	}
	return summary, nil
}
