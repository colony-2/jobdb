package remote

import (
	"context"
	"fmt"
	"net/http"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

var _ jobdb.JobDeletionRuntime = (*Runtime)(nil)

func (r *Runtime) DeleteJobs(ctx context.Context, req jobdb.DeleteJobsRequest) (jobdb.DeleteJobsResponse, error) {
	if err := req.Validate(); err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	predicates, err := metadataPredicatesToAPI(req.MetadataFilter)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, fmt.Errorf("%w: %v", jobdb.ErrInvalidJobDeletion, err)
	}
	body := runtimeapi.DeleteJobsRequest{CreatedBefore: req.CreatedBefore, MetadataPredicates: predicates, All: &req.All, DryRun: &req.DryRun}
	if len(req.JobIDs) > 0 {
		body.JobIds = &req.JobIDs
	}
	resp, err := r.client.DeleteJobsWithResponse(ctx, req.TenantID, body)
	if err != nil {
		return jobdb.DeleteJobsResponse{}, err
	}
	if resp.StatusCode() == http.StatusBadRequest {
		return jobdb.DeleteJobsResponse{}, fmt.Errorf("%w: %s", jobdb.ErrInvalidJobDeletion, resp.Body)
	}
	if resp.StatusCode() == http.StatusNotImplemented {
		return jobdb.DeleteJobsResponse{}, jobdb.ErrJobDeletionUnsupported
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.DeleteJobsResponse{}, responseError("delete jobs", resp.StatusCode(), resp.Body, nil)
	}
	return jobdb.DeleteJobsResponse{MatchedCount: resp.JSON200.MatchedCount, DeletedCount: resp.JSON200.DeletedCount}, nil
}

func (s *proxyServer) DeleteJobs(ctx context.Context, request runtimeapi.DeleteJobsRequestObject) (runtimeapi.DeleteJobsResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("request body is required")
	}
	filter, err := metadataFilterFromAPI(request.Body.MetadataPredicates)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	req := jobdb.DeleteJobsRequest{TenantID: request.TenantId, CreatedBefore: request.Body.CreatedBefore, MetadataFilter: filter, All: boolValue(request.Body.All), DryRun: boolValue(request.Body.DryRun)}
	if request.Body.JobIds != nil {
		req.JobIDs = *request.Body.JobIds
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	admin, ok := s.runtime.(jobdb.JobDeletionRuntime)
	if !ok {
		return nil, &httpStatusError{status: http.StatusNotImplemented, err: jobdb.ErrJobDeletionUnsupported}
	}
	result, err := admin.DeleteJobs(ctx, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.DeleteJobs200JSONResponse{MatchedCount: result.MatchedCount, DeletedCount: result.DeletedCount}, nil
}
