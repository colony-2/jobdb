package remote

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func isPrimaryGoalSelectionPath(path string) bool {
	if path == "/v1/capabilities/primary-goal-selection" {
		return true
	}
	p := strings.Split(path, "/")
	return len(p) == 7 && p[1] == "v1" && p[2] == "tenants" && p[4] == "jobs" && p[6] == "primary-goal"
}

func (s *proxyServer) GetPrimaryGoalSelectionCapability(ctx context.Context, req runtimeapi.GetPrimaryGoalSelectionCapabilityRequestObject) (runtimeapi.GetPrimaryGoalSelectionCapabilityResponseObject, error) {
	capability, ok := s.runtime.(jobdb.PrimaryGoalSelectionCapability)
	return runtimeapi.GetPrimaryGoalSelectionCapability200JSONResponse{Supported: ok && capability.SupportsPrimaryGoalSelection()}, nil
}

func (s *proxyServer) SubmitJobWithPrimaryGoal(ctx context.Context, request runtimeapi.SubmitJobWithPrimaryGoalRequestObject) (runtimeapi.SubmitJobWithPrimaryGoalResponseObject, error) {
	capability, ok := s.runtime.(jobdb.PrimaryGoalSelectionCapability)
	if !ok || !capability.SupportsPrimaryGoalSelection() {
		return nil, jobdb.ErrPrimaryGoalSelectionUnsupported
	}
	if request.Body == nil {
		return nil, jobdb.ErrInvalidDependency
	}
	req, err := submitJobRequestFromAPI(request.Body.Submission, request.TenantId, request.JobId)
	if err != nil {
		return nil, jobdb.ErrInvalidDependency
	}
	req.Job.GoalID = request.Body.GoalId
	if req.Job.GoalID == "" {
		return nil, jobdb.ErrInvalidDependency
	}
	if err = jobdb.ValidatePrimaryGoalSelection(req.Job); err != nil {
		return nil, err
	}
	if !jobdb.HasPrimaryGoalSelectionAuthorizer(ctx) {
		return nil, jobdb.ErrPrimaryGoalSelectionForbidden
	}
	if err = jobdb.AuthorizeCell(ctx, request.TenantId, "", "jobs.submit"); err != nil {
		return nil, jobdb.ErrPrimaryGoalSelectionForbidden
	}
	// The advertised backend reauthorizes goal access and selection using stored
	// identity during preparation, atomic publication and receipt replay.
	h, err := s.runtime.SubmitJob(ctx, req)
	if err != nil {
		return nil, err
	}
	if h.JobKey != (jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}) || h.GoalID != req.Job.GoalID || h.Cell == "" || h.CellID == "" || h.CellRepo == "" {
		return nil, jobdb.ErrSubmissionReceiptUnavailable
	}
	return runtimeapi.SubmitJobWithPrimaryGoal200JSONResponse(toAPIJobHandle(h)), nil
}

func (r *Runtime) submitJobWithPrimaryGoal(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if err := jobdb.ValidatePrimaryGoalSelection(req.Job); err != nil {
		return jobdb.JobHandle{}, err
	}
	discovery, err := r.client.GetPrimaryGoalSelectionCapabilityWithResponse(ctx)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if discovery.StatusCode() != http.StatusOK || discovery.JSON200 == nil || !discovery.JSON200.Supported {
		return jobdb.JobHandle{}, jobdb.ErrPrimaryGoalSelectionUnsupported
	}
	// The protected envelope contains selection; ordinary encoding remains unable
	// to silently drop it. Preserve the caller's explicit destination on all errors.
	ordinary := req
	ordinary.Job.GoalID = ""
	body, err := submitJobRequestToAPI(ctx, ordinary)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	response, err := r.client.SubmitJobWithPrimaryGoalWithResponse(ctx, req.Job.TenantId, req.Job.JobID, runtimeapi.SelectedSubmitJobRequest{GoalId: req.Job.GoalID, Submission: body})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if response.StatusCode() != http.StatusOK || response.JSON200 == nil {
		switch response.StatusCode() {
		case 404, 405, 501, 301, 302, 303, 307, 308:
			// A typed goal/cell 404 is distinct from a removed protected route.
			if typed := goalResponseError(response.Body); typed != nil {
				return jobdb.JobHandle{}, typed
			}
			if typed := cellResponseError(response.StatusCode(), response.Body); typed != nil {
				return jobdb.JobHandle{}, typed
			}
			return jobdb.JobHandle{}, jobdb.ErrPrimaryGoalSelectionUnsupported
		}
		return jobdb.JobHandle{}, responseError("submit job with primary goal", response.StatusCode(), response.Body, nil)
	}
	h, err := fromAPIJobHandle(*response.JSON200)
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("%w: %s", jobdb.ErrSubmissionReceiptUnavailable, err)
	}
	if h.JobKey != (jobdb.JobKey{TenantId: req.Job.TenantId, JobId: req.Job.JobID}) || h.GoalID != req.Job.GoalID {
		return jobdb.JobHandle{}, jobdb.ErrSubmissionReceiptUnavailable
	}
	return h, nil
}
