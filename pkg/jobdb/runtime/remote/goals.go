package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"net/http"
)

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
func dependenciesToAPI(d []jobdb.Dependency) *[]runtimeapi.Dependency {
	if len(d) == 0 {
		return nil
	}
	return &d
}
func dependenciesFromAPI(d *[]runtimeapi.Dependency) []jobdb.Dependency {
	if d == nil {
		return nil
	}
	return *d
}

func (r *Runtime) GetGoal(ctx context.Context, k jobdb.GoalKey) (jobdb.GoalInfo, error) {
	resp, err := r.client.GetGoalWithResponse(ctx, k.TenantId, k.GoalID)
	if err != nil {
		return jobdb.GoalInfo{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return jobdb.GoalInfo{}, responseError("get goal", resp.StatusCode(), resp.Body, jobdb.ErrGoalNotFound)
	}
	return *resp.JSON200, nil
}
func (r *Runtime) AddGoalCandidate(ctx context.Context, req jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	resp, err := r.client.AddGoalCandidateWithResponse(ctx, req.GoalKey.TenantId, req.GoalKey.GoalID, req.JobID)
	if err != nil {
		return jobdb.GoalCandidateInfo{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return jobdb.GoalCandidateInfo{}, responseError("add goal candidate", resp.StatusCode(), resp.Body, nil)
	}
	return *resp.JSON200, nil
}
func (r *Runtime) FailGoal(ctx context.Context, req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	if err := req.Validate(); err != nil {
		return jobdb.GoalInfo{}, err
	}
	resp, err := r.client.FailGoalWithResponse(ctx, req.GoalKey.TenantId, req.GoalKey.GoalID, req)
	if err != nil {
		return jobdb.GoalInfo{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return jobdb.GoalInfo{}, responseError("fail goal", resp.StatusCode(), resp.Body, nil)
	}
	return *resp.JSON200, nil
}
func (r *Runtime) GetDependencyWait(ctx context.Context, k jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	resp, err := r.client.GetDependencyWaitWithResponse(ctx, k.JobKey.TenantId, k.JobKey.JobId, k.WaitID)
	if err != nil {
		return jobdb.DependencyWaitInfo{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return jobdb.DependencyWaitInfo{}, responseError("get dependency wait", resp.StatusCode(), resp.Body, nil)
	}
	out := *resp.JSON200
	if out.JobKey != k.JobKey || out.WaitID != k.WaitID || len(out.Dependencies) == 0 {
		return out, fmt.Errorf("server response missing required dependency wait fields")
	}
	switch out.State {
	case jobdb.DependencyPending, jobdb.DependencySatisfied, jobdb.DependencyFailed, jobdb.DependencyTimedOut, jobdb.DependencyCancelled:
	default:
		return out, fmt.Errorf("unsupported dependency wait state %q", out.State)
	}
	return out, nil
}
func (s *proxyServer) GetGoal(ctx context.Context, req runtimeapi.GetGoalRequestObject) (runtimeapi.GetGoalResponseObject, error) {
	out, err := s.runtime.GetGoal(ctx, jobdb.GoalKey{TenantId: req.TenantId, GoalID: req.GoalId})
	return runtimeapi.GetGoal200JSONResponse(out), err
}
func (s *proxyServer) AddGoalCandidate(ctx context.Context, req runtimeapi.AddGoalCandidateRequestObject) (runtimeapi.AddGoalCandidateResponseObject, error) {
	out, err := s.runtime.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: req.TenantId, GoalID: req.GoalId}, JobID: req.JobId})
	return runtimeapi.AddGoalCandidate200JSONResponse(out), err
}
func (s *proxyServer) FailGoal(ctx context.Context, req runtimeapi.FailGoalRequestObject) (runtimeapi.FailGoalResponseObject, error) {
	if req.Body == nil {
		return nil, badRequest("body is required")
	}
	request := *req.Body
	request.GoalKey = jobdb.GoalKey{TenantId: req.TenantId, GoalID: req.GoalId}
	if err := request.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	out, err := s.runtime.FailGoal(ctx, request)
	return runtimeapi.FailGoal200JSONResponse(out), err
}
func (s *proxyServer) GetDependencyWait(ctx context.Context, req runtimeapi.GetDependencyWaitRequestObject) (runtimeapi.GetDependencyWaitResponseObject, error) {
	out, err := s.runtime.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}, WaitID: req.WaitId})
	return runtimeapi.GetDependencyWait200JSONResponse(out), err
}

var goalErrors = []struct {
	err    error
	code   string
	status int
}{
	{jobdb.ErrJobNotFound, "job_not_found", 404},
	{jobdb.ErrGoalNotFound, "goal_not_found", 404},
	{jobdb.ErrGoalConflict, "goal_conflict", 409},
	{jobdb.ErrInvalidDependency, "invalid_dependency", 400},
	{jobdb.ErrDependencyWaitNotFound, "dependency_wait_not_found", 404},
	{jobdb.ErrDependencyWaitConflict, "dependency_wait_conflict", 409},
	{jobdb.ErrDependencyCycle, "dependency_cycle", 409},
}

func writeGoalError(w http.ResponseWriter, err error) bool {
	for _, entry := range goalErrors {
		if errors.Is(err, entry.err) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(entry.status)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": entry.code, "message": err.Error()})
			return true
		}
	}
	return false
}
func goalResponseError(body []byte) error {
	var payload struct {
		Code    string
		Message string
	}
	if json.Unmarshal(body, &payload) != nil {
		return nil
	}
	for _, entry := range goalErrors {
		if entry.code == payload.Code {
			return fmt.Errorf("%w: %s", entry.err, payload.Message)
		}
	}
	return nil
}

func (r *Runtime) ListGoals(ctx context.Context, req jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	if err := req.Validate(); err != nil {
		return jobdb.ListGoalsResponse{}, err
	}
	resp, err := r.client.ListGoalsWithResponse(ctx, req.TenantId, req)
	if err != nil {
		return jobdb.ListGoalsResponse{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return jobdb.ListGoalsResponse{}, responseError("list goals", resp.StatusCode(), resp.Body, nil)
	}
	return *resp.JSON200, nil
}
func (r *Runtime) ListDependencyWaits(ctx context.Context, req jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	if err := req.Validate(); err != nil {
		return jobdb.ListDependencyWaitsResponse{}, err
	}
	resp, err := r.client.ListDependencyWaitsWithResponse(ctx, req.TenantId, req)
	if err != nil {
		return jobdb.ListDependencyWaitsResponse{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return jobdb.ListDependencyWaitsResponse{}, responseError("list dependency waits", resp.StatusCode(), resp.Body, nil)
	}
	return *resp.JSON200, nil
}
func (s *proxyServer) ListGoals(ctx context.Context, req runtimeapi.ListGoalsRequestObject) (runtimeapi.ListGoalsResponseObject, error) {
	if req.Body == nil {
		return nil, badRequest("body is required")
	}
	request := *req.Body
	request.TenantId = req.TenantId
	if err := request.Validate(); err != nil {
		return nil, err
	}
	out, err := s.runtime.ListGoals(ctx, request)
	return runtimeapi.ListGoals200JSONResponse(out), err
}
func (s *proxyServer) ListDependencyWaits(ctx context.Context, req runtimeapi.ListDependencyWaitsRequestObject) (runtimeapi.ListDependencyWaitsResponseObject, error) {
	if req.Body == nil {
		return nil, badRequest("body is required")
	}
	request := *req.Body
	request.TenantId = req.TenantId
	if err := request.Validate(); err != nil {
		return nil, err
	}
	out, err := s.runtime.ListDependencyWaits(ctx, request)
	return runtimeapi.ListDependencyWaits200JSONResponse(out), err
}
