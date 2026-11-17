package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func appendToAPI(ctx context.Context, q jobdb.StartOrAppendJobRequest) (runtimeapi.StartOrAppendJobRequest, error) {
	if !json.Valid(q.Job.Append) || q.Job.Job.JobID != "" || q.Job.Job.GoalID != "" || len(q.Job.Job.Dependencies) > 0 || len(q.Job.Job.Prerequisites) > 0 {
		return runtimeapi.StartOrAppendJobRequest{}, jobdb.ErrInvalidAppendRequest
	}
	base, err := submitJobRequestToAPI(ctx, jobdb.SubmitJobRequest{Job: q.Job.Job, WorkerID: q.WorkerID, RequestTime: q.RequestTime})
	if err != nil {
		return runtimeapi.StartOrAppendJobRequest{}, err
	}
	b := base.Job
	template := runtimeapi.AppendJobTemplate{JobType: b.JobType, Cell: b.Cell, CellRepo: b.CellRepo, Data: b.Data, RunPolicy: b.RunPolicy, Metadata: b.Metadata, Schema: b.Schema, AvailableAt: b.AvailableAt, ClientPayloadUpdate: b.ClientPayloadUpdate}
	return runtimeapi.StartOrAppendJobRequest{AppendKey: q.Job.AppendKey, MaxInputs: &q.Job.MaxInputs, Job: template, Append: q.Job.Append, AppendArtifacts: &q.Job.AppendArtifacts, WorkerId: base.WorkerId, RequestTime: base.RequestTime}, nil
}
func appendFromAPI(body *runtimeapi.StartOrAppendJobRequest, tenant string) (jobdb.StartOrAppendJobRequest, error) {
	if body == nil {
		return jobdb.StartOrAppendJobRequest{}, jobdb.ErrInvalidAppendRequest
	}
	b := body.Job
	if b.JobId != nil || b.GoalId != nil || b.Dependencies != nil || b.Prerequisites != nil {
		return jobdb.StartOrAppendJobRequest{}, jobdb.ErrInvalidAppendRequest
	}
	base, err := submitJobRequestFromAPI(runtimeapi.SubmitJobRequest{Job: runtimeapi.SubmitJob{JobType: b.JobType, Cell: b.Cell, CellRepo: b.CellRepo, Data: b.Data, RunPolicy: b.RunPolicy, Metadata: b.Metadata, Schema: b.Schema, AvailableAt: b.AvailableAt, ClientPayloadUpdate: b.ClientPayloadUpdate}, WorkerId: body.WorkerId, RequestTime: body.RequestTime}, tenant, "")
	if err != nil {
		return jobdb.StartOrAppendJobRequest{}, fmt.Errorf("%w: %v", jobdb.ErrInvalidAppendRequest, err)
	}
	q := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: body.AppendKey, Append: body.Append, Job: base.Job}, WorkerID: base.WorkerID, RequestTime: base.RequestTime}
	if body.MaxInputs != nil {
		q.Job.MaxInputs = *body.MaxInputs
	}
	if body.AppendArtifacts != nil {
		q.Job.AppendArtifacts = *body.AppendArtifacts
	}
	return q, nil
}
func (r *Runtime) StartOrAppendJob(ctx context.Context, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	body, err := appendToAPI(ctx, q)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	resp, err := r.client.StartOrAppendJobWithResponse(ctx, q.Job.Job.TenantId, body)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if resp.JSON200 == nil {
		return jobdb.JobHandle{}, responseError("start or append job", resp.StatusCode(), resp.Body, nil)
	}
	return fromAPIJobHandle(*resp.JSON200)
}
func (l *remoteExecutionLease) StartOrAppendJob(ctx context.Context, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	body, err := appendToAPI(ctx, q)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	resp, err := l.runtime.client.StartOrAppendJobWithLeaseWithResponse(ctx, l.jobKey.TenantId, l.jobKey.JobId, l.leaseID, &runtimeapi.StartOrAppendJobWithLeaseParams{XJobDBLeaseToken: l.LeaseToken()}, body)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if resp.JSON200 == nil {
		return jobdb.JobHandle{}, responseError("start or append job with lease", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
	}
	return fromAPIJobHandle(*resp.JSON200)
}
func (s *proxyServer) StartOrAppendJob(ctx context.Context, q runtimeapi.StartOrAppendJobRequestObject) (runtimeapi.StartOrAppendJobResponseObject, error) {
	req, err := appendFromAPI(q.Body, q.TenantId)
	if err != nil {
		return nil, err
	}
	h, err := s.runtime.StartOrAppendJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.StartOrAppendJob200JSONResponse(toAPIJobHandle(h)), nil
}
func (s *proxyServer) StartOrAppendJobWithLease(ctx context.Context, q runtimeapi.StartOrAppendJobWithLeaseRequestObject) (runtimeapi.StartOrAppendJobWithLeaseResponseObject, error) {
	key := jobdb.JobKey{TenantId: q.TenantId, JobId: q.ParentJobId}
	claims, err := s.validatedSubmissionLeaseClaims(ctx, q.Params.XJobDBLeaseToken, key, q.LeaseId)
	if err != nil {
		return nil, err
	}
	req, err := appendFromAPI(q.Body, q.TenantId)
	if err != nil {
		return nil, err
	}
	ops, ok := s.runtime.(interface {
		StartOrAppendJobWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error)
	})
	if !ok {
		return nil, fmt.Errorf("lease submission unavailable")
	}
	h, err := ops.StartOrAppendJobWithLeaseByID(leaseauth.WithClaims(ctx, claims.leaseAuthClaims()), key, q.LeaseId, claims.WorkerID, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.StartOrAppendJobWithLease200JSONResponse(toAPIJobHandle(h)), nil
}
func writeAppendError(w http.ResponseWriter, err error) bool {
	status, code := 0, ""
	if errors.Is(err, jobdb.ErrInvalidAppendRequest) {
		status, code = 400, "invalid_append_request"
	}
	if errors.Is(err, jobdb.ErrAppendBusy) {
		status, code = 409, "append_busy"
	}
	if status == 0 {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": err.Error()})
	return true
}
func appendResponseError(body []byte) error {
	var e struct{ Code, Message string }
	if json.Unmarshal(body, &e) != nil {
		return nil
	}
	switch e.Code {
	case "invalid_append_request":
		return fmt.Errorf("%w: %s", jobdb.ErrInvalidAppendRequest, e.Message)
	case "append_busy":
		return fmt.Errorf("%w: %s", jobdb.ErrAppendBusy, e.Message)
	}
	return nil
}
