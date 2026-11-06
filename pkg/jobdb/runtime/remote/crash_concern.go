package remote

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func (r *Runtime) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	resp, err := r.client.ClearCrashConcernWithResponse(ctx, req.JobKey.TenantId, req.JobKey.JobId, runtimeapi.ClearCrashConcernRequest{Reason: stringPtrOrNil(req.Reason), WorkerId: stringPtrOrNil(req.WorkerID)})
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode() == http.StatusBadRequest {
		return fmt.Errorf("%w: %s", jobdb.ErrInvalidCrashConcernRequest, resp.Body)
	}
	return responseErrorWithConflict("clear crash concern", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
}

func (s *proxyServer) ClearCrashConcern(ctx context.Context, request runtimeapi.ClearCrashConcernRequestObject) (runtimeapi.ClearCrashConcernResponseObject, error) {
	raw, _ := ctx.Value(requestBodyKey{}).([]byte)
	if request.Body == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, badRequest("request body is required")
	}
	req := jobdb.ClearCrashConcernRequest{JobKey: jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}, WorkerID: stringValue(request.Body.WorkerId), Reason: stringValue(request.Body.Reason)}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if err := s.runtime.ClearCrashConcern(ctx, req); err != nil {
		return nil, err
	}
	return runtimeapi.ClearCrashConcern204Response{}, nil
}
