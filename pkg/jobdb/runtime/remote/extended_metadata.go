package remote

import (
	"context"
	"net/http"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadataedit"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func (runtime *Runtime) EditJobMetadata(ctx context.Context, key jobdb.JobKey, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	if err := edit.Validate(); err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	response, err := runtime.client.EditJobMetadataWithResponse(ctx, key.TenantId, key.JobId, edit)
	if err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	return jobMetadataResponse(response.StatusCode(), response.Body, response.JSON200)
}

func (lease *remoteExecutionLease) EditJobMetadata(ctx context.Context, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	if err := edit.Validate(); err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	response, err := lease.runtime.client.EditJobMetadataWithLeaseWithResponse(ctx, lease.jobKey.TenantId, lease.jobKey.JobId, lease.leaseID, &runtimeapi.EditJobMetadataWithLeaseParams{XJobDBLeaseToken: lease.LeaseToken()}, edit)
	if err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	return jobMetadataResponse(response.StatusCode(), response.Body, response.JSON200)
}

func jobMetadataResponse(status int, body []byte, entry *jobdb.JobMetadataEntry) (jobdb.JobMetadataEntry, error) {
	if status == http.StatusOK && entry != nil {
		return *entry, nil
	}
	switch status {
	case http.StatusBadRequest:
		return jobdb.JobMetadataEntry{}, jobdb.ErrInvalidJobMetadata
	case http.StatusNotImplemented:
		return jobdb.JobMetadataEntry{}, jobdb.ErrJobMetadataUnsupported
	case http.StatusConflict:
		if strings.TrimSpace(string(body)) == jobdb.ErrExecutionLeaseLost.Error() {
			return jobdb.JobMetadataEntry{}, jobdb.ErrExecutionLeaseLost
		}
	}
	return jobdb.JobMetadataEntry{}, responseError("edit job metadata", status, body, jobdb.ErrJobNotFound)
}

func (s *proxyServer) EditJobMetadata(ctx context.Context, req runtimeapi.EditJobMetadataRequestObject) (runtimeapi.EditJobMetadataResponseObject, error) {
	if req.Body == nil {
		return nil, jobdb.ErrInvalidJobMetadata
	}
	writer, ok := s.runtime.(jobdb.JobMetadataRuntime)
	if !ok {
		return nil, jobdb.ErrJobMetadataUnsupported
	}
	entry, err := writer.EditJobMetadata(ctx, jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}, *req.Body)
	return runtimeapi.EditJobMetadata200JSONResponse(entry), err
}

func (s *proxyServer) EditJobMetadataWithLease(ctx context.Context, req runtimeapi.EditJobMetadataWithLeaseRequestObject) (runtimeapi.EditJobMetadataWithLeaseResponseObject, error) {
	if req.Body == nil {
		return nil, jobdb.ErrInvalidJobMetadata
	}
	key := jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}
	if isDelegationToken(req.Params.XJobDBLeaseToken) {
		return nil, delegationDenied()
	}
	claims, err := s.validatedLeaseClaims(ctx, req.Params.XJobDBLeaseToken, key, req.LeaseId)
	if err != nil {
		return nil, err
	}
	writer, ok := s.runtime.(jobmetadataedit.LeaseWriter)
	if !ok {
		return nil, jobdb.ErrJobMetadataUnsupported
	}
	entry, err := writer.EditJobMetadataWithVerifiedLease(leaseauth.WithClaims(ctx, claims.leaseAuthClaims()), key, *req.Body)
	return runtimeapi.EditJobMetadataWithLease200JSONResponse(entry), err
}

func metadataEditPath(request *http.Request) (edit, leaseBound bool) {
	if request.Method != http.MethodPatch {
		return false, false
	}
	parts := strings.Split(request.URL.EscapedPath(), "/")
	if (len(parts) != 7 && len(parts) != 9) || parts[1] != "v1" || (parts[2] != "tenants" && parts[2] != "projects") || parts[4] != "jobs" || parts[len(parts)-1] != "extended-metadata" {
		return false, false
	}
	if len(parts) == 7 {
		return true, false
	}
	return parts[6] == "leases", true
}
