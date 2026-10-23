package remote

import (
	"context"
	"encoding/json"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

// CreateDelegation returns an ordinary remote lease with restricted authority.
// ExportLease and ImportLease transport it using the existing capability format.
// KeepAlive/Renew refresh only this child, subject to its signed restrictions.
func (r *Runtime) CreateDelegation(ctx context.Context, parent jobdb.ExecutionLease, req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
	capability, err := ExportLease(parent)
	if err != nil {
		return nil, err
	}
	w := capability.wire
	resp, err := r.client.CreateLeaseDelegationWithResponse(ctx, w.TenantID, w.JobID, w.LeaseID, &runtimeapi.CreateLeaseDelegationParams{XJobDBLeaseToken: w.Token}, req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, responseError("create delegation", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
	}
	if resp.JSON200.Delegation == nil || !isDelegationToken(resp.JSON200.LeaseToken) {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	parentHandle := &remoteExecutionLease{runtime: r, jobKey: parent.Job().JobKey, leaseID: parent.LeaseID()}
	return parentHandle.renewedSnapshot(&runtimeapi.KeepAliveLeaseResponse{Lease: resp.JSON200, LeaseToken: resp.JSON200.LeaseToken}, "renew")
}

// Delegation returns a copy of the signed restrictions, or nil for an owner lease.
// Callers may discover this optional method without changing ExecutionLease.
func (l *remoteExecutionLease) Delegation() *jobdb.DelegationInfo {
	if l.delegation == nil {
		return nil
	}
	raw, _ := json.Marshal(l.delegation)
	var out jobdb.DelegationInfo
	_ = json.Unmarshal(raw, &out)
	return &out
}

func (r *Runtime) getDelegatedChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	resp, err := r.client.GetChapterWithLeaseWithResponse(ctx, ref.JobKey.TenantId, ref.JobKey.JobId, ref.LeaseID, ref.Ordinal, &runtimeapi.GetChapterWithLeaseParams{XJobDBLeaseToken: ref.LeaseToken})
	if err != nil {
		return jobdb.Chapter{}, err
	}
	if resp.StatusCode() == 404 {
		return jobdb.Chapter{}, jobdb.ErrChapterNotFound
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return jobdb.Chapter{}, responseError("read leased chapter", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
	}
	return fromAPIStoredChapter(*resp.JSON200)
}
func (r *Runtime) listDelegatedChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	resp, err := r.client.ListChaptersWithLeaseWithResponse(ctx, req.JobKey.TenantId, req.JobKey.JobId, req.LeaseID, &runtimeapi.ListChaptersWithLeaseParams{XJobDBLeaseToken: req.LeaseToken, StartOrdinal: req.StartOrdinal, EndOrdinal: req.EndOrdinal})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, responseError("list leased chapters", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
	}
	out := make([]jobdb.Chapter, 0, len(resp.JSON200.Chapters))
	for _, record := range resp.JSON200.Chapters {
		c, err := fromAPIStoredChapter(record)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// Decode only stable delegation error codes; never infer authority from text.
func delegationResponseError(status int, body []byte) error {
	var response struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &response) != nil {
		return nil
	}
	if status == 401 && (response.Code == "invalid_delegation_credential" || response.Code == "delegation_access_token_expired") || status == 409 && response.Code == "execution_lease_lost" {
		return jobdb.ErrExecutionLeaseLost
	}
	if status == 403 && (response.Code == "lease_permission_denied" || response.Code == "delegation_deadline_reached") {
		return jobdb.ErrLeasePermissionDenied
	}
	return nil
}
