package remote

import (
	"context"
	"encoding/json"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

// CreateDelegation locally derives an ordinary lease with narrower authority.
// It makes no network requests; JobDB validates the parent at import/use.
// ExportLease and ImportLease transport it using the existing capability format.
// KeepAlive/Renew refresh only this child, subject to its signed restrictions.
func (r *Runtime) CreateDelegation(ctx context.Context, parent jobdb.ExecutionLease, req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	capability, err := ExportLease(parent)
	if err != nil {
		return nil, err
	}
	w := capability.wire
	grant := w.Token
	if !isDelegationToken(grant) {
		owner, err := decodeOwner(grant)
		if err != nil {
			return nil, err
		}
		m, root, _, err := decodeGrant(owner.Grant, true)
		if err != nil || len(m.Caveats()) != 0 || root.Audience != owner.Audience || !sameGrantParent(root.Parent, parentOf(owner)) {
			return nil, jobdb.ErrExecutionLeaseLost
		}
		grant = owner.Grant
	}
	token, c, err := attenuateGrant(grant, req)
	if err != nil {
		return nil, err
	}
	if c.Parent.TenantID != w.TenantID || c.Parent.JobID != w.JobID || c.Parent.LeaseID != w.LeaseID {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	c.Route = parent.Route()
	c.setExpiry(leaseExpiry(parent))
	snapshot := delegationSnapshot(c, token)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lease, err := r.executionLeaseFromAPI(snapshot)
	if err != nil {
		return nil, err
	}
	return lease.(*remoteExecutionLease), nil
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
	if status == 401 && response.Code == "invalid_delegation_credential" || status == 409 && response.Code == "execution_lease_lost" {
		return jobdb.ErrExecutionLeaseLost
	}
	if status == 403 && (response.Code == "lease_permission_denied" || response.Code == "delegation_deadline_reached") {
		return jobdb.ErrLeasePermissionDenied
	}
	return nil
}
