package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

// LeaseCapability is a bearer credential. Only Encode deliberately exposes it.
// The connection target is supplied separately to New. Do not log encoded data.
type LeaseCapability struct{ wire leaseCapabilityWire }
type leaseCapabilityWire struct {
	Version  int    `json:"version"`
	TenantID string `json:"tenantId"`
	JobID    string `json:"jobId"`
	LeaseID  string `json:"leaseId"`
	Token    string `json:"leaseToken"`
}

func (LeaseCapability) String() string     { return "JobDB lease capability (redacted)" }
func (c LeaseCapability) GoString() string { return c.String() }

// Encode returns versioned JSON containing credentials. Store or transmit it only
// through a protected channel. Ordinary JSON marshaling does not export credentials.
func (c LeaseCapability) Encode() ([]byte, error) {
	if !c.valid() {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	return json.Marshal(c.wire)
}
func (c LeaseCapability) valid() bool {
	w := c.wire
	return w.Version == 1 && w.TenantID != "" && w.JobID != "" && w.LeaseID != "" && w.Token != ""
}

// DecodeLeaseCapability parses a credential without trusting it. ImportLease
// verifies its binding, expiration and backend authority before returning a lease.
func DecodeLeaseCapability(raw []byte) (LeaseCapability, error) {
	var c LeaseCapability
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c.wire); err != nil {
		return LeaseCapability{}, jobdb.ErrExecutionLeaseLost
	}
	if err := decoder.Decode(new(any)); err != io.EOF || !c.valid() {
		return LeaseCapability{}, jobdb.ErrExecutionLeaseLost
	}
	return c, nil
}

// ExportLease exports the current remote capability, including through wrappers
// that forward LeaseToken. It does not stop the dispatcher's execution/renewal;
// the caller must coordinate an owner handoff before the receiver starts. For
// a delegation, the owner should continue renewing its own parent lease.
func ExportLease(lease jobdb.ExecutionLease) (LeaseCapability, error) {
	token, ok := lease.(interface{ LeaseToken() string })
	if !ok {
		return LeaseCapability{}, jobdb.ErrLeaseRenewalUnsupported
	}
	key := lease.Job().JobKey
	c := LeaseCapability{wire: leaseCapabilityWire{Version: 1, TenantID: key.TenantId, JobID: key.JobId, LeaseID: lease.LeaseID(), Token: token.LeaseToken()}}
	if !c.valid() {
		return LeaseCapability{}, jobdb.ErrExecutionLeaseLost
	}
	return c, nil
}

// ImportLease validates a capability without acquiring work. Owner leases are
// renewed; delegated leases are inspected, with lazy child refresh if expired.
// Importing a delegation never extends its parent and requires no owner token.
func (r *Runtime) ImportLease(ctx context.Context, capability LeaseCapability) (jobdb.RenewableExecutionLease, error) {
	if !capability.valid() {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	w := capability.wire
	lease := &remoteExecutionLease{runtime: r, jobKey: jobdb.JobKey{TenantId: w.TenantID, JobId: w.JobID}, leaseID: w.LeaseID, leaseToken: w.Token}
	if isDelegationToken(w.Token) {
		resp, err := r.client.InspectLeaseWithResponse(ctx, w.TenantID, w.JobID, w.LeaseID, &runtimeapi.InspectLeaseParams{XJobDBLeaseToken: w.Token})
		if err != nil {
			return nil, err
		}
		if resp.StatusCode() != 200 || resp.JSON200 == nil {
			return nil, responseError("import delegated lease", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
		}
		if resp.JSON200.Delegation == nil || !isDelegationToken(resp.JSON200.LeaseToken) {
			return nil, jobdb.ErrExecutionLeaseLost
		}
		return lease.renewedSnapshot(&runtimeapi.KeepAliveLeaseResponse{Lease: resp.JSON200, LeaseToken: resp.JSON200.LeaseToken}, "renew")
	}
	return lease.Renew(ctx)
}

func (l *remoteExecutionLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	response, err := l.requestRenewal(ctx, "renew")
	if err != nil {
		return nil, err
	}
	return l.renewedSnapshot(response, "renew")
}
func (l *remoteExecutionLease) LeaseWorkerID() string { return l.workerID }
func (l *remoteExecutionLease) LeaseExpiry() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, expiry := l.runtime.delegations.cached(l.leaseToken)
	if !expiry.IsZero() {
		return expiry
	}
	return l.expiresAt
}
func (l *remoteExecutionLease) String() string   { return "remote execution lease (capability redacted)" }
func (l *remoteExecutionLease) GoString() string { return l.String() }
func timeValue(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
