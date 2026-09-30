package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
// the caller must coordinate that handoff before the receiver starts.
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

// ImportLease validates and renews a capability without acquiring work. The
// returned snapshot comes from the backend, never from serialized caller metadata.
func (r *Runtime) ImportLease(ctx context.Context, capability LeaseCapability) (jobdb.RenewableExecutionLease, error) {
	if !capability.valid() {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	w := capability.wire
	lease := &remoteExecutionLease{runtime: r, jobKey: jobdb.JobKey{TenantId: w.TenantID, JobId: w.JobID}, leaseID: w.LeaseID, leaseToken: w.Token}
	return lease.Renew(ctx)
}

// LeaseTransportError preserves HTTP/transport failures without reflecting a
// response body or credential into diagnostics. A failed renewal is not proof
// that the server did not extend the lease; callers must stop execution.
type LeaseTransportError struct {
	StatusCode int
	Err        error
}

func (e *LeaseTransportError) Error() string {
	return fmt.Sprintf("lease renewal transport failed (HTTP status %d)", e.StatusCode)
}
func (e *LeaseTransportError) Unwrap() error    { return e.Err }
func (e *LeaseTransportError) GoString() string { return e.Error() }

func (l *remoteExecutionLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Bound even direct import calls made without a caller deadline.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := l.runtime.client.KeepAliveLeaseWithResponse(ctx, l.jobKey.TenantId, l.jobKey.JobId, l.leaseID, &runtimeapi.KeepAliveLeaseParams{XJobDBLeaseToken: l.LeaseToken()})
	if err != nil {
		return nil, &LeaseTransportError{Err: err}
	}
	if resp.StatusCode() != http.StatusOK {
		switch resp.StatusCode() {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict:
			return nil, jobdb.ErrExecutionLeaseLost
		default:
			return nil, &LeaseTransportError{StatusCode: resp.StatusCode()}
		}
	}
	if resp.JSON200 == nil || resp.JSON200.Lease == nil {
		return nil, jobdb.ErrLeaseRenewalUnsupported
	}
	snapshot := resp.JSON200.Lease
	if snapshot.LeaseId != l.leaseID || fromAPIJobKey(snapshot.Job.JobKey) != l.jobKey || snapshot.WorkerId == nil || *snapshot.WorkerId == "" || (l.workerID != "" && *snapshot.WorkerId != l.workerID) || snapshot.ExpiresAt == nil || !snapshot.ExpiresAt.After(time.Now()) || snapshot.LeaseToken == "" {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	lease, err := l.runtime.executionLeaseFromAPI(*snapshot)
	if err != nil {
		return nil, &LeaseTransportError{StatusCode: resp.StatusCode()}
	}
	return lease.(jobdb.RenewableExecutionLease), nil
}
func (l *remoteExecutionLease) LeaseWorkerID() string { return l.workerID }
func (l *remoteExecutionLease) LeaseExpiry() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()
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
