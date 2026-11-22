package runtimecore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ResourceBackoffScheduler is additive. The installed backend/schema, rather
// than interface presence, determines the capability returned at mutation time.
// Implementations authenticate Identity and atomically persist the full request,
// sequence, payload CAS, selected continuation, budget, gate and receipt.
type ResourceBackoffScheduler interface {
	ResourceBackoffCapability(context.Context) (jobdb.ResourceBackoffCapability, error)
	RescheduleResourceBackoff(context.Context, ResourceBackoffMutation) (jobdb.ResourceBackoffReceipt, error)
	ReplayResourceBackoffReceipt(context.Context, ResourceBackoffMutation) (jobdb.ResourceBackoffReceipt, error)
}
type ResourceBackoffMutation struct {
	Identity LeaseIdentity
	Request  jobdb.ResourceBackoffRequest
}

func (l *executionLease) RescheduleResourceBackoff(ctx context.Context, req jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	receipt, err := l.runtime.RescheduleResourceBackoffByID(ctx, l.snapshot.Identity.JobKey, l.LeaseID(), l.snapshot.Identity.WorkerID, req)
	if err == nil {
		l.StopKeepAlive()
	}
	return receipt, err
}
func (r *Runtime) RescheduleResourceBackoffByID(ctx context.Context, key jobdb.JobKey, lease, worker string, req jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	return r.resourceBackoff(ctx, key, lease, worker, req, false)
}
func (r *Runtime) ReplayResourceBackoffReceiptByID(ctx context.Context, key jobdb.JobKey, lease, worker string, req jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	return r.resourceBackoff(ctx, key, lease, worker, req, true)
}
func (r *Runtime) resourceBackoff(ctx context.Context, key jobdb.JobKey, lease, worker string, req jobdb.ResourceBackoffRequest, replay bool) (jobdb.ResourceBackoffReceipt, error) {
	var zero jobdb.ResourceBackoffReceipt
	var cloneErr error
	req, cloneErr = jobdb.CloneResourceBackoffRequest(req)
	if cloneErr != nil {
		return zero, cloneErr
	}
	if err := r.validate(); err != nil {
		return zero, err
	}
	backend, ok := r.scheduler.(ResourceBackoffScheduler)
	if !ok {
		return zero, &jobdb.ResourceBackoffUnsupportedError{Feature: "scheduler port"}
	}
	capability, err := backend.ResourceBackoffCapability(ctx)
	if err != nil {
		return zero, err
	}
	if err = capability.Validate(false); err != nil {
		return zero, err
	}
	identity, err := leaseIdentityForTransport(key, lease, worker)
	if err != nil {
		return zero, err
	}
	mutation := ResourceBackoffMutation{Identity: identity, Request: req}
	if replay {
		return backend.ReplayResourceBackoffReceipt(ctx, mutation)
	}
	if err = jobdb.RequireProjectWrite(ctx, r.projects, key.TenantId); err != nil {
		return zero, err
	}
	// A backend must validate the immutable checkpoint under its transaction or
	// against its append-only chapter contract. The optional backend port owns that validation.
	// Receipt lookup must precede that validation inside the backend so retained
	// exact duplicates remain valid after finality or configuration changes.
	return backend.RescheduleResourceBackoff(ctx, mutation)
}
