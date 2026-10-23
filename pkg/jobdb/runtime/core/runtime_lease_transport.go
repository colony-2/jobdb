package runtimecore

import (
	"context"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ValidateExecutionLeaseByID checks a live parent without extending it.
func (r *Runtime) ValidateExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string) (jobdb.LeaseValidity, error) {
	identity, err := leaseIdentityForTransport(key, leaseID, workerID)
	if err != nil {
		return jobdb.LeaseValidity{}, err
	}
	if err := r.validate(); err != nil {
		return jobdb.LeaseValidity{}, err
	}
	row, err := r.scheduler.GetJob(ctx, key)
	if err != nil {
		return jobdb.LeaseValidity{}, err
	}
	if row.CancelRequested || row.Store == jobdb.JobStoreArchived || row.Lease == nil {
		return jobdb.LeaseValidity{}, jobdb.ErrExecutionLeaseLost
	}
	if row.Lease.JobKey != identity.JobKey || row.Lease.LeaseID != identity.LeaseID || row.Lease.WorkerID != identity.WorkerID {
		return jobdb.LeaseValidity{}, jobdb.ErrExecutionLeaseLost
	}
	snapshot, err := r.scheduler.ValidateLease(ctx, *row.Lease)
	if err != nil {
		return jobdb.LeaseValidity{}, err
	}
	if !snapshot.Identity.ExpiresAt.After(r.now()) {
		return jobdb.LeaseValidity{}, jobdb.ErrExecutionLeaseLost
	}
	lease, err := r.wrapLease(snapshot)
	if err != nil {
		return jobdb.LeaseValidity{}, err
	}
	return jobdb.LeaseValidity{ExpiresAt: snapshot.Identity.ExpiresAt, Route: lease.Route()}, nil
}

// KeepAliveLeaseByID renews the current lease for remote transport callers.
func (r *Runtime) KeepAliveLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, duration time.Duration) error {
	_, err := r.KeepAliveLeaseByIDWithExpiry(ctx, key, leaseID, workerID, duration)
	return err
}

// KeepAliveLeaseByIDWithExpiry returns the renewed expiration for token minting.
func (r *Runtime) KeepAliveLeaseByIDWithExpiry(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, duration time.Duration) (time.Time, error) {
	identity, err := leaseIdentityForTransport(key, leaseID, workerID)
	if err != nil {
		return time.Time{}, err
	}
	if err := r.validate(); err != nil {
		return time.Time{}, err
	}
	updated, err := r.scheduler.KeepAliveLease(ctx, LeaseMutation{
		Identity: identity, Duration: duration, Now: r.now(),
	})
	if err != nil {
		return time.Time{}, err
	}
	return updated.Identity.ExpiresAt, nil
}

// CompleteJobWithLeaseByID validates a remote lease and writes its outcome.
func (r *Runtime) CompleteJobWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.CompleteExecutionRequest) error {
	lease, err := r.leaseForTransport(ctx, key, leaseID, workerID)
	if err != nil {
		return err
	}
	return lease.Complete(ctx, req)
}

// RescheduleJobWithLeaseByID validates a remote lease and changes its route.
func (r *Runtime) RescheduleJobWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.RescheduleExecutionRequest) error {
	lease, err := r.leaseForTransport(ctx, key, leaseID, workerID)
	if err != nil {
		return err
	}
	return lease.Reschedule(ctx, req)
}

// SubmitJobWithLeaseByID starts a child through a live parent lease.
func (r *Runtime) SubmitJobWithLeaseByID(ctx context.Context, parent jobdb.JobKey, leaseID, workerID string, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	lease, err := r.leaseForTransport(ctx, parent, leaseID, workerID)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	return lease.SubmitJob(ctx, req)
}

// SubmitRestartJobWithLeaseByID starts a replay child through a live lease.
func (r *Runtime) SubmitRestartJobWithLeaseByID(ctx context.Context, parent jobdb.JobKey, leaseID, workerID string, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	lease, err := r.leaseForTransport(ctx, parent, leaseID, workerID)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	return lease.SubmitRestartJob(ctx, req)
}

func (r *Runtime) leaseForTransport(ctx context.Context, key jobdb.JobKey, leaseID, workerID string) (*executionLease, error) {
	identity, err := leaseIdentityForTransport(key, leaseID, workerID)
	if err != nil {
		return nil, err
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	snapshot, err := r.scheduler.ValidateLease(ctx, identity)
	if err != nil {
		return nil, err
	}
	return r.wrapLease(snapshot)
}

func leaseIdentityForTransport(key jobdb.JobKey, leaseID, workerID string) (LeaseIdentity, error) {
	if err := key.Validate(); err != nil {
		return LeaseIdentity{}, err
	}
	if leaseID == "" || workerID == "" {
		return LeaseIdentity{}, fmt.Errorf("lease id and worker id are required")
	}
	return LeaseIdentity{JobKey: key, LeaseID: leaseID, WorkerID: workerID}, nil
}

// RenewExecutionLeaseByID validates and renews an exact lease, returning its
// authoritative snapshot. This is for trusted transports, not acquisition.
func (r *Runtime) RenewExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, duration time.Duration) (jobdb.RenewableExecutionLease, error) {
	identity, err := leaseIdentityForTransport(key, leaseID, workerID)
	if err != nil {
		return nil, err
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	snapshot, err := r.scheduler.KeepAliveLease(ctx, LeaseMutation{Identity: identity, Duration: duration, Now: r.now()})
	if err != nil {
		return nil, err
	}
	return r.wrapLease(snapshot)
}

func (l *executionLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	if err := l.checkExpiry(); err != nil {
		return nil, err
	}
	return l.runtime.RenewExecutionLeaseByID(ctx, l.Job().JobKey, l.LeaseID(), l.LeaseWorkerID(), l.snapshot.Duration)
}
