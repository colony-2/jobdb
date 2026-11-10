package toyimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

func (r *Runtime) KeepAliveLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, leaseDuration time.Duration) error {
	_, err := r.KeepAliveLeaseByIDWithExpiry(ctx, jobKey, leaseID, workerID, leaseDuration)
	return err
}

func (r *Runtime) KeepAliveLeaseByIDWithExpiry(_ context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, leaseDuration time.Duration) (time.Time, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	record := r.engine.jobRecords[jobKey]
	if record == nil {
		return time.Time{}, jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if leaseID == "" || record.leaseID != leaseID || record.leaseWorkerID != workerID || !record.leased || !record.leaseExpiresAt.After(time.Now().UTC()) || record.cancelled || record.archived != nil {
		return time.Time{}, jobdb.ErrExecutionLeaseLost
	}
	now := time.Now().UTC()
	previous := record.leaseExpiresAt
	record.leaseExpiresAt = now.Add(toyLeaseDurationOrDefault(leaseDuration))
	r.engine.appendLeaseEventLocked(record, now, "", "", &previous)
	return record.leaseExpiresAt, nil
}

func (r *Runtime) CompleteJobWithLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.CompleteExecutionRequest) error {
	return r.completeLease(ctx, jobKey, leaseID, workerID, req)
}

func (r *Runtime) RescheduleJobWithLeaseByID(_ context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.RescheduleExecutionRequest) error {
	return r.rescheduleLease(jobKey, leaseID, workerID, req)
}

// ValidateExecutionLeaseByID checks a live parent without extending it.
func (r *Runtime) ValidateExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string) (jobdb.LeaseValidity, error) {
	if err := ctx.Err(); err != nil {
		return jobdb.LeaseValidity{}, err
	}
	record := r.engine.getJobRecord(key)
	if record == nil {
		return jobdb.LeaseValidity{}, jobdb.ErrExecutionLeaseLost
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if leaseID == "" || workerID == "" || record.leaseID != leaseID || record.leaseWorkerID != workerID || !record.leased || !record.leaseExpiresAt.After(time.Now().UTC()) || record.cancelled || record.archived != nil {
		return jobdb.LeaseValidity{}, jobdb.ErrExecutionLeaseLost
	}
	return jobdb.LeaseValidity{Cell: record.cell, GoalID: record.goalID, ExpiresAt: record.leaseExpiresAt, Route: record.route}, nil
}

func (r *Runtime) SubmitJobWithLeaseByID(ctx context.Context, parentJobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if req.Job.GoalID != "" {
		return jobdb.JobHandle{}, jobdb.ErrPrimaryGoalSelectionUnsupported
	}
	if _, err := r.ValidateExecutionLeaseByID(ctx, parentJobKey, leaseID, workerID); err != nil {
		return jobdb.JobHandle{}, err
	}
	req.Job.TenantId = parentJobKey.TenantId
	return r.submitJobWithParent(cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: parentJobKey, LeaseID: leaseID, WorkerID: workerID}), req, parentJobKey.JobId)
}

func (r *Runtime) SubmitRestartJobWithLeaseByID(ctx context.Context, parentJobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	if _, err := r.ValidateExecutionLeaseByID(ctx, parentJobKey, leaseID, workerID); err != nil {
		return jobdb.JobHandle{}, err
	}
	if req.Job.PriorJobKey.TenantId != "" && req.Job.PriorJobKey.TenantId != parentJobKey.TenantId {
		return jobdb.JobHandle{}, fmt.Errorf("prior job tenantId must match parent tenantId")
	}
	req.Job.PriorJobKey.TenantId = parentJobKey.TenantId
	return r.submitRestartJobWithParent(cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: parentJobKey, LeaseID: leaseID, WorkerID: workerID}), req, parentJobKey.JobId)
}

func (l *runtimeLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	if err := l.checkExpiry(); err != nil {
		return nil, err
	}
	return l.runtime.RenewExecutionLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, l.duration)
}

// RenewExecutionLeaseByID validates and renews the exact lease under the record lock.
func (r *Runtime) RenewExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, duration time.Duration) (jobdb.RenewableExecutionLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	record := r.engine.jobRecords[key]
	if record == nil {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if leaseID == "" || workerID == "" || record.leaseID != leaseID || record.leaseWorkerID != workerID || !record.leased || !record.leaseExpiresAt.After(time.Now().UTC()) || record.cancelled || record.archived != nil {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	duration = toyLeaseDurationOrDefault(duration)
	now := time.Now().UTC()
	previous := record.leaseExpiresAt
	record.leaseExpiresAt = now.Add(duration)
	r.engine.appendLeaseEventLocked(record, now, "", "", &previous)
	return &runtimeLease{runtime: r, jobKey: key, leaseID: leaseID, workerID: workerID,
		route: record.route, payload: cloneJSON(record.payload), clientPayload: cloneJSON(record.clientPayload), clientPayloadRevision: record.clientPayloadRevision,
		duration: duration, expiresAt: record.leaseExpiresAt, schemaHash: jobmetadata.SchemaHashFromStoredMetadata(record.metadata)}, nil
}
