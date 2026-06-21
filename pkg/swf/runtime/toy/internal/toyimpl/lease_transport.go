package toyimpl

import (
	"context"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func (r *Runtime) KeepAliveLeaseByID(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string, leaseDuration time.Duration) error {
	_, err := r.KeepAliveLeaseByIDWithExpiry(ctx, jobKey, leaseID, workerID, leaseDuration)
	return err
}

func (r *Runtime) KeepAliveLeaseByIDWithExpiry(_ context.Context, jobKey swf.JobKey, leaseID string, _ string, leaseDuration time.Duration) (time.Time, error) {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return time.Time{}, swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.leaseID != leaseID {
		return time.Time{}, swf.ErrExecutionLeaseLost
	}
	return time.Now().UTC().Add(toyLeaseDurationOrDefault(leaseDuration)), nil
}

func (r *Runtime) CompleteJobWithLeaseByID(_ context.Context, jobKey swf.JobKey, leaseID string, _ string, req swf.CompleteExecutionRequest) error {
	return r.completeLease(jobKey, leaseID, req)
}

func (r *Runtime) RescheduleJobWithLeaseByID(_ context.Context, jobKey swf.JobKey, leaseID string, _ string, req swf.RescheduleExecutionRequest) error {
	return r.rescheduleLease(jobKey, leaseID, req)
}
