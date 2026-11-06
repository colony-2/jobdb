package toyimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (r *Runtime) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	record := r.engine.jobRecords[req.JobKey]
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	now := time.Now().UTC()
	if record.archived != nil || record.cancelled || record.hidden || record.leased && record.leaseExpiresAt.After(now) {
		return fmt.Errorf("%w: cannot clear crash concern for a live, final, cancelled, or hidden job", jobdb.ErrConflict)
	}
	record.consecutiveExpirations = 0
	// Acquisition caches CRASH_CONCERN after observing an expired lease.
	// Restore the scheduling state without discarding any wait constraints.
	if record.status == jobdb.JobStatusCrashConcern {
		record.status = jobdb.JobStatusReady
		if record.availableAt.After(now) {
			record.status = jobdb.JobStatusAwaitingFuture
		}
		if len(record.waitFor) > 0 {
			record.status = jobdb.JobStatusPendingJobs
		}
	}
	return nil
}
