package runtimecore

import (
	"context"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// TaskCompletionStore coordinates external task completion across the scheduler
// and chapter log. A claim commits before publishing output; on process failure,
// lease expiry lets a job worker recover by replaying chapter history.
type TaskCompletionStore interface {
	// ClaimTask atomically compares the complete waiting slot, rejects cancellation
	// and live leases, requires scheduling eligibility, and installs a lease with
	// the job-only resume route. It must
	// make the job eligible for job workers after expiry, without another write.
	// A stale slot or competing claim returns jobdb.ErrConflict.
	ClaimTask(context.Context, ClaimTaskRequest) (LeaseIdentity, error)
	// PublishTaskOutput atomically validates and fences the live claim, applies
	// ClientPayloadUpdate, and publishes the chapter. Artifacts must be durable
	// before publication. No output or payload mutation may survive a failed write.
	// The lease remains held for the normal RescheduleLease call.
	PublishTaskOutput(context.Context, TaskOutputRequest) error
}

type ClaimTaskRequest struct {
	JobKey        jobdb.JobKey
	WorkerID      string
	Task          WaitingTaskSnapshot
	LeaseDuration time.Duration
	Now           time.Time
}

type TaskOutputRequest struct {
	Identity            LeaseIdentity
	Chapter             EncodedChapter
	ClientPayloadUpdate *jobdb.ClientPayloadUpdate
}
