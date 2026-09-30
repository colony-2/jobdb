package runtimecore

import (
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ClaimTaskRequest asks the scheduler to atomically compare an eligible waiting
// slot and install a finite lease with the job-only resume route. A stale slot,
// cancellation, or live lease must fail with jobdb.ErrConflict. The claim commits
// independently of chapter insertion: after expiry, a job handler reconstructs
// its next action from chapter history. Client payload is preserved unchanged.
type ClaimTaskRequest struct {
	JobKey        jobdb.JobKey
	WorkerID      string
	Task          WaitingTaskSnapshot
	LeaseDuration time.Duration
	Now           time.Time
}
