package jobdb

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidCrashConcernRequest = errors.New("invalid crash concern request")

// ClearCrashConcernRequest resets a job's consecutive expired-lease count.
// This operator action preserves history, routing, dependencies, deadlines, and
// lifetime expiration statistics. It does not acquire a lease or restart history.
// Missing jobs return ErrJobNotFound. Live leases, cancelled, completed, and
// hidden jobs return ErrConflict. Clearing an already-clear inactive job is a no-op.
type ClearCrashConcernRequest struct {
	JobKey   JobKey
	WorkerID string
	Reason   string
}

func (r ClearCrashConcernRequest) Validate() error {
	if strings.TrimSpace(r.JobKey.TenantId) == "" || strings.TrimSpace(r.JobKey.JobId) == "" {
		return fmt.Errorf("%w: tenant and job IDs are required", ErrInvalidCrashConcernRequest)
	}
	return nil
}
