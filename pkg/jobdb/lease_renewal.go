package jobdb

import (
	"context"
	"errors"
	"time"
)

// ErrLeaseRenewalUnsupported means the runtime cannot validate and renew a
// supplied lease. Callers must not fall back to acquiring work.
var ErrLeaseRenewalUnsupported = errors.New("execution lease renewal is unsupported")

// RenewableExecutionLease supports synchronous authoritative renewal. Renew
// preserves the lease and worker identities and returns a fresh execution
// snapshot. It must reject expired, cancelled, or superseded leases. It neither
// acquires work nor starts background heartbeats. Renew must honor context
// cancellation and deadlines. Callers must use the returned
// lease for subsequent operations, including its refreshed remote capability.
// Wrappers must explicitly forward these methods and any LeaseToken method.
type RenewableExecutionLease interface {
	ExecutionLease
	Renew(ctx context.Context) (RenewableExecutionLease, error)
	LeaseWorkerID() string
	// LeaseExpiry is the safe validity deadline, including token expiry for remote leases.
	LeaseExpiry() time.Time
}

// LeaseRenewalError reports an initial or subsequent renewal failure. Unwrap
// preserves authority errors (ErrExecutionLeaseLost) and transport errors.
type LeaseRenewalError struct{ Err error }

func (e *LeaseRenewalError) Error() string { return "execution lease renewal failed" }
func (e *LeaseRenewalError) Unwrap() error { return e.Err }

func (e *LeaseRenewalError) GoString() string { return e.Error() }
