package cellstate

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// LeaseProof is trusted backend input, never a wire authorization credential.
// Backends must revalidate these fields against stored live lease state on every
// resolution and publication. The proof itself conveys no permissions.
type LeaseProof struct {
	JobKey            jobdb.JobKey
	LeaseID, WorkerID string
}
type leaseKey struct{}

func WithLease(ctx context.Context, p LeaseProof) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, leaseKey{}, p)
}
func Lease(ctx context.Context) (LeaseProof, bool) {
	if ctx == nil {
		return LeaseProof{}, false
	}
	p, ok := ctx.Value(leaseKey{}).(LeaseProof)
	return p, ok
}

type scheduleKey struct{}

func WithSchedule(ctx context.Context, key jobdb.ScheduleKey) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, scheduleKey{}, key)
}
func Schedule(ctx context.Context) (jobdb.ScheduleKey, bool) {
	if ctx == nil {
		return jobdb.ScheduleKey{}, false
	}
	key, ok := ctx.Value(scheduleKey{}).(jobdb.ScheduleKey)
	return key, ok
}
