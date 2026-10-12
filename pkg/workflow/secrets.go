package workflow

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ResolveSecrets retrieves allowed names using the current task's live lease.
// Results are never recorded in workflow history. Retain them only as long as
// needed and do not include values in task outputs, artifacts, or errors.
func (tc TaskContext) ResolveSecrets(ctx context.Context, names []string) (SecretResolveResult, error) {
	if tc.resolveSecrets == nil {
		return SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	return tc.resolveSecrets(ctx, names)
}

func (r *workerRunner) ResolveSecrets(ctx context.Context, names []string) (SecretResolveResult, error) {
	if r.replay {
		return SecretResolveResult{}, replayMutationError("resolve secrets")
	}
	if r.lease == nil || r.rescheduled.Load() {
		return SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	callCtx, cancel := secretCallContext(ctx, r.ctx)
	defer cancel()
	if callCtx.Err() != nil {
		return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	result, err := r.lease.ResolveSecrets(callCtx, names)
	if r.rescheduled.Load() {
		return SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	if callCtx.Err() != nil {
		return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	return result, err
}

// Keep caller values and cancellation, while also bounding the call by the
// enclosing worker invocation (even when the caller uses context.Background).
func secretCallContext(ctx, invocation context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := context.WithCancel(ctx)
	if invocation == nil {
		return callCtx, cancel
	}
	if deadline, ok := invocation.Deadline(); ok {
		var deadlineCancel context.CancelFunc
		callCtx, deadlineCancel = context.WithDeadline(callCtx, deadline)
		oldCancel := cancel
		cancel = func() { deadlineCancel(); oldCancel() }
	}
	stop := context.AfterFunc(invocation, cancel)
	if invocation.Err() != nil {
		cancel()
	}
	return callCtx, func() { stop(); cancel() }
}
