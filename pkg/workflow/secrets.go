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
	callCtx, cancel := invocationCallContext(ctx, r.ctx)
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
