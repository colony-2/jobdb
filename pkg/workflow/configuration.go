package workflow

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// GetConfiguration reads the complete tenant configuration using the current
// invocation's live lease. Every call reaches storage; nothing is prefetched,
// cached, or automatically recorded in workflow history.
func (tc TaskContext) GetConfiguration(ctx context.Context) (ConfigurationSnapshot, error) {
	if tc.getConfiguration == nil {
		return ConfigurationSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	return tc.getConfiguration(ctx)
}

func (r *workerRunner) GetConfiguration(ctx context.Context) (ConfigurationSnapshot, error) {
	if r.replay {
		return ConfigurationSnapshot{}, replayMutationError("get configuration")
	}
	if r.lease == nil || r.rescheduled.Load() {
		return ConfigurationSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	callCtx, cancel := invocationCallContext(ctx, r.ctx)
	defer cancel()
	if callCtx.Err() != nil {
		return ConfigurationSnapshot{}, jobdb.ErrConfigurationUnavailable
	}
	result, err := r.lease.GetConfiguration(callCtx)
	if r.rescheduled.Load() {
		return ConfigurationSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	if callCtx.Err() != nil {
		return ConfigurationSnapshot{}, jobdb.ErrConfigurationUnavailable
	}
	return result, err
}
