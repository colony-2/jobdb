package workflow

import (
	"context"
	"encoding/json"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// GetConfiguration reads the complete tenant configuration using the current
// invocation's live lease. The first successful read is cached for the live job
// attempt and shared with other tasks. Each call returns a caller-owned copy.
// Nothing is prefetched or automatically recorded in workflow history.
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
	if r.lease == nil {
		return ConfigurationSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	callCtx, cancel := invocationCallContext(ctx, r.ctx)
	defer cancel()
	if r.configurationCache.lock(callCtx) != nil {
		return ConfigurationSnapshot{}, jobdb.ErrConfigurationUnavailable
	}
	defer r.configurationCache.unlock()
	result := r.configurationCache.value
	if result == nil {
		fetched, err := r.lease.GetConfiguration(callCtx)
		if err != nil {
			return ConfigurationSnapshot{}, err
		}
		result = &fetched
	}
	// Check the execution directly too: AfterFunc cancellation may still be queued.
	if callCtx.Err() != nil || (r.ctx != nil && r.ctx.Err() != nil) {
		return ConfigurationSnapshot{}, jobdb.ErrConfigurationUnavailable
	}
	if r.configurationCache.value == nil {
		snapshot := cloneConfiguration(*result)
		r.configurationCache.value = &snapshot
	}
	return cloneConfiguration(*r.configurationCache.value), nil
}

func cloneConfiguration(snapshot ConfigurationSnapshot) ConfigurationSnapshot {
	values := make(map[string]json.RawMessage, len(snapshot.Values))
	for name, value := range snapshot.Values {
		values[name] = append(json.RawMessage(nil), value...)
	}
	return ConfigurationSnapshot{Revision: snapshot.Revision, Values: values}
}
