package toyimpl

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/configurationbroker"
	"time"
)

func (r *Runtime) GetConfiguration(ctx context.Context, req jobdb.GetConfigurationRequest) (jobdb.ConfigurationSnapshot, error) {
	access, err := configurationbroker.FromRequest(ctx, req)
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	return r.getConfiguration(ctx, access)
}
func (l *runtimeLease) GetConfiguration(ctx context.Context) (jobdb.ConfigurationSnapshot, error) {
	return l.runtime.getConfiguration(ctx, configurationbroker.Access{JobKey: l.jobKey, LeaseID: l.leaseID, WorkerID: l.workerID})
}
func (r *Runtime) getConfiguration(ctx context.Context, access configurationbroker.Access) (jobdb.ConfigurationSnapshot, error) {
	return configurationbroker.Read(ctx, r.Service, access, func(ctx context.Context) error {
		record := r.engine.getJobRecord(access.JobKey)
		if record == nil {
			return jobdb.ErrExecutionLeaseLost
		}
		record.mu.Lock()
		defer record.mu.Unlock()
		if record.leaseID != access.LeaseID || record.leaseWorkerID != access.WorkerID || !record.leased || !record.leaseExpiresAt.After(time.Now()) || record.cancelled || record.archived != nil {
			return jobdb.ErrExecutionLeaseLost
		}
		return nil
	})
}
