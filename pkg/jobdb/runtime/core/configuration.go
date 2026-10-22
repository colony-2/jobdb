package runtimecore

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/configurationbroker"
)

func (r *Runtime) GetConfiguration(ctx context.Context, req jobdb.GetConfigurationRequest) (jobdb.ConfigurationSnapshot, error) {
	access, err := configurationbroker.FromRequest(ctx, req)
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	return r.getConfiguration(ctx, access)
}
func (l *executionLease) GetConfiguration(ctx context.Context) (jobdb.ConfigurationSnapshot, error) {
	if err := l.checkExpiry(); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	return l.runtime.getConfiguration(ctx, configurationbroker.Access{JobKey: l.snapshot.Identity.JobKey, LeaseID: l.snapshot.Identity.LeaseID, WorkerID: l.snapshot.Identity.WorkerID, TokenExpiry: l.LeaseExpiry()})
}
func (r *Runtime) getConfiguration(ctx context.Context, access configurationbroker.Access) (jobdb.ConfigurationSnapshot, error) {
	return configurationbroker.Read(ctx, r.Service, access, func(ctx context.Context) error {
		row, err := r.scheduler.GetJob(ctx, access.JobKey)
		if err != nil {
			return err
		}
		if row.CancelRequested || row.Store == jobdb.JobStoreArchived {
			return jobdb.ErrExecutionLeaseLost
		}
		// GetJob projections need not contain a complete lease identity. Ask the
		// scheduler to validate the exact issued identity, as chapter writes do.
		_, err = r.scheduler.ValidateLease(ctx, LeaseIdentity{
			JobKey: access.JobKey, LeaseID: access.LeaseID, WorkerID: access.WorkerID,
		})
		return err
	})
}
