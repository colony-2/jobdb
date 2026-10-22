package runtimecore

import (
	"context"
	"encoding/json"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsbroker"
)

func (r *Runtime) SecretResolutionLimits() jobdb.SecretLimits { return r.secretLimits.WithDefaults() }

func (r *Runtime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	access, err := secretsbroker.FromRequest(ctx, req)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return r.resolveSecrets(ctx, access, req.Names)
}

func (l *executionLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	if err := l.checkExpiry(); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	identity := l.snapshot.Identity
	return l.runtime.resolveSecrets(ctx, secretsbroker.Access{JobKey: identity.JobKey, LeaseID: identity.LeaseID, WorkerID: identity.WorkerID, TokenExpiry: l.LeaseExpiry()}, names)
}

func (r *Runtime) resolveSecrets(ctx context.Context, access secretsbroker.Access, names []string) (jobdb.SecretResolveResult, error) {
	return secretsbroker.Resolve(ctx, r.secretProvider, r.secretLimits, access, names, func(ctx context.Context) (secretsbroker.Snapshot, error) {
		row, err := r.scheduler.GetJob(ctx, access.JobKey)
		if err != nil {
			return secretsbroker.Snapshot{}, err
		}
		if row.CancelRequested || row.Store == jobdb.JobStoreArchived || row.Lease == nil {
			return secretsbroker.Snapshot{}, jobdb.ErrExecutionLeaseLost
		}
		identity := *row.Lease
		if identity.JobKey != access.JobKey || identity.LeaseID != access.LeaseID || identity.WorkerID != access.WorkerID {
			return secretsbroker.Snapshot{}, jobdb.ErrExecutionLeaseLost
		}
		snapshot, err := r.scheduler.ValidateLease(ctx, identity)
		if err != nil {
			return secretsbroker.Snapshot{}, err
		}

		return secretsbroker.Snapshot{AppMetadata: append(json.RawMessage(nil), row.AppMetadata...), LeaseExpiresAt: snapshot.Identity.ExpiresAt}, nil
	})
}
