package toyimpl

import (
	"context"
	"encoding/json"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsbroker"
)

func (r *Runtime) SecretResolutionLimits() jobdb.SecretLimits {
	return r.engine.secretLimits.WithDefaults()
}

func (r *Runtime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	access, err := secretsbroker.FromRequest(ctx, req)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return r.resolveSecrets(ctx, access, req.Names)
}

func (l *runtimeLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	if err := l.checkExpiry(); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return l.runtime.resolveSecrets(ctx, secretsbroker.Access{JobKey: l.jobKey, LeaseID: l.leaseID, WorkerID: l.workerID, TokenExpiry: l.LeaseExpiry()}, names)
}

func (r *Runtime) resolveSecrets(ctx context.Context, access secretsbroker.Access, names []string) (jobdb.SecretResolveResult, error) {
	return secretsbroker.Resolve(ctx, r.engine.secretProvider, r.engine.secretLimits, access, names, func(ctx context.Context) (secretsbroker.Snapshot, error) {
		record := r.engine.getJobRecord(access.JobKey)
		if record == nil {
			return secretsbroker.Snapshot{}, jobdb.ErrExecutionLeaseLost
		}
		record.mu.Lock()
		defer record.mu.Unlock()
		if record.leaseID != access.LeaseID || record.leaseWorkerID != access.WorkerID || !record.leased || !record.leaseExpiresAt.After(time.Now()) || record.cancelled || record.archived != nil {
			return secretsbroker.Snapshot{}, jobdb.ErrExecutionLeaseLost
		}
		var envelope jobdb.JobMetadataEnvelope
		if len(record.metadata) > 0 && json.Unmarshal(record.metadata, &envelope) != nil {
			return secretsbroker.Snapshot{}, jobdb.ErrSecretResolutionFailed
		}
		return secretsbroker.Snapshot{AppMetadata: envelope.App, LeaseExpiresAt: record.leaseExpiresAt}, nil
	})
}
