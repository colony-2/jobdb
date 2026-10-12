package sqlite

import (
	"context"
	"encoding/json"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsbroker"
)

func (r *Runtime) SecretResolutionLimits() jobdb.SecretLimits { return r.secretLimits.WithDefaults() }

// ResolveSecrets accepts claims verified by the HTTP transport. Embedded workers
// use their issued lease's ResolveSecrets method instead of presenting raw IDs.
func (r *Runtime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	access, err := secretsbroker.FromRequest(ctx, req)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return r.resolveSecrets(ctx, access.JobKey, access.LeaseID, access.WorkerID, access.Principal, access.TokenExpiry, req.Names)
}

func (l *executionLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	return l.runtime.resolveSecrets(ctx, l.jobKey, l.leaseID, l.workerID, nil, time.Time{}, names)
}

func (r *Runtime) resolveSecrets(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, principal *string, tokenExpiry time.Time, names []string) (jobdb.SecretResolveResult, error) {
	return secretsbroker.Resolve(ctx, r.secretProvider, r.secretLimits, secretsbroker.Access{
		JobKey: key, LeaseID: leaseID, WorkerID: workerID, Principal: principal, TokenExpiry: tokenExpiry,
	}, names, func(ctx context.Context) (json.RawMessage, error) {
		row, err := r.validateLease(ctx, key, leaseID, workerID)
		if err != nil {
			return nil, err
		}
		var envelope jobdb.JobMetadataEnvelope
		if len(row.metadata) > 0 && json.Unmarshal(row.metadata, &envelope) != nil {
			return nil, jobdb.ErrSecretResolutionFailed
		}
		return envelope.App, nil
	})
}

// SetDevSecret and RemoveDevSecret are development administration operations.
// HTTP exposure is deliberately limited to runtimes implementing these methods.
func (r *Runtime) SetDevSecret(ctx context.Context, tenant, name, value string) error {
	if r.devSecrets == nil {
		return jobdb.ErrDevSecretsNotConfigured
	}
	raw, _ := json.Marshal(struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}{name, value})
	if int64(len(raw)) > r.SecretResolutionLimits().MaxResponseBytes {
		return jobdb.ErrSecretRequestTooLarge
	}
	return r.devSecrets.Set(ctx, tenant, name, value)
}
func (r *Runtime) RemoveDevSecret(ctx context.Context, tenant, name string) error {
	if r.devSecrets == nil {
		return jobdb.ErrDevSecretsNotConfigured
	}
	return r.devSecrets.Remove(ctx, tenant, name)
}
