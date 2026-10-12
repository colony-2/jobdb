package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
)

func (r *Runtime) SecretResolutionLimits() jobdb.SecretLimits { return r.secretLimits.WithDefaults() }

// ResolveSecrets accepts claims verified by the HTTP transport. Embedded workers
// use their issued lease's ResolveSecrets method instead of presenting raw IDs.
func (r *Runtime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	authorized, err := leaseauth.Authorize(ctx, req.JobKey, req.LeaseID)
	claims, _ := leaseauth.ClaimsFromContext(ctx)
	if !authorized || err != nil || claims.WorkerID == "" {
		return jobdb.SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	return r.resolveSecrets(ctx, req.JobKey, req.LeaseID, claims.WorkerID, claims.Principal, claims.ExpiresAt, req.Names)
}

func (l *executionLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	return l.runtime.resolveSecrets(ctx, l.jobKey, l.leaseID, l.workerID, nil, time.Time{}, names)
}

func (r *Runtime) resolveSecrets(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, principal *string, tokenExpiry time.Time, names []string) (jobdb.SecretResolveResult, error) {
	limits := r.SecretResolutionLimits()
	if err := secretsutil.Names(names, limits.MaxRequestBytes); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if key.Validate() != nil || leaseID == "" || workerID == "" {
		return jobdb.SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	check := func() (jobRow, error) {
		if !tokenExpiry.IsZero() && !time.Now().Before(tokenExpiry) {
			return jobRow{}, jobdb.ErrExecutionLeaseLost
		}
		row, err := r.validateLease(ctx, key, leaseID, workerID)
		if errors.Is(err, jobdb.ErrJobNotFound) {
			err = jobdb.ErrExecutionLeaseLost
		}
		return row, secretsutil.SafeError(err)
	}
	row, err := check()
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if r.secretProvider == nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrNoSecretProvider
	}
	var envelope jobdb.JobMetadataEnvelope
	if len(row.metadata) > 0 && json.Unmarshal(row.metadata, &envelope) != nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
	}
	deadline := time.Now().Add(limits.LookupTimeout)
	if !tokenExpiry.IsZero() && tokenExpiry.Before(deadline) {
		deadline = tokenExpiry
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if principal != nil {
		p := *principal
		principal = &p
	}
	result, providerErr := r.secretProvider.ResolveSecrets(callCtx, jobdb.SecretResolveRequest{
		TenantID: key.TenantId, JobID: key.JobId, Principal: principal,
		AppMetadata: append(json.RawMessage(nil), envelope.App...), Names: append([]string(nil), names...),
	})
	if _, err := check(); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if err := callCtx.Err(); err != nil {
		return jobdb.SecretResolveResult{}, secretsutil.SafeError(err)
	}
	if providerErr != nil {
		return jobdb.SecretResolveResult{}, secretsutil.SafeError(providerErr)
	}
	result, err = secretsutil.Result(result, names, limits.MaxResponseBytes)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if _, err := check(); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return result, nil
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
