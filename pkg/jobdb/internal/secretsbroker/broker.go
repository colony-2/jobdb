// Package secretsbroker enforces the common lease boundary around providers.
// Backends supply only live-lease validation and application metadata.
package secretsbroker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
)

// Access is constructed from verified transport claims or an issued local lease.
type Access struct {
	JobKey            jobdb.JobKey
	LeaseID, WorkerID string
	Principal         *string
	// TokenExpiry also bounds embedded requests to the issued handle's expiry.
	TokenExpiry time.Time
}

func FromRequest(ctx context.Context, req jobdb.ResolveSecretsRequest) (Access, error) {
	authorized, err := leaseauth.Authorize(ctx, req.JobKey, req.LeaseID)
	claims, _ := leaseauth.ClaimsFromContext(ctx)
	if !authorized || err != nil || claims.WorkerID == "" {
		return Access{}, jobdb.ErrExecutionLeaseLost
	}
	return Access{req.JobKey, req.LeaseID, claims.WorkerID, claims.Principal, claims.ExpiresAt}, nil
}

// Snapshot is supplied by authoritative backend validation.
type Snapshot struct {
	AppMetadata    json.RawMessage
	LeaseExpiresAt time.Time
}

// Resolve never persists or caches results. validate must check the exact live
// lease (including cancellation) on every call and return owned metadata bytes.
func Resolve(ctx context.Context, provider jobdb.SecretProvider, limits jobdb.SecretLimits, access Access, names []string, validate func(context.Context) (Snapshot, error)) (jobdb.SecretResolveResult, error) {
	limits = limits.WithDefaults()
	if err := secretsutil.Names(names, limits.MaxRequestBytes); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if access.JobKey.Validate() != nil || access.LeaseID == "" || access.WorkerID == "" {
		return jobdb.SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	check := func() (Snapshot, error) {
		if !access.TokenExpiry.IsZero() && !time.Now().Before(access.TokenExpiry) {
			return Snapshot{}, jobdb.ErrExecutionLeaseLost
		}
		if err := ctx.Err(); err != nil {
			return Snapshot{}, secretsutil.SafeError(err)
		}
		metadata, err := validate(ctx)
		if errors.Is(err, jobdb.ErrJobNotFound) {
			err = jobdb.ErrExecutionLeaseLost
		}
		if err == nil && !metadata.LeaseExpiresAt.IsZero() && !metadata.LeaseExpiresAt.After(time.Now()) {
			err = jobdb.ErrExecutionLeaseLost
		}
		return metadata, secretsutil.SafeError(err)
	}
	initial, err := check()
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if provider == nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrNoSecretProvider
	}
	// Missing backend or capability expiry must not become an unbounded grant.
	authorizedDeadline := func(snapshot Snapshot) time.Time {
		if snapshot.LeaseExpiresAt.IsZero() || access.TokenExpiry.IsZero() {
			return time.Time{}
		}
		if access.TokenExpiry.Before(snapshot.LeaseExpiresAt) {
			return access.TokenExpiry
		}
		return snapshot.LeaseExpiresAt
	}
	initialDeadline := authorizedDeadline(initial)
	deadline := time.Now().Add(limits.LookupTimeout)
	if !access.TokenExpiry.IsZero() && access.TokenExpiry.Before(deadline) {
		deadline = access.TokenExpiry
	}
	if !initial.LeaseExpiresAt.IsZero() && initial.LeaseExpiresAt.Before(deadline) {
		deadline = initial.LeaseExpiresAt
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	principal := access.Principal
	if principal != nil {
		p := *principal
		principal = &p
	}
	result, providerErr := provider.ResolveSecrets(callCtx, jobdb.SecretResolveRequest{
		TenantID: access.JobKey.TenantId, JobID: access.JobKey.JobId, Principal: principal,
		AppMetadata: append(json.RawMessage(nil), initial.AppMetadata...), Names: append([]string(nil), names...),
		Lease: jobdb.SecretLeaseContext{LeaseID: access.LeaseID, WorkerID: access.WorkerID,
			LeaseExpiresAt: initial.LeaseExpiresAt, AuthorizedNotAfter: initialDeadline},
	})
	current, err := check()
	if err != nil {
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
	validateBounds := func(snapshot Snapshot) error {
		deadline := authorizedDeadline(snapshot)
		for _, value := range result.Secrets {
			if value.ExpiresAt != nil && !value.ExpiresAt.After(time.Now()) {
				return jobdb.ErrSecretsUnavailable
			}
			if value.LeaseBound && (value.ExpiresAt == nil || initialDeadline.IsZero() || deadline.IsZero() || value.ExpiresAt.After(initialDeadline) || value.ExpiresAt.After(deadline)) {
				return jobdb.ErrSecretResolutionFailed
			}
		}
		return nil
	}
	if err := validateBounds(current); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	current, err = check()
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if err := validateBounds(current); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if err := callCtx.Err(); err != nil {
		return jobdb.SecretResolveResult{}, secretsutil.SafeError(err)
	}
	return result, nil
}
