package workflow

import (
	"context"
	"time"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ResolveSecrets retrieves allowed names using the current task's live lease.
// Values and omissions are cached by name for the live job attempt. Expired
// values fail resolution without refreshing. Each result is a caller-owned copy.
// Results are never recorded in workflow history; do not include values in task
// outputs, artifacts, or errors.
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
	if r.secretCache.lock(callCtx) != nil {
		return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	defer r.secretCache.unlock()
	if r.rescheduled.Load() {
		return SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	// Validate even cache hits; malformed requests must not become valid merely
	// because some of their names have already been resolved.
	if len(names) == 0 {
		return SecretResolveResult{}, jobdb.ErrInvalidSecretRequest
	}
	seen := make(map[string]bool, len(names))
	missing := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" || !utf8.ValidString(name) || seen[name] {
			return SecretResolveResult{}, jobdb.ErrInvalidSecretRequest
		}
		seen[name] = true
		if _, ok := r.secretCache.value[name]; !ok {
			missing = append(missing, name)
		}
	}
	for _, name := range names {
		if value := r.secretCache.value[name]; value != nil && secretExpired(*value) {
			return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
		}
	}
	var fetched SecretResolveResult
	if len(missing) > 0 {
		var err error
		fetched, err = r.lease.ResolveSecrets(callCtx, missing)
		if err != nil {
			return SecretResolveResult{}, err
		}
	}
	if r.rescheduled.Load() {
		return SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	if callCtx.Err() != nil {
		return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	result := SecretResolveResult{Secrets: make(map[string]SecretValue)}
	for _, name := range names {
		value, cached := r.secretCache.value[name]
		if !cached {
			if secret, ok := fetched.Secrets[name]; ok {
				value = &secret
			}
		}
		if value != nil {
			if secretExpired(*value) {
				return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
			}
			result.Secrets[name] = cloneSecret(*value)
		}
	}
	if r.secretCache.value == nil {
		r.secretCache.value = make(map[string]*SecretValue)
	}
	for _, name := range missing {
		var value *SecretValue
		if secret, ok := result.Secrets[name]; ok {
			copy := cloneSecret(secret)
			value = &copy
		}
		r.secretCache.value[name] = value
	}
	return result, nil
}

func secretExpired(value SecretValue) bool {
	return value.ExpiresAt != nil && !value.ExpiresAt.After(time.Now())
}

func cloneSecret(value SecretValue) SecretValue {
	if value.ExpiresAt != nil {
		expiresAt := *value.ExpiresAt
		value.ExpiresAt = &expiresAt
	}
	return value
}
