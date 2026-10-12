package workflow

import (
	"context"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ResolveSecrets retrieves allowed names using the current task's live lease.
// Values and omissions are cached by name for the live job attempt. Requesting
// a new name refreshes all previously available names together. Known omissions
// are not retried. Each result is a caller-owned copy.
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
	next := r.secretCache.value
	if len(missing) > 0 {
		// An absent entry has never been retrieved; a nil entry is a known
		// omission. Refresh every available value with the new names, while
		// preserving omissions without asking the provider for them again.
		lookup := append([]string(nil), missing...)
		next = make(map[string]*SecretValue, len(r.secretCache.value)+len(missing))
		for name, value := range r.secretCache.value {
			if value == nil {
				next[name] = nil
			} else {
				lookup = append(lookup, name)
			}
		}
		sort.Strings(lookup)
		fetched, err := r.lease.ResolveSecrets(callCtx, lookup)
		if err != nil {
			return SecretResolveResult{}, err
		}
		for _, name := range lookup {
			var value *SecretValue
			if secret, ok := fetched.Secrets[name]; ok {
				if secretExpired(secret) {
					return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
				}
				copy := cloneSecret(secret)
				value = &copy
			}
			next[name] = value
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
		if value := next[name]; value != nil {
			if secretExpired(*value) {
				return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
			}
			result.Secrets[name] = cloneSecret(*value)
		}
	}
	// Publish the whole refresh only after validation and cancellation checks.
	r.secretCache.value = next
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
