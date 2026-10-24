package workflow

import (
	"context"
	"sort"
	"sync"
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

// ResolveSecretsFresh consults the provider again and replaces the requested
// names in this attempt's cache, including omissions. Providers may reuse valid
// credentials. Failed calls leave the cache unchanged.
func (tc TaskContext) ResolveSecretsFresh(ctx context.Context, names []string) (SecretResolveResult, error) {
	if tc.resolveSecretsFresh == nil {
		return SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	return tc.resolveSecretsFresh(ctx, names)
}

func (r *workerRunner) ResolveSecrets(ctx context.Context, names []string) (SecretResolveResult, error) {
	return r.resolveSecrets(ctx, names, false)
}

func (r *workerRunner) ResolveSecretsFresh(ctx context.Context, names []string) (SecretResolveResult, error) {
	return r.resolveSecrets(ctx, names, true)
}

func (r *workerRunner) secretsCache() *attemptCache[map[string]*SecretValue] {
	r.secretCacheInit.Do(func() {
		if r.secretCache == nil {
			r.secretCache = &attemptCache[map[string]*SecretValue]{}
		}
	})
	return r.secretCache
}

func (r *workerRunner) resolveSecrets(ctx context.Context, names []string, fresh bool) (SecretResolveResult, error) {
	if r.replay {
		return SecretResolveResult{}, replayMutationError("resolve secrets")
	}
	if r.lease == nil {
		return SecretResolveResult{}, jobdb.ErrExecutionLeaseLost
	}
	callCtx, cancel := invocationCallContext(ctx, r.ctx)
	defer cancel()
	cache := r.secretsCache()
	if cache.lock(callCtx) != nil {
		return SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	defer cache.unlock()
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
		if _, ok := cache.value[name]; fresh || !ok {
			missing = append(missing, name)
		}
	}
	next := cache.value
	if len(missing) > 0 {
		// Ordinary reads expand the cache by refreshing available names. Fresh
		// reads replace only the explicitly requested names, including omissions.
		lookup := append([]string(nil), missing...)
		next = make(map[string]*SecretValue, len(cache.value)+len(missing))
		for name, value := range cache.value {
			if fresh || value == nil {
				next[name] = value
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
	// Check the execution directly too: AfterFunc cancellation may still be queued.
	if callCtx.Err() != nil || (r.ctx != nil && r.ctx.Err() != nil) {
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
	cache.value = next
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

// SecretResolver is a narrow, execution-bound secret capability. It cannot
// select another job, renew a lease, or acquire work.
type SecretResolver interface {
	ResolveSecrets(context.Context, []string) (SecretResolveResult, error)
	ResolveSecretsFresh(context.Context, []string) (SecretResolveResult, error)
}

type secretResolverKey struct{}

// SecretResolverFromContext returns the active execution's resolver, including
// in GetJobForRunRequest.BeforeRun before application bootstrap. The resolver
// follows the current job attempt's cache and is valid only while ctx and the
// execution remain live. It is unavailable outside execution or during replay.
func SecretResolverFromContext(ctx context.Context) (SecretResolver, bool) {
	if ctx == nil {
		return nil, false
	}
	resolver, ok := ctx.Value(secretResolverKey{}).(*executionSecretResolver)
	if !ok {
		return nil, false
	}
	return boundSecretResolver{ctx: ctx, resolver: resolver}, true
}

type boundSecretResolver struct {
	ctx      context.Context
	resolver *executionSecretResolver
}

func (boundSecretResolver) String() string     { return "execution secret resolver" }
func (s boundSecretResolver) GoString() string { return s.String() }

func (s boundSecretResolver) ResolveSecrets(ctx context.Context, names []string) (SecretResolveResult, error) {
	return s.resolve(ctx, names, false)
}
func (s boundSecretResolver) ResolveSecretsFresh(ctx context.Context, names []string) (SecretResolveResult, error) {
	return s.resolve(ctx, names, true)
}
func (s boundSecretResolver) resolve(ctx context.Context, names []string, fresh bool) (SecretResolveResult, error) {
	callCtx, cancel := invocationCallContext(ctx, s.ctx)
	defer cancel()
	s.resolver.mu.Lock()
	owner := s.resolver.owner
	s.resolver.mu.Unlock()
	return owner.resolveSecrets(callCtx, names, fresh)
}

// Bootstrap uses the first live attempt's cache. Later whole-job attempts get
// new caches; a retained execution resolver follows the current owner.
type executionSecretResolver struct {
	mu        sync.Mutex
	owner     *workerRunner
	bootstrap bool
}

func (r *workerRunner) bindExecutionSecrets(owner *workerRunner) {
	if r.executionSecrets == nil {
		return
	}
	s := r.executionSecrets
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bootstrap {
		owner.secretCache = s.owner.secretsCache()
		s.bootstrap = false
	}
	s.owner = owner
}
