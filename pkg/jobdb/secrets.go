package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoSecretProvider        = errors.New("no secret provider configured")
	ErrSecretsUnavailable      = errors.New("secret resolution is unavailable")
	ErrSecretResolutionFailed  = errors.New("secret resolution failed")
	ErrInvalidSecretRequest    = errors.New("invalid secret request")
	ErrSecretRequestTooLarge   = errors.New("secret request too large")
	ErrDevSecretsNotConfigured = errors.New("development secret store is not configured")
)

// SecretProvider owns authorization and retrieval, independently of job storage.
// Return only allowed requested names; omit denied or unknown names. Any error
// discards the entire result. Implementations must support concurrent calls and
// honor context cancellation. Core never caches provider output or denials.
// The caller owns provider configuration and lifecycle; runtimes do not close
// injected providers. A provider may implement its own caching policy.
// Cached values must remain authorized for the request. Lease-bound values must
// encode an expiry no later than Lease.AuthorizedNotAfter in the credential
// itself; the broker cannot shorten a signed credential by changing metadata.
// Workflow job/task contexts separately cache resolutions by name for one live
// job attempt. Requesting a new name also refreshes all cached available names
// in the same provider request; ordinary reads do not retry omitted names.
// Workflow ResolveSecretsFresh explicitly refreshes requested names, including
// omissions, and updates only those entries in the attempt's cache.
type SecretProvider interface {
	ResolveSecrets(context.Context, SecretResolveRequest) (SecretResolveResult, error)
}

type SecretResolveRequest struct {
	TenantID string
	JobID    string
	// Principal is copied from a verified lease claim; absent claims yield nil.
	Principal *string
	// AppMetadata is copied from the job, not supplied by the resolution caller.
	AppMetadata json.RawMessage
	Names       []string
	// Lease is verified by the broker, never taken from application metadata.
	Lease SecretLeaseContext
}

// SecretLeaseContext describes authority for this lookup without exposing its
// bearer token. AuthorizedNotAfter includes the presenting capability's expiry,
// which may precede LeaseExpiresAt even after the backend lease is renewed.
type SecretLeaseContext struct {
	LeaseID            string
	WorkerID           string
	LeaseExpiresAt     time.Time
	AuthorizedNotAfter time.Time
}

type SecretValue struct {
	Value     string     `json:"value"`
	Version   string     `json:"version,omitempty"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	// LeaseBound requires ExpiresAt within the verified lease authority. False
	// preserves static-secret semantics; any supplied ExpiresAt still applies.
	LeaseBound bool `json:"leaseBound,omitempty"`
}

func (SecretValue) String() string     { return "secret (redacted)" }
func (s SecretValue) GoString() string { return s.String() }

type SecretResolveResult struct {
	Secrets map[string]SecretValue `json:"secrets"`
}

func (SecretResolveResult) String() string     { return "secrets (redacted)" }
func (s SecretResolveResult) GoString() string { return s.String() }

// ResolveSecretsRequest carries the lease capability; no principal override exists.
type ResolveSecretsRequest struct {
	JobKey     JobKey
	LeaseID    string
	LeaseToken string
	Names      []string
}

func (ResolveSecretsRequest) String() string                 { return "secret request (capability redacted)" }
func (r ResolveSecretsRequest) GoString() string             { return r.String() }
func (r ResolveSecretsRequest) MarshalJSON() ([]byte, error) { return json.Marshal(r.String()) }

// SecretLimits bounds secret operations. Zero fields use defaults.
type SecretLimits struct {
	MaxRequestBytes  int64
	MaxResponseBytes int64
	LookupTimeout    time.Duration
}

func (l SecretLimits) WithDefaults() SecretLimits {
	if l.MaxRequestBytes <= 0 {
		l.MaxRequestBytes = 64 << 10
	}
	if l.MaxResponseBytes <= 0 {
		l.MaxResponseBytes = 1 << 20
	}
	if l.LookupTimeout <= 0 {
		l.LookupTimeout = 10 * time.Second
	}
	return l
}
