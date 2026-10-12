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

// SecretProvider owns authorization and retrieval. Core never caches its output.
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
}

type SecretValue struct {
	Value     string     `json:"value"`
	Version   string     `json:"version,omitempty"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
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

// SecretResolver is an optional runtime capability. Unsupported runtimes return
// ErrNoSecretProvider through ResolveJobSecrets.
type SecretResolver interface {
	ResolveSecrets(context.Context, ResolveSecretsRequest) (SecretResolveResult, error)
}

func ResolveJobSecrets(ctx context.Context, runtime WorkflowRuntime, req ResolveSecretsRequest) (SecretResolveResult, error) {
	if r, ok := runtime.(SecretResolver); ok {
		return r.ResolveSecrets(ctx, req)
	}
	return SecretResolveResult{}, ErrNoSecretProvider
}

// SecretResolvingLease is the embedded/remote worker capability for secret reads.
type SecretResolvingLease interface {
	ResolveSecrets(context.Context, []string) (SecretResolveResult, error)
}

func ResolveLeaseSecrets(ctx context.Context, lease ExecutionLease, names []string) (SecretResolveResult, error) {
	if l, ok := lease.(SecretResolvingLease); ok {
		return l.ResolveSecrets(ctx, names)
	}
	return SecretResolveResult{}, ErrNoSecretProvider
}

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
