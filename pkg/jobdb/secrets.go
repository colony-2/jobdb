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
// Workflow job/task contexts separately cache resolutions by name for one live
// job attempt, so providers receive only names not yet resolved in that scope.
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
