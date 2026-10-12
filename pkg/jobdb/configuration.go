package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrInvalidConfigurationRequest   = errors.New("invalid configuration request")
	ErrConfigurationTooLarge         = errors.New("configuration exceeds size limit")
	ErrConfigurationReadTooLarge     = errors.New("stored configuration exceeds read limit")
	ErrConfigurationRevisionMismatch = errors.New("configuration revision mismatch")
	ErrConfigurationUnavailable      = errors.New("configuration is unavailable")
	ErrConfigurationFailed           = errors.New("configuration operation failed")
)

// ConfigurationSnapshot is one coherent, caller-owned tenant snapshot. Each read
// fetches current storage; runtimes and workflow contexts never cache results.
type ConfigurationSnapshot struct {
	Revision string                     `json:"revision"`
	Values   map[string]json.RawMessage `json:"values"`
}

// GetConfigurationRequest is a transport capability, not an HTTP request body.
// Embedded callers use their issued ExecutionLease instead of raw identifiers.
type GetConfigurationRequest struct {
	JobKey     JobKey
	LeaseID    string
	LeaseToken string
}

func (GetConfigurationRequest) String() string                 { return "configuration request (capability redacted)" }
func (r GetConfigurationRequest) GoString() string             { return r.String() }
func (r GetConfigurationRequest) MarshalJSON() ([]byte, error) { return json.Marshal(r.String()) }

type SetTenantConfigurationValueRequest struct {
	TenantID string
	Name     string
	Value    json.RawMessage
	// ExpectedRevision is an unquoted snapshot revision; nil means unconditional.
	ExpectedRevision *string
}
type RemoveTenantConfigurationValueRequest struct {
	TenantID         string
	Name             string
	ExpectedRevision *string
}

// ConfigurationAdmin is required by runtime servers and provided by all shipped
// backends. Deployment authentication controls access; a lease is not admin authority.
type ConfigurationAdmin interface {
	GetTenantConfiguration(context.Context, string) (ConfigurationSnapshot, error)
	SetTenantConfigurationValue(context.Context, SetTenantConfigurationValueRequest) (string, error)
	RemoveTenantConfigurationValue(context.Context, RemoveTenantConfigurationValueRequest) (string, error)
}

// ConfigurationLimits apply to all server instances sharing a store. Zero fields
// select defaults. MaxSnapshotBytes includes the JSON response envelope.
type ConfigurationLimits struct {
	MaxEntries       int
	MaxNameBytes     int
	MaxValueBytes    int
	MaxSnapshotBytes int
	MaxRequestBytes  int
	ReadTimeout      time.Duration
}

func (l ConfigurationLimits) WithDefaults() ConfigurationLimits {
	if l.MaxEntries <= 0 {
		l.MaxEntries = 1024
	}
	if l.MaxNameBytes <= 0 {
		l.MaxNameBytes = 256
	}
	if l.MaxValueBytes <= 0 {
		l.MaxValueBytes = 64 << 10
	}
	if l.MaxSnapshotBytes <= 0 {
		l.MaxSnapshotBytes = 1 << 20
	}
	if l.MaxRequestBytes <= 0 {
		l.MaxRequestBytes = 1 << 20
	}
	if l.ReadTimeout <= 0 {
		l.ReadTimeout = 10 * time.Second
	}
	return l
}
