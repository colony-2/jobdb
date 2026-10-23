package jobdb

import (
	"errors"
	"time"
)

// ErrLeasePermissionDenied means a restricted lease does not authorize an operation.
var ErrLeasePermissionDenied = errors.New("lease permission denied")

// DelegationNames selects exact, case-sensitive names, or explicitly all names.
// An empty selector permits nothing. Wildcards have no special meaning.
type DelegationNames struct {
	Any   bool     `json:"any,omitempty"`
	Names []string `json:"names,omitempty"`
}

// DelegationOrdinalRange selects an inclusive range of chapter ordinals.
type DelegationOrdinalRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// DelegationChapters selects chapter ordinals. Empty selectors permit nothing.
type DelegationChapters struct {
	Any      bool                     `json:"any,omitempty"`
	Ordinals []int64                  `json:"ordinals,omitempty"`
	Ranges   []DelegationOrdinalRange `json:"ranges,omitempty"`
}

type DelegationSubmitPermission struct {
	JobTypes DelegationNames `json:"jobTypes"`
	JobIDs   DelegationNames `json:"jobIds"`
}

// DelegationWritePermission permits task outcome chapters only. ArtifactNames
// authorizes fresh uploads, never references to previously stored artifacts.
type DelegationWritePermission struct {
	Ordinals      []int64         `json:"ordinals"`
	TaskTypes     DelegationNames `json:"taskTypes"`
	ArtifactNames DelegationNames `json:"artifactNames"`
}

// DelegationRenewPermission permits refreshing the child while its parent is
// live, but only if the grant has no NotAfter deadline. It never extends the parent.
type DelegationRenewPermission struct {
	MaxTTLSeconds int `json:"maxTtlSeconds"`
}

// DelegationPermissions is an immutable allowlist; omitted actions are denied.
// Delegation does not authorize completion, rescheduling, configuration reads,
// restart submission, schema registration, artifact downloads, or further delegation.
type DelegationPermissions struct {
	Submit  *DelegationSubmitPermission `json:"jobs.submit,omitempty"`
	Secrets []string                    `json:"secrets.resolve,omitempty"`
	Read    *DelegationChapters         `json:"chapters.read,omitempty"`
	Write   *DelegationWritePermission  `json:"chapters.write,omitempty"`
	Renew   *DelegationRenewPermission  `json:"delegation.renew,omitempty"`
}

// CreateDelegationRequest restricts a lease. TTLSeconds defaults to 30 (maximum
// 300). NotAfter is an optional hard deadline; its presence disables renewal,
// even if Permissions.Renew is present. An omitted deadline does not imply renewal.
type CreateDelegationRequest struct {
	Permissions DelegationPermissions `json:"permissions"`
	TTLSeconds  int                   `json:"ttlSeconds,omitempty"`
	NotAfter    *time.Time            `json:"notAfter,omitempty"`
}

// DelegationInfo describes a restricted lease without exposing its bearer token.
type DelegationInfo struct {
	ID          string                `json:"id"`
	Permissions DelegationPermissions `json:"permissions"`
	NotAfter    *time.Time            `json:"notAfter,omitempty"`
}

// LeaseValidity is a read-only transport check of the current execution lease.
// It deliberately excludes job payloads, configuration and chapter contents.
type LeaseValidity struct {
	ExpiresAt time.Time
	Route     Route
}
