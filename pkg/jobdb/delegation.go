package jobdb

import (
	"context"
	"errors"
	"time"
)

// ErrLeasePermissionDenied means a restricted lease does not authorize an operation.
var ErrLeasePermissionDenied = errors.New("lease permission denied")

// ErrLeaseDelegationUnsupported means the runtime cannot issue delegated leases.
var ErrLeaseDelegationUnsupported = errors.New("execution lease delegation is unsupported")

// LeaseDelegator is an optional runtime capability. Wrappers supporting delegation
// must forward this method; WorkflowRuntime alone does not require it.
// The remote runtime derives signed restrictions locally, without a server call.
type LeaseDelegator interface {
	CreateDelegation(context.Context, ExecutionLease, CreateDelegationRequest) (RenewableExecutionLease, error)
}

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

// DelegationPermissions is an immutable allowlist; omitted actions are denied.
// Delegation does not authorize completion, rescheduling, configuration reads,
// restart submission, schema registration, or artifact downloads. Further delegation may only narrow this allowlist.
type DelegationPermissions struct {
	Submit  *DelegationSubmitPermission `json:"jobs.submit,omitempty"`
	Secrets []string                    `json:"secrets.resolve,omitempty"`
	Read    *DelegationChapters         `json:"chapters.read,omitempty"`
	Write   *DelegationWritePermission  `json:"chapters.write,omitempty"`
}

// CreateDelegationRequest restricts a lease to Permissions until NotAfter.
// Without NotAfter, authority lasts while the exact parent lease remains live.
// Credential refresh is automatic and never extends the deadline or parent lease.
type CreateDelegationRequest struct {
	Permissions DelegationPermissions `json:"permissions"`
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
	GoalID    string
	ExpiresAt time.Time
	Route     Route
}
