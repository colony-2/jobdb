package jobdb

import "errors"

// ErrJobGoalSemanticsUnsupported means a backend cannot enforce the requested
// job-goal guarantees. Callers must not fall back to ordinary submission, drop
// GoalID, or emulate protected failure with submit followed by completion.
var ErrJobGoalSemanticsUnsupported = errors.New("job goal semantics are unsupported")

// JobGoalCapabilities describes independently advertised job-goal guarantees.
// Missing support is unsupported; PrimaryGoalSelectionCapability alone does
// not imply any of these guarantees.
type JobGoalCapabilities struct {
	// ImmutableMembership permits one primary goal selected only at creation.
	ImmutableMembership bool `json:"immutableMembership"`
	// AtomicAdmission rejects fresh nonterminal publication while any member is
	// nonterminal. It does not certify that inherited overlaps have drained or
	// that obsolete writers have stopped. Accepted receipt replay is preserved.
	AtomicAdmission bool `json:"atomicAdmission"`
	// TerminalFailureRecords publishes an inspectable, protected failed job and
	// settles goal waits atomically, without creating execution history.
	TerminalFailureRecords bool `json:"terminalFailureRecords"`
}

// JobGoalCapability is optional. Backends advertise only guarantees enforced
// by their transaction and storage implementation. Capability discovery must
// not inspect current goal openness, visibility or active member counts.
type JobGoalCapability interface {
	JobGoalCapabilities() JobGoalCapabilities
}
