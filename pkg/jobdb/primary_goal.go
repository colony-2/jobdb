package jobdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrPrimaryGoalSelectionUnsupported means selection cannot be safely
	// enforced. Callers must not retry through ordinary submission.
	ErrPrimaryGoalSelectionUnsupported = errors.New("primary goal selection is unsupported")
	// ErrPrimaryGoalSelectionForbidden means the caller lacks explicit goal
	// selection authority, even when it can submit jobs to the destination.
	ErrPrimaryGoalSelectionForbidden = errors.New("primary goal selection is forbidden")
	// ErrSubmissionReceiptUnavailable means a committed submission cannot be
	// reconciled. Callers must retain the destination ID rather than republish.
	ErrSubmissionReceiptUnavailable = errors.New("submission receipt is unavailable")
)

// ValidatePrimaryGoalSelection validates request identity without performing
// storage or authority checks. Empty GoalID retains ordinary submission.
func ValidatePrimaryGoalSelection(job SubmitJob) error {
	if job.GoalID == "" {
		return nil
	}
	if err := (GoalKey{TenantId: job.TenantId, GoalID: job.GoalID}).Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(job.JobID) == "" {
		return fmt.Errorf("%w: primary goal selection requires explicit destination job ID", ErrInvalidDependency)
	}
	return (JobKey{TenantId: job.TenantId, JobId: job.JobID}).Validate()
}

// PrimaryGoalSelectionAuthorizer is a trusted host hook. It must verify current
// access to the goal and explicit authority to select it as a primary goal.
// It is called during preparation, atomic publication and receipt replay; it
// must not call back into the runtime or depend on caller-supplied assertions.
type PrimaryGoalSelectionAuthorizer func(context.Context, GoalKey, ResolvedCell) error

type primaryGoalSelectionAuthorityKey struct{}

// WithPrimaryGoalSelectionAuthorizer installs verified host authority. HTTP
// authentication middleware may install it; request bodies must never do so.
func WithPrimaryGoalSelectionAuthorizer(ctx context.Context, authorize PrimaryGoalSelectionAuthorizer) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, primaryGoalSelectionAuthorityKey{}, authorize)
}

// AuthorizePrimaryGoalSelection also checks independent destination submission
// authority. A lease or jobs.submit permission alone never grants selection.
func AuthorizePrimaryGoalSelection(ctx context.Context, goal GoalKey, target ResolvedCell) error {
	if ctx == nil {
		return ErrPrimaryGoalSelectionForbidden
	}
	authorize, _ := ctx.Value(primaryGoalSelectionAuthorityKey{}).(PrimaryGoalSelectionAuthorizer)
	if authorize == nil {
		return ErrPrimaryGoalSelectionForbidden
	}
	if err := AuthorizeCell(ctx, goal.TenantId, target.CellID, "jobs.submit"); err != nil {
		return ErrPrimaryGoalSelectionForbidden
	}
	return authorize(ctx, goal, target)
}

// PrimaryGoalSelectionCapability is optional. Implementers advertise true only
// when preparation, atomic publication and retained receipt replay implement
// the complete selection contract. Unknown support is unsupported.
type PrimaryGoalSelectionCapability interface{ SupportsPrimaryGoalSelection() bool }

// HasPrimaryGoalSelectionAuthorizer reports an explicit trusted hook, without
// treating ordinary submission or execution lease authority as a grant.
func HasPrimaryGoalSelectionAuthorizer(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	a, _ := ctx.Value(primaryGoalSelectionAuthorityKey{}).(PrimaryGoalSelectionAuthorizer)
	return a != nil
}
