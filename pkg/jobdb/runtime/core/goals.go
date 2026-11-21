package runtimecore

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
)

// JobPreparation reserves destination and goal identities across interrupted
// history creation. Publication through CreateJob must atomically create the
// goal (if new), job and submission wait. SelectedGoalID requires current goal
// access/selection and destination authority, matching stable ownership, an open
// goal and acyclic prospective dependencies. Published selected retries must
// return their captured Receipt before fresh eligibility/visibility checks.
// DiscardJobPreparation must preserve committed reconciliation identity.
type JobPreparation = goalstate.Preparation
type PreparedJob = goalstate.Prepared

// GoalScheduler supplies goal state in the same transaction domain as job
// publication, completion, cancellation and rescheduling. CompleteLease must
// resolve associated goals and settle waits. RescheduleLease registers the
// structured wait and releases its lease atomically. Cancellation closes the
// consumer's own waits and settles exact-job waiters, without failing goals.
// A backend advertising TerminalFailureRecords must publish the protected
// archived job, FailureJobID, closure and saved failure/wait settlement in this
// same transaction, without chapters or leases. Equal-message replay returns
// that retained identity; later execution success cannot replace closure. The
// reusable runtimetest.RunTerminalFailureConformance checks these semantics.
type GoalScheduler interface {
	jobdb.ConsumerGoalRuntime
	PrepareJob(context.Context, JobPreparation) (PreparedJob, error)
	DiscardJobPreparation(context.Context, jobdb.JobKey) error
}

func (r *Runtime) GetGoal(ctx context.Context, k jobdb.GoalKey) (jobdb.GoalInfo, error) {
	return r.scheduler.GetGoal(ctx, k)
}
func (r *Runtime) FailGoal(ctx context.Context, req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	if err := req.Validate(); err != nil {
		return jobdb.GoalInfo{}, err
	}
	if !r.JobGoalCapabilities().TerminalFailureRecords {
		return jobdb.GoalInfo{}, jobdb.ErrJobGoalSemanticsUnsupported
	}
	if err := jobdb.RequireProjectWrite(ctx, r.projects, req.GoalKey.TenantId); err != nil {
		return jobdb.GoalInfo{}, err
	}
	return r.scheduler.FailGoal(ctx, req)
}
func (r *Runtime) GetDependencyWait(ctx context.Context, k jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	return r.scheduler.GetDependencyWait(ctx, k)
}

func (r *Runtime) ListGoals(ctx context.Context, req jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	return r.scheduler.ListGoals(ctx, req)
}
func (r *Runtime) ListDependencyWaits(ctx context.Context, req jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	return r.scheduler.ListDependencyWaits(ctx, req)
}

// SupportsPrimaryGoalSelection forwards positive scheduler support only.
func (r *Runtime) SupportsPrimaryGoalSelection() bool {
	capability, ok := r.scheduler.(jobdb.PrimaryGoalSelectionCapability)
	return ok && capability.SupportsPrimaryGoalSelection()
}

// JobGoalCapabilities forwards only explicitly advertised backend guarantees.
// Selection support alone says nothing about admission or protected records.
func (r *Runtime) JobGoalCapabilities() jobdb.JobGoalCapabilities {
	if capability, ok := r.scheduler.(jobdb.JobGoalCapability); ok {
		return capability.JobGoalCapabilities()
	}
	return jobdb.JobGoalCapabilities{}
}
