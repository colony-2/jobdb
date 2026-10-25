package runtimecore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
)

// JobPreparation reserves destination and goal identities across interrupted
// history creation. Publication through CreateJob must atomically create the
// goal (if new), job and submission wait; it validates an existing goal is open.
type JobPreparation = goalstate.Preparation
type PreparedJob = goalstate.Prepared

// GoalScheduler supplies goal state in the same transaction domain as job
// publication, completion, cancellation and rescheduling. CompleteLease must
// resolve associated goals and settle waits. RescheduleLease registers the
// structured wait and releases its lease atomically. Cancellation closes the
// consumer's own waits and settles exact-job waiters, without failing goals.
type GoalScheduler interface {
	jobdb.GoalRuntime
	PrepareJob(context.Context, JobPreparation) (PreparedJob, error)
}

func (r *Runtime) GetGoal(ctx context.Context, k jobdb.GoalKey) (jobdb.GoalInfo, error) {
	return r.scheduler.GetGoal(ctx, k)
}
func (r *Runtime) AddGoalCandidate(ctx context.Context, req jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	return r.scheduler.AddGoalCandidate(ctx, req)
}
func (r *Runtime) FailGoal(ctx context.Context, req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
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
