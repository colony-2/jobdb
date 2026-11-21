package runtimecore

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type selectionOnlyGoalScheduler struct{ Scheduler }

func (selectionOnlyGoalScheduler) SupportsPrimaryGoalSelection() bool { return true }

type explicitGoalScheduler struct {
	Scheduler
	capabilities jobdb.JobGoalCapabilities
}

func (s explicitGoalScheduler) JobGoalCapabilities() jobdb.JobGoalCapabilities {
	return s.capabilities
}

func TestJobGoalCapabilitiesRequireExplicitBackendSupport(t *testing.T) {
	for _, scheduler := range []Scheduler{struct{ Scheduler }{}, selectionOnlyGoalScheduler{}} {
		rt := &Runtime{scheduler: scheduler}
		require.Equal(t, jobdb.JobGoalCapabilities{}, rt.JobGoalCapabilities())
	}
}

func TestJobGoalCapabilitiesForwardIndependentGuarantees(t *testing.T) {
	for bits := range 8 {
		want := jobdb.JobGoalCapabilities{
			ImmutableMembership:    bits&1 != 0,
			AtomicAdmission:        bits&2 != 0,
			TerminalFailureRecords: bits&4 != 0,
		}
		rt := &Runtime{scheduler: explicitGoalScheduler{capabilities: want}}
		require.Equal(t, want, rt.JobGoalCapabilities())
	}
}

func (s explicitGoalScheduler) SupportsPrimaryGoalSelection() bool { return true }

func TestJobGoalOperationsRejectMissingGuaranteesBeforeBackendAccess(t *testing.T) {
	ctx := context.Background()
	for bits := range 8 {
		caps := jobdb.JobGoalCapabilities{ImmutableMembership: bits&1 != 0, AtomicAdmission: bits&2 != 0, TerminalFailureRecords: bits&4 != 0}
		// All backend methods are nil: any preparation, history lookup, mutation or
		// ordinary-submit fallback would panic instead of returning unsupported.
		rt := &Runtime{scheduler: explicitGoalScheduler{capabilities: caps}}
		if !caps.ImmutableMembership || !caps.AtomicAdmission {
			_, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work"}})
			require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
			_, err = rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{RequestID: "restart", Job: jobdb.SubmitRestartJob{PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "prior"}, JobID: "dest", ShareGoal: true}})
			require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
		}
		if !caps.TerminalFailureRecords {
			_, err := rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: "goal"}, Message: "reason"})
			require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
		}
	}
	rt := &Runtime{scheduler: selectionOnlyGoalScheduler{}}
	_, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", JobType: "work"}})
	require.ErrorIs(t, err, jobdb.ErrJobGoalSemanticsUnsupported)
}
