package toyimpl

import (
	"context"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestToyRetainedBlockersAndPreparation(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	r := New(WithProjects(celltest.UnrestrictedProjects{}))
	h := failureSource(t, r)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: h.JobKey.TenantId, JobID: "prepared", GoalID: h.GoalID, Cell: h.Cell, CellRepo: h.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	p, err := goalstate.SubmitPreparation(ctx, req, "")
	require.NoError(t, err)
	prep, err := r.PrepareJob(ctx, p)
	require.NoError(t, err)
	require.False(t, prep.Published)
	// A hidden nonterminal primary member remains an admission blocker. This
	// state can be inherited from a backend retention policy; public deletion
	// additionally cancels, so it cannot construct this fixture.
	r.engine.mu.Lock()
	r.engine.jobRecords[h.JobKey].hidden = true
	r.engine.mu.Unlock()
	_, err = r.SubmitJob(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
	// The unused preparation does not reserve capacity for its identity.
	competitor := req
	competitor.Job.JobID = "competitor"
	accepted, err := r.SubmitJob(ctx, competitor)
	require.NoError(t, err)
	_, err = r.SubmitJob(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: accepted.JobKey}))
	recovery, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h.GoalID, recovery.GoalID)
	rows, err := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: h.JobKey.TenantId})
	require.NoError(t, err)
	require.Len(t, rows.Goals, 1)
	require.EqualValues(t, 3, rows.Goals[0].CandidateCount)
	require.EqualValues(t, 1, rows.Goals[0].NonTerminalCandidateCount)
	// Both the first failed attempt and its hidden origin remain ordinary
	// retained records. Cancelling recovery leaves an open, discoverable gap.
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: recovery.JobKey}))
	gap, err := r.GetGoal(ctx, jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID})
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalOpen, gap.State)
	cell, err := r.GetCell(ctx, jobdb.CellKey{TenantID: h.JobKey.TenantId, CellID: h.CellID})
	require.NoError(t, err)
	retire := jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: h.JobKey.TenantId, CellID: h.CellID}, IfMatch: fmt.Sprintf("%q", cell.Revision)}
	_, err = r.RetireCell(ctx, retire)
	require.ErrorIs(t, err, jobdb.ErrCellBusy)
	_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: gap.GoalKey, Message: "close recovery gap"})
	require.NoError(t, err)
	retired, err := r.RetireCell(ctx, retire)
	require.NoError(t, err)
	require.Equal(t, jobdb.CellRetired, retired.State)
}

func TestToyRetainedOriginAndClosureOnJobs(t *testing.T) {
	r := New(WithProjects(celltest.UnrestrictedProjects{}))
	h := failureSource(t, r)
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	key := jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}
	g, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: key, Message: "first"})
	require.NoError(t, err)
	origin := r.engine.jobRecords[h.JobKey]
	failure := r.engine.jobRecords[jobdb.JobKey{TenantId: key.TenantId, JobId: g.FailureJobID}]
	require.True(t, origin.goalOrigin)
	require.Nil(t, origin.goalClosure)
	require.False(t, failure.goalOrigin)
	require.Equal(t, jobdb.GoalFailed, failure.goalClosure.state)
	require.Empty(t, r.engine.goalWaits)
	// Projections are detached; changing a returned failure cannot change the
	// retained snapshot or equal-message receipt.
	g.Failure.Message = "changed"
	g.Failure.NonTerminalJobIDs[0] = "changed"
	replay, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: key, Message: "first"})
	require.NoError(t, err)
	require.Equal(t, []string{h.JobKey.JobId}, replay.Failure.NonTerminalJobIDs)
	require.Equal(t, "first", replay.Failure.Message)
	// Later native success must not move the first closure onto the execution.
	origin.mu.Lock()
	at := time.Now().UTC()
	origin.archived = &at
	origin.completionStatus = "success"
	origin.mu.Unlock()
	r.engine.mu.Lock()
	require.NoError(t, (goalstate.Engine{Store: r.engine.goalStoreLocked(), Now: at}).Complete(h.JobKey))
	r.engine.mu.Unlock()
	after, err := r.GetGoal(ctx, key)
	require.NoError(t, err)
	require.Equal(t, replay, after)
	require.Nil(t, origin.goalClosure)
}
