package runtimetest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

type selectionBarrierData struct {
	jobdb.TaskData
	prepared *atomic.Bool
	reached  chan struct{}
	release  chan struct{}
	fired    atomic.Bool
	failure  error
}

func (d *selectionBarrierData) GetData() (jobdb.Data, error) {
	if d.prepared.Load() && d.fired.CompareAndSwap(false, true) {
		close(d.reached)
		<-d.release
		if d.failure != nil {
			return nil, d.failure
		}
	}
	return d.TaskData.GetData()
}

// RunPrimaryGoalSelectionBoundaries controls the interval between reservation
// and publication without sleeps. This suite requires a native runtime, since
// HTTP serializes input bytes before the backend reserves work.
func RunPrimaryGoalSelectionBoundaries(t *testing.T, r jobdb.WorkflowRuntime) {
	t.Run("revoked-before-publication", func(t *testing.T) { runSelectionRevocation(t, r) })
	for _, closure := range []string{"success", "failure"} {
		for _, ordering := range []string{"before-preparation", "after-reservation", "after-publication"} {
			t.Run(closure+"/"+ordering, func(t *testing.T) {
				ctx := PrimaryGoalContext(context.Background())
				id := closure + "-" + ordering
				source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "boundary", JobID: id, Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
				require.NoError(t, err)
				beforeGoals, e := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "boundary"})
				require.NoError(t, e)
				req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "boundary", JobID: id + "-new", GoalID: source.GoalID, Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic("fresh")}}
				closeGoal := func() {
					if closure == "failure" {
						_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "boundary", GoalID: source.GoalID}, Message: "stop"})
						require.NoError(t, err)
						return
					}
					lease, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: source.JobKey, WorkerID: "closer", Routes: []jobdb.Route{{JobType: "work"}}})
					require.NoError(t, e)
					require.NotNil(t, lease)
					e = lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "succeeded", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}})
					require.NoError(t, e)
				}
				if ordering == "before-preparation" {
					closeGoal()
					_, err = r.SubmitJob(ctx, req)
					require.ErrorIs(t, err, jobdb.ErrGoalConflict)
				}
				if ordering == "after-reservation" {
					prepared := &atomic.Bool{}
					barrier := &selectionBarrierData{TaskData: req.Job.Data, prepared: prepared, reached: make(chan struct{}), release: make(chan struct{})}
					req.Job.Data = barrier
					selectedCtx := jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error { prepared.Store(true); return nil })
					done := make(chan error, 1)
					go func() { _, e := r.SubmitJob(selectedCtx, req); done <- e }()
					select {
					case <-barrier.reached:
					case <-time.After(5 * time.Second):
						t.Fatal("did not reach post-reservation barrier")
					}
					closeGoal()
					close(barrier.release)
					require.ErrorIs(t, <-done, jobdb.ErrGoalConflict)
				}
				if ordering == "after-publication" {
					dependent, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "boundary", JobID: id + "-dependent", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: source.GoalID}, Condition: jobdb.JobPrereqSuccess}}}})
					require.NoError(t, e)
					h, e := r.SubmitJob(ctx, req)
					require.NoError(t, e)
					closeGoal()
					wait, e := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: dependent.JobKey, WaitID: "submission"})
					require.NoError(t, e)
					if closure == "success" {
						require.Equal(t, jobdb.DependencySatisfied, wait.State)
					} else {
						require.Equal(t, jobdb.DependencyFailed, wait.State)
					}
					replay, e := r.SubmitJob(ctx, req)
					require.NoError(t, e)
					require.Equal(t, h, replay)
					info, e := r.GetJob(ctx, h.JobKey)
					require.NoError(t, e)
					require.NotEqual(t, jobdb.JobStatusCancelled, info.Status)
					g, e := r.GetGoal(ctx, jobdb.GoalKey{TenantId: "boundary", GoalID: h.GoalID})
					require.NoError(t, e)
					require.NotEqual(t, jobdb.GoalOpen, g.State)
					if closure == "failure" {
						require.Contains(t, g.Failure.NonTerminalJobIDs, h.JobKey.JobId)
					}
					return
				}
				_, e = r.GetJob(ctx, jobdb.JobKey{TenantId: "boundary", JobId: req.Job.JobID})
				require.ErrorIs(t, e, jobdb.ErrJobNotFound)
				afterGoals, e := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "boundary"})
				require.NoError(t, e)
				require.Len(t, afterGoals.Goals, len(beforeGoals.Goals))
				sourceGoal, e := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "boundary", GoalIDs: []string{source.GoalID}})
				require.NoError(t, e)
				require.Len(t, sourceGoal.Goals, 1)
				require.Equal(t, int64(1), sourceGoal.Goals[0].CandidateCount)
				// Retry after cleanup must revalidate closure rather than publish staging.
				_, e = r.SubmitJob(ctx, req)
				require.ErrorIs(t, e, jobdb.ErrGoalConflict)
			})
		}
	}
	t.Run("authority-and-interruption", func(t *testing.T) {
		ctx := PrimaryGoalContext(context.Background())
		source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "boundary", JobID: "interruption", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "boundary", JobID: "interrupted-new", GoalID: source.GoalID, Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
		_, err = r.SubmitJob(celltest.Context(context.Background()), req)
		require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionForbidden)
		prepared := &atomic.Bool{}
		sentinel := errors.New("interrupted materialization")
		barrier := &selectionBarrierData{TaskData: req.Job.Data, prepared: prepared, reached: make(chan struct{}), release: make(chan struct{}), failure: sentinel}
		failed := req
		failed.Job.Data = barrier
		selectedCtx := jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error { prepared.Store(true); return nil })
		done := make(chan error, 1)
		go func() { _, e := r.SubmitJob(selectedCtx, failed); done <- e }()
		select {
		case <-barrier.reached:
		case <-time.After(5 * time.Second):
			t.Fatal("did not reach interruption barrier")
		}
		close(barrier.release)
		require.ErrorIs(t, <-done, sentinel)
		_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: "boundary", JobId: req.Job.JobID})
		require.ErrorIs(t, err, jobdb.ErrJobNotFound)
		h, err := r.SubmitJob(ctx, req)
		require.NoError(t, err)
		_, err = r.SubmitJob(celltest.Context(context.Background()), req)
		require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionForbidden)
		replay, err := r.SubmitJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, h, replay)
	})
}

// runSelectionRevocation checks the publication-time hook independently of the
// preparation hook, with the same controlled staging barrier as closure tests.
func runSelectionRevocation(t *testing.T, r jobdb.WorkflowRuntime) {
	ctx := PrimaryGoalContext(context.Background())
	source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "revoked", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	prepared, revoked := &atomic.Bool{}, &atomic.Bool{}
	barrier := &selectionBarrierData{TaskData: jobdb.NewTaskDataOrPanic(nil), prepared: prepared, reached: make(chan struct{}), release: make(chan struct{})}
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "revoked", JobID: "dest", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: barrier}}
	ctx = jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error {
		if revoked.Load() {
			return jobdb.ErrPrimaryGoalSelectionForbidden
		}
		prepared.Store(true)
		return nil
	})
	done := make(chan error, 1)
	go func() { _, e := r.SubmitJob(ctx, req); done <- e }()
	select {
	case <-barrier.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("did not reach revocation barrier")
	}
	revoked.Store(true)
	close(barrier.release)
	require.ErrorIs(t, <-done, jobdb.ErrPrimaryGoalSelectionForbidden)
	_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: "revoked", JobId: "dest"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
}
