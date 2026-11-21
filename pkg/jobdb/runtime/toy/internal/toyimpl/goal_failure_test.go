package toyimpl

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestToyGoalAdmissionConformance(t *testing.T) {
	runtimetest.RunGoalAdmissionConformance(t, New(WithProjects(celltest.UnrestrictedProjects{})))
}

func TestToyTerminalFailureConformance(t *testing.T) {
	runtimetest.RunTerminalFailureConformance(t, New(WithProjects(celltest.UnrestrictedProjects{})))
}

func failureSource(t *testing.T, r *Runtime) jobdb.JobHandle {
	t.Helper()
	h, err := celltest.Submit(r, runtimetest.PrimaryGoalContext(context.Background()), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "failure", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	return h
}

func TestToyTerminalFailureRollsBackBeforeCommit(t *testing.T) {
	for _, mode := range []string{"allocation-error", "collision", "preparation", "wrong-tenant", "cancel-before-commit"} {
		t.Run(mode, func(t *testing.T) {
			r := New(WithProjects(celltest.UnrestrictedProjects{}))
			h := failureSource(t, r)
			key := jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}
			ctx, cancel := context.WithCancel(runtimetest.PrimaryGoalContext(context.Background()))
			defer cancel()
			consumer, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: h.JobKey.TenantId, JobID: "consumer", Cell: h.Cell, CellRepo: h.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: h.GoalID}, Condition: jobdb.JobPrereqSuccess}}}})
			require.NoError(t, err)
			if mode == "preparation" {
				_, err := r.PrepareJob(ctx, goalstate.Preparation{Key: jobdb.JobKey{TenantId: h.JobKey.TenantId, JobId: "marker"}, Cell: h.Cell, CellRepo: h.CellRepo, Descriptor: "reserved"})
				require.NoError(t, err)
			}
			before, err := r.GetGoal(ctx, key)
			require.NoError(t, err)
			waitKey := jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "submission"}
			waitBefore, err := r.GetDependencyWait(ctx, waitKey)
			require.NoError(t, err)
			r.engine.idGenerator = func(tenant string) (jobdb.JobKey, error) {
				generated := jobdb.JobKey{TenantId: tenant, JobId: "marker"}
				switch mode {
				case "allocation-error":
					return generated, errors.New("injected allocation failure")
				case "collision":
					return h.JobKey, nil
				case "wrong-tenant":
					generated.TenantId = "another"
				case "cancel-before-commit":
					cancel()
				}
				return generated, nil
			}
			_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: key, Message: "reason"})
			require.Error(t, err)
			if mode == "cancel-before-commit" {
				require.ErrorIs(t, err, context.Canceled)
			}
			after, err := r.GetGoal(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, before, after)
			waitAfter, err := r.GetDependencyWait(context.Background(), waitKey)
			require.NoError(t, err)
			require.Equal(t, waitBefore, waitAfter)
			require.Len(t, r.engine.jobRecords, 2)
			require.Nil(t, r.engine.runtimeChapters[jobdb.JobKey{TenantId: h.JobKey.TenantId, JobId: "marker"}])
		})
	}
}

func TestToyTerminalFailureConcurrentReplay(t *testing.T) {
	r := New(WithProjects(celltest.UnrestrictedProjects{}))
	h := failureSource(t, r)
	req := jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}, Message: "exact"}
	const count = 16
	start := make(chan struct{})
	results := make(chan jobdb.GoalInfo, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			g, err := r.FailGoal(context.Background(), req)
			results <- g
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var expected *jobdb.GoalInfo
	for g := range results {
		if expected == nil {
			expected = &g
		}
		require.Equal(t, *expected, g)
	}
	require.Len(t, r.engine.jobRecords, 2)
	require.Empty(t, r.engine.leaseEvents)
}

func TestToyTerminalFailureSuccessRace(t *testing.T) {
	for range 16 {
		r := New(WithProjects(celltest.UnrestrictedProjects{}))
		h := failureSource(t, r)
		ctx := runtimetest.PrimaryGoalContext(context.Background())
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		require.NotNil(t, lease)
		start := make(chan struct{})
		completeErr := make(chan error, 1)
		failErr := make(chan error, 1)
		key := jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}
		go func() {
			<-start
			chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
			completeErr <- lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter})
		}()
		go func() {
			<-start
			_, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: key, Message: "reason"})
			failErr <- err
		}()
		close(start)
		require.NoError(t, <-completeErr)
		err = <-failErr
		g, getErr := r.GetGoal(ctx, key)
		require.NoError(t, getErr)
		if g.State == jobdb.GoalSucceeded {
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			require.Equal(t, h.JobKey.JobId, g.SuccessfulJobID)
			require.Empty(t, g.FailureJobID)
			require.Len(t, r.engine.jobRecords, 1)
		} else {
			require.NoError(t, err)
			require.Equal(t, jobdb.GoalFailed, g.State)
			require.NotEmpty(t, g.FailureJobID)
			require.Empty(t, g.SuccessfulJobID)
			require.Len(t, r.engine.jobRecords, 2)
		}
	}
}

func TestToyTerminalFailureDifferentMessageRace(t *testing.T) {
	for range 16 {
		r := New(WithProjects(celltest.UnrestrictedProjects{}))
		h := failureSource(t, r)
		key := jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}
		start := make(chan struct{})
		type result struct {
			goal jobdb.GoalInfo
			err  error
		}
		results := make(chan result, 2)
		for _, message := range []string{"first", "second"} {
			go func() {
				<-start
				g, err := r.FailGoal(context.Background(), jobdb.FailGoalRequest{GoalKey: key, Message: message})
				results <- result{g, err}
			}()
		}
		close(start)
		a, b := <-results, <-results
		if a.err != nil {
			a, b = b, a
		}
		require.NoError(t, a.err)
		require.ErrorIs(t, b.err, jobdb.ErrGoalConflict)
		g, err := r.GetGoal(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, a.goal, g)
		require.Len(t, r.engine.jobRecords, 2)
	}
}

func TestToyTerminalFailureDoesNotRequireOrGrantSubmission(t *testing.T) {
	r := New(WithProjects(celltest.UnrestrictedProjects{}))
	h := failureSource(t, r)
	ctx := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "operator", TenantID: h.JobKey.TenantId, Permissions: map[string]bool{}})
	submit := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: h.JobKey.TenantId, JobID: "denied", Cell: h.Cell, CellRepo: h.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	_, err := r.SubmitJob(ctx, submit)
	require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	g, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}, Message: "reason"})
	require.NoError(t, err)
	require.NotEmpty(t, g.FailureJobID)
	_, err = r.SubmitJob(ctx, submit)
	require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	require.Len(t, r.engine.jobRecords, 2)
}
