package sqlite

import (
	"context"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
	sqlitedriver "modernc.org/sqlite"
)

// Test-only SQL triggers hold an actual public operation inside its scheduler
// write transaction. No production callback or alternate transition path is used.
var publicGoalBarriers sync.Map
var publicGoalBarrierID atomic.Int64

type publicGoalBarrier struct {
	entered, release chan struct{}
	once             sync.Once
}

func init() {
	sqlitedriver.MustRegisterScalarFunction("jobdb_test_goal_barrier", 1, func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
		if value, ok := publicGoalBarriers.Load(args[0].(string)); ok {
			b := value.(*publicGoalBarrier)
			b.once.Do(func() { close(b.entered); <-b.release })
		}
		return int64(0), nil
	})
}
func holdPublicGoalTransaction(t *testing.T, r *Runtime, trigger string) *publicGoalBarrier {
	t.Helper()
	token := fmt.Sprint(publicGoalBarrierID.Add(1))
	b := &publicGoalBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	publicGoalBarriers.Store(token, b)
	_, err := r.db.Exec(fmt.Sprintf("CREATE TRIGGER public_goal_barrier %s BEGIN SELECT jobdb_test_goal_barrier('%s'); END", trigger, token))
	require.NoError(t, err)
	t.Cleanup(func() {
		select {
		case <-b.release:
		default:
			close(b.release)
		}
		publicGoalBarriers.Delete(token)
	})
	return b
}
func waitPublicGoalTransaction(t *testing.T, b *publicGoalBarrier) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("public operation never reached transactional barrier")
	}
}

func publicGoalPair(t *testing.T) (context.Context, Config, *Runtime, *Runtime, jobdb.JobHandle) {
	t.Helper()
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "public-race.db")}
	a, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { a.Close(ctx) })
	h, err := celltest.Submit(a, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "origin", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	b, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close(ctx) })
	b.db.SetMaxOpenConns(1)
	_, err = b.db.Exec("PRAGMA busy_timeout=5")
	require.NoError(t, err)
	return ctx, cfg, a, b, h
}
func publicGoalFailure(h jobdb.JobHandle) jobdb.FailGoalRequest {
	return jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: h.JobKey.TenantId, GoalID: h.GoalID}, Message: "permanent"}
}
func publicGoalSubmission(h jobdb.JobHandle, id string) jobdb.SubmitJobRequest {
	return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: h.JobKey.TenantId, JobID: id, Cell: h.Cell, CellRepo: h.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
}
func assertPublicGoalWriterExcluded(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "busy")
}

func TestGoalPublicFailureSuccessCommitOrders(t *testing.T) {
	for _, first := range []string{"failure", "success"} {
		t.Run(first, func(t *testing.T) {
			ctx, cfg, a, b, h := publicGoalPair(t)
			lease, err := a.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			complete := func(r *Runtime) error {
				chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
				return r.CompleteJobWithLeaseByID(ctx, h.JobKey, lease.LeaseID(), "worker", jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter})
			}
			failure := func(r *Runtime) error { _, err := r.FailGoal(ctx, publicGoalFailure(h)); return err }
			firstOp, secondOp := failure, complete
			trigger := "AFTER INSERT ON jobdb_jobs WHEN NEW.record_kind='GOAL_FAILURE'"
			if first == "success" {
				firstOp, secondOp = complete, failure
				trigger = "AFTER UPDATE OF goal_closure_state ON jobdb_jobs WHEN NEW.goal_closure_state='SUCCEEDED'"
			}
			barrier := holdPublicGoalTransaction(t, a, trigger)
			done := make(chan error, 1)
			go func() { done <- firstOp(a) }()
			waitPublicGoalTransaction(t, barrier)
			excluded := secondOp(b)
			close(barrier.release)
			require.NoError(t, <-done)
			assertPublicGoalWriterExcluded(t, excluded)
			err = secondOp(b)
			if first == "success" {
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, a.Close(ctx))
			require.NoError(t, b.Close(ctx))
			reopened, err := NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			defer reopened.Close(ctx)
			g, err := reopened.GetGoal(ctx, publicGoalFailure(h).GoalKey)
			require.NoError(t, err)
			info, err := reopened.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			require.Equal(t, "success", info.CompletionStatus)
			rows, err := reopened.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{h.GoalID}, IncludeHidden: true})
			require.NoError(t, err)
			if first == "success" {
				require.Equal(t, jobdb.GoalSucceeded, g.State)
				require.Equal(t, h.JobKey.JobId, g.SuccessfulJobID)
				require.Len(t, rows.Jobs, 1)
			} else {
				require.Equal(t, jobdb.GoalFailed, g.State)
				require.NotEmpty(t, g.FailureJobID)
				require.Equal(t, []string{"origin"}, g.Failure.NonTerminalJobIDs)
				require.Len(t, rows.Jobs, 2)
			}
		})
	}
}

func TestGoalPublicFailureReplayCommitOrders(t *testing.T) {
	for _, secondMessage := range []string{"permanent", "different"} {
		t.Run(secondMessage, func(t *testing.T) {
			ctx, cfg, a, b, h := publicGoalPair(t)
			req := publicGoalFailure(h)
			barrier := holdPublicGoalTransaction(t, a, "AFTER INSERT ON jobdb_jobs WHEN NEW.record_kind='GOAL_FAILURE'")
			done := make(chan error, 1)
			var first jobdb.GoalInfo
			go func() { var err error; first, err = a.FailGoal(ctx, req); done <- err }()
			waitPublicGoalTransaction(t, barrier)
			second := req
			second.Message = secondMessage
			_, excluded := b.FailGoal(ctx, second)
			close(barrier.release)
			require.NoError(t, <-done)
			assertPublicGoalWriterExcluded(t, excluded)
			replay, err := b.FailGoal(ctx, second)
			if secondMessage == req.Message {
				require.NoError(t, err)
				require.Equal(t, first, replay)
			} else {
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			}
			require.NoError(t, a.Close(ctx))
			require.NoError(t, b.Close(ctx))
			reopened, err := NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			defer reopened.Close(ctx)
			retained, err := reopened.GetGoal(ctx, req.GoalKey)
			require.NoError(t, err)
			require.Equal(t, first, retained)
			rows, err := reopened.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{h.GoalID}, IncludeHidden: true})
			require.NoError(t, err)
			require.Len(t, rows.Jobs, 2)
		})
	}
}

func TestGoalPublicAdmissionFailureCommitOrders(t *testing.T) {
	for _, first := range []string{"admission", "failure"} {
		t.Run(first, func(t *testing.T) {
			ctx, cfg, a, b, h := publicGoalPair(t)
			require.NoError(t, a.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
			req := publicGoalSubmission(h, "recovery")
			req.Job.GoalID = h.GoalID
			admission := func(r *Runtime) error { _, err := r.SubmitJob(ctx, req); return err }
			failure := func(r *Runtime) error { _, err := r.FailGoal(ctx, publicGoalFailure(h)); return err }
			firstOp, secondOp := admission, failure
			trigger := "AFTER INSERT ON jobdb_jobs WHEN NEW.job_id='recovery'"
			if first == "failure" {
				firstOp, secondOp = failure, admission
				trigger = "AFTER INSERT ON jobdb_jobs WHEN NEW.record_kind='GOAL_FAILURE'"
			}
			barrier := holdPublicGoalTransaction(t, a, trigger)
			done := make(chan error, 1)
			go func() { done <- firstOp(a) }()
			waitPublicGoalTransaction(t, barrier)
			excluded := secondOp(b)
			close(barrier.release)
			require.NoError(t, <-done)
			assertPublicGoalWriterExcluded(t, excluded)
			err := secondOp(b)
			if first == "failure" {
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, a.Close(ctx))
			require.NoError(t, b.Close(ctx))
			reopened, err := NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			defer reopened.Close(ctx)
			g, err := reopened.GetGoal(ctx, publicGoalFailure(h).GoalKey)
			require.NoError(t, err)
			require.Equal(t, jobdb.GoalFailed, g.State)
			_, err = reopened.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: "recovery"})
			if first == "failure" {
				require.ErrorIs(t, err, jobdb.ErrJobNotFound)
				require.Empty(t, g.Failure.NonTerminalJobIDs)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"recovery"}, g.Failure.NonTerminalJobIDs)
			}
		})
	}
}

func TestGoalPublicWaitClosureCommitOrders(t *testing.T) {
	for _, first := range []string{"registration", "failure"} {
		t.Run(first, func(t *testing.T) {
			ctx, cfg, a, b, h := publicGoalPair(t)
			req := publicGoalSubmission(h, "consumer")
			req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: h.GoalID}, Condition: jobdb.JobPrereqSuccess}}
			registration := func(r *Runtime) error { _, err := r.SubmitJob(ctx, req); return err }
			failure := func(r *Runtime) error { _, err := r.FailGoal(ctx, publicGoalFailure(h)); return err }
			firstOp, secondOp := registration, failure
			trigger := "AFTER INSERT ON jobdb_dependency_waits WHEN NEW.job_id='consumer'"
			if first == "failure" {
				firstOp, secondOp = failure, registration
				trigger = "AFTER INSERT ON jobdb_jobs WHEN NEW.record_kind='GOAL_FAILURE'"
			}
			barrier := holdPublicGoalTransaction(t, a, trigger)
			done := make(chan error, 1)
			go func() { done <- firstOp(a) }()
			waitPublicGoalTransaction(t, barrier)
			excluded := secondOp(b)
			close(barrier.release)
			require.NoError(t, <-done)
			assertPublicGoalWriterExcluded(t, excluded)
			require.NoError(t, secondOp(b))
			require.NoError(t, a.Close(ctx))
			require.NoError(t, b.Close(ctx))
			reopened, err := NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			defer reopened.Close(ctx)
			g, err := reopened.GetGoal(ctx, publicGoalFailure(h).GoalKey)
			require.NoError(t, err)
			w, err := reopened.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "consumer"}, WaitID: "submission"})
			require.NoError(t, err)
			require.Equal(t, jobdb.DependencyFailed, w.State)
			require.Equal(t, g.Failure, w.Failure)
			require.Equal(t, g.Failure, w.Dependencies[0].Failure)
		})
	}
}
