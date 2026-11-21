package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestSQLiteGoalAdmissionConformance(t *testing.T) {
	embedded, err := StartEmbeddedRuntime(context.Background())
	require.NoError(t, err)
	defer embedded.Shutdown()
	runtimetest.RunGoalAdmissionConformance(t, embedded.Runtime)
}

func TestGoalAdmissionPublicIndependentWriters(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "admission.db")}
	a, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer a.Close(ctx)
	origin, err := celltest.Submit(a, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "origin", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	require.NoError(t, a.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: origin.JobKey}))
	b, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer b.Close(ctx)
	start := make(chan struct{})
	results := make(chan error, 12)
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r := a
			if i%2 == 1 {
				r = b
			}
			_, e := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: fmt.Sprintf("fresh-%02d", i), GoalID: origin.GoalID, Cell: origin.Cell, CellRepo: origin.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			results <- e
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for e := range results {
		if e == nil {
			wins++
		} else {
			require.ErrorIs(t, e, jobdb.ErrGoalConflict)
		}
	}
	require.Equal(t, 1, wins)
	var active, origins int
	require.NoError(t, a.db.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_id=? AND archived_at_ns IS NULL`, origin.GoalID).Scan(&active))
	require.Equal(t, 1, active)
	require.NoError(t, a.db.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_id=? AND goal_origin=1`, origin.GoalID).Scan(&origins))
	require.Equal(t, 1, origins)
}

func TestGoalAdmissionPublicRetainedBlockersAndPreparation(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "states.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	for _, state := range []string{"ready", "delayed", "blocked", "expired", "cancel-requested", "task-routed", "hidden"} {
		t.Run(state, func(t *testing.T) {
			h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: state, JobID: "origin", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			query := ""
			switch state {
			case "delayed":
				query = `UPDATE jobdb_jobs SET available_at_ns=9223372036854775807`
			case "blocked":
				query = `UPDATE jobdb_jobs SET wait_for=x'5b226f726967696e225d'`
			case "expired":
				query = `UPDATE jobdb_jobs SET lease_id='old',lease_worker_id='worker',lease_expires_at_ns=1`
			case "cancel-requested":
				query = `UPDATE jobdb_jobs SET cancel_requested=1`
			case "task-routed":
				query = `UPDATE jobdb_jobs SET route_task_type='external'`
			case "hidden":
				query = `UPDATE jobdb_jobs SET hidden_at_ns=1`
			}
			if query != "" {
				_, err = r.db.Exec(query+` WHERE tenant_id=? AND job_id='origin'`, state)
				require.NoError(t, err)
			}
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: state, JobID: "recovery", GoalID: h.GoalID, Cell: h.Cell, CellRepo: h.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
			prep, err := goalstate.SubmitPreparation(ctx, req, "")
			require.NoError(t, err)
			reserved, err := r.PrepareJob(ctx, prep)
			require.NoError(t, err)
			require.False(t, reserved.Published)
			_, err = r.SubmitJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: state, JobId: "recovery"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
			accepted, err := r.SubmitJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, h.GoalID, accepted.GoalID)
			_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: state, GoalID: h.GoalID}, Message: "permanent"})
			require.NoError(t, err)
			replay, err := r.SubmitJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accepted, replay)
			changed := req
			changed.Job.JobID = "after-closure"
			_, err = r.SubmitJob(ctx, changed)
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		})
	}
}

func TestGoalAdmissionPublicSharedRestartRechecks(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "restarts.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: source.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "ordinary failure"}}}}
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
	req := jobdb.SubmitRestartJobRequest{RequestID: "first", Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, JobID: "restart", Cell: source.Cell, ShareGoal: true}}
	h, err := r.SubmitRestartJob(ctx, req)
	require.NoError(t, err)
	competing := req
	competing.RequestID = "second"
	competing.Job.JobID = "competing"
	_, err = r.SubmitRestartJob(ctx, competing)
	require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: "competing"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: source.GoalID}, Message: "closed"})
	require.NoError(t, err)
	replay, err := r.SubmitRestartJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
	_, err = r.SubmitRestartJob(ctx, competing)
	require.ErrorIs(t, err, jobdb.ErrGoalConflict)
}

func TestGoalAdmissionPublicIdenticalTenantGoalStrings(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "tenants.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	origins := map[string]jobdb.JobHandle{}
	for _, tenant := range []string{"first", "second"} {
		_, err := r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tenant, Name: "work", Repo: "example.test/work"})
		require.NoError(t, err)
		req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: "origin", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
		p, err := goalstate.SubmitPreparation(ctx, req, "")
		require.NoError(t, err)
		_, err = r.PrepareJob(ctx, p)
		require.NoError(t, err)
		// Control only the uncommitted generated identity. The first publication,
		// immutable primary membership and all chapters still use the public API.
		_, err = r.db.Exec(`UPDATE jobdb_goal_preparations SET goal_id='same-goal-string' WHERE tenant_id=? AND job_id='origin'`, tenant)
		require.NoError(t, err)
		h, err := r.SubmitJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, "same-goal-string", h.GoalID)
		origins[tenant] = h
	}
	recovery := func(tenant string) error {
		h := origins[tenant]
		req := publicGoalSubmission(h, "recovery")
		req.Job.GoalID = h.GoalID
		_, err := r.SubmitJob(ctx, req)
		return err
	}
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: origins["first"].JobKey}))
	require.NoError(t, recovery("first"))
	require.ErrorIs(t, recovery("second"), jobdb.ErrGoalConflict)
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: origins["second"].JobKey}))
	require.NoError(t, recovery("second"))
	_, err = r.FailGoal(ctx, publicGoalFailure(origins["first"]))
	require.NoError(t, err)
	g, err := r.GetGoal(ctx, publicGoalFailure(origins["second"]).GoalKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalOpen, g.State)
	for _, tenant := range []string{"first", "second"} {
		rows, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, GoalIDs: []string{"same-goal-string"}, IncludeHidden: true})
		require.NoError(t, err)
		active := 0
		for _, row := range rows.Jobs {
			require.Equal(t, tenant, row.JobKey.TenantId)
			if row.ArchivedAt == nil {
				active++
			}
		}
		require.Equal(t, 1, active)
	}
}
