package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestSQLiteTerminalFailurePublicConformance(t *testing.T) {
	embedded, err := StartEmbeddedRuntime(context.Background())
	require.NoError(t, err)
	defer embedded.Shutdown()
	runtimetest.RunTerminalFailureConformance(t, embedded.Runtime)
}

func TestSQLiteTerminalFailurePublicRollbackAndReopen(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "rollback.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	origin, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "tenant", JobID: "origin", Cell: "work", CellRepo: "example.test/work",
		JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil),
	}})
	require.NoError(t, err)
	consumer, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "tenant", JobID: "consumer", Cell: "work", CellRepo: "example.test/work",
		JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil),
		Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: origin.GoalID}, Condition: jobdb.JobPrereqSuccess}},
	}})
	require.NoError(t, err)
	_, err = r.db.Exec(`CREATE TRIGGER reject_failure_settlement BEFORE UPDATE ON jobdb_dependency_waits
 WHEN NEW.state='FAILED' BEGIN SELECT RAISE(ABORT,'injected public waiter failure'); END`)
	require.NoError(t, err)
	before, definitions := compatibilitySnapshot(t, r.db), schemaSnapshot(t, r.db)
	req := jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: origin.GoalID}, Message: " \tpermanent 保存\n"}
	_, err = r.FailGoal(ctx, req)
	require.ErrorContains(t, err, "injected public waiter failure")
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	require.Equal(t, before, compatibilitySnapshot(t, r.db), "job, closure and every wait change must roll back")
	require.Equal(t, definitions, schemaSnapshot(t, r.db))
	g, err := r.GetGoal(ctx, req.GoalKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalOpen, g.State)
	waitKey := jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "submission"}
	wait, err := r.GetDependencyWait(ctx, waitKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.DependencyPending, wait.State)
	_, err = r.db.Exec(`DROP TRIGGER reject_failure_settlement`)
	require.NoError(t, err)
	failed, err := r.FailGoal(ctx, req)
	require.NoError(t, err)
	_, err = r.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{origin.JobKey.JobId, failed.FailureJobID}})
	require.NoError(t, err)
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	replay, err := r.FailGoal(ctx, req)
	require.NoError(t, err)
	require.Equal(t, failed, replay, "hidden origin and closure replay exact identity, time and snapshot after reopen")
	wait, err = r.GetDependencyWait(ctx, waitKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.DependencyFailed, wait.State)
	require.Equal(t, failed.Failure, wait.Failure)
	visible, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{jobdb.GoalFailureJobType}})
	require.NoError(t, err)
	require.Empty(t, visible.Jobs, "replay must not unhide the committed record")
	all, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{jobdb.GoalFailureJobType}, IncludeHidden: true})
	require.NoError(t, err)
	require.Len(t, all.Jobs, 1)
	require.Equal(t, failed.FailureJobID, all.Jobs[0].JobKey.JobId)
}
