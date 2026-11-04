package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/stretchr/testify/require"
)

func TestPreCellFormatRejected(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec("CREATE TABLE jobdb_jobs(job_id TEXT); INSERT INTO jobdb_jobs VALUES('old'); PRAGMA user_version=9;")
	require.NoError(t, err)
	require.ErrorContains(t, migrate(context.Background(), db), "reset required")
	var id string
	require.NoError(t, db.QueryRow("SELECT job_id FROM jobdb_jobs").Scan(&id))
	require.Equal(t, "old", id)
}
func TestGoalDurabilityAndIndependentWriters(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
	r1, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r1.Close(ctx)
	r2, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r2.Close(ctx)
	submit := func(id string) jobdb.JobHandle {
		h, err := celltest.Submit(r1, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobID: id, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
		require.NoError(t, err)
		return h
	}
	a, b := submit("a"), submit("b")
	g := jobdb.GoalKey{TenantId: "tenant", GoalID: a.GoalID}
	_, err = r1.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: g, JobID: b.JobKey.JobId})
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, rt := range []*Runtime{r1, r2} {
		wg.Add(1)
		go func(i int, rt *Runtime) {
			defer wg.Done()
			// The transaction establishes the durable outcome then resolves all associated goals.
			key := []jobdb.JobKey{a.JobKey, b.JobKey}[i]
			errs <- rt.withTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "UPDATE jobdb_jobs SET archived_at_ns=?,completion_status='success' WHERE tenant_id=? AND job_id=?", time.Now().UnixNano(), key.TenantId, key.JobId)
				if err != nil {
					return err
				}
				return goalEngine(ctx, tx).Complete(key)
			})
		}(i, rt)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	winner, err := r1.GetGoal(ctx, g)
	require.NoError(t, err)
	require.EqualValues(t, 2, winner.Revision)
	require.Contains(t, []string{"a", "b"}, winner.SuccessfulJobID)
	r3, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r3.Close(ctx)
	durable, err := r3.GetGoal(ctx, g)
	require.NoError(t, err)
	require.Equal(t, winner, durable)
	var links int
	require.NoError(t, r3.db.QueryRow("SELECT count(*) FROM jobdb_goal_candidates").Scan(&links))
	require.Equal(t, 1, links)
}
func TestGoalReservationSurvivesReopenBeforePublication(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobID: "new", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	prep, err := goalstate.SubmitPreparation(ctx, req, "")
	require.NoError(t, err)
	_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	require.NoError(t, err)
	first, err := r.PrepareJob(ctx, prep)
	require.NoError(t, err)
	_, err = r.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: first.GoalID})
	require.ErrorIs(t, err, jobdb.ErrGoalNotFound)
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	h, err := celltest.Submit(r, ctx, req)
	require.NoError(t, err)
	require.Equal(t, first.GoalID, h.GoalID)
	retry, err := celltest.Submit(r, ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, retry)
}
