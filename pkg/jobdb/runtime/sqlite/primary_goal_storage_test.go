package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestPrimaryGoalReceiptReopenCleanupAndPurge(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "receipts.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = r.Close(ctx) }()
	source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: source.GoalID, Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic("fresh")}}
	h, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	c, err := r.GetCell(ctx, jobdb.CellKey{TenantID: "tenant", CellID: h.CellID})
	require.NoError(t, err)
	name := "renamed"
	_, err = r.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: h.CellID}, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &name})
	require.NoError(t, err)
	_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: h.GoalID}, Message: "done"})
	require.NoError(t, err)
	_, err = r.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{"dest"}})
	require.NoError(t, err)
	require.NoError(t, r.DiscardJobPreparation(ctx, h.JobKey))
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	replay, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
	_, err = r.SubmitJob(celltest.Context(context.Background()), req)
	require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionForbidden)
	// No public hard-purge operation exists. Simulate storage reclamation to prove
	// the receipt has no cascading dependency on job/preparation rows.
	_, err = r.db.ExecContext(ctx, `DELETE FROM jobdb_jobs WHERE tenant_id='tenant' AND job_id='dest'; DELETE FROM jobdb_goal_preparations WHERE tenant_id='tenant' AND job_id='dest'`)
	require.NoError(t, err)
	require.NoError(t, r.DiscardJobPreparation(ctx, h.JobKey))
	replay, err = r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
	var n int
	require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE tenant_id='tenant' AND job_id='dest'`).Scan(&n))
	require.Zero(t, n)
}

func TestPrimaryGoalUnknownIdentityAndUnavailableReceipt(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	embedded, err := StartEmbeddedRuntime(ctx)
	require.NoError(t, err)
	defer embedded.Shutdown()
	r := embedded.Runtime
	for _, bad := range []string{"state", "identity", "retired", "unavailable"} {
		t.Run(bad, func(t *testing.T) {
			source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: bad, JobID: "source", Cell: "work", CellRepo: "example.test/" + bad, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: bad, JobID: "dest", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
			if bad == "unavailable" {
				_, err = r.SubmitJob(ctx, req)
				require.NoError(t, err)
				_, err = r.db.ExecContext(ctx, `DELETE FROM jobdb_submission_receipts WHERE tenant_id=? AND job_id='dest'`, bad)
				require.NoError(t, err)
				_, err = r.SubmitJob(ctx, req)
				require.ErrorIs(t, err, jobdb.ErrSubmissionReceiptUnavailable)
				return
			}
			err = r.withTx(ctx, func(tx *sql.Tx) error {
				if bad == "retired" {
					_, e := tx.ExecContext(ctx, `UPDATE jobdb_cells SET state='RETIRED' WHERE tenant_id=?`, bad)
					return e
				}
				store := goalStore{ctx, tx}
				g, e := store.Goal(jobdb.GoalKey{TenantId: bad, GoalID: source.GoalID})
				if e != nil {
					return e
				}
				if bad == "state" {
					g.State = "UNKNOWN"
				} else {
					g.CellID = ""
					raw, e := json.Marshal(g)
					if e != nil {
						return e
					}
					_, e = tx.ExecContext(ctx, `UPDATE jobdb_goals SET payload=? WHERE tenant_id=?`, raw, bad)
					return e
				}
				return store.SaveGoal(g)
			})
			require.NoError(t, err)
			_, err = r.SubmitJob(ctx, req)
			require.Error(t, err)
			var n int
			require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE tenant_id=? AND job_id='dest'`, bad).Scan(&n))
			require.Zero(t, n)
			require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM jobdb_goal_candidates WHERE tenant_id=?`, bad).Scan(&n))
			require.Zero(t, n)
			require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM jobdb_goals WHERE tenant_id=?`, bad).Scan(&n))
			require.Equal(t, 1, n)
		})
	}
}

func TestPrimaryGoalLegacyPreparationUpgrade(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "legacy.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "work", Repo: "example.test/work"})
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "legacy", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	prep, err := goalstate.SubmitPreparation(ctx, req, "")
	require.NoError(t, err)
	var d map[string]any
	require.NoError(t, json.Unmarshal([]byte(prep.Descriptor), &d))
	require.NotContains(t, d["Job"], "GoalID")
	reserved, err := r.PrepareJob(ctx, prep)
	require.NoError(t, err)
	_, err = r.db.ExecContext(ctx, `DROP TABLE jobdb_submission_receipts`)
	require.NoError(t, err)
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	h, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reserved.GoalID, h.GoalID)
}

type primaryCoreScheduler struct{ *coreEventScheduler }

func (s *primaryCoreScheduler) SupportsPrimaryGoalSelection() bool { return true }
func (s *primaryCoreScheduler) CreateJob(ctx context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	if req.SelectedGoalID != "" && req.SelectedGoalID != req.GoalID {
		return runtimecore.StoredJob{}, jobdb.ErrGoalConflict
	}
	return s.coreEventScheduler.CreateJob(ctx, req)
}

func TestPrimaryGoalCoreRetainedReceipt(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	storage, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "core.db")})
	require.NoError(t, err)
	defer storage.Close(ctx)
	source, err := celltest.Submit(storage, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	scheduler := &coreEventScheduler{r: storage, CellRuntime: storage, GoalScheduler: storage}
	cfg := runtimecore.Config{Scheduler: scheduler, Chapters: coreEventChapters{r: storage}, Schemas: sqliteSchemaStore{runtime: storage}, Configurations: storage.Service.Store}
	unsupported, err := runtimecore.NewRuntime(cfg)
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic("new")}}
	_, err = unsupported.SubmitJob(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
	cfg.Scheduler = &primaryCoreScheduler{scheduler}
	core, err := runtimecore.NewRuntime(cfg)
	require.NoError(t, err)
	h, err := core.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, source.GoalID, h.GoalID)
	_, err = storage.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: h.GoalID}, Message: "closed"})
	require.NoError(t, err)
	_, err = storage.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{"dest"}})
	require.NoError(t, err)
	_, err = storage.db.ExecContext(ctx, `DELETE FROM jobdb_jobs WHERE tenant_id='tenant' AND job_id='dest'`)
	require.NoError(t, err)
	replay, err := core.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
}

func TestPrimaryGoalIndependentWriters(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "writers.db")}
	first, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer first.Close(ctx)
	source, err := celltest.Submit(first, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	second, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer second.Close(ctx)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic("fresh")}}
	start := make(chan struct{})
	results := make(chan jobdb.JobHandle, 2)
	failures := make(chan error, 2)
	for _, r := range []*Runtime{first, second} {
		go func(r *Runtime) { <-start; h, e := r.SubmitJob(ctx, req); results <- h; failures <- e }(r)
	}
	close(start)
	require.NoError(t, <-failures)
	require.NoError(t, <-failures)
	a, b := <-results, <-results
	require.Equal(t, a, b)
	require.Equal(t, source.GoalID, a.GoalID)
	require.Equal(t, source.CellID, a.CellID)
	var count int
	require.NoError(t, first.db.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE tenant_id='tenant' AND job_id='dest'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, second.db.QueryRow(`SELECT count(*) FROM jobdb_submission_receipts WHERE tenant_id='tenant' AND job_id='dest'`).Scan(&count))
	require.Equal(t, 1, count)
}
