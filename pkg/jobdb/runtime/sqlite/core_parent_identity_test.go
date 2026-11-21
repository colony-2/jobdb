package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	chaptercore "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

func parentSQLite(t *testing.T) *Runtime {
	t.Helper()
	r, err := NewFromConfig(context.Background(), Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "parent.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close(context.Background())) })
	return r
}

type parentGateChapters struct {
	coreEventChapters
	afterStage func()
}

type parentGateScheduler struct {
	*coreEventScheduler
	publications int
	preparations int
	afterPrepare func(runtimecore.PreparedJob)
}

func (s *parentGateScheduler) PrepareJob(ctx context.Context, req runtimecore.JobPreparation) (runtimecore.PreparedJob, error) {
	s.preparations++
	p, err := s.coreEventScheduler.PrepareJob(ctx, req)
	if err == nil && s.afterPrepare != nil {
		s.afterPrepare(p)
	}
	return p, err
}

func (s *parentGateScheduler) CreateJob(ctx context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	s.publications++
	return s.coreEventScheduler.CreateJob(ctx, req)
}

func (c parentGateChapters) Create(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	err := c.coreEventChapters.Create(ctx, key, ch)
	c.afterStage()
	if errors.Is(err, chaptercore.ErrConflict) {
		return jobdb.ErrConflict
	}
	return err
}
func (c parentGateChapters) ClonePrefix(ctx context.Context, req runtimecore.ClonePrefixRequest) error {
	err := c.coreEventChapters.ClonePrefix(ctx, req)
	c.afterStage()
	if errors.Is(err, chaptercore.ErrConflict) {
		return jobdb.ErrConflict
	}
	return err
}

func TestParentIdentityCoreStagingAndExistingShortcuts(t *testing.T) {
	for _, kind := range []string{"submit", "restart"} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing=%t", kind, existing), func(t *testing.T) {
				storage := parentSQLite(t)
				ctx := celltest.Context(context.Background())
				var armed bool
				var reachedStage bool
				var parent jobdb.JobKey
				chapters := parentGateChapters{coreEventChapters: coreEventChapters{r: storage}, afterStage: func() {
					if armed {
						require.NoError(t, storage.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: parent}))
						reachedStage = true
					}
				}}
				scheduler := &parentGateScheduler{coreEventScheduler: &coreEventScheduler{r: storage, CellRuntime: storage, GoalScheduler: storage}}
				r, err := runtimecore.NewRuntime(runtimecore.Config{Projects: celltest.UnrestrictedProjects{}, Scheduler: scheduler, Chapters: chapters, Schemas: sqliteSchemaStore{runtime: storage}, Configurations: storage.Service.Store})
				require.NoError(t, err)
				submit := func(id string) jobdb.JobHandle {
					h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
					require.NoError(t, err)
					return h
				}
				acquire := func(key jobdb.JobKey) jobdb.ExecutionLease {
					l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					require.NotNil(t, l)
					return l
				}
				parent = submit("A").JobKey
				l := acquire(parent)
				authority := cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: parent, LeaseID: l.LeaseID(), WorkerID: "worker"})
				req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "child", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}}
				var p runtimecore.JobPreparation
				var publish func() (jobdb.JobHandle, error)
				if kind == "submit" {
					p, err = goalstate.SubmitPreparation(authority, req, "")
					require.NoError(t, err)
					publish = func() (jobdb.JobHandle, error) { return l.SubmitJob(ctx, req) }
				} else {
					source := submit("source")
					sl := acquire(source.JobKey)
					final := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}
					require.NoError(t, sl.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
					restart := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{JobID: "child", PriorJobKey: source.JobKey, Cell: "test", CellRepo: "example.test/test", LastStepToKeep: 0}}
					p, err = goalstate.RestartPreparation(authority, restart, "")
					require.NoError(t, err)
					publish = func() (jobdb.JobHandle, error) { return l.SubmitRestartJob(ctx, restart) }
				}
				if existing {
					_, err = publish()
					require.NoError(t, err)
					// Earlier accepted work can outlive its preparation. Rebuild the actual
					// reservation so core must reconcile its existing history and scheduler row.
					_, err = storage.db.Exec("DELETE FROM jobdb_goal_preparations WHERE tenant_id='tenant' AND job_id='child'")
					require.NoError(t, err)
				}
				if !existing {
					_, err = storage.PrepareJob(authority, p)
					require.NoError(t, err)
				}
				armed = true
				publications := scheduler.publications
				_, err = publish()
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				require.True(t, reachedStage, "chapter staging barrier was not reached")
				require.Equal(t, publications, scheduler.publications, "shared core must reject before dispatching publication")
				var reservations int
				require.NoError(t, storage.db.QueryRow("SELECT count(*) FROM jobdb_goal_preparations WHERE tenant_id='tenant' AND job_id='child'").Scan(&reservations))
				require.Equal(t, 1, reservations, "lost authority must preserve stable retry identity")
				_, err = storage.GetJob(ctx, p.Key)
				if existing {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, jobdb.ErrJobNotFound)
				}
			})
		}
	}
}

func TestParentIdentityCorePublishedRetryRechecksLease(t *testing.T) {
	storage := parentSQLite(t)
	ctx := celltest.Context(context.Background())
	scheduler := &parentGateScheduler{coreEventScheduler: &coreEventScheduler{r: storage, CellRuntime: storage, GoalScheduler: storage}}
	r, err := runtimecore.NewRuntime(runtimecore.Config{Projects: celltest.UnrestrictedProjects{}, Scheduler: scheduler, Chapters: coreEventChapters{r: storage}, Schemas: sqliteSchemaStore{runtime: storage}, Configurations: storage.Service.Store})
	require.NoError(t, err)
	parent, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "A", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, l)
	request := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobID: "child", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}}
	child, err := l.SubmitJob(ctx, request)
	require.NoError(t, err)
	var recovered bool
	scheduler.afterPrepare = func(p runtimecore.PreparedJob) {
		recovered = p.Published
		require.NoError(t, storage.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: parent.JobKey}))
	}
	_, err = l.SubmitJob(ctx, request)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.True(t, recovered, "test must reach the published shortcut")
	info, err := storage.GetJob(ctx, child.JobKey)
	require.NoError(t, err)
	require.Equal(t, child.GoalID, info.GoalID)
}

func TestParentIdentityCoreRejectsUntrustedLeaseContext(t *testing.T) {
	storage := parentSQLite(t)
	ctx := celltest.Context(context.Background())
	scheduler := &parentGateScheduler{coreEventScheduler: &coreEventScheduler{r: storage, CellRuntime: storage, GoalScheduler: storage}}
	r, err := runtimecore.NewRuntime(runtimecore.Config{Projects: celltest.UnrestrictedProjects{}, Scheduler: scheduler, Chapters: coreEventChapters{r: storage}, Schemas: sqliteSchemaStore{runtime: storage}, Configurations: storage.Service.Store})
	require.NoError(t, err)
	parent, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "A", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, l)
	valid := cellstate.LeaseProof{JobKey: parent.JobKey, LeaseID: l.LeaseID(), WorkerID: "worker"}
	for name, proof := range map[string]cellstate.LeaseProof{
		"malformed":    {},
		"missing-job":  {JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "missing"}, LeaseID: valid.LeaseID, WorkerID: valid.WorkerID},
		"wrong-lease":  {JobKey: valid.JobKey, LeaseID: "forged", WorkerID: valid.WorkerID},
		"wrong-worker": {JobKey: valid.JobKey, LeaseID: valid.LeaseID, WorkerID: "forged"},
		"cross-tenant": {JobKey: jobdb.JobKey{TenantId: "other", JobId: "A"}, LeaseID: valid.LeaseID, WorkerID: valid.WorkerID},
	} {
		t.Run(name, func(t *testing.T) {
			before := scheduler.preparations
			_, err := r.SubmitJob(cellstate.WithLease(ctx, proof), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "child", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.Equal(t, before, scheduler.preparations, "untrusted context reached native preparation")
		})
	}
	child, err := r.SubmitJob(cellstate.WithLease(ctx, valid), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "child", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	rows, err := storage.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{child.JobKey}})
	require.NoError(t, err)
	require.Len(t, rows.Jobs, 1)
	require.Equal(t, "A", rows.Jobs[0].ParentJobID)
}
