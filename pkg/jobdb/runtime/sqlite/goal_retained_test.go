package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/stretchr/testify/require"
)

// These are real, durable format-13 storage tests, not runtime or migration
// conformance. Ordinary rows intentionally have no chapter fixture. Constructor
// adoption, conversion and public HTTP inspection remain separate acceptance.
type retainedFixture struct {
	db   *sql.DB
	path string
	at   time.Time
}

func openRetainedHandle(t *testing.T, path string, busy int) *sql.DB {
	t.Helper()
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = url.Values{"mode": {"rw"}, "_pragma": {"foreign_keys(1)", fmt.Sprintf("busy_timeout(%d)", busy)}}.Encode()
	db, err := sql.Open(sqliteDriverName, u.String())
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
func newRetainedFixture(t *testing.T) retainedFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "retained ?#%.db")
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = url.Values{"_pragma": {"foreign_keys(1)", "busy_timeout(5000)"}}.Encode()
	db, err := sql.Open(sqliteDriverName, u.String())
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`PRAGMA journal_mode=WAL`)
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, createRetainedGoalSchema(context.Background(), tx))
	_, err = tx.Exec(`PRAGMA user_version=13`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	f := retainedFixture{db, path, time.Date(2026, 11, 1, 1, 0, 0, 123, time.UTC)}
	for _, tenant := range []string{"tenant", "other"} {
		_, err = db.Exec(`INSERT INTO jobdb_cells VALUES(?,'cell','renamed','example.test/current','',x'7b7d','ACTIVE','1',?,?,NULL)`, tenant, f.at.UnixNano(), f.at.UnixNano())
		require.NoError(t, err)
	}
	return f
}
func (f retainedFixture) transaction(fn func(retainedGoalStore) error) error {
	return (&Runtime{db: f.db}).withTx(context.Background(), func(tx *sql.Tx) error { return fn(retainedStore(context.Background(), tx)) })
}
func insertRetainedExecution(ctx context.Context, q goalQuery, tenant, id, goal, status string, at time.Time) error {
	var archived, completion any
	if status != "" {
		archived = at.Add(time.Minute).UnixNano()
		completion = status
	}
	_, err := q.ExecContext(ctx, `INSERT INTO jobdb_jobs(tenant_id,job_id,goal_id,cell_id,cell_name,cell_repo,job_type,route_job_type,initial_payload_digest,available_at_ns,created_at_ns,updated_at_ns,archived_at_ns,completion_status) VALUES(?,?,?,'cell','original','example.test/original','work','work','digest',?,?,?,?,?)`, tenant, id, goal, at.UnixNano(), at.UnixNano(), at.UnixNano(), archived, completion)
	return err
}
func (f retainedFixture) job(t *testing.T, tenant, id, goal, status string, origin bool) {
	t.Helper()
	require.NoError(t, f.transaction(func(s retainedGoalStore) error {
		if err := insertRetainedExecution(s.ctx, s.q, tenant, id, goal, status, f.at); err != nil {
			return err
		}
		if origin {
			return s.MarkOrigin(jobdb.JobKey{TenantId: tenant, JobId: id})
		}
		return nil
	}))
}
func retainedKey(goal string) jobdb.GoalKey { return jobdb.GoalKey{TenantId: "tenant", GoalID: goal} }
func retainedJob(id string) jobdb.JobKey    { return jobdb.JobKey{TenantId: "tenant", JobId: id} }

func TestRetainedGoalSchemaAndIdentity(t *testing.T) {
	f := newRetainedFixture(t)
	f.job(t, "tenant", "origin", "G", "failed_app", true)
	f.job(t, "tenant", "another", "G", "cancelled", false)
	var n int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('jobdb_goals','jobdb_goal_candidates')`).Scan(&n))
	require.Zero(t, n)
	require.Error(t, f.transaction(func(s retainedGoalStore) error { return s.MarkOrigin(retainedJob("another")) }), "one origin per tenant/goal")
	f.job(t, "other", "origin", "G", "cancelled", true)
	for _, change := range []string{"goal_id='changed'", "tenant_id='other'", "cell_name='changed'", "cell_repo='changed'", "created_at_ns=1", "goal_origin=0", "record_kind='GOAL_FAILURE'"} {
		_, err := f.db.Exec(`UPDATE jobdb_jobs SET ` + change + ` WHERE tenant_id='tenant' AND job_id='origin'`)
		require.Error(t, err, change)
	}
	_, err := f.db.Exec(`DELETE FROM jobdb_jobs WHERE tenant_id='tenant' AND job_id='origin'`)
	require.Error(t, err)
	_, err = f.db.Exec(`UPDATE jobdb_jobs SET hidden_at_ns=2 WHERE tenant_id='tenant' AND job_id='origin'`)
	require.NoError(t, err)
	require.NoError(t, f.transaction(func(s retainedGoalStore) error {
		g, err := s.Goal(retainedKey("G"))
		if err != nil {
			return err
		}
		require.Equal(t, jobdb.GoalOpen, g.State)
		require.Equal(t, "original", g.Cell)
		require.Equal(t, "example.test/original", g.CellRepo)
		require.Equal(t, f.at, g.CreatedAt)
		require.NoError(t, s.CheckAdmission(retainedKey("G")))
		_, err = s.Goal(retainedKey("unpublished"))
		require.ErrorIs(t, err, jobdb.ErrGoalNotFound)
		return nil
	}))
	// Separate handles and durable reopen preserve hidden origin ownership.
	reopened := openRetainedHandle(t, f.path, 5000)
	require.NoError(t, (&Runtime{db: reopened}).withTx(context.Background(), func(tx *sql.Tx) error {
		g, err := retainedStore(context.Background(), tx).Goal(retainedKey("G"))
		require.Equal(t, "original", g.Cell)
		require.Equal(t, jobdb.GoalOpen, g.State)
		return err
	}))
}

func TestRetainedGoalAdmissionIncludesEveryActiveMember(t *testing.T) {
	for _, condition := range []string{"unleased", "hidden", "delayed", "blocked", "expired", "cancel_requested", "task_routed", "overlap"} {
		t.Run(condition, func(t *testing.T) {
			f := newRetainedFixture(t)
			f.job(t, "tenant", "origin", "G", "failed_app", true)
			f.job(t, "tenant", "active", "G", "", false)
			updates := map[string]string{"unleased": "lease_id=NULL", "hidden": "hidden_at_ns=1", "delayed": "available_at_ns=9223372036854775807", "blocked": "dependency_wait_id='wait'", "expired": "lease_id='old',lease_expires_at_ns=1", "cancel_requested": "cancel_requested=1", "task_routed": "route_task_type='task'", "overlap": "lease_id=NULL"}
			_, err := f.db.Exec(`UPDATE jobdb_jobs SET ` + updates[condition] + ` WHERE job_id='active'`)
			require.NoError(t, err)
			if condition == "overlap" {
				f.job(t, "tenant", "second", "G", "", false)
			}
			f.job(t, "other", "origin", "G", "failed_app", true)
			require.NoError(t, f.transaction(func(s retainedGoalStore) error {
				require.ErrorIs(t, s.CheckAdmission(retainedKey("G")), jobdb.ErrGoalConflict)
				require.NoError(t, s.CheckAdmission(jobdb.GoalKey{TenantId: "other", GoalID: "G"}))
				return nil
			}))
			_, err = f.db.Exec(`UPDATE jobdb_jobs SET archived_at_ns=3,completion_status='cancelled' WHERE job_id='active'`)
			require.NoError(t, err)
			require.NoError(t, f.transaction(func(s retainedGoalStore) error {
				err := s.CheckAdmission(retainedKey("G"))
				if condition == "overlap" {
					require.ErrorIs(t, err, jobdb.ErrGoalConflict)
					return nil
				}
				return err
			}))
		})
	}
}

func TestRetainedGoalFailureDurableReplayAndWaits(t *testing.T) {
	f := newRetainedFixture(t)
	f.job(t, "tenant", "z-origin", "G", "failed_app", true)
	f.job(t, "tenant", "a-active", "G", "", false)
	f.job(t, "tenant", "consumer", "W", "", true)
	var failed jobdb.GoalInfo
	at := f.at.Add(time.Hour)
	require.NoError(t, f.transaction(func(s retainedGoalStore) error {
		e := goalstate.Engine{Store: s, Now: at}
		for id, target := range map[string]jobdb.DependencyTarget{"goal": {GoalID: "G"}, "exact": {JobID: "a-active"}} {
			w, err := e.Register(retainedJob("consumer"), jobdb.DependencyWait{WaitID: id, Dependencies: []jobdb.Dependency{{Target: target, Condition: jobdb.JobPrereqSuccess}}}, false)
			if err != nil {
				return err
			}
			require.Equal(t, jobdb.DependencyPending, w.State)
		}
		s.newFailureID = func() (string, error) { return "failure", nil }
		e.Store = s
		var err error
		failed, err = e.FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: " \tfailure 雪\n"})
		return err
	}))
	require.Equal(t, "failure", failed.FailureJobID)
	require.Equal(t, []string{"z-origin"}, failed.Failure.TerminalJobIDs)
	require.Equal(t, []string{"a-active"}, failed.Failure.NonTerminalJobIDs)
	require.NoError(t, f.transaction(func(s retainedGoalStore) error {
		w, err := s.Wait(jobdb.DependencyWaitKey{JobKey: retainedJob("consumer"), WaitID: "goal"})
		if err != nil {
			return err
		}
		require.Equal(t, jobdb.DependencyFailed, w.State)
		require.Equal(t, failed.Failure, w.Failure)
		w, err = s.Wait(jobdb.DependencyWaitKey{JobKey: retainedJob("consumer"), WaitID: "exact"})
		if err != nil {
			return err
		}
		require.Equal(t, jobdb.DependencyPending, w.State)
		j, err := s.Job(retainedJob("a-active"))
		require.NoError(t, err)
		require.False(t, j.Terminal)
		_, err = s.q.ExecContext(s.ctx, `UPDATE jobdb_jobs SET archived_at_ns=?,completion_status='success' WHERE tenant_id='tenant' AND job_id='a-active'`, at.Add(time.Minute).UnixNano())
		if err != nil {
			return err
		}
		return (goalstate.Engine{Store: s, Now: at.Add(time.Minute)}).Complete(retainedJob("a-active"))
	}))
	_, err := f.db.Exec(`UPDATE jobdb_jobs SET hidden_at_ns=1 WHERE job_id IN ('failure','z-origin')`)
	require.NoError(t, err)
	reopened := openRetainedHandle(t, f.path, 5000)
	require.NoError(t, (&Runtime{db: reopened}).withTx(context.Background(), func(tx *sql.Tx) error {
		s := retainedStore(context.Background(), tx)
		s.newFailureID = func() (string, error) { return "", errors.New("replay must not allocate") }
		e := goalstate.Engine{Store: s, Now: at.Add(2 * time.Hour)}
		replay, err := e.FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: failed.Failure.Message})
		if err != nil {
			return err
		}
		require.Equal(t, failed, replay)
		_, err = e.FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: "different"})
		require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		require.ErrorIs(t, s.CheckAdmission(retainedKey("G")), jobdb.ErrGoalConflict)
		w, err := s.Wait(jobdb.DependencyWaitKey{JobKey: retainedJob("consumer"), WaitID: "exact"})
		if err != nil {
			return err
		}
		require.Equal(t, jobdb.DependencySatisfied, w.State)
		require.Equal(t, "a-active", w.Dependencies[0].SatisfiedByJobID)
		return nil
	}))
	var kind, route string
	var hidden, lease sql.NullInt64
	require.NoError(t, reopened.QueryRow(`SELECT record_kind,route_job_type,hidden_at_ns,lease_expires_at_ns FROM jobdb_jobs WHERE job_id='failure'`).Scan(&kind, &route, &hidden, &lease))
	require.Equal(t, "GOAL_FAILURE", kind)
	require.Empty(t, route)
	require.True(t, hidden.Valid)
	require.False(t, lease.Valid)
	var events int
	require.NoError(t, reopened.QueryRow(`SELECT count(*) FROM jobdb_lease_events`).Scan(&events))
	require.Zero(t, events)
	for _, change := range []string{"goal_failure=x'7b7d'", "completion_detail='changed'", "archived_at_ns=NULL", "goal_closure_state=NULL,goal_closed_at_ns=NULL,goal_closure_origin=NULL"} {
		_, err = reopened.Exec(`UPDATE jobdb_jobs SET ` + change + ` WHERE job_id='failure'`)
		require.Error(t, err, change)
	}
}

func TestRetainedGoalFailureRollbackAndIDCollision(t *testing.T) {
	for _, failure := range []string{"allocation", "job_collision", "preparation_collision", "receipt_collision", "wait_write"} {
		t.Run(failure, func(t *testing.T) {
			f := newRetainedFixture(t)
			f.job(t, "tenant", "origin", "G", "failed_app", true)
			f.job(t, "tenant", "consumer", "W", "", true)
			require.NoError(t, f.transaction(func(s retainedGoalStore) error {
				_, err := (goalstate.Engine{Store: s, Now: f.at}).Register(retainedJob("consumer"), jobdb.DependencyWait{WaitID: "goal", Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: "G"}, Condition: jobdb.JobPrereqSuccess}}}, false)
				return err
			}))
			switch failure {
			case "preparation_collision":
				_, err := f.db.Exec(`INSERT INTO jobdb_goal_preparations VALUES('tenant','failure',NULL,'unborn',0,x'7b7d','original','example.test/original','cell','original','example.test/original','','',x'5b5d')`)
				require.NoError(t, err)
			case "receipt_collision":
				_, err := f.db.Exec(`INSERT INTO jobdb_submission_receipts VALUES('tenant','failure',x'7b7d','original','G','cell','original','example.test/original')`)
				require.NoError(t, err)
			case "wait_write":
				_, err := f.db.Exec(`CREATE TRIGGER abort_wait_settlement BEFORE UPDATE ON jobdb_dependency_waits WHEN NEW.state='FAILED' BEGIN SELECT RAISE(ABORT,'injected settlement failure'); END`)
				require.NoError(t, err)
			}
			before := compatibilitySnapshot(t, f.db)
			err := f.transaction(func(s retainedGoalStore) error {
				s.newFailureID = func() (string, error) {
					if failure == "allocation" {
						return "", errors.New("injected allocation failure")
					}
					if failure == "job_collision" {
						return "origin", nil
					}
					return "failure", nil
				}
				_, err := (goalstate.Engine{Store: s, Now: f.at.Add(time.Hour)}).FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: "fail"})
				return err
			})
			require.Error(t, err)
			reopened := openRetainedHandle(t, f.path, 5000)
			require.Equal(t, before, compatibilitySnapshot(t, reopened), "record, closure, waits, subscriptions, schema and format must roll back")
		})
	}
}

func TestRetainedGoalMigratedClosureFacts(t *testing.T) {
	t.Run("cross-primary-success", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.job(t, "tenant", "origin", "G", "failed_app", true)
		f.job(t, "tenant", "producer", "H", "success", true)
		at := f.at.Add(time.Hour)
		require.NoError(t, f.transaction(func(s retainedGoalStore) error {
			g, err := s.Goal(retainedKey("G"))
			if err != nil {
				return err
			}
			g.State = jobdb.GoalSucceeded
			g.SuccessfulJobID = "producer"
			g.Revision = 2
			g.ClosedAt = &at
			g.UpdatedAt = at
			require.ErrorIs(t, s.saveClosure(g, "origin", "native"), jobdb.ErrInvalidJobRecord)
			return s.saveClosure(g, "origin", "migrated")
		}))
		reopened := openRetainedHandle(t, f.path, 5000)
		require.NoError(t, (&Runtime{db: reopened}).withTx(context.Background(), func(tx *sql.Tx) error {
			s := retainedStore(context.Background(), tx)
			g, err := s.Goal(retainedKey("G"))
			if err != nil {
				return err
			}
			require.Equal(t, "producer", g.SuccessfulJobID)
			require.Equal(t, at, *g.ClosedAt)
			producer, err := s.Job(retainedJob("producer"))
			require.NoError(t, err)
			require.Equal(t, "H", producer.GoalID)
			members, err := s.Candidates(retainedKey("G"))
			require.NoError(t, err)
			require.Len(t, members, 1)
			require.Equal(t, "origin", members[0].Key.JobId)
			require.ErrorIs(t, s.CheckAdmission(retainedKey("G")), jobdb.ErrGoalConflict)
			_, err = (goalstate.Engine{Store: s, Now: at.Add(time.Hour)}).FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: "late"})
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			return nil
		}))
	})
	t.Run("historical-failure-snapshot", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.job(t, "tenant", "origin", "G", "failed_app", true)
		f.job(t, "tenant", "secondary", "H", "success", true)
		at := f.at.Add(time.Minute)
		failure := jobdb.GoalFailure{Code: "goal_failed", GoalID: "G", Message: " \tsaved 雪\n", FailedAt: at, TerminalJobIDs: []string{"secondary", "origin"}, NonTerminalJobIDs: []string{}}
		var saved jobdb.GoalInfo
		require.NoError(t, f.transaction(func(s retainedGoalStore) error {
			g, err := s.Goal(retainedKey("G"))
			if err != nil {
				return err
			}
			s.newFailureID = func() (string, error) { return "migrated", nil }
			key, err := s.insertFailure(g, failure, jobdb.JobRecordMigratedGoalFailure)
			if err != nil {
				return err
			}
			g.State = jobdb.GoalFailed
			g.Revision = 2
			g.FailureJobID = key.JobId
			g.Failure = jobdb.CloneGoalFailure(&failure)
			g.ClosedAt = &at
			g.UpdatedAt = at
			if err = s.saveClosure(g, key.JobId, "migrated"); err != nil {
				return err
			}
			saved = g
			return nil
		}))
		require.NoError(t, f.transaction(func(s retainedGoalStore) error {
			actual, err := s.Goal(retainedKey("G"))
			if err != nil {
				return err
			}
			require.Equal(t, saved, actual)
			require.Equal(t, &failure, actual.Failure)
			actual.Failure.TerminalJobIDs[0] = "caller-mutated"
			replay, err := (goalstate.Engine{Store: s, Now: at.Add(time.Hour)}).FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: failure.Message})
			if err != nil {
				return err
			}
			require.Equal(t, saved, replay)
			return nil
		}))
	})
}

func TestRetainedGoalIndependentAdmissionWriters(t *testing.T) {
	f := newRetainedFixture(t)
	f.job(t, "tenant", "origin", "G", "failed_app", true)
	other := openRetainedHandle(t, f.path, 5000)
	start := make(chan struct{})
	outcomes := make(chan error, 12)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			db := f.db
			if i%2 == 1 {
				db = other
			}
			outcomes <- (&Runtime{db: db}).withTx(context.Background(), func(tx *sql.Tx) error {
				s := retainedStore(context.Background(), tx)
				if err := s.CheckAdmission(retainedKey("G")); err != nil {
					return err
				}
				return insertRetainedExecution(s.ctx, s.q, "tenant", fmt.Sprintf("new-%02d", i), "G", "", f.at.Add(time.Hour))
			})
		}(i)
	}
	close(start)
	wg.Wait()
	close(outcomes)
	successes := 0
	for err := range outcomes {
		if err == nil {
			successes++
		} else {
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
		}
	}
	require.Equal(t, 1, successes)
	var active int
	require.NoError(t, other.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_id='G' AND archived_at_ns IS NULL`).Scan(&active))
	require.Equal(t, 1, active)
}

func TestRetainedGoalWriterBarrierAndFirstClosureOrders(t *testing.T) {
	for _, first := range []string{"failure", "success"} {
		t.Run(first, func(t *testing.T) {
			f := newRetainedFixture(t)
			f.job(t, "tenant", "origin", "G", "failed_app", true)
			f.job(t, "tenant", "active", "G", "", false)
			other := openRetainedHandle(t, f.path, 5)
			entered := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			apply := func(s retainedGoalStore, operation string) error {
				e := goalstate.Engine{Store: s, Now: f.at.Add(time.Hour)}
				if operation == "failure" {
					_, err := e.FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: "first failure"})
					return err
				}
				_, err := s.q.ExecContext(s.ctx, `UPDATE jobdb_jobs SET archived_at_ns=?,completion_status='success' WHERE tenant_id='tenant' AND job_id='active'`, f.at.Add(time.Hour).UnixNano())
				if err != nil {
					return err
				}
				return e.Complete(retainedJob("active"))
			}
			go func() {
				done <- f.transaction(func(s retainedGoalStore) error { close(entered); <-release; return apply(s, first) })
			}()
			select {
			case <-entered:
			case err := <-done:
				require.NoError(t, err)
				t.Fatal("first writer ended before entering the transaction")
			}
			second := "failure"
			if first == "failure" {
				second = "success"
			}
			called := false
			err := (&Runtime{db: other}).withTx(context.Background(), func(tx *sql.Tx) error { called = true; return apply(retainedStore(context.Background(), tx), second) })
			// The first transaction has reserved the writer without modifying any row.
			// The second independent handle cannot read scheduler facts in its callback.
			close(release)
			firstErr := <-done
			require.NoError(t, firstErr)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), "busy")
			require.False(t, called)
			err = (&Runtime{db: other}).withTx(context.Background(), func(tx *sql.Tx) error { return apply(retainedStore(context.Background(), tx), second) })
			if first == "success" {
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, f.transaction(func(s retainedGoalStore) error {
				g, err := s.Goal(retainedKey("G"))
				if err != nil {
					return err
				}
				if first == "failure" {
					require.Equal(t, jobdb.GoalFailed, g.State)
				} else {
					require.Equal(t, jobdb.GoalSucceeded, g.State)
					require.Equal(t, "active", g.SuccessfulJobID)
				}
				return nil
			}))
		})
	}
}

func TestRetainedGoalIndexedLookups(t *testing.T) {
	f := newRetainedFixture(t)
	for _, tc := range []struct{ sql, index string }{
		{`SELECT job_id FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_id='G' AND archived_at_ns IS NULL`, "jobdb_jobs_goal_active_idx"},
		{`SELECT job_id FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_id='G' AND goal_origin=1`, "jobdb_jobs_goal_origin_unique"},
		{`SELECT job_id FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_id='G' AND goal_closure_state IS NOT NULL`, "jobdb_jobs_goal_closure_unique"},
	} {
		rows, err := f.db.Query(`EXPLAIN QUERY PLAN ` + tc.sql)
		require.NoError(t, err)
		plans := ""
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			plans += detail
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Contains(t, plans, tc.index)
	}
}

func TestRetainedGoalVerificationRefusesIncompleteFacts(t *testing.T) {
	t.Run("read-only-reopen", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.job(t, "tenant", "origin", "G", "failed_app", true)
		before := compatibilitySnapshot(t, f.db)
		reopened := openRetainedHandle(t, f.path, 5000)
		tx, err := reopened.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		require.NoError(t, err)
		require.NoError(t, validateRetainedGoalSchema(context.Background(), tx))
		require.NoError(t, verifyRetainedGoalFacts(context.Background(), tx))
		require.NoError(t, tx.Rollback())
		require.Equal(t, before, compatibilitySnapshot(t, reopened))
	})
	t.Run("missing-origin", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.job(t, "tenant", "orphan", "G", "failed_app", false)
		require.ErrorContains(t, f.transaction(func(s retainedGoalStore) error { return verifyRetainedGoalFacts(s.ctx, s.q.(*sql.Tx)) }), "goal \"G\"")
	})
	t.Run("unclosed-failure", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.job(t, "tenant", "origin", "G", "failed_app", true)
		err := f.transaction(func(s retainedGoalStore) error {
			g, err := s.Goal(retainedKey("G"))
			if err != nil {
				return err
			}
			s.newFailureID = func() (string, error) { return "incomplete", nil }
			_, err = s.InsertGoalFailure(g, jobdb.GoalFailure{Code: "goal_failed", GoalID: "G", Message: "incomplete", FailedAt: f.at.Add(time.Hour), TerminalJobIDs: []string{"origin"}, NonTerminalJobIDs: []string{}})
			if err != nil {
				return err
			}
			return verifyRetainedGoalFacts(s.ctx, s.q.(*sql.Tx))
		})
		require.ErrorContains(t, err, "has no closure")
		var n int
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE record_kind<>'EXECUTION'`).Scan(&n))
		require.Zero(t, n)
	})
	t.Run("legacy-table", func(t *testing.T) {
		f := newRetainedFixture(t)
		_, err := f.db.Exec(`CREATE TABLE jobdb_goals(value TEXT)`)
		require.NoError(t, err)
		require.ErrorContains(t, f.transaction(func(s retainedGoalStore) error { return validateRetainedGoalSchema(s.ctx, s.q.(*sql.Tx)) }), "legacy goal storage remains")
	})
	t.Run("missing-index", func(t *testing.T) {
		f := newRetainedFixture(t)
		_, err := f.db.Exec(`DROP INDEX jobdb_jobs_goal_active_idx`)
		require.NoError(t, err)
		require.ErrorContains(t, f.transaction(func(s retainedGoalStore) error { return validateRetainedGoalSchema(s.ctx, s.q.(*sql.Tx)) }), "jobdb_jobs_goal_active_idx")
	})
}

func TestRetainedGoalConcurrentFailureDeduplicates(t *testing.T) {
	for _, equal := range []bool{true, false} {
		t.Run(fmt.Sprintf("equal=%t", equal), func(t *testing.T) {
			f := newRetainedFixture(t)
			f.job(t, "tenant", "origin", "G", "failed_app", true)
			other := openRetainedHandle(t, f.path, 5000)
			type result struct {
				g   jobdb.GoalInfo
				err error
			}
			outcomes := make(chan result, 2)
			start := make(chan struct{})
			for i, db := range []*sql.DB{f.db, other} {
				go func(i int, db *sql.DB) {
					<-start
					message := "same"
					if !equal && i == 1 {
						message = "different"
					}
					var g jobdb.GoalInfo
					err := (&Runtime{db: db}).withTx(context.Background(), func(tx *sql.Tx) error {
						var err error
						g, err = (goalstate.Engine{Store: retainedStore(context.Background(), tx), Now: f.at.Add(time.Hour)}).FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: message})
						return err
					})
					outcomes <- result{g, err}
				}(i, db)
			}
			close(start)
			a, b := <-outcomes, <-outcomes
			if equal {
				require.NoError(t, a.err)
				require.NoError(t, b.err)
				require.Equal(t, a.g, b.g)
			} else {
				if a.err == nil {
					require.ErrorIs(t, b.err, jobdb.ErrGoalConflict)
				} else {
					require.ErrorIs(t, a.err, jobdb.ErrGoalConflict)
					require.NoError(t, b.err)
				}
			}
			var n int
			require.NoError(t, other.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE record_kind='GOAL_FAILURE'`).Scan(&n))
			require.Equal(t, 1, n)
		})
	}
}

func TestRetainedGoalAdmissionFailureCommitOrders(t *testing.T) {
	for _, first := range []string{"admission", "failure"} {
		t.Run(first, func(t *testing.T) {
			f := newRetainedFixture(t)
			f.job(t, "tenant", "origin", "G", "failed_app", true)
			other := openRetainedHandle(t, f.path, 5)
			operation := func(s retainedGoalStore, kind string) error {
				if kind == "admission" {
					if err := s.CheckAdmission(retainedKey("G")); err != nil {
						return err
					}
					return insertRetainedExecution(s.ctx, s.q, "tenant", "new", "G", "", f.at.Add(time.Hour))
				}
				_, err := (goalstate.Engine{Store: s, Now: f.at.Add(2 * time.Hour)}).FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: "closed"})
				return err
			}
			second := "failure"
			if first == "failure" {
				second = "admission"
			}
			staged := make(chan error, 1)
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- f.transaction(func(s retainedGoalStore) error {
					err := operation(s, first)
					staged <- err
					if err != nil {
						return err
					}
					<-release
					return nil
				})
			}()
			var stagedErr error
			select {
			case stagedErr = <-staged:
			case err := <-done:
				require.NoError(t, err)
				t.Fatal("first writer ended before staging its operation")
			}
			if stagedErr != nil {
				close(release)
				<-done
				require.NoError(t, stagedErr)
				return
			}
			called := false
			err := (&Runtime{db: other}).withTx(context.Background(), func(tx *sql.Tx) error {
				called = true
				return operation(retainedStore(context.Background(), tx), second)
			})
			close(release)
			require.NoError(t, <-done)
			require.Error(t, err)
			require.False(t, called)
			err = (&Runtime{db: other}).withTx(context.Background(), func(tx *sql.Tx) error { return operation(retainedStore(context.Background(), tx), second) })
			if first == "failure" {
				require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, f.transaction(func(s retainedGoalStore) error {
				g, err := s.Goal(retainedKey("G"))
				if err != nil {
					return err
				}
				require.Equal(t, jobdb.GoalFailed, g.State)
				j, err := s.Job(retainedJob("new"))
				if first == "failure" {
					require.ErrorIs(t, err, jobdb.ErrJobNotFound)
					require.Empty(t, g.Failure.NonTerminalJobIDs)
				} else {
					require.NoError(t, err)
					require.False(t, j.Terminal)
					require.Equal(t, []string{"new"}, g.Failure.NonTerminalJobIDs)
				}
				return nil
			}))
		})
	}
}

func TestRetainedGoalWaitClosureCommitOrders(t *testing.T) {
	for _, first := range []string{"registration", "closure"} {
		t.Run(first, func(t *testing.T) {
			f := newRetainedFixture(t)
			f.job(t, "tenant", "origin", "G", "failed_app", true)
			f.job(t, "tenant", "consumer", "W", "", true)
			other := openRetainedHandle(t, f.path, 5)
			operation := func(s retainedGoalStore, kind string) error {
				e := goalstate.Engine{Store: s, Now: f.at.Add(time.Hour)}
				if kind == "closure" {
					_, err := e.FailWithRecord(jobdb.FailGoalRequest{GoalKey: retainedKey("G"), Message: "durable"})
					return err
				}
				_, err := e.Register(retainedJob("consumer"), jobdb.DependencyWait{WaitID: "target", Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: "G"}, Condition: jobdb.JobPrereqSuccess}}}, false)
				return err
			}
			second := "closure"
			if first == "closure" {
				second = "registration"
			}
			staged := make(chan error, 1)
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- f.transaction(func(s retainedGoalStore) error {
					err := operation(s, first)
					staged <- err
					if err != nil {
						return err
					}
					<-release
					return nil
				})
			}()
			var stagedErr error
			select {
			case stagedErr = <-staged:
			case err := <-done:
				require.NoError(t, err)
				t.Fatal("first writer ended before staging its operation")
			}
			if stagedErr != nil {
				close(release)
				<-done
				require.NoError(t, stagedErr)
				return
			}
			err := (&Runtime{db: other}).withTx(context.Background(), func(tx *sql.Tx) error { return operation(retainedStore(context.Background(), tx), second) })
			close(release)
			require.NoError(t, <-done)
			require.Error(t, err)
			require.NoError(t, (&Runtime{db: other}).withTx(context.Background(), func(tx *sql.Tx) error { return operation(retainedStore(context.Background(), tx), second) }))
			reopened := openRetainedHandle(t, f.path, 5000)
			require.NoError(t, (&Runtime{db: reopened}).withTx(context.Background(), func(tx *sql.Tx) error {
				s := retainedStore(context.Background(), tx)
				g, err := s.Goal(retainedKey("G"))
				if err != nil {
					return err
				}
				w, err := s.Wait(jobdb.DependencyWaitKey{JobKey: retainedJob("consumer"), WaitID: "target"})
				if err != nil {
					return err
				}
				require.Equal(t, jobdb.DependencyFailed, w.State)
				require.Equal(t, g.Failure, w.Failure)
				waits, err := s.TargetWaits("tenant", jobdb.DependencyTarget{GoalID: "G"})
				require.NoError(t, err)
				require.Empty(t, waits)
				return verifyRetainedGoalFacts(s.ctx, tx)
			}))
		})
	}
}

func TestRetainedGoalVerificationRejectsContradictoryClosure(t *testing.T) {
	t.Run("primary-winner-on-another-row", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.job(t, "tenant", "origin", "G", "failed_app", true)
		f.job(t, "tenant", "winner", "G", "success", false)
		_, err := f.db.Exec(`UPDATE jobdb_jobs SET goal_closure_state='SUCCEEDED',goal_closed_at_ns=?,goal_winner_job_id='winner',goal_closure_origin='migrated' WHERE job_id='origin'`, f.at.Add(time.Hour).UnixNano())
		require.NoError(t, err)
		require.ErrorIs(t, f.transaction(func(s retainedGoalStore) error { _, err := s.Goal(retainedKey("G")); return err }), jobdb.ErrInvalidJobRecord)
	})
	t.Run("wrong-failure-provenance", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.job(t, "tenant", "origin", "G", "failed_app", true)
		err := f.transaction(func(s retainedGoalStore) error {
			g, err := s.Goal(retainedKey("G"))
			if err != nil {
				return err
			}
			s.newFailureID = func() (string, error) { return "failure", nil }
			key, err := s.InsertGoalFailure(g, jobdb.GoalFailure{Code: "goal_failed", GoalID: "G", Message: "native", FailedAt: f.at.Add(time.Hour), TerminalJobIDs: []string{"origin"}, NonTerminalJobIDs: []string{}})
			if err != nil {
				return err
			}
			_, err = s.q.ExecContext(s.ctx, `UPDATE jobdb_jobs SET goal_closure_state='FAILED',goal_closed_at_ns=?,goal_closure_origin='migrated' WHERE job_id=?`, f.at.Add(time.Hour).UnixNano(), key.JobId)
			if err != nil {
				return err
			}
			return verifyRetainedGoalFacts(s.ctx, s.q.(*sql.Tx))
		})
		require.ErrorIs(t, err, jobdb.ErrInvalidJobRecord)
	})
}
