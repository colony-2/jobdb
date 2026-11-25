package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func storedQuery(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	rows, err := db.Query(q)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	out := []string{}
	for rows.Next() {
		values := make([]any, len(cols))
		p := make([]any, len(cols))
		for i := range values {
			p[i] = &values[i]
		}
		require.NoError(t, rows.Scan(p...))
		out = append(out, fmt.Sprintf("%#v", values))
	}
	require.NoError(t, rows.Err())
	return out
}
func schemaSnapshot(t *testing.T, db *sql.DB) []string {
	return storedQuery(t, db, `SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name`)
}

func TestGoalMigrationConvertsPreservingRetainedState(t *testing.T) {
	for _, test := range []struct {
		version          int
		events, receipts bool
	}{{11, false, true}, {11, true, true}, {12, true, true}} {
		t.Run(fmt.Sprintf("%d/%t/%t", test.version, test.events, test.receipts), func(t *testing.T) {
			path, db := migrationFixture(t, test.version, test.events, test.receipts, true)
			_, err := db.Exec(`CREATE TABLE conversion_saved(value BLOB);INSERT INTO conversion_saved VALUES(x'0001ff'); CREATE INDEX application_jobs_idx ON jobdb_jobs(job_type); CREATE TRIGGER application_jobs_trigger AFTER UPDATE OF payload ON jobdb_jobs BEGIN INSERT INTO conversion_saved VALUES(NEW.payload); END`)
			require.NoError(t, err)
			before := compatibilitySnapshot(t, db)
			var columns []string
			rows, err := db.Query(`SELECT name FROM pragma_table_info('jobdb_jobs') ORDER BY cid`)
			require.NoError(t, err)
			for rows.Next() {
				var col string
				require.NoError(t, rows.Scan(&col))
				columns = append(columns, `"`+col+`"`)
			}
			require.NoError(t, rows.Close())
			legacyJobs := storedQuery(t, db, `SELECT rowid,`+strings.Join(columns, ",")+` FROM jobdb_jobs ORDER BY tenant_id,job_id`)
			report, err := MigrateGoals(context.Background(), path)
			require.NoError(t, err, "%+v", report.Issues)
			require.Equal(t, "MIGRATED", report.Verdict)
			var version, n int
			require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
			require.Equal(t, 15, version)
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('jobdb_goals','jobdb_goal_candidates')`).Scan(&n))
			require.Zero(t, n)
			require.Equal(t, legacyJobs, storedQuery(t, db, `SELECT rowid,`+strings.Join(columns, ",")+` FROM jobdb_jobs WHERE record_kind='EXECUTION' ORDER BY tenant_id,job_id`))
			after := compatibilitySnapshot(t, db)
			for table, values := range before {
				if table != "jobdb_jobs" && table != "jobdb_goals" && table != "jobdb_goal_candidates" {
					require.Equal(t, values, after[table], table)
				}
			}
			for _, name := range []string{"application_jobs_idx", "application_jobs_trigger"} {
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name=?`, name).Scan(&n))
				require.Equal(t, 1, n)
			}
			store := retainedQueryStore(context.Background(), db)
			cross, err := store.Goal(retainedKey("C"))
			require.NoError(t, err)
			require.Equal(t, "producer", cross.SuccessfulJobID)
			producer, err := store.Job(retainedJob("producer"))
			require.NoError(t, err)
			require.Equal(t, "H", producer.GoalID)
			failed, err := store.Goal(retainedKey("F"))
			require.NoError(t, err)
			require.Equal(t, "  saved\n保存  ", failed.Failure.Message)
			require.Equal(t, []string{"failed-origin", "producer"}, failed.Failure.TerminalJobIDs)
			marker := retainedJob(failed.FailureJobID)
			require.Regexp(t, `^C2[0-9A-Za-z]{27}$`, marker.JobId)
			job, err := store.Job(marker)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobRecordMigratedGoalFailure, job.RecordKind)
			for _, p := range report.Goals {
				if p.GoalKey == failed.GoalKey {
					require.Equal(t, marker.JobId, p.ClosureJobID)
				}
			}
			// Reopen through the real runtime. Fixture executions have no chapters;
			// protected inspection and exact replay must not need them.
			u := url.URL{Scheme: "file", Path: path}
			rt, err := NewFromConfig(context.Background(), Config{Projects: celltest.UnrestrictedProjects{}, DSN: u.String(), BlobDir: filepath.Join(t.TempDir(), "blobs")})
			require.NoError(t, err)
			defer rt.Close(context.Background())
			info, err := rt.GetJob(context.Background(), marker)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobRecordMigratedGoalFailure, info.RecordKind)
			data, err := info.Data.GetData()
			require.Nil(t, data)
			require.ErrorIs(t, err, jobdb.ErrGoalFailed)
			_, err = rt.DeleteJobs(context.Background(), jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{marker.JobId}})
			require.NoError(t, err)
			replay, err := rt.FailGoal(context.Background(), jobdb.FailGoalRequest{GoalKey: failed.GoalKey, Message: failed.Failure.Message})
			require.NoError(t, err)
			require.Equal(t, failed, replay)
			_, err = rt.FailGoal(context.Background(), jobdb.FailGoalRequest{GoalKey: failed.GoalKey, Message: "different"})
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			// All inherited primary blockers, including hidden rows, must terminate.
			require.ErrorIs(t, store.CheckAdmission(retainedKey("G")), jobdb.ErrGoalConflict)
			require.NoError(t, rt.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: retainedJob("active")}))
			require.ErrorIs(t, store.CheckAdmission(retainedKey("G")), jobdb.ErrGoalConflict)
			require.NoError(t, rt.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: retainedJob("hidden-active")}))
			require.NoError(t, store.CheckAdmission(retainedKey("G")))
			// Frozen settled bindings and original pending target identities survive.
			settled, err := store.Wait(jobdb.DependencyWaitKey{JobKey: retainedJob("settled"), WaitID: "saved"})
			require.NoError(t, err)
			require.Equal(t, "producer", settled.Dependencies[0].SatisfiedByJobID)
			require.NoError(t, rt.Close(context.Background()))
			stable := compatibilitySnapshot(t, db)
			require.NoError(t, db.Close())
			again, err := MigrateGoals(context.Background(), path)
			require.NoError(t, err)
			require.Equal(t, "CURRENT", again.Verdict)
			reopened, err := sql.Open(sqliteDriverName, u.String())
			require.NoError(t, err)
			defer reopened.Close()
			require.Equal(t, stable, compatibilitySnapshot(t, reopened))

		})
	}
}

func TestGoalMigrationRollsBackEveryBoundary(t *testing.T) {
	for _, stage := range []string{"locked", "inspected", "copied", "closures", "verified", "marked"} {
		t.Run(stage, func(t *testing.T) {
			path, db := migrationFixture(t, 12, true, true, true)
			before, schema := compatibilitySnapshot(t, db), schemaSnapshot(t, db)
			injected := errors.New("conversion interrupted")
			_, err := migrateGoals(context.Background(), path, func(at string) error {
				if at == stage {
					return injected
				}
				return nil
			})
			require.ErrorIs(t, err, injected)
			require.Equal(t, before, compatibilitySnapshot(t, db))
			require.Equal(t, schema, schemaSnapshot(t, db))
			var version int
			require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
			require.Equal(t, 12, version)
			report, err := MigrateGoals(context.Background(), path)
			require.NoError(t, err)
			require.Equal(t, "MIGRATED", report.Verdict)
		})
	}
}

func TestGoalMigrationRechecksLockedInputsAndExcludesWriters(t *testing.T) {
	t.Run("stale-preflight", func(t *testing.T) {
		path, db := migrationFixture(t, 11, false, true, true)
		_, err := CheckGoalMigration(context.Background(), path)
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE jobdb_goals SET cell_repo='example.test/incompatible' WHERE tenant_id='tenant' AND goal_id='G'`)
		require.NoError(t, err)
		before := compatibilitySnapshot(t, db)
		report, err := MigrateGoals(context.Background(), path)
		require.ErrorIs(t, err, ErrGoalMigrationBlocked)
		require.True(t, hasMigrationIssue(report, "missing_compatible_origin"))
		require.Equal(t, before, compatibilitySnapshot(t, db))
	})
	t.Run("writer-held", func(t *testing.T) {
		path, db := migrationFixture(t, 12, true, true, true)
		_, err := db.Exec(`PRAGMA journal_mode=WAL`)
		require.NoError(t, err)
		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.Exec(`UPDATE jobdb_jobs SET job_id=job_id WHERE 0`)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = MigrateGoals(ctx, path)
		require.Error(t, err)
		require.NoError(t, tx.Rollback())
		var version int
		require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
		require.Equal(t, 12, version)
	})
	t.Run("conversion-held", func(t *testing.T) {
		path, db := migrationFixture(t, 12, true, true, true)
		_, err := db.Exec(`PRAGMA journal_mode=WAL`)
		require.NoError(t, err)
		locked, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, e := migrateGoals(context.Background(), path, func(stage string) error {
				if stage == "locked" {
					close(locked)
					<-release
				}
				return nil
			})
			done <- e
		}()
		select {
		case <-locked:
		case <-time.After(10 * time.Second):
			t.Fatal("conversion did not acquire exclusive lock")
		}
		_, err = db.Exec(`PRAGMA busy_timeout=1`)
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE jobdb_jobs SET updated_at_ns=updated_at_ns+1 WHERE job_id='active'`)
		require.Error(t, err)
		close(release)
		require.NoError(t, <-done)
	})
}

// Keep the shared transition import exercised by conversion inspection as well
// as public runtime tests: migrated primary-only candidates are never attached.
var _ goalstate.Store = retainedGoalStore{}

func TestGoalMigrationRealLegacyRuntimePreservesReaders(t *testing.T) {
	for _, format := range []int{11, 12} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "real-legacy.db")
			db, err := sql.Open(sqliteDriverName, path)
			require.NoError(t, err)
			fixture, err := os.ReadFile(filepath.Join("testdata", "goal-runtime-11.sql"))
			require.NoError(t, err)
			_, err = db.Exec(string(fixture))
			require.NoError(t, err)
			_, err = db.Exec(fmt.Sprintf("PRAGMA user_version=%d", format))
			require.NoError(t, err)
			before := compatibilitySnapshot(t, db)
			var goalID string
			require.NoError(t, db.QueryRow(`SELECT goal_id FROM jobdb_jobs WHERE tenant_id='tenant' AND job_id='source'`).Scan(&goalID))
			require.NoError(t, db.Close())
			report, err := MigrateGoals(context.Background(), path)
			require.NoError(t, err, "%+v", report.Issues)
			ctx := runtimetest.PrimaryGoalContext(context.Background())
			r, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: path})
			require.NoError(t, err)
			defer r.Close(ctx)
			after := compatibilitySnapshot(t, r.db)
			for table, values := range before {
				if table != "jobdb_jobs" && table != "jobdb_goals" && table != "jobdb_goal_candidates" {
					require.Equal(t, values, after[table], table)
				}
			}
			source := jobdb.JobKey{TenantId: "tenant", JobId: "source"}
			chapter, err := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: source, Ordinal: 1})
			require.NoError(t, err)
			require.Equal(t, int64(1), chapter.Ordinal)
			assertStartArtifact(t, ctx, r, source, "saved.bin", []byte("legacy artifact\x00"))
			info, err := r.GetJob(ctx, source)
			require.NoError(t, err)
			require.Equal(t, "failed_app", info.CompletionStatus)
			_, err = info.Data.GetData()
			require.Error(t, err)
			replay, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "selected", GoalID: goalID, Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			require.Equal(t, goalID, replay.GoalID)
			goal, err := r.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: goalID})
			require.NoError(t, err)
			require.NotEmpty(t, goal.FailureJobID)
			marker, err := r.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: goal.FailureJobID})
			require.NoError(t, err)
			require.Equal(t, jobdb.JobRecordMigratedGoalFailure, marker.RecordKind)
			wait, err := r.GetDependencyWait(ctx, jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "consumer"}, WaitID: "submission"})
			require.NoError(t, err)
			require.Equal(t, goal.Failure, wait.Failure)
			configuration, err := r.GetTenantConfiguration(ctx, "tenant")
			require.NoError(t, err)
			require.Equal(t, json.RawMessage(`{"exact":9007199254740993}`), configuration.Values["retained"])
		})
	}
}

func TestGoalMigrationConvertedCellRetirement(t *testing.T) {
	path, db := migrationFixture(t, 12, true, true, true)
	report, err := MigrateGoals(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, "MIGRATED", report.Verdict)
	require.NoError(t, db.Close())
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	u := url.URL{Scheme: "file", Path: path}
	r, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DSN: u.String(), BlobDir: filepath.Join(t.TempDir(), "blobs")})
	require.NoError(t, err)
	defer r.Close(ctx)
	cell, err := r.GetCell(ctx, jobdb.CellKey{TenantID: "tenant", CellID: "cell"})
	require.NoError(t, err)
	req := jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: "cell"}, IfMatch: fmt.Sprintf("%q", cell.Revision)}
	_, err = r.RetireCell(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrCellBusy)
	// Drain actual inherited members through public cancellation, including hidden
	// rows, then remove only unpublished reservations using their public cleanup.
	for _, id := range []string{"active", "hidden-active", "secondary", "consumer", "settled"} {
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: id}}))
	}
	for _, id := range []string{"never-published", "pending-recovery"} {
		require.NoError(t, r.DiscardJobPreparation(ctx, jobdb.JobKey{TenantId: "tenant", JobId: id}))
	}
	for _, goal := range []string{"U", "W", "S"} {
		_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: goal}, Message: "closed"})
		require.NoError(t, err)
	}
	// G has no active member, but its hidden retained origin keeps the recovery
	// gap open. Retirement must still conflict until explicit closure.
	gap, err := r.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: "G"})
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalOpen, gap.State)
	page, err := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant", States: []jobdb.GoalState{jobdb.GoalOpen}})
	require.NoError(t, err)
	require.Len(t, page.Goals, 1)
	require.Zero(t, page.Goals[0].NonTerminalCandidateCount)
	_, err = r.RetireCell(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrCellBusy)
	_, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: gap.GoalKey, Message: "close gap"})
	require.NoError(t, err)
	retired, err := r.RetireCell(ctx, req)
	require.NoError(t, err)
	require.Equal(t, jobdb.CellRetired, retired.State)
	// Same opaque cell/goal strings in the other tenant do not prevent retirement.
	other, err := r.GetGoal(ctx, jobdb.GoalKey{TenantId: "other", GoalID: "G"})
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalOpen, other.State)
}
