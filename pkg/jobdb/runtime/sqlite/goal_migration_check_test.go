package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func migrationFixture(t *testing.T, version int, events, receipts, rich bool) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "saved ?#% legacy.db")
	u := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open(sqliteDriverName, u.String())
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	for _, file := range []struct {
		name string
		use  bool
	}{{"goal-format-11.sql", true}, {"goal-lease-events.sql", events}, {"goal-receipts.sql", receipts}} {
		if file.use {
			raw, err := os.ReadFile(filepath.Join("testdata", file.name))
			require.NoError(t, err)
			_, err = db.Exec(string(raw))
			require.NoError(t, err)
		}
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA user_version=%d", version))
	require.NoError(t, err)
	if rich {
		populateMigrationFixture(t, db)
	}
	return path, db
}

func populateMigrationFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	exec := func(q string, args ...any) { t.Helper(); _, err := db.Exec(q, args...); require.NoError(t, err) }
	// Current presentation/repository can differ from immutable captured facts.
	for _, tenant := range []string{"tenant", "other"} {
		exec(`INSERT INTO jobdb_cells VALUES(?,?,'renamed','example.test/current','',x'7b7d','ACTIVE','1',?,?,NULL)`, tenant, "cell", at.UnixNano(), at.UnixNano())
	}
	job := func(tenant, id, goal, status string, hidden bool) {
		var archived, hiddenAt any
		if status != "" {
			archived = at.Add(time.Hour).UnixNano()
		}
		if hidden {
			hiddenAt = at.Add(2 * time.Hour).UnixNano()
		}
		exec(`INSERT INTO jobdb_jobs(tenant_id,job_id,goal_id,cell_id,cell_name,cell_repo,job_type,route_job_type,initial_payload_digest,available_at_ns,created_at_ns,updated_at_ns,archived_at_ns,hidden_at_ns,completion_status) VALUES(?,?,?,'cell','original','example.test/original','work','work','digest',?,?,?,?,?,?)`, tenant, id, goal, at.UnixNano(), at.UnixNano(), at.UnixNano(), archived, hiddenAt, status)
	}
	for _, j := range []struct {
		id, goal, status string
		hidden           bool
	}{{"z-old", "G", "failed_app", false}, {"a-old", "G", "cancelled", true}, {"active", "G", "", false}, {"hidden-active", "G", "", true}, {"producer", "H", "success", false}, {"cross-origin", "C", "failed_app", false}, {"failed-origin", "F", "failed_app", true}, {"secondary", "U", "", false}, {"consumer", "W", "", false}, {"settled", "S", "", false}} {
		job("tenant", j.id, j.goal, j.status, j.hidden)
	}
	job("other", "active", "G", "", true)
	goal := func(tenant, id string, state jobdb.GoalState, winner string, failure *jobdb.GoalFailure) {
		g := jobdb.GoalInfo{GoalKey: jobdb.GoalKey{TenantId: tenant, GoalID: id}, State: state, Revision: 1, CreatedAt: at.Add(time.Minute), UpdatedAt: at.Add(time.Minute), SuccessfulJobID: winner, Failure: failure}
		if state != jobdb.GoalOpen {
			closed := at.Add(time.Hour)
			g.ClosedAt = &closed
			g.UpdatedAt = closed
			g.Revision = 2
		}
		raw, err := json.Marshal(g)
		require.NoError(t, err)
		// Legacy storage omits ownership in the JSON; columns supply it.
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &fields))
		delete(fields, "cell")
		delete(fields, "cellId")
		delete(fields, "cellRepo")
		raw, err = json.Marshal(fields)
		require.NoError(t, err)
		exec(`INSERT INTO jobdb_goals VALUES(?,?,?,?,?,?,'original','example.test/original',?)`, tenant, id, state, g.CreatedAt.UnixNano(), g.UpdatedAt.UnixNano(), "cell", raw)
	}
	for _, g := range []string{"G", "U", "W", "S"} {
		goal("tenant", g, jobdb.GoalOpen, "", nil)
	}
	goal("other", "G", jobdb.GoalOpen, "", nil)
	goal("tenant", "H", jobdb.GoalSucceeded, "producer", nil)
	goal("tenant", "C", jobdb.GoalSucceeded, "producer", nil)
	goal("tenant", "F", jobdb.GoalFailed, "", &jobdb.GoalFailure{Code: "goal_failed", GoalID: "F", Message: "  saved\n保存  ", FailedAt: at.Add(time.Hour), TerminalJobIDs: []string{"failed-origin", "producer"}, NonTerminalJobIDs: []string{}})
	for _, c := range []struct{ goal, job string }{{"G", "secondary"}, {"C", "producer"}, {"F", "producer"}} {
		exec(`INSERT INTO jobdb_goal_candidates VALUES('tenant',?,?,?)`, c.goal, c.job, at.UnixNano())
	}
	wait := func(w jobdb.DependencyWaitInfo) {
		raw, err := json.Marshal(w)
		require.NoError(t, err)
		exec(`INSERT INTO jobdb_dependency_waits VALUES(?,?,?,?,?)`, w.JobKey.TenantId, w.JobKey.JobId, w.WaitID, w.State, raw)
	}
	wait(jobdb.DependencyWaitInfo{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "consumer"}, WaitID: "submission", State: jobdb.DependencyPending, Dependencies: []jobdb.DependencyResolution{{Target: jobdb.DependencyTarget{GoalID: "G"}, Condition: jobdb.JobPrereqSuccess, State: jobdb.DependencyPending}, {Target: jobdb.DependencyTarget{JobID: "producer"}, Condition: jobdb.JobPrereqSuccess, State: jobdb.DependencySatisfied, SatisfiedByJobID: "producer"}}})
	exec(`INSERT INTO jobdb_dependency_targets VALUES('tenant','consumer','submission','goal','G')`)
	exec(`UPDATE jobdb_jobs SET dependency_wait_id='submission' WHERE tenant_id='tenant' AND job_id='consumer'`)
	wait(jobdb.DependencyWaitInfo{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "settled"}, WaitID: "saved", State: jobdb.DependencySatisfied, Dependencies: []jobdb.DependencyResolution{{Target: jobdb.DependencyTarget{GoalID: "C"}, Condition: jobdb.JobPrereqSuccess, State: jobdb.DependencySatisfied, SatisfiedByJobID: "producer"}}})
	prep := func(id, goal, selected string, shared bool, deps []jobdb.Dependency) string {
		p, err := goalstate.SubmitPreparation(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, GoalID: selected, JobType: "work", Dependencies: deps}}, "")
		require.NoError(t, err)
		raw, err := json.Marshal(deps)
		require.NoError(t, err)
		exec(`INSERT INTO jobdb_goal_preparations VALUES('tenant',?,NULL,?,?,?,'original','example.test/original','cell','original','example.test/original','','',?)`, id, goal, shared, p.Descriptor, raw)
		return p.Descriptor
	}
	prep("never-published", "unborn", "", false, []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: "G"}, Condition: jobdb.JobPrereqSuccess}})
	prep("pending-recovery", "G", "G", true, nil)
	descriptor := prep("active", "G", "G", true, nil)
	exec(`INSERT INTO jobdb_submission_receipts VALUES('tenant','active',?,'original','G','cell','original','example.test/original')`, descriptor)
	exec(`CREATE TABLE unrelated_saved(value BLOB);INSERT INTO unrelated_saved VALUES(x'0001ff')`)
}

func TestGoalMigrationCheckFrozenLegacyReadOnly(t *testing.T) {
	for _, version := range []int{11, 12} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			path, db := migrationFixture(t, version, true, true, true)
			before := compatibilitySnapshot(t, db)
			bytesBefore, err := os.ReadFile(path)
			require.NoError(t, err)
			report, err := CheckGoalMigration(context.Background(), path)
			require.NoError(t, err)
			require.Equal(t, "CONVERTIBLE_WITH_NOTICES", report.Verdict)
			require.Equal(t, version, report.SourceFormat)
			require.Equal(t, 13, report.TargetFormat)
			require.Len(t, report.DroppedMemberships, 3)
			require.Len(t, report.Tenants, 2)
			require.Equal(t, GoalMigrationTenant{TenantID: "tenant", Jobs: 10, HiddenJobs: 3, Goals: 7, DroppedMemberships: 3, PendingWaitsAffected: 1, PreparationsAffected: 2, LiveOverlaps: 1}, report.Tenants[1])
			plans := map[string]GoalMigrationPlan{}
			for _, p := range report.Goals {
				plans[p.GoalKey.TenantId+"/"+p.GoalKey.GoalID] = p
			}
			require.Equal(t, "a-old", plans["tenant/G"].OriginJobID)
			require.Equal(t, []string{"active", "hidden-active"}, plans["tenant/G"].NonTerminalJobIDs)
			require.Equal(t, "producer", plans["tenant/C"].SuccessfulJobID)
			require.Equal(t, "cross-origin", plans["tenant/C"].ClosureJobID)
			require.True(t, plans["tenant/F"].CreateFailureRecord)
			require.Empty(t, plans["tenant/F"].ClosureJobID)
			require.NotEqual(t, plans["tenant/G"].OriginalCreatedAt, plans["tenant/G"].DerivedCreatedAt)
			_, published := plans["tenant/unborn"]
			require.False(t, published)
			for _, code := range []string{"cross_primary_winner", "live_overlap", "pending_wait_loses_secondary", "preparation_loses_secondary", "migrated_failure_record", "derived_discovery_metadata"} {
				require.True(t, hasMigrationIssue(report, code), code)
			}
			require.Equal(t, before, compatibilitySnapshot(t, db))
			bytesAfter, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, bytesBefore, bytesAfter)
			entries, err := os.ReadDir(filepath.Dir(path))
			require.NoError(t, err)
			require.Len(t, entries, 1, "checking must not create ancillary stores")
			replay, err := CheckGoalMigration(context.Background(), path)
			require.NoError(t, err)
			require.Equal(t, report, replay)
		})
	}
}

func hasMigrationIssue(r GoalMigrationReport, code string) bool {
	for _, i := range r.Issues {
		if i.Code == code {
			return true
		}
	}
	return false
}

func TestGoalMigrationCheckSupportedOptionalExtensions(t *testing.T) {
	for _, tc := range []struct {
		version          int
		events, receipts bool
	}{{11, false, false}, {11, true, false}, {11, false, true}, {11, true, true}, {12, true, false}, {12, true, true}} {
		t.Run(fmt.Sprintf("%d/events=%v/receipts=%v", tc.version, tc.events, tc.receipts), func(t *testing.T) {
			path, db := migrationFixture(t, tc.version, tc.events, tc.receipts, false)
			before := compatibilitySnapshot(t, db)
			r, err := CheckGoalMigration(nil, path)
			require.NoError(t, err)
			require.Equal(t, "CONVERTIBLE", r.Verdict)
			require.Equal(t, before, compatibilitySnapshot(t, db))
		})
	}
}

func TestGoalMigrationCheckPreservesUnrelatedSchema(t *testing.T) {
	path, db := migrationFixture(t, 11, false, false, false)
	_, err := db.Exec(`CREATE TABLE application_jobdb_goals_notes(value TEXT DEFAULT 'jobdb_goal_candidates');
CREATE VIEW unrelated_labels AS SELECT 'jobdb_goals' AS value /* jobdb_goal_candidates */;
INSERT INTO application_jobdb_goals_notes DEFAULT VALUES`)
	require.NoError(t, err)
	before := compatibilitySnapshot(t, db)
	r, err := CheckGoalMigration(nil, path)
	require.NoError(t, err)
	require.Equal(t, "CONVERTIBLE", r.Verdict)
	require.Equal(t, before, compatibilitySnapshot(t, db))
}

func TestGoalMigrationCheckBlocksInconsistentInputs(t *testing.T) {
	for _, tc := range []struct{ name, query, code string }{
		{"unsupported-format", "PRAGMA user_version=9", "unsupported_format"},
		{"current-marker-with-legacy-schema", "PRAGMA user_version=13", "inconsistent_schema"},
		{"missing-table", "DROP TABLE jobdb_goal_candidates", "inconsistent_schema"},
		{"required-format12-events", "DROP TABLE jobdb_lease_events;PRAGMA user_version=12", "inconsistent_schema"},
		{"malformed-optional-receipts", "DROP TABLE jobdb_submission_receipts;CREATE TABLE jobdb_submission_receipts(value TEXT)", "inconsistent_schema"},
		{"receipt-view-collision", "DROP TABLE jobdb_submission_receipts;CREATE VIEW jobdb_submission_receipts AS SELECT 1", "inconsistent_schema"},
		{"dependent-view", "CREATE VIEW saved_goal_view AS SELECT * FROM jobdb_goals", "unsupported_schema_reference"},
		{"misleading-object-name", "CREATE TRIGGER jobdb_goals_unrecognized AFTER INSERT ON jobdb_jobs BEGIN DELETE FROM jobdb_goals WHERE goal_id='unused'; END", "unsupported_schema_reference"},
		{"missing-candidate-job", "INSERT INTO jobdb_goal_candidates VALUES('tenant','G','absent',1)", "missing_candidate_job"},
		{"missing-primary-goal", "UPDATE jobdb_jobs SET goal_id='missing' WHERE tenant_id='other'", "missing_primary_goal"},
		{"malformed-goal", "UPDATE jobdb_goals SET payload=x'7b' WHERE tenant_id='tenant' AND goal_id='G'", "invalid_goal_payload"},
		{"invalid-utf8-failure-message", "UPDATE jobdb_goals SET payload=CAST(replace(CAST(payload AS TEXT),'saved',CAST(x'ff' AS TEXT)) AS BLOB) WHERE tenant_id='tenant' AND goal_id='F'", "invalid_goal_payload"},
		{"invalid-utf8-wait-identity", "UPDATE jobdb_dependency_waits SET payload=CAST(replace(CAST(payload AS TEXT),'producer',CAST(x'ff' AS TEXT)) AS BLOB) WHERE job_id='settled'", "invalid_wait_payload"},
		{"invalid-utf8-captured-owner", "UPDATE jobdb_jobs SET cell_name=CAST(x'ff' AS TEXT) WHERE job_id='z-old'", "invalid_job_identity"},
		{"goal-payload-identity", "UPDATE jobdb_goals SET payload=json_set(payload,'$.goalKey.goalId','changed') WHERE tenant_id='tenant' AND goal_id='G'", "goal_identity_mismatch"},
		{"goal-state-columns", "UPDATE jobdb_goals SET state='FAILED' WHERE tenant_id='tenant' AND goal_id='G'", "goal_columns_mismatch"},
		{"missing-compatible-origin", "UPDATE jobdb_goals SET cell_repo='example.test/other' WHERE tenant_id='tenant' AND goal_id='G'", "missing_compatible_origin"},
		{"broken-winner", "UPDATE jobdb_goals SET payload=json_set(payload,'$.successfulJobId','absent') WHERE tenant_id='tenant' AND goal_id='C'", "invalid_success_witness"},
		{"invalid-failure-message", "UPDATE jobdb_goals SET payload=json_set(payload,'$.failure.message',' ') WHERE tenant_id='tenant' AND goal_id='F'", "invalid_failure_snapshot"},
		{"missing-failure-history", "UPDATE jobdb_goals SET payload=json_set(payload,'$.failure.terminalJobIds[0]','absent') WHERE tenant_id='tenant' AND goal_id='F'", "invalid_failure_snapshot"},
		{"missing-subscription-index", "DELETE FROM jobdb_dependency_targets", "missing_target_index"},
		{"stale-subscription-index", "INSERT INTO jobdb_dependency_targets VALUES('tenant','settled','saved','goal','C')", "stale_target_index"},
		{"missing-settled-producer", "UPDATE jobdb_dependency_waits SET payload=json_set(payload,'$.dependencies[0].satisfiedByJobId','absent') WHERE job_id='settled'", "invalid_settled_producer"},
		{"changed-settled-goal-witness", "UPDATE jobdb_dependency_waits SET payload=json_set(payload,'$.dependencies[0].target.goalId','G') WHERE job_id='settled'", "invalid_settled_goal_witness"},
		{"contradictory-wait-outcome", "UPDATE jobdb_dependency_waits SET state='SATISFIED',payload=json_set(payload,'$.state','SATISFIED') WHERE job_id='consumer'", "inconsistent_wait_outcome"},
		{"incompatible-receipt", "UPDATE jobdb_submission_receipts SET goal_id='U'", "incompatible_receipt"},
		{"missing-selected-receipt", "DELETE FROM jobdb_submission_receipts", "missing_accepted_receipt"},
		{"invalid-preparation-descriptor", "UPDATE jobdb_goal_preparations SET descriptor='{}' WHERE job_id='never-published'", "invalid_preparation_descriptor"},
		{"invalid-utf8-descriptor", "UPDATE jobdb_goal_preparations SET descriptor=CAST(replace(CAST(descriptor AS TEXT),'work',CAST(x'ff' AS TEXT)) AS BLOB) WHERE job_id='never-published'", "invalid_preparation_descriptor"},
		{"changed-preparation-dependencies", "UPDATE jobdb_goal_preparations SET dependencies='[]' WHERE job_id='never-published'", "preparation_dependencies_mismatch"},
		{"preparation-published-goal-collision", "UPDATE jobdb_goal_preparations SET goal_id='G' WHERE job_id='never-published'", "preparation_goal_collision"},
		{"missing-shared-preparation-goal", "UPDATE jobdb_goal_preparations SET goal_id='missing' WHERE job_id='pending-recovery'", "missing_preparation_goal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, db := migrationFixture(t, 11, true, true, true)
			_, err := db.Exec(tc.query)
			require.NoError(t, err)
			before := compatibilitySnapshot(t, db)
			bytesBefore, err := os.ReadFile(path)
			require.NoError(t, err)
			r, err := CheckGoalMigration(context.Background(), path)
			require.ErrorIs(t, err, ErrGoalMigrationBlocked)
			require.Equal(t, "BLOCKED", r.Verdict)
			require.True(t, hasMigrationIssue(r, tc.code), "%s: %+v", tc.code, r.Issues)
			converted, conversionErr := MigrateGoals(context.Background(), path)
			require.ErrorIs(t, conversionErr, ErrGoalMigrationBlocked)
			require.True(t, hasMigrationIssue(converted, tc.code), "%s: %+v", tc.code, converted.Issues)
			require.Equal(t, before, compatibilitySnapshot(t, db))
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, bytesBefore, after)
		})
	}
}

func TestGoalMigrationCheckRuntimeProducedDatabase(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	path := filepath.Join(t.TempDir(), "actual.db")
	rt, err := NewFromConfig(ctx, Config{DBPath: path})
	require.NoError(t, err)
	defer rt.Close(ctx)
	source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: source.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "ordinary failure"}}}}
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &chapter}))
	restarted, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{RequestID: "recover", Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, ShareGoal: true, Cell: source.Cell, CellRepo: source.CellRepo, JobID: "restart"}})
	require.NoError(t, err)
	// Use a terminal source before selected recovery, matching the target model.
	require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: restarted.JobKey}))
	selected, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "selected", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	_, err = celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "consumer", Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: source.GoalID}, Condition: jobdb.JobPrereqSuccess}}}})
	require.NoError(t, err)
	_, err = rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: source.GoalID}, Message: "  original failure\n保存  "})
	require.NoError(t, err)
	_, err = rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{selected.JobKey.JobId}})
	require.NoError(t, err)
	before := compatibilitySnapshot(t, rt.db)
	report, err := CheckGoalMigration(ctx, path)
	require.NoError(t, err, "issues: %+v", report.Issues)
	require.Equal(t, 13, report.SourceFormat)
	require.Equal(t, "CURRENT", report.Verdict)
	require.Equal(t, before, compatibilitySnapshot(t, rt.db))
}

func TestGoalMigrationCheckMissingCorruptAndEmptyFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	r, err := CheckGoalMigration(nil, path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, "ERROR", r.Verdict)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, os.WriteFile(path, []byte("not a SQLite database"), 0600))
	r, err = CheckGoalMigration(nil, path)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrGoalMigrationBlocked)
	require.Equal(t, "ERROR", r.Verdict)
	bytes, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "not a SQLite database", string(bytes))
	require.NoError(t, os.WriteFile(path, nil, 0600))
	r, err = CheckGoalMigration(nil, path)
	require.NoError(t, err)
	require.Equal(t, "EMPTY", r.Verdict)
	bytes, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Empty(t, bytes)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = CheckGoalMigration(ctx, path)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, "ERROR", r.Verdict)
	_, err = CheckGoalMigration(nil, "")
	require.Error(t, err)
	_, err = CheckGoalMigration(nil, filepath.Dir(path))
	require.Error(t, err)
}

func TestGoalMigrationInspectorUsesOneWALSnapshot(t *testing.T) {
	path, db := migrationFixture(t, 11, true, true, true)
	_, err := db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer tx.Rollback()
	var count int
	require.NoError(t, tx.QueryRow("SELECT count(*) FROM jobdb_jobs").Scan(&count))
	u := url.URL{Scheme: "file", Path: path}
	writer, err := sql.Open(sqliteDriverName, u.String())
	require.NoError(t, err)
	defer writer.Close()
	_, err = writer.Exec("DELETE FROM jobdb_goal_candidates WHERE tenant_id='tenant' AND goal_id='G'")
	require.NoError(t, err)
	r := newGoalMigrationReport()
	require.NoError(t, inspectGoalMigration(context.Background(), tx, &r))
	require.Len(t, r.DroppedMemberships, 3, "inspection must retain its original consistent snapshot")
	require.NoError(t, tx.Rollback())
	fresh, err := CheckGoalMigration(context.Background(), path)
	require.NoError(t, err)
	require.Len(t, fresh.DroppedMemberships, 2)
}
