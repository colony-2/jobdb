package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestGoalDiscoveryMigrationAndReadOnlyProjection(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "v7.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.ExecContext(ctx, schemaSQL+goalSchemaSQL+"PRAGMA user_version=7;")
	require.NoError(t, err)
	at := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	g := jobdb.GoalInfo{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: "goal"}, State: jobdb.GoalOpen, Revision: 1, CreatedAt: at, UpdatedAt: at}
	raw, err := json.Marshal(g)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO jobdb_goals(tenant_id,goal_id,payload) VALUES(?,?,?)", "tenant", "goal", raw)
	require.NoError(t, err)
	for _, id := range []string{"producer", "consumer"} {
		_, err = db.ExecContext(ctx, `INSERT INTO jobdb_jobs(tenant_id,job_id,goal_id,job_type,route_job_type,initial_payload_digest,available_at_ns,created_at_ns,updated_at_ns) VALUES('tenant',?,'goal','work','work','',1,1,1)`, id)
		require.NoError(t, err)
	}
	require.NoError(t, migrate(ctx, db))
	require.NoError(t, migrate(ctx, db))
	rt := &Runtime{db: db}
	page, err := rt.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant", States: []jobdb.GoalState{jobdb.GoalOpen}, CreatedAfter: &at, CreatedBefore: &at})
	require.NoError(t, err)
	require.Len(t, page.Goals, 1)
	require.Equal(t, g, page.Goals[0].GoalInfo)
	require.EqualValues(t, 2, page.Goals[0].NonTerminalCandidateCount)
	past := time.Now().Add(-time.Minute)
	w := jobdb.DependencyWaitInfo{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "consumer"}, WaitID: "expired", State: jobdb.DependencyPending, Deadline: &past, Dependencies: []jobdb.DependencyResolution{{Target: jobdb.DependencyTarget{GoalID: "goal"}, Condition: jobdb.JobPrereqSuccess, State: jobdb.DependencyPending}}}
	store := goalStore{ctx, db}
	require.NoError(t, store.SaveWait(w))
	before, err := store.Wait(jobdb.DependencyWaitKey{JobKey: w.JobKey, WaitID: w.WaitID})
	require.NoError(t, err)
	for range 2 {
		waits, err := rt.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", ConsumerJobID: "consumer", States: []jobdb.DependencyState{jobdb.DependencyTimedOut}})
		require.NoError(t, err)
		require.Len(t, waits.Waits, 1)
		require.Equal(t, jobdb.DependencyTimedOut, waits.Waits[0].State)
		reverse, err := rt.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", Target: &w.Dependencies[0].Target})
		require.NoError(t, err)
		require.Empty(t, reverse.Waits)
	}
	after, err := store.Wait(jobdb.DependencyWaitKey{JobKey: w.JobKey, WaitID: w.WaitID})
	require.NoError(t, err)
	require.Equal(t, before, after)
	var edges int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM jobdb_dependency_targets").Scan(&edges))
	require.Equal(t, 1, edges, "read-only inspection must not prune subscriptions")
	for _, test := range []struct{ query, index string }{
		{"SELECT goal_id FROM jobdb_goals WHERE tenant_id='tenant' AND state='OPEN' AND goal_id>'' ORDER BY goal_id LIMIT 100", "jobdb_goals_state_idx"},
		{"SELECT job_id,wait_id FROM jobdb_dependency_targets WHERE tenant_id='tenant' AND target_kind='goal' AND target_id='goal' ORDER BY job_id,wait_id LIMIT 100", "jobdb_dependency_targets_discovery_idx"},
	} {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+test.query)
		require.NoError(t, err)
		var plans []string
		for rows.Next() {
			var a, b, c int
			var plan string
			require.NoError(t, rows.Scan(&a, &b, &c, &plan))
			plans = append(plans, plan)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Contains(t, strings.Join(plans, " "), test.index)
	}
}
