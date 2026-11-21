package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestGoalDiscoveryFreshSchemaAndReadOnlyProjection(t *testing.T) {
	ctx := celltest.Context(context.Background())
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "cells.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.ExecContext(ctx, retainedGoalSchemaSQL+"PRAGMA user_version=13;")
	require.NoError(t, err)
	at := time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)
	g := jobdb.GoalInfo{CellID: "cell-test", Cell: "test", CellRepo: "example.test/test", GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: "goal"}, State: jobdb.GoalOpen, Revision: 1, CreatedAt: at, UpdatedAt: at}
	for _, id := range []string{"producer", "consumer"} {
		_, err = db.ExecContext(ctx, `INSERT INTO jobdb_jobs(cell_id,cell_name,cell_repo,tenant_id,job_id,goal_id,job_type,route_job_type,initial_payload_digest,available_at_ns,created_at_ns,updated_at_ns,goal_origin) VALUES('cell-test','test','example.test/test','tenant',?,'goal','work','work','',1,?,?,?)`, id, at.UnixNano(), at.UnixNano(), id == "producer")
		require.NoError(t, err)
	}
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
		{"SELECT goal_id FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_origin=1 AND goal_id>'' ORDER BY goal_id LIMIT 100", "jobdb_jobs_goal_origin_unique"},
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

func TestGoalDiscoveryRetainedFiltersBeforePagination(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "discovery.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	expected := []jobdb.GoalInfo{}
	for i := range 8 {
		tenant := "tenant"
		if i == 7 {
			tenant = "other"
		}
		cell := "work"
		if i%2 == 1 || i == 4 {
			cell = "other-work"
		}
		typ := "work"
		if i == 6 {
			typ = "excluded"
		}
		h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: fmt.Sprintf("origin-%d", i), Cell: cell, CellRepo: "example.test/" + cell, JobType: typ, Data: jobdb.NewTaskDataOrPanic(nil), Metadata: json.RawMessage(`{"owner":"matching"}`)}})
		require.NoError(t, err)
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
		g, err := r.GetGoal(ctx, jobdb.GoalKey{TenantId: tenant, GoalID: h.GoalID})
		require.NoError(t, err)
		if i%2 == 0 {
			g, err = r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: g.GoalKey, Message: "closed"})
			require.NoError(t, err)
		}
		if i%3 == 0 {
			ids := []string{h.JobKey.JobId}
			if g.FailureJobID != "" {
				ids = append(ids, g.FailureJobID)
			}
			_, err = r.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: tenant, JobIDs: ids})
			require.NoError(t, err)
		}
		if i < 6 {
			expected = append(expected, g)
		}
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].GoalKey.GoalID < expected[j].GoalKey.GoalID })
	earliest, latest := expected[0].CreatedAt, expected[0].UpdatedAt
	for _, g := range expected {
		if g.CreatedAt.Before(earliest) {
			earliest = g.CreatedAt
		}
		if g.UpdatedAt.After(latest) {
			latest = g.UpdatedAt
		}
	}
	for _, state := range []jobdb.GoalState{jobdb.GoalOpen, jobdb.GoalFailed} {
		req := jobdb.ListGoalsRequest{TenantId: "tenant", States: []jobdb.GoalState{state}, Candidate: &jobdb.GoalCandidateFilter{JobTypes: []string{"work"}, MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"owner"}, Values: []any{"matching"}}}}, CreatedAfter: &earliest, UpdatedBefore: &latest, PageSize: 1}
		want := []jobdb.GoalInfo{}
		for _, g := range expected {
			if g.State == state {
				want = append(want, g)
			}
		}
		got := []jobdb.GoalInfo{}
		for {
			page, err := r.ListGoals(ctx, req)
			require.NoError(t, err)
			require.Len(t, page.Goals, 1)
			g := page.Goals[0]
			got = append(got, g.GoalInfo)
			require.Equal(t, int64(0), g.NonTerminalCandidateCount)
			if state == jobdb.GoalFailed {
				require.EqualValues(t, 2, g.CandidateCount)
			} else {
				require.EqualValues(t, 1, g.CandidateCount)
			}
			if page.NextPageToken == "" {
				break
			}
			req.PageToken = page.NextPageToken
			require.LessOrEqual(t, len(got), len(want), "pagination must advance")
		}
		require.Equal(t, want, got, "filters and hidden facts must apply before pagination; exact origin ownership is retained")
	}
	// Protected member discovery also preserves the existing cell ownership,
	// visibility and archive filters before job-list pagination.
	for _, includeHidden := range []bool{false, true} {
		req := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, CellNames: []string{"work"}, JobTypes: []string{jobdb.GoalFailureJobType}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}, Statuses: []jobdb.JobStatus{jobdb.JobStatusCompleted}, CreatedAfter: &earliest, CreatedBefore: &latest, IncludeHidden: includeHidden, PageSize: 1}
		count := 0
		for {
			page, err := r.ListJobs(ctx, req)
			require.NoError(t, err)
			require.Len(t, page.Jobs, 1)
			row := page.Jobs[0]
			require.Equal(t, "work", row.Cell)
			require.Equal(t, "tenant", row.JobKey.TenantId)
			require.Equal(t, jobdb.JobRecordGoalFailure, row.RecordKind)
			require.NotNil(t, row.ArchivedAt)
			count++
			if page.NextPageToken == "" {
				break
			}
			req.PageToken = page.NextPageToken
			require.LessOrEqual(t, count, 2)
		}
		if includeHidden {
			require.Equal(t, 2, count)
		} else {
			require.Equal(t, 1, count)
		}
	}
	// Explain the complete production query, including correlated primary member
	// type/metadata filtering, closure state/time bounds and pagination.
	req := jobdb.ListGoalsRequest{TenantId: "tenant", States: []jobdb.GoalState{jobdb.GoalFailed}, Candidate: &jobdb.GoalCandidateFilter{JobTypes: []string{"work"}, MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"owner"}, Values: []any{"matching"}}}}, CreatedAfter: &earliest, UpdatedBefore: &latest}
	query, args, err := goalDiscoveryQuery(req, "", 10)
	require.NoError(t, err)
	rows, err := r.db.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()
	plans := []string{}
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plans = append(plans, detail)
	}
	require.NoError(t, rows.Err())
	plan := strings.Join(plans, "\n")
	t.Log("complete discovery query plan:\n" + plan)
	require.Contains(t, plan, "jobdb_jobs_goal_origin_unique")
	require.Contains(t, plan, "jobdb_jobs_goal_closure_unique")
	require.Contains(t, plan, "jobdb_jobs_goal_idx")
	require.NotContains(t, plan, "SCAN g")
	require.NotContains(t, plan, "SCAN j")
	require.NotContains(t, strings.ToLower(query), "chapter")
}
