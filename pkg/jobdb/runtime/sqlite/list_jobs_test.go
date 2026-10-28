package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore"
	"github.com/stretchr/testify/require"
)

func TestCompactListDoesNotReadDetailsOrGoalDiagnostics(t *testing.T) {
	ctx := context.Background()
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "list.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	producer, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "producer", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	consumer, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "consumer", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: producer.GoalID}, Condition: jobdb.JobPrereqSuccess}}}})
	require.NoError(t, err)
	waitKey := jobdb.DependencyWaitKey{JobKey: consumer.JobKey, WaitID: "submission"}
	_, err = r.GetDependencyWait(ctx, waitKey)
	require.NoError(t, err)
	// Evaluated wait diagnostics require the goal payload. Compact lists must not.
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_goals SET payload=x'ff'`)
	require.NoError(t, err)
	_, err = r.GetDependencyWait(ctx, waitKey)
	require.Error(t, err, "fixture must make diagnostic evaluation fail")
	check := func() {
		t.Helper()
		page, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{consumer.JobKey}})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 1)
		require.Equal(t, jobdb.JobStatusPendingJobs, page.Jobs[0].Status)
		require.Equal(t, &jobdb.DependencyWaitSummary{WaitID: "submission"}, page.Jobs[0].BlockingWaitSummary)
	}
	check()
	// No membership query is needed without an explicit GoalIDs filter.
	_, err = r.db.ExecContext(ctx, `DROP TABLE jobdb_goal_candidates`)
	require.NoError(t, err)
	check()
	// A list must not decode run policy/task coordinates or client payload.
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET payload=x'ff', client_payload=x'ff'`)
	require.NoError(t, err)
	// Nor may it read chapters or artifacts.
	original := r.chapterStore
	r.chapterStore = &chapterstore.Store{}
	defer func() { r.chapterStore = original }()
	check()
}

func TestCompactListWaitStateAndDeadlineWithoutEvaluation(t *testing.T) {
	ctx := context.Background()
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "waits.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	for _, tc := range []struct {
		name     string
		state    jobdb.DependencyState
		deadline *time.Time
		pending  bool
	}{
		{"pending", jobdb.DependencyPending, nil, true},
		{"future", jobdb.DependencyPending, timePtrForList(time.Now().Add(time.Hour)), true},
		{"expired", jobdb.DependencyPending, timePtrForList(time.Now().Add(-time.Hour)), false},
		{"satisfied", jobdb.DependencySatisfied, nil, false},
		{"timed-out", jobdb.DependencyTimedOut, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: tc.name, JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			// Malformed dependency detail is deliberate: listing only needs state/deadline.
			raw, err := json.Marshal(map[string]any{"state": tc.state, "deadline": tc.deadline, "dependencies": "not a dependency array"})
			require.NoError(t, err)
			_, err = r.db.ExecContext(ctx, `INSERT INTO jobdb_dependency_waits(tenant_id,job_id,wait_id,state,payload) VALUES('tenant',?,'wait',?,?)`, tc.name, string(tc.state), raw)
			require.NoError(t, err)
			_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET dependency_wait_id='wait' WHERE tenant_id='tenant' AND job_id=?`, tc.name)
			require.NoError(t, err)
			page, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{h.JobKey}})
			require.NoError(t, err)
			require.Len(t, page.Jobs, 1)
			if tc.pending {
				require.Equal(t, jobdb.JobStatusPendingJobs, page.Jobs[0].Status)
				require.Equal(t, &jobdb.DependencyWaitSummary{WaitID: "wait", Deadline: tc.deadline}, page.Jobs[0].BlockingWaitSummary)
			} else {
				require.Equal(t, jobdb.JobStatusReady, page.Jobs[0].Status)
				require.Nil(t, page.Jobs[0].BlockingWaitSummary)
			}
		})
	}
}

func timePtrForList(at time.Time) *time.Time { at = at.UTC(); return &at }

func TestCompactListFiltersAndCursorTies(t *testing.T) {
	ctx := context.Background()
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "pages.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	at := time.Now().UTC().Truncate(time.Second)
	var expected []string
	for _, tenant := range []string{"a", "a-tenant", "a/tenant"} {
		for _, id := range []string{"a", "z"} {
			h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: id, JobType: "work", Data: jobdb.NewTaskDataOrPanic(1), Metadata: json.RawMessage(`{"nested":{"a":[1,true]},"key\"quoted":"v"}`)}})
			require.NoError(t, err)
			expected = append(expected, h.JobKey.String())
		}
	}
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET created_at_ns=?`, at.UnixNano())
	require.NoError(t, err)
	sort.Sort(sort.Reverse(sort.StringSlice(expected)))
	filter, err := jobdb.Metadata().EqualFilter("nested", map[string]any{"a": []any{1, true}})
	require.NoError(t, err)
	filter, err = filter.EqualFilter("key\"quoted", "v")
	require.NoError(t, err)
	req := jobdb.ListJobsRequest{TenantIds: []string{"a", "a-tenant", "a/tenant"}, MetadataFilter: filter, CreatedAfter: &at, CreatedBefore: &at, RootOnly: true, PageSize: 1}
	var actual []string
	for {
		page, err := r.ListJobs(ctx, req)
		require.NoError(t, err)
		require.Len(t, page.Jobs, 1)
		actual = append(actual, page.Jobs[0].JobKey.String())
		require.LessOrEqual(t, len(actual), len(expected), "cursor loop")
		if page.NextPageToken == "" {
			break
		}
		req.PageToken = page.NextPageToken
	}
	require.Equal(t, expected, actual)
}
