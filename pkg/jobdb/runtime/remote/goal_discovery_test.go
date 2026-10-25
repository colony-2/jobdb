package remote

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestGoalDiscovery(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		b, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "b", JobType: "special", Data: jobdb.NewTaskDataOrPanic(nil), Metadata: json.RawMessage(`{"repo":"repo-b"}`)}})
		require.NoError(t, err)
		c := goalSubmit(t, r, "c")
		_, err = r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goalKey(a), JobID: b.JobKey.JobId})
		require.NoError(t, err)
		original, err := r.GetGoal(ctx, goalKey(a))
		require.NoError(t, err)
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: b.JobKey}))
		page, err := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant", Candidate: &jobdb.GoalCandidateFilter{MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"repo"}, Values: []any{"repo-b"}}}}})
		require.NoError(t, err)
		require.Len(t, page.Goals, 2)
		for _, g := range page.Goals {
			if g.GoalKey == goalKey(a) {
				require.EqualValues(t, 2, g.CandidateCount)
				require.EqualValues(t, 1, g.NonTerminalCandidateCount)
				require.EqualValues(t, 1, g.TerminalCandidateCount)
				require.Equal(t, original.Revision, g.Revision)
				require.True(t, original.UpdatedAt.Equal(g.UpdatedAt))
			}
		}
		deleted, err := r.(jobdb.JobDeletionRuntime).DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{b.JobKey.JobId}})
		require.NoError(t, err)
		require.EqualValues(t, 1, deleted.DeletedCount)
		hidden, err := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant", Candidate: &jobdb.GoalCandidateFilter{JobIDs: []string{b.JobKey.JobId}}})
		require.NoError(t, err)
		require.Len(t, hidden.Goals, 2)
		for _, g := range hidden.Goals {
			if g.GoalKey == goalKey(a) {
				require.EqualValues(t, 2, g.CandidateCount)
				require.EqualValues(t, 1, g.TerminalCandidateCount)
			}
		}
		req := jobdb.ListGoalsRequest{TenantId: "tenant", PageSize: 1}
		var ids []string
		for {
			page, err = r.ListGoals(ctx, req)
			require.NoError(t, err)
			require.Len(t, page.Goals, 1)
			ids = append(ids, page.Goals[0].GoalKey.GoalID)
			if page.NextPageToken == "" {
				break
			}
			bad := req
			bad.TenantId = "another"
			bad.PageToken = page.NextPageToken
			_, err = r.ListGoals(ctx, bad)
			require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
			bad = req
			bad.States = []jobdb.GoalState{jobdb.GoalOpen}
			bad.PageToken = page.NextPageToken
			_, err = r.ListGoals(ctx, bad)
			require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
			req.PageToken = page.NextPageToken
		}
		expected := []string{a.GoalID, b.GoalID, c.GoalID}
		sort.Strings(expected)
		require.Equal(t, expected, ids)
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: a.JobKey}))
		page, err = r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant", GoalIDs: []string{a.GoalID}, States: []jobdb.GoalState{jobdb.GoalOpen}})
		require.NoError(t, err)
		require.Len(t, page.Goals, 1)
		require.Zero(t, page.Goals[0].NonTerminalCandidateCount)
		require.EqualValues(t, 2, page.Goals[0].TerminalCandidateCount)
		failed, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goalKey(a), Message: "no more attempts"})
		require.NoError(t, err)
		page, err = r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant", States: []jobdb.GoalState{jobdb.GoalFailed}, UpdatedAfter: &failed.UpdatedAt})
		require.NoError(t, err)
		require.Len(t, page.Goals, 1)
		require.Equal(t, failed.Failure, page.Goals[0].Failure)
		future := time.Now().Add(time.Hour)
		page, err = r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "tenant", CreatedAfter: &future})
		require.NoError(t, err)
		require.Empty(t, page.Goals)
		require.Empty(t, page.NextPageToken)
	})
}

func TestDependencyDiscovery(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		b := goalSubmit(t, r, "b")
		da := jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess}
		db := jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: b.JobKey.JobId}, Condition: jobdb.JobPrereqComplete}
		c := goalSubmit(t, r, "consumer", da, db)
		_ = goalSubmit(t, r, "consumer-2", da)
		req := jobdb.ListDependencyWaitsRequest{TenantId: "tenant", Target: &da.Target, PageSize: 1}
		first, err := r.ListDependencyWaits(ctx, req)
		require.NoError(t, err)
		require.Len(t, first.Waits, 1)
		require.NotEmpty(t, first.NextPageToken)
		bad := req
		bad.TenantId = "other"
		bad.PageToken = first.NextPageToken
		_, err = r.ListDependencyWaits(ctx, bad)
		require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
		req.PageToken = first.NextPageToken
		second, err := r.ListDependencyWaits(ctx, req)
		require.NoError(t, err)
		require.Len(t, second.Waits, 1)
		require.Empty(t, second.NextPageToken)
		info, err := r.GetJob(ctx, c.JobKey)
		require.NoError(t, err)
		require.NotNil(t, info.BlockingWait)
		require.Equal(t, "submission", info.BlockingWait.WaitID)
		require.Len(t, info.BlockingWait.Dependencies, 2)
		jobs, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{c.JobKey}})
		require.NoError(t, err)
		require.Len(t, jobs.Jobs, 1)
		require.Equal(t, info.BlockingWait, jobs.Jobs[0].BlockingWait)

		// Satisfying one dependency removes only that reverse edge; the wait remains.
		goalComplete(t, r, a, true)
		req.PageToken = ""
		gone, err := r.ListDependencyWaits(ctx, req)
		require.NoError(t, err)
		require.Empty(t, gone.Waits)
		forward, err := r.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", ConsumerJobID: c.JobKey.JobId})
		require.NoError(t, err)
		require.Len(t, forward.Waits, 1)
		require.Equal(t, jobdb.DependencyPending, forward.Waits[0].State)
		require.Equal(t, a.JobKey.JobId, forward.Waits[0].Dependencies[0].SatisfiedByJobID)
		reverseB, err := r.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", Target: &db.Target})
		require.NoError(t, err)
		require.Len(t, reverseB.Waits, 1)
		require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: b.JobKey}))
		done, err := r.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", ConsumerJobID: c.JobKey.JobId, States: []jobdb.DependencyState{jobdb.DependencySatisfied}})
		require.NoError(t, err)
		require.Len(t, done.Waits, 1)
		require.Equal(t, a.JobKey.JobId, done.Waits[0].Dependencies[0].SatisfiedByJobID)
		info, err = r.GetJob(ctx, c.JobKey)
		require.NoError(t, err)
		require.Nil(t, info.BlockingWait)
		_, err = r.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", Target: &da.Target, States: []jobdb.DependencyState{jobdb.DependencySatisfied}})
		require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
	})
}

func TestDependencyDiscoverySkipsExpiredPages(t *testing.T) {
	goalRuntimes(t, func(t *testing.T, r jobdb.WorkflowRuntime) {
		ctx := context.Background()
		a := goalSubmit(t, r, "a")
		d := jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: a.GoalID}, Condition: jobdb.JobPrereqSuccess}
		// Prefix becomes expired without a scheduler mutation; a later consumer stays blocked.
		expires := time.Now().Add(250 * time.Millisecond)
		for _, id := range []string{"c1", "c2", "c3"} {
			h := goalSubmit(t, r, id)
			l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			deadline := expires
			if id == "c3" {
				deadline = time.Now().Add(time.Hour)
			}
			require.NoError(t, l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}, DependencyWait: &jobdb.DependencyWait{WaitID: "wait", Dependencies: []jobdb.Dependency{d}, Deadline: &deadline}}))
		}
		time.Sleep(time.Until(expires) + time.Millisecond)
		for range 2 {
			page, err := r.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", Target: &d.Target, PageSize: 1})
			require.NoError(t, err)
			require.Len(t, page.Waits, 1)
			require.Equal(t, "c3", page.Waits[0].JobKey.JobId)
			require.Empty(t, page.NextPageToken)
			expired, err := r.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", ConsumerJobID: "c1", States: []jobdb.DependencyState{jobdb.DependencyTimedOut}})
			require.NoError(t, err)
			require.Len(t, expired.Waits, 1)
		}
	})
}
