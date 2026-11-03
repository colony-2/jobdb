package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func cellAdmin(tenant string) context.Context {
	return jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "admin", TenantID: tenant, Permissions: map[string]bool{"cells.create": true, "cells.bootstrapRoot": true, "cells.get": true, "cells.list": true, "cells.update": true, "cells.retire": true, "jobs.submit": true, "jobs.acquire": true, "jobs.list": true, "jobs.get": true}})
}

// RunCellConformance tests the mandatory named-cell contract against a fresh tenant.
func RunCellConformance(t *testing.T, rt jobdb.WorkflowRuntime) {
	t.Helper()
	tenant := "cells-" + ksuid.New().String()
	ctx := cellAdmin(tenant)
	_, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: "root", CellRepo: "github.com/org/root", JobType: "recipe", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.ErrorIs(t, err, jobdb.ErrRootCellRequired)
	root, e := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tenant, Name: "root", Repo: "git@github.com:org/root.git"})
	require.NoError(t, e)
	require.True(t, root.IsRoot)
	require.Equal(t, "github.com/org/root", root.Repo)
	renamed := "other"
	_, e = rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: root.ID}, IfMatch: fmt.Sprintf("%q", root.Revision), Name: &renamed})
	require.ErrorIs(t, e, jobdb.ErrRootCellProtected)
	submit := func(name, repo, id string) jobdb.SubmitJobRequest {
		return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: name, CellRepo: repo, JobID: id, JobType: "recipe", Data: jobdb.NewTaskDataOrPanic(map[string]int{"n": 1})}}
	}
	_, e = rt.SubmitJob(ctx, submit("", "", "invalid"))
	require.ErrorIs(t, e, jobdb.ErrInvalidCell)
	h, e := rt.SubmitJob(ctx, submit("root", "", "root-job"))
	require.NoError(t, e)
	require.Equal(t, root.ID, h.CellID)
	lease, e := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "recipe"}}})
	require.NoError(t, e)
	require.NotNil(t, lease)
	_, e = lease.SubmitJob(context.Background(), submit("needs-repo", "", "missing-repo"))
	require.ErrorIs(t, e, jobdb.ErrCellRepoRequired)
	child, e := lease.SubmitJob(context.Background(), submit("payments", "https://github.com/org/payments.git", "child"))
	require.NoError(t, e)
	require.NotEmpty(t, child.CellID)
	require.Equal(t, "github.com/org/payments", child.CellRepo)
	cells, e := rt.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, Name: "payments"})
	require.NoError(t, e)
	require.Len(t, cells.Cells, 1)
	c := cells.Cells[0]
	newRepo := "github.com/org/payments-v2"
	metadata := map[string]json.RawMessage{"nested": json.RawMessage(`{"values":[null,true,3]}`)}
	changed, e := rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Repo: &newRepo, Metadata: &metadata})
	require.NoError(t, e)
	require.Equal(t, newRepo, changed.Repo)
	old, e := lease.SubmitJob(context.Background(), submit("payments", "github.com/org/payments", "child"))
	require.NoError(t, e)
	require.Equal(t, child, old)
	_, e = lease.SubmitJob(context.Background(), submit("payments", "github.com/org/payments", "fresh-stale"))
	require.ErrorIs(t, e, jobdb.ErrCellRepoMismatch)
	fresh, e := lease.SubmitJob(context.Background(), submit("payments", "", "fresh"))
	require.NoError(t, e)
	require.Equal(t, newRepo, fresh.CellRepo)
	target, e := lease.ResolveCell(context.Background(), "payments")
	require.NoError(t, e)
	require.Equal(t, newRepo, target.CellRepo)
	childLease, e := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: child.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "recipe"}}})
	require.NoError(t, e)
	require.NotNil(t, childLease)
	_, e = childLease.SubmitJob(context.Background(), submit("another", "github.com/org/another", "denied"))
	require.Error(t, e)
	list, e := rt.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, Name: "another"})
	require.NoError(t, e)
	require.Empty(t, list.Cells)

	t.Run("patch aliases and retirement", func(t *testing.T) {
		key := jobdb.CellKey{TenantID: tenant, CellID: changed.ID}
		name := "payments-renamed"
		_, err := rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, Name: &name})
		require.ErrorIs(t, err, jobdb.ErrCellPreconditionRequired)
		_, err = rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &name})
		require.ErrorIs(t, err, jobdb.ErrCellPrecondition)
		collision := "root"
		_, err = rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", changed.Revision), Name: &collision})
		require.Error(t, err)
		got, err := rt.GetCell(ctx, key)
		require.NoError(t, err)
		require.Equal(t, changed, got)
		renamed, err := rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", got.Revision), Name: &name})
		require.NoError(t, err)
		require.Contains(t, renamed.Aliases, "payments")
		throughAlias, err := lease.SubmitJob(context.Background(), submit("payments", "", "alias-job"))
		require.NoError(t, err)
		require.Equal(t, renamed.ID, throughAlias.CellID)
		require.Equal(t, name, throughAlias.Cell)
		original, err := rt.GetJob(ctx, child.JobKey)
		require.NoError(t, err)
		require.Equal(t, name, original.Cell)
		require.Equal(t, child.CellRepo, original.CellRepo)
		empty := map[string]json.RawMessage{}
		cleared, err := rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", renamed.Revision), Metadata: &empty})
		require.NoError(t, err)
		require.Empty(t, cleared.Metadata)
		_, err = rt.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", cleared.Revision)})
		require.ErrorIs(t, err, jobdb.ErrCellBusy)
		_, err = rt.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: root.ID}, IfMatch: fmt.Sprintf("%q", root.Revision)})
		require.ErrorIs(t, err, jobdb.ErrRootCellProtected)
		idle, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tenant, Name: "idle", Repo: "github.com/org/idle"})
		require.NoError(t, err)
		retired, err := rt.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: idle.ID}, IfMatch: fmt.Sprintf("%q", idle.Revision)})
		require.NoError(t, err)
		require.Equal(t, jobdb.CellRetired, retired.State)
		_, err = lease.SubmitJob(context.Background(), submit("idle", "", "retired-job"))
		require.ErrorIs(t, err, jobdb.ErrCellRetired)
		_, err = rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tenant, Name: "idle", Repo: "github.com/org/replacement"})
		require.ErrorIs(t, err, jobdb.ErrCellConflict)
	})
	t.Run("concurrent first submission", func(t *testing.T) {
		const n = 8
		var wg sync.WaitGroup
		handles := make([]jobdb.JobHandle, n)
		errs := make([]error, n)
		for i := range n {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				handles[i], errs[i] = lease.SubmitJob(context.Background(), submit("race", "github.com/org/race", fmt.Sprintf("race-%d", i)))
			}(i)
		}
		wg.Wait()
		for i := range n {
			require.NoError(t, errs[i])
			require.Equal(t, handles[0].CellID, handles[i].CellID)
		}
		_, err := lease.SubmitJob(context.Background(), submit("race", "github.com/org/conflicting", "conflicting"))
		require.ErrorIs(t, err, jobdb.ErrCellRepoMismatch)
		_, err = rt.GetJob(ctx, jobdb.JobKey{TenantId: tenant, JobId: "conflicting"})
		require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	})
	t.Run("cell list cursor binding", func(t *testing.T) {
		first, err := rt.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, PageSize: 1})
		require.NoError(t, err)
		require.NotEmpty(t, first.NextPageToken)
		next, err := rt.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, PageSize: 2, PageToken: first.NextPageToken})
		require.NoError(t, err)
		require.NotEqual(t, first.Cells[0].ID, next.Cells[0].ID)
		_, err = rt.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, State: "ALL", PageToken: first.NextPageToken})
		require.ErrorIs(t, err, jobdb.ErrInvalidCellPageToken)
		_, err = rt.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, PageToken: "corrupt." + first.NextPageToken})
		require.ErrorIs(t, err, jobdb.ErrInvalidCellPageToken)
	})
	t.Run("schedules pin repository", func(t *testing.T) {
		c, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tenant, Name: "scheduled", Repo: "github.com/org/scheduled"})
		require.NoError(t, err)
		req := jobdb.UpsertScheduleRequest{TenantId: tenant, ScheduleId: "schedule", Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Hour}, Target: jobdb.ScheduleTarget{Cell: c.Name, JobType: "recipe", Data: jobdb.NewTaskDataOrPanic(1)}}
		before, err := rt.UpsertSchedule(ctx, req)
		require.NoError(t, err)
		require.Equal(t, c.ID, before.Target.CellID)
		require.Equal(t, c.Repo, before.Target.CellRepo)
		repo := "github.com/org/scheduled-v2"
		_, err = rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Repo: &repo})
		require.NoError(t, err)
		paused, err := rt.GetSchedule(ctx, before.ScheduleKey)
		require.NoError(t, err)
		require.Equal(t, jobdb.ScheduleStatePaused, paused.State)
		require.Equal(t, "cell_repo_mismatch", paused.PauseReason)
		_, err = rt.ResumeSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: before.ScheduleKey})
		require.ErrorIs(t, err, jobdb.ErrCellRepoMismatch)
		after, err := rt.UpsertSchedule(ctx, req)
		require.NoError(t, err)
		require.Equal(t, repo, after.Target.CellRepo)
		require.Empty(t, after.PauseReason)
	})

	t.Run("failed submission publishes nothing", func(t *testing.T) {
		req := submit("invalid-cell", "github.com/org/invalid", "invalid-child")
		req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: "does-not-exist"}, Condition: jobdb.JobPrereqSuccess}}
		_, err := lease.SubmitJob(context.Background(), req)
		require.Error(t, err)
		list, err := rt.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, Name: "invalid-cell"})
		require.NoError(t, err)
		require.Empty(t, list.Cells)
		_, err = rt.GetJob(ctx, jobdb.JobKey{TenantId: tenant, JobId: "invalid-child"})
		require.ErrorIs(t, err, jobdb.ErrJobNotFound)
		_, err = rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: jobdb.JobKey{TenantId: tenant, JobId: "invalid-child"}, Ordinal: 0})
		require.Error(t, err)
	})
	t.Run("restart retains submitted repo", func(t *testing.T) {
		require.NoError(t, childLease.Complete(context.Background(), jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "recipe", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}}))
		restarted, err := lease.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "payments", PriorJobKey: child.JobKey, LastStepToKeep: 0, JobID: "restart"}})
		require.NoError(t, err)
		require.Equal(t, child.CellID, restarted.CellID)
		require.Equal(t, child.CellRepo, restarted.CellRepo)
	})

}
