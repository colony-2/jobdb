package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

// PrimaryGoalContext supplies explicit test-only access/selection authority.
// HTTP harnesses must install equivalent authority in trusted middleware.
func PrimaryGoalContext(ctx context.Context) context.Context {
	return jobdb.WithPrimaryGoalSelectionAuthorizer(celltest.Context(ctx), func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error { return nil })
}

// RunPrimaryGoalSelectionConformance exercises fresh primary selection and
// retained receipt behavior. Run it with fresh toy/SQLite direct/HTTP runtimes.
func RunPrimaryGoalSelectionConformance(t *testing.T, r jobdb.WorkflowRuntime) {
	ctx := PrimaryGoalContext(context.Background())
	request := func(id string) jobdb.SubmitJobRequest {
		return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "selection", JobID: id, Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(map[string]any{"fresh": id}), Metadata: json.RawMessage(`{"fresh":true}`), RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 4}}}}
	}
	sourceReq := request("source")
	sourceReq.Job.Metadata = json.RawMessage(`{"source":true}`)
	sourceReq.Job.RunPolicy.Retry.MaximumAttempts = 2
	source, err := celltest.Submit(r, ctx, sourceReq)
	require.NoError(t, err)
	req := request("selected")
	req.Job.GoalID = source.GoalID
	h, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, source.GoalID, h.GoalID)
	require.Equal(t, req.Job.JobID, h.JobKey.JobId)
	require.Equal(t, "selection", h.JobKey.TenantId)
	require.Equal(t, source.Cell, h.Cell)
	require.Equal(t, source.CellID, h.CellID)
	require.Equal(t, source.CellRepo, h.CellRepo)
	info, err := r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Empty(t, info.OtherGoalIDs)
	require.Equal(t, h.GoalID, info.GoalID)

	chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
	require.NoError(t, err)
	require.Len(t, chapters, 1)
	require.Equal(t, int64(0), chapters[0].Ordinal)
	body, ok := chapters[0].Body.(jobdb.JobStartChapter)
	require.True(t, ok)
	require.JSONEq(t, `{"fresh":"selected"}`, string(body.Input.Data))
	// Active/runnable candidates are not an exclusive claim on the goal.
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: source.JobKey, WorkerID: "original", Routes: []jobdb.Route{{JobType: "work"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	deniedChild := req
	deniedChild.Job.JobID = "lease-selected"
	_, err = lease.SubmitJob(ctx, deniedChild)
	require.ErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionUnsupported)
	concurrent := request("concurrent")
	concurrent.Job.GoalID = h.GoalID
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, e := r.SubmitJob(ctx, concurrent)
			expected := h
			expected.JobKey.JobId = "concurrent"
			if e == nil && got != expected {
				e = fmt.Errorf("incomplete or wrong receipt: %+v", got)
			}
			errors <- e
		}()
	}
	wg.Wait()
	close(errors)
	for e := range errors {
		require.NoError(t, e)
	}
	for name, mutate := range map[string]func(*jobdb.SubmitJob){"selector": func(j *jobdb.SubmitJob) { j.GoalID = "missing" }, "omission": func(j *jobdb.SubmitJob) { j.GoalID = "" }, "metadata": func(j *jobdb.SubmitJob) { j.Metadata = json.RawMessage(`{"different":true}`) }, "data": func(j *jobdb.SubmitJob) { j.Data = jobdb.NewTaskDataOrPanic("changed") }, "destination": func(j *jobdb.SubmitJob) { j.Cell = "other" }} {
		t.Run("conflict_"+name, func(t *testing.T) {
			changed := req
			mutate(&changed.Job)
			_, err := r.SubmitJob(ctx, changed)
			require.ErrorIs(t, err, jobdb.ErrExistingJobMismatch)
		})
	}
	_, err = r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "selection", GoalID: h.GoalID, JobType: "work"}})
	require.Error(t, err)
	missing := request("missing-goal")
	missing.Job.GoalID = "missing"
	_, err = r.SubmitJob(ctx, missing)
	require.ErrorIs(t, err, jobdb.ErrGoalNotFound)
	_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: "selection", JobId: "missing-goal"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	wrong := request("wrong-tenant")
	wrong.Job.TenantId = "other"
	wrong.Job.GoalID = h.GoalID
	_, err = celltest.Submit(r, ctx, wrong)
	require.ErrorIs(t, err, jobdb.ErrGoalNotFound)
	_, err = r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{JobID: req.Job.JobID, PriorJobKey: source.JobKey, Cell: source.Cell, CellRepo: source.CellRepo}})
	require.ErrorIs(t, err, jobdb.ErrExistingJobMismatch)
	cycle := request("cycle")
	cycle.Job.GoalID = h.GoalID
	cycle.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: h.GoalID}, Condition: jobdb.JobPrereqSuccess}}
	_, err = r.SubmitJob(ctx, cycle)
	require.ErrorIs(t, err, jobdb.ErrDependencyCycle)
	_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: "selection", JobId: "cycle"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	goals, err := r.ListGoals(ctx, jobdb.ListGoalsRequest{TenantId: "selection"})
	require.NoError(t, err)
	require.Len(t, goals.Goals, 1)
	require.Equal(t, int64(3), goals.Goals[0].CandidateCount)
	summaries, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"selection"}, JobKeys: []jobdb.JobKey{h.JobKey}})
	require.NoError(t, err)
	require.Len(t, summaries.Jobs, 1)
	require.JSONEq(t, string(req.Job.Metadata), string(summaries.Jobs[0].Metadata))
	require.Equal(t, int32(4), summaries.Jobs[0].ExecutionState.RunPolicy.Retry.MaximumAttempts)
	// Publication before explicit closure is retained, including the frozen set.
	g, err := r.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "selection", GoalID: h.GoalID}, Message: "stop"})
	require.NoError(t, err)
	require.Contains(t, g.Failure.NonTerminalJobIDs, "selected")
	require.Contains(t, g.Failure.NonTerminalJobIDs, "concurrent")
	replay, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
	closed := request("after-close")
	closed.Job.GoalID = h.GoalID
	_, err = r.SubmitJob(ctx, closed)
	require.ErrorIs(t, err, jobdb.ErrGoalConflict)
	_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: "selection", JobId: "after-close"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	deletion, ok := r.(jobdb.JobDeletionRuntime)
	require.True(t, ok)
	_, err = deletion.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "selection", JobIDs: []string{"selected"}})
	require.NoError(t, err)
	replay, err = r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
	jobs, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"selection"}})
	require.NoError(t, err)
	for _, job := range jobs.Jobs {
		require.NotEqual(t, "selected", job.JobKey.JobId)
	}
	runPrimaryGoalOwnership(t, r)
	runPrimaryGoalArchiveReplay(t, r)
}

func runPrimaryGoalOwnership(t *testing.T, r jobdb.WorkflowRuntime) {
	ctx := PrimaryGoalContext(context.Background())
	source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "ownership", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "ownership", JobID: "selected", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
	h, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	cells := r.(jobdb.CellRuntime)
	_, err = cells.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "ownership", Name: "other", Repo: "example.test/other"})
	require.NoError(t, err)
	changed := req
	changed.Job.JobID = "wrong-cell"
	changed.Job.Cell = "other"
	changed.Job.CellRepo = "example.test/other"
	_, err = r.SubmitJob(ctx, changed)
	require.ErrorIs(t, err, jobdb.ErrCellConflict)
	changed = req
	changed.Job.JobID = "missing-cell"
	changed.Job.Cell = "absent"
	_, err = r.SubmitJob(ctx, changed)
	require.ErrorIs(t, err, jobdb.ErrCellNotFound)
	listing, err := cells.ListCells(ctx, jobdb.ListCellsRequest{TenantID: "ownership"})
	require.NoError(t, err)
	require.Len(t, listing.Cells, 2)
	c, err := cells.GetCell(ctx, jobdb.CellKey{TenantID: "ownership", CellID: h.CellID})
	require.NoError(t, err)
	name := "renamed"
	c, err = cells.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "ownership", CellID: h.CellID}, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &name})
	require.NoError(t, err)
	replay, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
	alias := req
	alias.Job.JobID = "alias"
	alias.Job.Cell = "work"
	a, err := r.SubmitJob(ctx, alias)
	require.NoError(t, err)
	require.Equal(t, h.CellID, a.CellID)
	require.Equal(t, "renamed", a.Cell)
	repo := "example.test/drift"
	_, err = cells.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "ownership", CellID: h.CellID}, IfMatch: fmt.Sprintf("%q", c.Revision), Repo: &repo})
	require.NoError(t, err)
	changed = req
	changed.Job.JobID = "drift"
	changed.Job.CellRepo = repo
	_, err = r.SubmitJob(ctx, changed)
	require.ErrorIs(t, err, jobdb.ErrCellRepoMismatch)
	replay, err = r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
}

func runPrimaryGoalArchiveReplay(t *testing.T, r jobdb.WorkflowRuntime) {
	ctx := PrimaryGoalContext(context.Background())
	source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "archive", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "archive", JobID: "dest", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic("fresh")}}
	h, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "archiver", Routes: []jobdb.Route{{JobType: "work"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	err = lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "succeeded", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}})
	require.NoError(t, err)
	archived, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"archive"}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}, JobKeys: []jobdb.JobKey{h.JobKey}})
	require.NoError(t, err)
	require.Len(t, archived.Jobs, 1)
	require.NotNil(t, archived.Jobs[0].ArchivedAt)
	replay, err := r.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, replay)
}
