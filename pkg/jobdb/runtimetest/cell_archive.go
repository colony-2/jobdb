package runtimetest

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

// RunCellArchiveConformance tests the optional cell archive contract on a
// consumer backed by an archive-capable runtime and a writable fresh tenant.
func RunCellArchiveConformance(t *testing.T, runtime jobdb.ConsumerRuntime) {
	t.Helper()
	archive, ok := runtime.(jobdb.CellArchiveRuntime)
	require.True(t, ok, "runtime must implement CellArchiveRuntime")
	tenant := "archive-" + ksuid.New().String()
	ctx := cellAdmin(tenant)
	create := func(name string) jobdb.Cell {
		cell, err := runtime.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: tenant, Name: name, Repo: "example.test/org/" + name, Description: "kept", Metadata: map[string]json.RawMessage{"number": json.RawMessage(`1`)}})
		require.NoError(t, err)
		require.Nil(t, cell.ArchivedAt)
		return cell
	}
	request := func(cell jobdb.Cell) jobdb.ArchiveCellRequest {
		return jobdb.ArchiveCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: cell.ID}, IfMatch: fmt.Sprintf("%q", cell.Revision)}
	}
	cell := create("original")
	name := "renamed"
	cell, err := runtime.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: request(cell).CellKey, IfMatch: request(cell).IfMatch, Name: &name})
	require.NoError(t, err)
	initial := request(cell)
	_, err = archive.ArchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: initial.CellKey})
	require.ErrorIs(t, err, jobdb.ErrCellPreconditionRequired)
	_, err = archive.UnarchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: initial.CellKey, IfMatch: `W/"weak"`})
	require.ErrorIs(t, err, jobdb.ErrInvalidCell)
	unchanged, err := archive.UnarchiveCell(ctx, initial)
	require.NoError(t, err)
	require.Equal(t, cell, unchanged)

	type archiveResult struct {
		cell jobdb.Cell
		err  error
	}
	start := make(chan struct{})
	results := make(chan archiveResult, 2)
	for range 2 {
		go func() {
			<-start
			changed, err := archive.ArchiveCell(ctx, initial)
			results <- archiveResult{changed, err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil {
		first, second = second, first
	}
	require.NoError(t, first.err)
	require.ErrorIs(t, second.err, jobdb.ErrCellPrecondition)
	marked := first.cell
	require.NotNil(t, marked.ArchivedAt)
	require.True(t, marked.UpdatedAt.Equal(*marked.ArchivedAt))
	require.True(t, marked.UpdatedAt.After(cell.UpdatedAt))
	require.NotEqual(t, cell.Revision, marked.Revision)
	expected := cell
	expected.Revision, expected.UpdatedAt, expected.ArchivedAt = marked.Revision, marked.UpdatedAt, marked.ArchivedAt
	require.Equal(t, expected, marked)
	repeated, err := archive.ArchiveCell(ctx, request(marked))
	require.NoError(t, err)
	require.Equal(t, marked, repeated)

	for _, query := range []string{"original", "renamed"} {
		list, err := runtime.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, Name: query})
		require.NoError(t, err)
		require.Equal(t, []jobdb.Cell{marked}, list.Cells)
	}
	available := time.Now().UTC().Add(-time.Minute)
	deferred := time.Now().UTC().Add(time.Hour)
	_, err = runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: "original", JobType: "archive-recipe", Data: jobdb.NewTaskDataOrPanic(2), AvailableAt: &deferred}})
	require.NoError(t, err)
	job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: "original", JobType: "archive-recipe", Data: jobdb.NewTaskDataOrPanic(1), AvailableAt: &available}})
	require.NoError(t, err)
	require.Equal(t, marked.Resolved(), jobdb.ResolvedCell{Cell: job.Cell, CellID: job.CellID, CellRepo: job.CellRepo})
	_, err = runtime.RetireCell(ctx, jobdb.RetireCellRequest(request(marked)))
	require.ErrorIs(t, err, jobdb.ErrCellBusy)
	stillMarked, err := runtime.GetCell(ctx, initial.CellKey)
	require.NoError(t, err)
	require.Equal(t, marked, stillMarked)
	leases, err := runtime.PollWork(ctx, jobdb.PollWorkRequest{TenantId: tenant, WorkerID: "archive-worker", Routes: []jobdb.Route{{JobType: "archive-recipe"}}, Limit: 2})
	require.NoError(t, err)
	require.Len(t, leases, 1)
	lease := leases[0]
	require.Equal(t, job.JobKey, lease.Job().JobKey)
	resolved, err := lease.LookupCell(ctx, "original")
	require.NoError(t, err)
	require.Equal(t, marked.Resolved(), resolved)
	require.NoError(t, lease.KeepAlive(ctx))
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "archive-recipe", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}}))
	history, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: job.JobKey, Ordinal: 1})
	require.NoError(t, err)
	require.Equal(t, 1, int(history.Ordinal))
	restart, err := runtime.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: job.JobKey, Cell: "original", LastStepToKeep: 0}})
	require.NoError(t, err)
	require.Equal(t, job.CellID, restart.CellID)
	require.Equal(t, job.CellRepo, restart.CellRepo)

	cleared, err := archive.UnarchiveCell(ctx, request(marked))
	require.NoError(t, err)
	require.Nil(t, cleared.ArchivedAt)
	require.True(t, cleared.UpdatedAt.After(marked.UpdatedAt))
	require.NotEqual(t, cleared.Revision, marked.Revision)
	_, err = archive.UnarchiveCell(ctx, request(marked))
	require.ErrorIs(t, err, jobdb.ErrCellPrecondition)
	repeated, err = archive.UnarchiveCell(ctx, request(cleared))
	require.NoError(t, err)
	require.Equal(t, cleared, repeated)

	idle, err := archive.ArchiveCell(ctx, request(create("idle")))
	require.NoError(t, err)
	retired, err := runtime.RetireCell(ctx, jobdb.RetireCellRequest(request(idle)))
	require.NoError(t, err)
	require.Equal(t, jobdb.CellRetired, retired.State)
	require.Nil(t, retired.ArchivedAt)
	_, err = archive.ArchiveCell(ctx, request(retired))
	require.ErrorIs(t, err, jobdb.ErrCellRetired)
	_, err = archive.UnarchiveCell(ctx, request(retired))
	require.ErrorIs(t, err, jobdb.ErrCellRetired)
}
