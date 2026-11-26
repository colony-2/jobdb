package runtimecore

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

type archiveScheduler struct {
	Scheduler
	calls []jobdb.ArchiveCellRequest
	ops   []string
}

func (scheduler *archiveScheduler) ArchiveCell(_ context.Context, req jobdb.ArchiveCellRequest) (jobdb.Cell, error) {
	scheduler.ops = append(scheduler.ops, "archive")
	scheduler.calls = append(scheduler.calls, req)
	return jobdb.Cell{ID: req.CellID}, nil
}

func (scheduler *archiveScheduler) UnarchiveCell(_ context.Context, req jobdb.ArchiveCellRequest) (jobdb.Cell, error) {
	scheduler.ops = append(scheduler.ops, "unarchive")
	scheduler.calls = append(scheduler.calls, req)
	return jobdb.Cell{}, jobdb.ErrCellPrecondition
}

func TestCellArchiveCoreForwarding(t *testing.T) {
	ctx := celltest.Context(context.Background())
	key := jobdb.CellKey{TenantID: "project", CellID: "immutable-id"}
	req := jobdb.ArchiveCellRequest{CellKey: key, IfMatch: `"opaque.42"`}
	scheduler := &archiveScheduler{}
	runtime := &Runtime{scheduler: scheduler, projects: celltest.UnrestrictedProjects{}}
	_, err := runtime.ArchiveCell(context.Background(), req)
	require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	cell, err := runtime.ArchiveCell(ctx, req)
	require.NoError(t, err)
	require.Equal(t, key.CellID, cell.ID)
	_, err = runtime.UnarchiveCell(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrCellPrecondition)
	require.Equal(t, []jobdb.ArchiveCellRequest{req, req}, scheduler.calls)
	require.Equal(t, []string{"archive", "unarchive"}, scheduler.ops)
	runtime.projects = nil
	_, err = runtime.ArchiveCell(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrProjectUnavailable)
	require.Len(t, scheduler.calls, 2)
	runtime.projects = celltest.UnrestrictedProjects{}
	runtime.scheduler = struct{ Scheduler }{}
	_, err = runtime.ArchiveCell(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrCellArchiveUnsupported)
	_, err = runtime.UnarchiveCell(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrCellArchiveUnsupported)
}
