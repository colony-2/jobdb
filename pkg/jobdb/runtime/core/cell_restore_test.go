package runtimecore

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	"github.com/stretchr/testify/require"
)

type restoreLegacyScheduler struct {
	Scheduler
	state *cellstate.State
}

func (scheduler restoreLegacyScheduler) GetCell(ctx context.Context, key jobdb.CellKey) (jobdb.Cell, error) {
	return scheduler.state.Get(ctx, key)
}

type restoreScheduler struct{ restoreLegacyScheduler }

func (scheduler restoreScheduler) RestoreCell(ctx context.Context, req jobdb.RestoreCellRequest) (jobdb.Cell, error) {
	return scheduler.state.Restore(ctx, req)
}

var (
	_ Scheduler                = restoreLegacyScheduler{}
	_ CellRestoreScheduler     = restoreScheduler{}
	_ jobdb.CellRestoreRuntime = (*Runtime)(nil)
)

func TestCellRestoreSchedulerIsOptional(test *testing.T) {
	cell := jobdb.Cell{ID: "cell", TenantID: "tenant", Revision: "retired", State: jobdb.CellRetired}
	ctx := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{
		Principal: "restorer", TenantID: cell.TenantID,
		Permissions: map[string]bool{"cells.get": true, "cells.restore": true},
		Cells:       map[string]bool{cell.ID: true},
	})
	state := cellstate.New()
	state.Cells[cell.ID] = cell
	legacy := restoreLegacyScheduler{state: state}
	require.NotImplements(test, (*CellRestoreScheduler)(nil), legacy)
	runtime := &Runtime{scheduler: legacy, projects: celltest.UnrestrictedProjects{}}
	req := jobdb.RestoreCellRequest{CellKey: jobdb.CellKey{TenantID: cell.TenantID, CellID: cell.ID}, IfMatch: `"retired"`}
	_, err := runtime.RestoreCell(ctx, req)
	require.ErrorIs(test, err, jobdb.ErrCellRestoreUnsupported)
	require.Equal(test, cell, state.Cells[cell.ID])

	runtime.scheduler = restoreScheduler{legacy}
	runtime.projects = nil
	_, err = runtime.RestoreCell(ctx, req)
	require.ErrorIs(test, err, jobdb.ErrProjectUnavailable)
	require.Equal(test, cell, state.Cells[cell.ID])
	runtime.projects = celltest.UnrestrictedProjects{}
	restored, err := runtime.RestoreCell(ctx, req)
	require.NoError(test, err)
	require.Equal(test, cell.ID, restored.ID)
	require.Equal(test, jobdb.CellActive, restored.State)
	require.NotEqual(test, cell.Revision, restored.Revision)
}
