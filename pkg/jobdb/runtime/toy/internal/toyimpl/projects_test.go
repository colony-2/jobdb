package toyimpl

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func installedProjectFixture(t *testing.T) (context.Context, *projectcatalog.Service) {
	t.Helper()
	ctx := jobdb.WithProjectAdministration(celltest.Context(context.Background()), func(context.Context) error { return nil })
	c := &projectcatalog.Service{Store: &projectcatalog.MemoryStore{}, CanDelete: func(context.Context, string) error { return nil }}
	require.NoError(t, c.Install(ctx, projectcatalog.Installation{Manifest: projectcatalog.InstallationDigest([]byte("isolated toy runtime without external participants"))}))
	return ctx, c
}
func TestProjectCellAdmissionAndDeletionSerialize(t *testing.T) {
	for range 50 {
		ctx, c := installedProjectFixture(t)
		p, e := c.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "work", RequestID: "request"})
		require.NoError(t, e)
		rt := New(WithProjects(c))
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var writeErr, deleteErr error
		go func() {
			defer wg.Done()
			<-start
			_, writeErr = rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "test", Repo: "example.test/test"})
		}()
		go func() {
			defer wg.Done()
			<-start
			deleteErr = c.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag})
		}()
		close(start)
		wg.Wait()
		if deleteErr == nil {
			require.ErrorIs(t, writeErr, jobdb.ErrProjectNotFound)
			require.Empty(t, rt.engine.cells)
		} else {
			require.NoError(t, writeErr)
			require.ErrorIs(t, deleteErr, jobdb.ErrProjectConflict)
			require.Len(t, rt.engine.cells[p.Project.ID].Cells, 1)
		}
	}
}
func TestProjectPureInvalidOrUnauthorizedCreateDoesNotSeal(t *testing.T) {
	ctx, c := installedProjectFixture(t)
	rt := New(WithProjects(c))
	p, e := c.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "work", RequestID: "request"})
	require.NoError(t, e)
	_, e = rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "bad/name", Repo: "example.test/test"})
	require.ErrorIs(t, e, jobdb.ErrInvalidCell)
	_, e = rt.CreateCell(context.Background(), jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "test", Repo: "example.test/test"})
	require.ErrorIs(t, e, jobdb.ErrCellForbidden)
	_, e = rt.UpdateCell(context.Background(), jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: p.Project.ID, CellID: "missing"}})
	require.ErrorIs(t, e, jobdb.ErrCellForbidden)
	_, e = rt.CreateCellWithLeaseByID(ctx, jobdb.JobKey{TenantId: p.Project.ID, JobId: "missing"}, "not-a-lease", "worker", jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "test", Repo: "example.test/test"})
	require.Error(t, e)
	require.NoError(t, c.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}))
	for _, state := range rt.engine.cells {
		require.Empty(t, state.Cells)
	}
}
func TestProjectToyConstructorDoesNotImportScope(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt := New()
	_, e := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "c2", Name: "test", Repo: "example.test/test"})
	require.ErrorIs(t, e, jobdb.ErrProjectUnavailable)
	require.Empty(t, rt.engine.cells)
}

func TestProjectValidationAndDryRunDoNotSeal(t *testing.T) {
	ctx, c := installedProjectFixture(t)
	p, err := c.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "unused", RequestID: "unused"})
	require.NoError(t, err)
	rt := New(WithProjects(c))
	_, err = rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: p.Project.ID, Routes: []jobdb.Route{{JobType: ""}}})
	require.Error(t, err)
	_, err = rt.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: p.Project.ID, All: true, DryRun: true})
	require.NoError(t, err)
	require.NoError(t, c.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}))
}
