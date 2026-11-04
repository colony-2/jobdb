package sqlite_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestCells(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote-toy", "remote-sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var rt jobdb.WorkflowRuntime
			if backend == "toy" || backend == "remote-toy" {
				rt = toy.New()
			} else {
				r, e := sqlite.NewFromConfig(context.Background(), sqlite.Config{DBPath: t.TempDir() + "/jobdb.db"})
				require.NoError(t, e)
				t.Cleanup(func() { _ = r.Close(context.Background()) })
				rt = r
			}
			if backend == "remote-toy" || backend == "remote-sqlite" {
				server := httptest.NewServer(celltest.Handler(remote.NewServer(rt)))
				t.Cleanup(server.Close)
				client, e := remote.New(server.URL, server.Client())
				require.NoError(t, e)
				rt = client
			}
			runtimetest.RunCellConformance(t, rt)
		})
	}
}

func TestCellTransactionsAcrossSQLiteConnections(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := sqlite.Config{DBPath: t.TempDir() + "/cells.db"}
	first, err := sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer first.Close(ctx)
	second, err := sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer second.Close(ctx)
	root, err := first.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: "root", Repo: "github.com/org/root"})
	require.NoError(t, err)
	_, err = first.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: "same", Repo: "github.com/org/same"})
	require.NoError(t, err)
	const n = 8
	handles := make([]jobdb.JobHandle, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rt := first
			if i%2 == 1 {
				rt = second
			}
			handles[i], errs[i] = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "t", JobID: fmt.Sprint(i), Cell: "same", CellRepo: "github.com/org/same", JobType: "job", Data: jobdb.NewTaskDataOrPanic(i)}})
		}(i)
	}
	wg.Wait()
	for i := range n {
		require.NoError(t, errs[i])
		require.Equal(t, handles[0].CellID, handles[i].CellID)
	}
	page, err := first.ListCells(ctx, jobdb.ListCellsRequest{TenantID: "t", PageSize: 1})
	require.NoError(t, err)
	require.NotEmpty(t, page.NextPageToken)
	next, err := second.ListCells(ctx, jobdb.ListCellsRequest{TenantID: "t", PageToken: page.NextPageToken})
	require.NoError(t, err)
	require.Len(t, next.Cells, 1)
	got, err := second.GetCell(ctx, jobdb.CellKey{TenantID: "t", CellID: root.ID})
	require.NoError(t, err)
	require.Equal(t, root, got)
}

func TestCellSelectorsRestrictDiscoveryAndAcquisition(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			var rt jobdb.WorkflowRuntime = toy.New()
			if backend == "sqlite" {
				r, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: t.TempDir() + "/db"})
				require.NoError(t, err)
				defer r.Close(ctx)
				rt = r
			}
			var allowed jobdb.JobHandle
			for _, name := range []string{"allowed", "hidden"} {
				_, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: name, Repo: "github.com/org/" + name})
				require.NoError(t, err)
				for i := range 2 {
					h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "t", Cell: name, CellRepo: "github.com/org/" + name, JobType: "job", Data: jobdb.NewTaskDataOrPanic(i)}})
					require.NoError(t, err)
					if name == "allowed" {
						allowed = h
					}
				}
			}
			restricted := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "reader", TenantID: "t", Permissions: map[string]bool{"jobs.list": true, "jobs.acquire": true}, Cells: map[string]bool{allowed.CellID: true}})
			page, err := rt.ListJobs(restricted, jobdb.ListJobsRequest{TenantIds: []string{"t"}, PageSize: 1})
			require.NoError(t, err)
			require.Len(t, page.Jobs, 1)
			require.Equal(t, allowed.CellID, page.Jobs[0].CellID)
			next, err := rt.ListJobs(restricted, jobdb.ListJobsRequest{TenantIds: []string{"t"}, PageSize: 1, PageToken: page.NextPageToken})
			require.NoError(t, err)
			require.Len(t, next.Jobs, 1)
			require.Equal(t, allowed.CellID, next.Jobs[0].CellID)
			require.Empty(t, next.NextPageToken)
			_, err = rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"t"}, PageToken: page.NextPageToken})
			require.ErrorIs(t, err, jobdb.ErrInvalidCellPageToken)
			_, err = rt.PollWork(restricted, jobdb.PollWorkRequest{TenantId: "t", WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, CellNames: []string{"hidden"}})
			require.ErrorIs(t, err, jobdb.ErrCellNotFound)
			leases, err := rt.PollWork(restricted, jobdb.PollWorkRequest{TenantId: "t", WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, Limit: 10})
			require.NoError(t, err)
			require.Len(t, leases, 2)
			for _, l := range leases {
				require.Equal(t, allowed.CellID, l.Job().CellID)
			}
		})
	}
}

func TestPendingPreparationPreventsRetirementAfterRename(t *testing.T) {
	type preparedRuntime interface {
		jobdb.WorkflowRuntime
		PrepareJob(context.Context, runtimecore.JobPreparation) (runtimecore.PreparedJob, error)
		DiscardJobPreparation(context.Context, jobdb.JobKey) error
	}
	ctx := celltest.Context(context.Background())
	sqliteRuntime, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: t.TempDir() + "/cells.db"})
	require.NoError(t, err)
	defer sqliteRuntime.Close(ctx)
	for name, rt := range map[string]preparedRuntime{"toy": toy.New(), "sqlite": sqliteRuntime} {
		t.Run(name, func(t *testing.T) {
			c, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: "work", Repo: "example.test/work"})
			require.NoError(t, err)
			prep, err := rt.PrepareJob(ctx, runtimecore.JobPreparation{Key: jobdb.JobKey{TenantId: "t", JobId: "pending"}, Cell: "work", CellRepo: "example.test/work", Descriptor: "pending"})
			require.NoError(t, err)
			require.Equal(t, prep.Target.CellID, c.ID)
			renamed := "renamed"
			c, err = rt.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "t", CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &renamed})
			require.NoError(t, err)
			req := jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: "t", CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision)}
			_, err = rt.RetireCell(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrCellBusy)
			require.NoError(t, rt.DiscardJobPreparation(ctx, prep.Key))
			_, err = rt.RetireCell(ctx, req)
			require.NoError(t, err)
		})
	}
}
