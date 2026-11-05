package runtimecore

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

func (r *Runtime) CreateCell(ctx context.Context, q jobdb.CreateCellRequest) (jobdb.Cell, error) {
	return r.scheduler.CreateCell(ctx, q)
}

func (r *Runtime) CreateCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	return r.scheduler.CreateCellWithLeaseByID(ctx, key, leaseID, workerID, req)
}

func (l *executionLease) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	if err := l.checkExpiry(); err != nil {
		return jobdb.Cell{}, err
	}
	p := l.snapshot.Identity
	return l.runtime.CreateCellWithLeaseByID(ctx, p.JobKey, p.LeaseID, p.WorkerID, req)
}
func (r *Runtime) GetCell(ctx context.Context, q jobdb.CellKey) (jobdb.Cell, error) {
	return r.scheduler.GetCell(ctx, q)
}
func (r *Runtime) ListCells(ctx context.Context, q jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	return r.scheduler.ListCells(ctx, q)
}
func (r *Runtime) UpdateCell(ctx context.Context, q jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	return r.scheduler.UpdateCell(ctx, q)
}
func (r *Runtime) RetireCell(ctx context.Context, q jobdb.RetireCellRequest) (jobdb.Cell, error) {
	return r.scheduler.RetireCell(ctx, q)
}
func (r *Runtime) ResolveCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID, name string) (jobdb.ResolvedCell, error) {
	return r.scheduler.ResolveCellWithLeaseByID(ctx, key, leaseID, workerID, name)
}
func (l *executionLease) ResolveCell(ctx context.Context, name string) (jobdb.ResolvedCell, error) {
	p := l.snapshot.Identity
	return l.runtime.ResolveCellWithLeaseByID(ctx, p.JobKey, p.LeaseID, p.WorkerID, name)
}
func (l *executionLease) cellContext(ctx context.Context) context.Context {
	p := l.snapshot.Identity
	return cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: p.JobKey, LeaseID: p.LeaseID, WorkerID: p.WorkerID})
}
