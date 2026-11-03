package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

func loadCells(ctx context.Context, q queryer, tenant string) (*cellstate.State, error) {
	s := cellstate.New()
	rows, e := q.QueryContext(ctx, "SELECT payload FROM jobdb_cells WHERE tenant_id=?", tenant)
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		var raw []byte
		var c jobdb.Cell
		if e = rows.Scan(&raw); e != nil {
			rows.Close()
			return nil, e
		}
		if e = json.Unmarshal(raw, &c); e != nil {
			rows.Close()
			return nil, e
		}
		s.Cells[c.ID] = c
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	var raw []byte
	e = q.QueryRowContext(ctx, "SELECT payload FROM jobdb_cell_requests WHERE tenant_id=?", tenant).Scan(&raw)
	if e == nil {
		var meta cellCatalogMetadata
		e = json.Unmarshal(raw, &meta)
		s.Creations = meta.Creations
		s.CursorKey = meta.CursorKey
	}
	if e != nil && e != sql.ErrNoRows {
		return nil, e
	}
	return s, nil
}
func saveCells(ctx context.Context, tx *sql.Tx, tenant string, before, after *cellstate.State) error {
	for id, c := range after.Cells {
		if reflect.DeepEqual(before.Cells[id], c) {
			continue
		}
		raw, e := json.Marshal(c)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO jobdb_cells(tenant_id,cell_id,name,repo,payload) VALUES(?,?,?,?,?) ON CONFLICT(tenant_id,cell_id) DO UPDATE SET name=excluded.name,repo=excluded.repo,payload=excluded.payload`, tenant, id, c.Name, c.Repo, raw)
		if e != nil {
			return e
		}
		for _, name := range append([]string{c.Name}, c.Aliases...) {
			if _, e = tx.ExecContext(ctx, `INSERT INTO jobdb_cell_names(tenant_id,name,cell_id) VALUES(?,?,?) ON CONFLICT(tenant_id,name) DO NOTHING`, tenant, name, id); e != nil {
				return e
			}
		}
	}
	{
		raw, e := json.Marshal(cellCatalogMetadata{Creations: after.Creations, CursorKey: after.CursorKey})
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO jobdb_cell_requests(tenant_id,payload) VALUES(?,?) ON CONFLICT(tenant_id) DO UPDATE SET payload=excluded.payload`, tenant, raw)
		if e != nil {
			return e
		}
	}
	return nil
}
func (r *Runtime) cellMutation(ctx context.Context, tenant string, fn func(*sql.Tx, *cellstate.State) error) error {
	ctx = ctxOrBackground(ctx)
	return r.withTx(ctx, func(tx *sql.Tx) error {
		s, e := loadCells(ctx, tx, tenant)
		if e != nil {
			return e
		}
		old := cellstate.Clone(*s)
		if e = fn(tx, s); e != nil {
			return e
		}
		return saveCells(ctx, tx, tenant, &old, s)
	})
}
func (r *Runtime) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (out jobdb.Cell, err error) {
	err = r.cellMutation(ctx, req.TenantID, func(_ *sql.Tx, s *cellstate.State) error { var e error; out, e = s.Create(ctx, req); return e })
	return
}
func (r *Runtime) GetCell(ctx context.Context, key jobdb.CellKey) (out jobdb.Cell, err error) {
	err = r.cellMutation(ctx, key.TenantID, func(_ *sql.Tx, s *cellstate.State) error { var e error; out, e = s.Get(ctx, key); return e })
	return
}
func (r *Runtime) ListCells(ctx context.Context, req jobdb.ListCellsRequest) (out jobdb.ListCellsResponse, err error) {
	err = r.cellMutation(ctx, req.TenantID, func(_ *sql.Tx, s *cellstate.State) error { var e error; out, e = s.List(ctx, req); return e })
	return
}
func (r *Runtime) UpdateCell(ctx context.Context, req jobdb.UpdateCellRequest) (out jobdb.Cell, err error) {
	err = r.cellMutation(ctx, req.TenantID, func(tx *sql.Tx, s *cellstate.State) error {
		var e error
		old := s.Cells[req.CellID]
		out, e = s.Update(ctx, req)
		if e == nil && old.Repo != out.Repo {
			_, e = tx.ExecContext(ctx, `UPDATE jobdb_schedules SET state='PAUSED',pause_reason='cell_repo_mismatch' WHERE tenant_id=? AND state='ACTIVE' AND json_extract(target_json,'$.cellId')=?`, req.TenantID, req.CellID)
		}
		return e
	})
	return
}
func (r *Runtime) RetireCell(ctx context.Context, req jobdb.RetireCellRequest) (out jobdb.Cell, err error) {
	err = r.cellMutation(ctx, req.TenantID, func(tx *sql.Tx, s *cellstate.State) error {
		var e error
		out, e = s.Retire(ctx, req, func(id string) (bool, error) {
			var n int
			e := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_jobs WHERE tenant_id=? AND cell_id=? AND ((archived_at_ns IS NULL AND cancel_requested=0) OR lease_expires_at_ns>?)`, req.TenantID, id, timeToNS(time.Now())).Scan(&n)
			if e != nil || n > 0 {
				return n > 0, e
			}
			// Preparations, open goals, and active schedules also prevent retirement.
			e = tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_goal_preparations p WHERE tenant_id=? AND json_extract(cell_target,'$.Target.cellId')=? AND NOT EXISTS(SELECT 1 FROM jobdb_jobs j WHERE j.tenant_id=p.tenant_id AND j.job_id=p.job_id)`, req.TenantID, id).Scan(&n)
			if e != nil || n > 0 {
				return n > 0, e
			}
			e = tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_goals g WHERE g.tenant_id=? AND g.state='OPEN' AND json_extract(g.payload,'$.cellId')=?`, req.TenantID, id).Scan(&n)
			if e != nil || n > 0 {
				return n > 0, e
			}
			e = tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_schedules WHERE tenant_id=? AND state='ACTIVE' AND json_extract(target_json,'$.cellId')=?`, req.TenantID, id).Scan(&n)
			return n > 0, e
		})
		return e
	})
	return
}
func (r *Runtime) cellParent(ctx context.Context, tx *sql.Tx, s *cellstate.State, tenant string) (*jobdb.Cell, error) {
	p, ok := cellstate.Lease(ctx)
	if !ok {
		return nil, nil
	}
	if p.JobKey.TenantId != tenant {
		return nil, jobdb.ErrCellForbidden
	}
	row, e := r.loadJobRowTx(ctx, tx, p.JobKey)
	if e != nil {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	if p.LeaseID == "" || p.WorkerID == "" || row.leaseID.String != p.LeaseID || row.leaseWorkerID.String != p.WorkerID || !row.leaseExpiresAtNS.Valid || row.leaseExpiresAtNS.Int64 <= timeToNS(time.Now()) || row.archivedAtNS.Valid || row.cancelRequested {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	c, ok := s.Cells[row.cell.CellID]
	if !ok || c.State != jobdb.CellActive {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	return &c, nil
}
func (r *Runtime) prepareCell(ctx context.Context, tx *sql.Tx, req *goalstate.Preparation) (jobdb.ResolvedCell, error) {
	s, e := loadCells(ctx, tx, req.Key.TenantId)
	if e != nil {
		return jobdb.ResolvedCell{}, e
	}
	parent, e := r.cellParent(ctx, tx, s, req.Key.TenantId)
	if key, ok := cellstate.Schedule(ctx); ok {
		if key.TenantId != req.Key.TenantId {
			return jobdb.ResolvedCell{}, jobdb.ErrCellForbidden
		}
		schedule, found, err := r.loadScheduleRowTx(ctx, tx, key)
		if err != nil {
			return jobdb.ResolvedCell{}, err
		}
		if !found || schedule.state == jobdb.ScheduleStateArchived {
			return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
		}
		c, found := s.Cells[schedule.target.CellID]
		if !found {
			return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
		}
		if c.Repo != schedule.target.CellRepo || req.CellRepo != schedule.target.CellRepo {
			return jobdb.ResolvedCell{}, jobdb.ErrCellRepoMismatch
		}
		parent = &c
	}
	if e != nil {
		return jobdb.ResolvedCell{}, e
	}
	var existing *jobdb.ResolvedCell
	confirmation := false
	if req.Key.JobId != "" {
		row, e := r.loadJobRowTx(ctx, tx, req.Key)
		if e == nil {
			existing = &row.cell
			confirmation = true
		} else if !errors.Is(e, jobdb.ErrJobNotFound) {
			return jobdb.ResolvedCell{}, e
		}
	}
	if existing == nil && req.PriorJobID != "" {
		row, e := r.loadJobRowTx(ctx, tx, jobdb.JobKey{TenantId: req.Key.TenantId, JobId: req.PriorJobID})
		if e != nil {
			return jobdb.ResolvedCell{}, e
		}
		existing = &row.cell
	}
	return s.Resolve(ctx, req.Key.TenantId, req.Cell, req.CellRepo, parent, existing, confirmation)
}
func (r *Runtime) publishCell(ctx context.Context, tx *sql.Tx, key jobdb.JobKey) (jobdb.ResolvedCell, error) {
	var raw []byte
	if e := tx.QueryRowContext(ctx, `SELECT cell_target FROM jobdb_goal_preparations WHERE tenant_id=? AND job_id=?`, key.TenantId, key.JobId).Scan(&raw); e != nil {
		return jobdb.ResolvedCell{}, e
	}
	var req goalstate.Preparation
	if e := json.Unmarshal(raw, &req); e != nil {
		return jobdb.ResolvedCell{}, e
	}
	target, e := r.prepareCell(ctx, tx, &req)
	if e != nil {
		return target, e
	}
	if target.CellRepo != req.Target.CellRepo {
		return target, jobdb.ErrCellRepoMismatch
	}
	s, e := loadCells(ctx, tx, key.TenantId)
	if e != nil {
		return target, e
	}
	before := cellstate.Clone(*s)
	if _, ok := s.ByName(target.Cell); !ok {
		target.CellID = req.Target.CellID
	}
	if req.PriorJobID == "" {
		if e = s.Publish(target, key.TenantId); e != nil {
			return target, e
		}
	}
	if e = saveCells(ctx, tx, key.TenantId, &before, s); e != nil {
		return target, e
	}
	return target, nil
}
func (r *Runtime) ResolveCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID, name string) (out jobdb.ResolvedCell, err error) {
	ctx = cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: key, LeaseID: leaseID, WorkerID: workerID})
	err = r.cellMutation(ctx, key.TenantId, func(tx *sql.Tx, s *cellstate.State) error {
		p, e := r.cellParent(ctx, tx, s, key.TenantId)
		if e != nil {
			return e
		}
		if _, ok := s.ByName(name); !ok {
			return jobdb.ErrCellNotFound
		}
		out, e = s.Resolve(ctx, key.TenantId, name, "", p, nil, false)
		return e
	})
	return
}
func (l *executionLease) ResolveCell(ctx context.Context, name string) (jobdb.ResolvedCell, error) {
	return l.runtime.ResolveCellWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, name)
}

type cellCatalogMetadata struct {
	Creations []cellstate.Creation
	CursorKey []byte
}
