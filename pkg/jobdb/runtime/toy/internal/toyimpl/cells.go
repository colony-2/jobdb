package toyimpl

import (
	"context"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

func (e *ToyEngine) cellsLocked(tenant string) *cellstate.State {
	if e.cells == nil {
		e.cells = map[string]*cellstate.State{}
	}
	s := e.cells[tenant]
	if s == nil {
		s = cellstate.New()
		e.cells[tenant] = s
	}
	return s
}
func (r *Runtime) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	return r.engine.cellsLocked(req.TenantID).Create(ctx, req)
}
func (r *Runtime) GetCell(ctx context.Context, key jobdb.CellKey) (jobdb.Cell, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	return r.engine.cellsLocked(key.TenantID).Get(ctx, key)
}
func (r *Runtime) ListCells(ctx context.Context, req jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	return r.engine.cellsLocked(req.TenantID).List(ctx, req)
}
func (r *Runtime) UpdateCell(ctx context.Context, req jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	state := r.engine.cellsLocked(req.TenantID)
	old := state.Cells[req.CellID]
	c, err := state.Update(ctx, req)
	if err == nil && old.Repo != c.Repo {
		for key, rec := range r.engine.schedules {
			if key.TenantId == req.TenantID && rec.info.Target.CellID == c.ID && rec.info.State == jobdb.ScheduleStateActive {
				rec.info.State = jobdb.ScheduleStatePaused
				rec.info.EffectiveState = jobdb.ScheduleStatePaused
				rec.info.PauseReason = "cell_repo_mismatch"
			}
		}
	}
	return c, err
}
func (r *Runtime) RetireCell(ctx context.Context, req jobdb.RetireCellRequest) (jobdb.Cell, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	e := r.engine
	e.initGoalsLocked()
	return e.cellsLocked(req.TenantID).Retire(ctx, req, func(id string) (bool, error) {
		for key, rec := range e.jobRecords {
			if key.TenantId != req.TenantID || rec.cell.CellID != id {
				continue
			}
			rec.mu.Lock()
			busy := rec.archived == nil && !rec.cancelled || rec.leased && rec.leaseExpiresAt.After(time.Now())
			rec.mu.Unlock()
			if busy {
				return true, nil
			}
			g, err := e.goals.Goal(jobdb.GoalKey{TenantId: key.TenantId, GoalID: rec.goalID})
			if err != nil {
				return false, err
			}
			if g.State == jobdb.GoalOpen {
				return true, nil
			}
		}
		for key, g := range e.goals.GoalRows {
			if key.TenantId == req.TenantID && g.CellID == id && g.State == jobdb.GoalOpen {
				return true, nil
			}
		}
		for key, p := range e.preparedGoals {
			named, found := e.cellsLocked(req.TenantID).ByName(p.Request.Target.Cell)
			if key.TenantId == req.TenantID && (p.Request.Target.CellID == id || found && named.ID == id) && e.jobRecords[key] == nil {
				return true, nil
			}
		}
		for key, rec := range e.schedules {
			if key.TenantId == req.TenantID && rec.info.State == jobdb.ScheduleStateActive && rec.info.Target.CellID == id {
				return true, nil
			}
		}
		return false, nil
	})
}
func (e *ToyEngine) cellParentLocked(ctx context.Context, tenant string) (*jobdb.Cell, error) {
	p, ok := cellstate.Lease(ctx)
	if !ok {
		return nil, nil
	}
	if p.JobKey.TenantId != tenant {
		return nil, jobdb.ErrCellForbidden
	}
	rec := e.jobRecords[p.JobKey]
	if rec == nil {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if p.LeaseID == "" || p.WorkerID == "" || rec.leaseID != p.LeaseID || rec.leaseWorkerID != p.WorkerID || !rec.leased || !rec.leaseExpiresAt.After(time.Now()) || rec.cancelled || rec.archived != nil {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	c, ok := e.cellsLocked(tenant).Cells[rec.cell.CellID]
	if !ok || c.State != jobdb.CellActive {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	return &c, nil
}
func (e *ToyEngine) prepareCellLocked(ctx context.Context, req goalstate.Preparation) (jobdb.ResolvedCell, error) {
	parent, err := e.cellParentLocked(ctx, req.Key.TenantId)
	if key, ok := cellstate.Schedule(ctx); ok {
		if key.TenantId != req.Key.TenantId {
			return jobdb.ResolvedCell{}, jobdb.ErrCellForbidden
		}
		schedule := e.schedules[key]
		if schedule == nil || schedule.info.State == jobdb.ScheduleStateArchived {
			return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
		}
		c, found := e.cellsLocked(key.TenantId).Cells[schedule.info.Target.CellID]
		if !found {
			return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
		}
		if c.Repo != schedule.info.Target.CellRepo || req.CellRepo != schedule.info.Target.CellRepo {
			return jobdb.ResolvedCell{}, jobdb.ErrCellRepoMismatch
		}
		parent = &c
	}
	if err != nil {
		return jobdb.ResolvedCell{}, err
	}
	var existing *jobdb.ResolvedCell
	confirmation := false
	if rec := e.jobRecords[req.Key]; rec != nil && rec.cell.CellID != "" {
		existing = &rec.cell
		confirmation = true
	}
	if existing == nil && req.PriorJobID != "" {
		rec := e.jobRecords[jobdb.JobKey{TenantId: req.Key.TenantId, JobId: req.PriorJobID}]
		if rec == nil {
			return jobdb.ResolvedCell{}, jobdb.ErrJobNotFound
		}
		existing = &rec.cell
	}
	return e.cellsLocked(req.Key.TenantId).Resolve(ctx, req.Key.TenantId, req.Cell, req.CellRepo, parent, existing, confirmation)
}
func (e *ToyEngine) publishCellLocked(ctx context.Context, key jobdb.JobKey) (jobdb.ResolvedCell, *cellstate.State, error) {
	p, ok := e.preparedGoals[key]
	if !ok {
		return jobdb.ResolvedCell{}, nil, jobdb.ErrInvalidCell
	}
	target, err := e.prepareCellLocked(ctx, p.Request)
	if err != nil {
		return target, nil, err
	}
	if target.CellRepo != p.Request.Target.CellRepo {
		return target, nil, jobdb.ErrCellRepoMismatch
	}
	state := cellstate.Clone(*e.cellsLocked(key.TenantId))
	if _, ok := state.ByName(target.Cell); !ok {
		target.CellID = p.Request.Target.CellID
	}
	if p.Request.PriorJobID == "" {
		if err = state.Publish(target, key.TenantId); err != nil {
			return target, nil, err
		}
	}
	return target, &state, nil
}
func (r *Runtime) ResolveCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID, name string) (jobdb.ResolvedCell, error) {
	ctx = cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: key, LeaseID: leaseID, WorkerID: workerID})
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	p, e := r.engine.cellParentLocked(ctx, key.TenantId)
	if e != nil {
		return jobdb.ResolvedCell{}, e
	}
	s := r.engine.cellsLocked(key.TenantId)
	if _, ok := s.ByName(name); !ok {
		return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
	}
	return s.Resolve(ctx, key.TenantId, name, "", p, nil, false)
}
func (l *runtimeLease) ResolveCell(ctx context.Context, name string) (jobdb.ResolvedCell, error) {
	return l.runtime.ResolveCellWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, name)
}
func (e *ToyEngine) cellHandleLocked(key jobdb.JobKey, rec *jobRecord) jobdb.JobHandle {
	target := rec.cell
	if c, ok := e.cellsLocked(key.TenantId).Cells[target.CellID]; ok {
		target.Cell = c.Name
	}
	return target.Handle(key, rec.goalID)
}
