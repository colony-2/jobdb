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
	rows, err := q.QueryContext(ctx, `SELECT cell_id,name,repo,description,metadata,state,revision,created_at_ns,updated_at_ns,retired_at_ns FROM jobdb_cells WHERE tenant_id=?`, tenant)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		c := jobdb.Cell{TenantID: tenant, Aliases: []string{}}
		var metadata []byte
		var created, updated int64
		var retired sql.NullInt64
		if err = rows.Scan(&c.ID, &c.Name, &c.Repo, &c.Description, &metadata, &c.State, &c.Revision, &created, &updated, &retired); err == nil {
			err = json.Unmarshal(metadata, &c.Metadata)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		c.CreatedAt = timeFromNS(created)
		c.UpdatedAt = timeFromNS(updated)
		if retired.Valid {
			t := timeFromNS(retired.Int64)
			c.RetiredAt = &t
		}
		s.Cells[c.ID] = c
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, `SELECT cell_id,name FROM jobdb_cell_names WHERE tenant_id=? ORDER BY name`, tenant)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, name string
		if err = rows.Scan(&id, &name); err != nil {
			rows.Close()
			return nil, err
		}
		c := s.Cells[id]
		if c.Name != name {
			c.Aliases = append(c.Aliases, name)
			s.Cells[id] = c
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	err = q.QueryRowContext(ctx, `SELECT cursor_key FROM jobdb_cell_catalog WHERE tenant_id=?`, tenant).Scan(&s.CursorKey)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, `SELECT principal,request_id,descriptor,cell_id,name,repo,description,metadata,revision,created_at_ns FROM jobdb_cell_requests WHERE tenant_id=? ORDER BY principal,request_id`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var r cellstate.Creation
		var raw []byte
		var created int64
		c := &r.Result
		c.TenantID = tenant
		c.State = jobdb.CellActive
		c.Aliases = []string{}
		if err = rows.Scan(&r.Principal, &r.RequestID, &r.Descriptor, &c.ID, &c.Name, &c.Repo, &c.Description, &raw, &c.Revision, &created); err == nil {
			err = json.Unmarshal(raw, &c.Metadata)
		}
		if err != nil {
			return nil, err
		}
		c.CreatedAt = timeFromNS(created)
		c.UpdatedAt = c.CreatedAt
		s.Creations = append(s.Creations, r)
	}
	return s, rows.Err()
}
func saveCells(ctx context.Context, tx *sql.Tx, tenant string, before, after *cellstate.State) error {
	for id, c := range after.Cells {
		if reflect.DeepEqual(before.Cells[id], c) {
			continue
		}
		raw, err := json.Marshal(c.Metadata)
		if err != nil {
			return err
		}
		var retired any
		if c.RetiredAt != nil {
			retired = timeToNS(*c.RetiredAt)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO jobdb_cells(tenant_id,cell_id,name,repo,description,metadata,state,revision,created_at_ns,updated_at_ns,retired_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,cell_id) DO UPDATE SET name=excluded.name,repo=excluded.repo,description=excluded.description,metadata=excluded.metadata,state=excluded.state,revision=excluded.revision,updated_at_ns=excluded.updated_at_ns,retired_at_ns=excluded.retired_at_ns`, tenant, id, c.Name, c.Repo, c.Description, raw, c.State, c.Revision, timeToNS(c.CreatedAt), timeToNS(c.UpdatedAt), retired)
		if err != nil {
			return err
		}
		for _, name := range append([]string{c.Name}, c.Aliases...) {
			if _, err = tx.ExecContext(ctx, `INSERT INTO jobdb_cell_names VALUES(?,?,?) ON CONFLICT(tenant_id,name) DO NOTHING`, tenant, name, id); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobdb_cell_catalog VALUES(?,?) ON CONFLICT(tenant_id) DO NOTHING`, tenant, after.CursorKey); err != nil {
		return err
	}
	for _, r := range after.Creations {
		c := r.Result
		raw, err := json.Marshal(c.Metadata)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO jobdb_cell_requests(tenant_id,principal,request_id,descriptor,cell_id,name,repo,description,metadata,revision,created_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,principal,request_id) DO NOTHING`, tenant, r.Principal, r.RequestID, r.Descriptor, c.ID, c.Name, c.Repo, c.Description, raw, c.Revision, timeToNS(c.CreatedAt)); err != nil {
			return err
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
		out, e = s.Update(ctx, req)
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
			// Preparations and open goals also prevent retirement.
			e = tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_goal_preparations p WHERE p.tenant_id=? AND p.cell_id=? AND NOT EXISTS(SELECT 1 FROM jobdb_jobs j WHERE j.tenant_id=p.tenant_id AND j.job_id=p.job_id)`, req.TenantID, id).Scan(&n)
			if e != nil || n > 0 {
				return n > 0, e
			}
			e = tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_goals g WHERE g.tenant_id=? AND g.state='OPEN' AND g.cell_id=?`, req.TenantID, id).Scan(&n)
			if e != nil || n > 0 {
				return n > 0, e
			}
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
	if req.SelectedGoalID != "" {
		if _, ok := cellstate.Lease(ctx); ok {
			return jobdb.ResolvedCell{}, jobdb.ErrPrimaryGoalSelectionUnsupported
		}
		c, ok := s.ByName(req.Cell)
		if !ok {
			return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
		}
		if c.State != jobdb.CellActive {
			return jobdb.ResolvedCell{}, jobdb.ErrCellRetired
		}
		if req.CellRepo != "" {
			repo, err := jobdb.NormalizeCellRepo(req.CellRepo)
			if err != nil {
				return jobdb.ResolvedCell{}, err
			}
			if repo != c.Repo {
				return jobdb.ResolvedCell{}, jobdb.ErrCellRepoMismatch
			}
		}
		target := jobdb.ResolvedCell{Cell: c.Name, CellID: c.ID, CellRepo: c.Repo}
		if err := jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: req.Key.TenantId, GoalID: req.SelectedGoalID}, target); err != nil {
			return jobdb.ResolvedCell{}, err
		}
		return target, nil
	}
	parent, e := r.cellParent(ctx, tx, s, req.Key.TenantId)
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
	req := goalstate.Preparation{Key: key}
	if e := tx.QueryRowContext(ctx, `SELECT requested_cell,requested_repo,cell_id,cell_name,cell_repo,parent_job_id,prior_job_id,descriptor FROM jobdb_goal_preparations WHERE tenant_id=? AND job_id=?`, key.TenantId, key.JobId).Scan(&req.Cell, &req.CellRepo, &req.Target.CellID, &req.Target.Cell, &req.Target.CellRepo, &req.ParentJobID, &req.PriorJobID, &req.Descriptor); e != nil {
		return jobdb.ResolvedCell{}, e
	}
	var err error
	req.SelectedGoalID, err = goalstate.SelectedGoal(req.Descriptor)
	if err != nil {
		return jobdb.ResolvedCell{}, err
	}
	target, e := r.prepareCell(ctx, tx, &req)
	if e != nil {
		return target, e
	}
	if target.CellID != req.Target.CellID {
		return target, jobdb.NewExistingJobMismatchError("cell identity differs")
	}
	if target.CellRepo != req.Target.CellRepo {
		return target, jobdb.ErrCellRepoMismatch
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

func (r *Runtime) LookupCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID, name string) (out jobdb.ResolvedCell, err error) {
	ctx = cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: key, LeaseID: leaseID, WorkerID: workerID})
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		s, e := loadCells(ctx, tx, key.TenantId)
		if e != nil {
			return e
		}
		if _, e = r.cellParent(ctx, tx, s, key.TenantId); e != nil {
			return e
		}
		out, e = s.Lookup(name)
		return e
	})
	return
}

func (l *executionLease) LookupCell(ctx context.Context, name string) (jobdb.ResolvedCell, error) {
	if err := l.checkExpiry(); err != nil {
		return jobdb.ResolvedCell{}, err
	}
	return l.runtime.LookupCellWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, name)
}

func (r *Runtime) CreateCellWithLeaseByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.CreateCellRequest) (out jobdb.Cell, err error) {
	p := cellstate.LeaseProof{JobKey: key, LeaseID: leaseID, WorkerID: workerID}
	ctx = cellstate.WithLease(ctx, p)
	err = r.cellMutation(ctx, key.TenantId, func(tx *sql.Tx, s *cellstate.State) error {
		if _, e := r.cellParent(ctx, tx, s, key.TenantId); e != nil {
			return e
		}
		var e error
		out, e = s.CreateWithLease(ctx, p, req)
		return e
	})
	return
}

func (l *executionLease) CreateCell(ctx context.Context, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	if err := l.checkExpiry(); err != nil {
		return jobdb.Cell{}, err
	}
	return l.runtime.CreateCellWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

// withoutCellJSON retains the legacy non-cell snapshot format. Cell identity is
// stored only in relational columns, never duplicated in these JSON objects.
func withoutCellJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	delete(fields, "cell")
	delete(fields, "cellId")
	delete(fields, "cellRepo")
	return json.Marshal(fields)
}
