package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	"math"
	"strconv"
	"strings"
	"time"
)

// insertLeaseEvent snapshots the updated row and catalog inside the writer transaction.
// Missing immutable facts are an error, rather than a successful mutation without history.
func (r *Runtime) insertLeaseEvent(ctx context.Context, tx *sql.Tx, old jobRow, now time.Time, source jobdb.LeaseAcquisitionSource, previous *time.Time) error {
	typ := jobdb.LeaseEventAcquired
	var prevExpiry, src, prevID any
	if previous != nil {
		typ = jobdb.LeaseEventRenewed
		prevExpiry = timeToNS(*previous)
	} else {
		src = string(source)
		if old.leaseID.Valid {
			prevID = old.leaseID.String
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO jobdb_lease_events(tenant_id,job_id,event_type,occurred_at_ns,cell_id,cell_name,cell_repo,goal_id,job_type,route_job_type,route_task_type,lease_id,worker_id,lease_expires_at_ns,previous_lease_expires_at_ns,acquisition_source,previous_lease_id)
 SELECT j.tenant_id,j.job_id,?,?,j.cell_id,c.name,c.repo,j.goal_id,j.job_type,j.route_job_type,j.route_task_type,j.lease_id,j.lease_worker_id,j.lease_expires_at_ns,?,?,?
 FROM jobdb_jobs j JOIN jobdb_cells c ON c.tenant_id=j.tenant_id AND c.cell_id=j.cell_id WHERE j.tenant_id=? AND j.job_id=?`, typ, timeToNS(now), prevExpiry, src, prevID, old.tenantID, old.jobID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("lease event: expected one snapshot, got %d", n)
	}
	return nil
}

// loadLeaseEventScope is deliberately one narrow read, including aliases and the persisted key.
func loadLeaseEventScope(ctx context.Context, q queryer, tenant string) (*cellstate.State, error) {
	s := &cellstate.State{Cells: map[string]jobdb.Cell{}}
	rows, err := q.QueryContext(ctx, `SELECT k.cursor_key,c.cell_id,c.name,c.repo,n.name FROM jobdb_cell_catalog k LEFT JOIN jobdb_cells c ON c.tenant_id=k.tenant_id LEFT JOIN jobdb_cell_names n ON n.tenant_id=c.tenant_id AND n.cell_id=c.cell_id WHERE k.tenant_id=?`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, name, repo, alias sql.NullString
		if err = rows.Scan(&s.CursorKey, &id, &name, &repo, &alias); err != nil {
			return nil, err
		}
		if !id.Valid {
			continue
		}
		c := s.Cells[id.String]
		c.ID = id.String
		c.TenantID = tenant
		c.Name = name.String
		c.Repo = repo.String
		if alias.Valid && alias.String != c.Name {
			c.Aliases = append(c.Aliases, alias.String)
		}
		s.Cells[c.ID] = c
	}
	return s, rows.Err()
}

func (r *Runtime) ListLeaseEvents(ctx context.Context, req jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	empty := jobdb.ListLeaseEventsResponse{Events: []jobdb.LeaseEvent{}}
	req, err := jobdb.NormalizeLeaseEventRequest(req)
	if err != nil {
		return empty, err
	}
	if err = jobdb.AuthorizeCell(ctx, req.TenantID, "", "jobs.list"); err != nil {
		return empty, err
	}
	if err = r.validate(); err != nil {
		return empty, err
	}
	ctx = ctxOrBackground(ctx)
	s, err := loadLeaseEventScope(ctx, r.db, req.TenantID)
	if err != nil {
		return empty, err
	}
	ids, err := s.LeaseEventScope(ctx, req)
	if err != nil {
		return empty, err
	}
	binding := cellstate.LeaseEventBinding(ctx, req, ids)
	pos, err := cellstate.DecodeLeaseEventCursor(s.CursorKey, binding, req.PageToken)
	if err != nil {
		return empty, err
	}
	if len(ids) == 0 {
		return empty, nil
	}
	query, args := leaseEventQuery(req, ids, pos)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return empty, err
	}
	defer rows.Close()
	events := []jobdb.LeaseEvent{}
	for rows.Next() {
		var e jobdb.LeaseEvent
		var id, at, expiry int64
		var prev sql.NullInt64
		var source, prevID sql.NullString
		if err = rows.Scan(&id, &e.JobKey.TenantId, &e.JobKey.JobId, &e.EventType, &at, &e.CellID, &e.Cell, &e.CellRepo, &e.GoalID, &e.JobType, &e.Route.JobType, &e.Route.TaskType, &e.LeaseID, &e.WorkerID, &expiry, &prev, &source, &prevID); err != nil {
			return empty, err
		}
		e.EventID = strconv.FormatInt(id, 10)
		e.OccurredAt = timeFromNS(at)
		e.LeaseExpiresAt = timeFromNS(expiry)
		if prev.Valid {
			t := timeFromNS(prev.Int64)
			e.PreviousLeaseExpiresAt = &t
		}
		e.AcquisitionSource = jobdb.LeaseAcquisitionSource(source.String)
		e.PreviousLeaseID = prevID.String
		events = append(events, e)
	}
	if err = rows.Err(); err != nil {
		return empty, err
	}
	return cellstate.LeaseEventPage(events, req.PageSize, s.CursorKey, binding), nil
}
func leaseEventQuery(req jobdb.ListLeaseEventsRequest, ids []string, pos *cellstate.LeaseEventPosition) (string, []any) {
	args := []any{req.TenantID}
	where := []string{"e.tenant_id=?"}
	add := func(col string, values []string) {
		if len(values) == 0 {
			return
		}
		where = append(where, col+" IN ("+placeholders(len(values))+")")
		for _, v := range values {
			args = append(args, v)
		}
	}
	add("e.cell_id", ids)
	add("e.job_id", req.JobIDs)
	add("e.worker_id", req.WorkerIDs)
	add("e.lease_id", req.LeaseIDs)
	add("e.job_type", req.JobTypes)
	types := []string{}
	for _, v := range req.EventTypes {
		types = append(types, string(v))
	}
	add("e.event_type", types)
	join := ""
	if len(req.GoalIDs) > 0 {
		// One deduplicated relation; no evaluator or procedural queries.
		join = " LEFT JOIN (SELECT DISTINCT tenant_id,job_id FROM jobdb_goal_candidates WHERE tenant_id=? AND goal_id IN (" + placeholders(len(req.GoalIDs)) + ")) g ON g.tenant_id=e.tenant_id AND g.job_id=e.job_id"
		prefix := []any{req.TenantID}
		for _, v := range req.GoalIDs {
			prefix = append(prefix, v)
		}
		args = append(prefix, args...)
		where = append(where, "(e.goal_id IN ("+placeholders(len(req.GoalIDs))+") OR g.job_id IS NOT NULL)")
		for _, v := range req.GoalIDs {
			args = append(args, v)
		}
	}
	if !req.IncludeHidden {
		where = append(where, "(f.hidden_at_ns IS NULL)")
	}
	// RFC3339 bounds can lie outside SQLite's int64 nanosecond range.
	minTime, maxTime := time.Unix(0, math.MinInt64).UTC(), time.Unix(0, math.MaxInt64).UTC()
	if req.OccurredAfter != nil {
		if req.OccurredAfter.After(maxTime) {
			where = append(where, "0")
		} else if req.OccurredAfter.After(minTime) {
			where = append(where, "e.occurred_at_ns>=?")
			args = append(args, timeToNS(*req.OccurredAfter))
		}
	}
	if req.OccurredBefore != nil {
		if !req.OccurredBefore.After(minTime) {
			where = append(where, "0")
		} else if !req.OccurredBefore.After(maxTime) {
			where = append(where, "e.occurred_at_ns<?")
			args = append(args, timeToNS(*req.OccurredBefore))
		}
	}
	if len(req.Routes) > 0 {
		ors := []string{}
		for _, v := range req.Routes {
			ors = append(ors, "(e.route_job_type=? AND e.route_task_type=?)")
			args = append(args, v.JobType, v.TaskType)
		}
		where = append(where, "("+strings.Join(ors, " OR ")+")")
	}
	if pos != nil {
		where = append(where, "(e.occurred_at_ns,e.event_id)<(?,?)")
		args = append(args, timeToNS(pos.Time), pos.ID)
	}
	args = append(args, req.PageSize+1)
	return `SELECT e.event_id,e.tenant_id,e.job_id,e.event_type,e.occurred_at_ns,e.cell_id,e.cell_name,e.cell_repo,e.goal_id,e.job_type,e.route_job_type,e.route_task_type,e.lease_id,e.worker_id,e.lease_expires_at_ns,e.previous_lease_expires_at_ns,e.acquisition_source,e.previous_lease_id FROM jobdb_lease_events e LEFT JOIN jobdb_jobs f ON f.tenant_id=e.tenant_id AND f.job_id=e.job_id` + join + " WHERE " + strings.Join(where, " AND ") + " ORDER BY e.occurred_at_ns DESC,e.event_id DESC LIMIT ?", args
}
