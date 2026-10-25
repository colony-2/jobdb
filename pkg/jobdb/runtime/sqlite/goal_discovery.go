package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
)

func (r *Runtime) ListGoals(ctx context.Context, req jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	out := jobdb.ListGoalsResponse{Goals: []jobdb.GoalSummary{}}
	if err := req.Validate(); err != nil {
		return out, err
	}
	size := jobdb.DiscoveryPageSize(req.PageSize)
	filter := req
	filter.PageToken = ""
	filter.PageSize = 0
	scope, err := jobdb.DiscoveryPageScope(filter)
	if err != nil {
		return out, err
	}
	after, err := jobdb.DecodeDiscoveryPage(req.PageToken, scope, 1)
	if err != nil {
		return out, err
	}
	ctx = ctxOrBackground(ctx)
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	where := []string{"g.tenant_id=?", "g.goal_id>?"}
	args := []any{req.TenantId, after[0]}
	addSet := func(column string, values []string) {
		if len(values) > 0 {
			where = append(where, column+" IN ("+placeholders(len(values))+")")
			for _, v := range values {
				args = append(args, v)
			}
		}
	}
	addSet("g.goal_id", req.GoalIDs)
	states := []string{}
	for _, v := range req.States {
		states = append(states, string(v))
	}
	addSet("g.state", states)
	for _, b := range []struct {
		column, op string
		value      *time.Time
	}{{"g.created_at_ns", ">=", req.CreatedAfter}, {"g.created_at_ns", "<=", req.CreatedBefore}, {"g.updated_at_ns", ">=", req.UpdatedAfter}, {"g.updated_at_ns", "<=", req.UpdatedBefore}} {
		if b.value != nil {
			where = append(where, b.column+b.op+"?")
			args = append(args, b.value.UnixNano())
		}
	}
	// Query membership with an EXISTS so extra associations never duplicate goals.
	if f := req.Candidate; f != nil {
		sub := []string{"j.tenant_id=g.tenant_id", "j.job_id IN (SELECT job_id FROM jobdb_jobs WHERE tenant_id=g.tenant_id AND goal_id=g.goal_id UNION SELECT job_id FROM jobdb_goal_candidates WHERE tenant_id=g.tenant_id AND goal_id=g.goal_id)"}
		for _, set := range []struct {
			col string
			v   []string
		}{{"j.job_id", f.JobIDs}, {"j.job_type", f.JobTypes}} {
			if len(set.v) > 0 {
				sub = append(sub, set.col+" IN ("+placeholders(len(set.v))+")")
				for _, v := range set.v {
					args = append(args, v)
				}
			}
		}
		for _, p := range f.MetadataEquals {
			path := "$.app"
			for _, part := range p.Path {
				raw, _ := json.Marshal(part)
				path += "." + string(raw)
			}
			choices := []string{}
			for _, v := range p.Values {
				raw, e := json.Marshal(v)
				if e != nil {
					return out, fmt.Errorf("%w: invalid metadata value", jobdb.ErrInvalidDependency)
				}
				choices = append(choices, "(replace(json_type(j.metadata,?),'integer','real')=replace(json_type(?,'$'),'integer','real') AND json_extract(j.metadata,?) IS json_extract(?,'$'))")
				args = append(args, path, string(raw), path, string(raw))
			}
			sub = append(sub, "("+strings.Join(choices, " OR ")+")")
		}
		where = append(where, "EXISTS (SELECT 1 FROM jobdb_jobs j WHERE "+strings.Join(sub, " AND ")+")")
	}
	args = append(args, size+1)
	rows, err := tx.QueryContext(ctx, "SELECT g.payload FROM jobdb_goals g WHERE "+strings.Join(where, " AND ")+" ORDER BY g.goal_id LIMIT ?", args...)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var raw []byte
		var g jobdb.GoalInfo
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return out, err
		}
		if err = json.Unmarshal(raw, &g); err != nil {
			rows.Close()
			return out, err
		}
		out.Goals = append(out.Goals, jobdb.GoalSummary{GoalInfo: g})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	if len(out.Goals) > size {
		out.Goals = out.Goals[:size]
		out.NextPageToken = jobdb.EncodeDiscoveryPage(scope, out.Goals[size-1].GoalKey.GoalID)
	}
	for i := range out.Goals {
		g := &out.Goals[i]
		err = tx.QueryRowContext(ctx, `WITH members AS (
   SELECT job_id FROM jobdb_jobs WHERE tenant_id=? AND goal_id=?
   UNION SELECT job_id FROM jobdb_goal_candidates WHERE tenant_id=? AND goal_id=?
  ) SELECT count(*),coalesce(sum(j.job_id IS NOT NULL AND j.archived_at_ns IS NULL),0),
   coalesce(sum(j.archived_at_ns IS NOT NULL),0),coalesce(sum(j.job_id IS NULL),0)
   FROM members m LEFT JOIN jobdb_jobs j ON j.tenant_id=? AND j.job_id=m.job_id`,
			req.TenantId, g.GoalKey.GoalID, req.TenantId, g.GoalKey.GoalID, req.TenantId).Scan(&g.CandidateCount, &g.NonTerminalCandidateCount, &g.TerminalCandidateCount, &g.UnavailableCandidateCount)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func (r *Runtime) ListDependencyWaits(ctx context.Context, req jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	out := jobdb.ListDependencyWaitsResponse{Waits: []jobdb.DependencyWaitInfo{}}
	if err := req.Validate(); err != nil {
		return out, err
	}
	size := jobdb.DiscoveryPageSize(req.PageSize)
	filter := req
	filter.PageToken = ""
	filter.PageSize = 0
	scope, err := jobdb.DiscoveryPageScope(filter)
	if err != nil {
		return out, err
	}
	after, err := jobdb.DecodeDiscoveryPage(req.PageToken, scope, 2)
	if err != nil {
		return out, err
	}
	ctx = ctxOrBackground(ctx)
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	engine := goalstate.Engine{Store: goalStore{ctx, tx}, Now: now}
	for len(out.Waits) <= size {
		from := "jobdb_dependency_waits w"
		where := []string{"w.tenant_id=?", "(w.job_id,w.wait_id)>(?,?)"}
		args := []any{req.TenantId, after[0], after[1]}
		if req.ConsumerJobID != "" {
			where = append(where, "w.job_id=?")
			args = append(args, req.ConsumerJobID)
		}
		if req.Target != nil {
			from += " JOIN jobdb_dependency_targets t USING(tenant_id,job_id,wait_id)"
			kind, id := targetParts(*req.Target)
			where = append(where, "t.target_kind=?", "t.target_id=?", "w.state='PENDING'")
			args = append(args, kind, id)
		}
		args = append(args, size+1)
		rows, e := tx.QueryContext(ctx, "SELECT w.payload FROM "+from+" WHERE "+strings.Join(where, " AND ")+" ORDER BY w.job_id,w.wait_id LIMIT ?", args...)
		if e != nil {
			return out, e
		}
		batch := []jobdb.DependencyWaitInfo{}
		for rows.Next() {
			var raw []byte
			var w jobdb.DependencyWaitInfo
			if e = rows.Scan(&raw); e != nil {
				rows.Close()
				return out, e
			}
			if e = json.Unmarshal(raw, &w); e != nil {
				rows.Close()
				return out, e
			}
			batch = append(batch, w)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return out, e
		}
		for _, w := range batch {
			after = []string{w.JobKey.JobId, w.WaitID}
			w, e = engine.Evaluate(w)
			if e != nil {
				return out, e
			}
			if req.Target != nil {
				pending := false
				for _, d := range w.Dependencies {
					if d.Target == *req.Target && d.State == jobdb.DependencyPending {
						pending = true
					}
				}
				if w.State != jobdb.DependencyPending || !pending {
					continue
				}
			}
			if len(req.States) > 0 && !slices.Contains(req.States, w.State) {
				continue
			}
			out.Waits = append(out.Waits, w)
			if len(out.Waits) > size {
				break
			}
		}
		if len(batch) < size+1 {
			break
		}
	}
	if len(out.Waits) > size {
		out.Waits = out.Waits[:size]
		last := out.Waits[size-1]
		out.NextPageToken = jobdb.EncodeDiscoveryPage(scope, last.JobKey.JobId, last.WaitID)
	}
	return out, nil
}

func blockingWait(ctx context.Context, q goalQuery, row jobRow, now time.Time) (*jobdb.DependencyWaitInfo, error) {
	if row.dependencyWaitID == "" || row.archivedAtNS.Valid {
		return nil, nil
	}
	w, err := (goalStore{ctx, q}).Wait(jobdb.DependencyWaitKey{JobKey: jobdb.JobKey{TenantId: row.tenantID, JobId: row.jobID}, WaitID: row.dependencyWaitID})
	if err != nil {
		return nil, err
	}
	w, err = (goalstate.Engine{Store: goalStore{ctx, q}, Now: now}).Evaluate(w)
	if err != nil {
		return nil, err
	}
	if w.State != jobdb.DependencyPending {
		return nil, nil
	}
	return &w, nil
}
