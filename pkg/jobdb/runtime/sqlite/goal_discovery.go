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
	query, args, err := goalDiscoveryQuery(req, after[0], size)
	if err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return out, err
	}

	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return out, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	store := retainedStore(ctx, tx)
	for _, id := range ids {
		g, e := store.Goal(jobdb.GoalKey{TenantId: req.TenantId, GoalID: id})
		if e != nil {
			return out, e
		}
		out.Goals = append(out.Goals, jobdb.GoalSummary{GoalInfo: g})
	}
	if len(out.Goals) > size {
		out.Goals = out.Goals[:size]
		out.NextPageToken = jobdb.EncodeDiscoveryPage(scope, out.Goals[size-1].GoalKey.GoalID)
	}
	for i := range out.Goals {
		g := &out.Goals[i]
		err = tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(archived_at_ns IS NULL),0),coalesce(sum(archived_at_ns IS NOT NULL),0) FROM jobdb_jobs WHERE tenant_id=? AND goal_id=?`, req.TenantId, g.GoalKey.GoalID).Scan(&g.CandidateCount, &g.NonTerminalCandidateCount, &g.TerminalCandidateCount)
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
	engine := goalstate.Engine{Store: retainedStore(ctx, tx), Now: now}
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
	w, err = (goalstate.Engine{Store: retainedQueryStore(ctx, q), Now: now}).Evaluate(w)
	if err != nil {
		return nil, err
	}
	if w.State != jobdb.DependencyPending {
		return nil, nil
	}
	return &w, nil
}

// goalDiscoveryQuery is shared by execution and query-plan checks. All filters
// apply before the stable goal-ID pagination limit, including hidden membership.
func goalDiscoveryQuery(req jobdb.ListGoalsRequest, afterGoal string, size int) (string, []any, error) {
	where := []string{"g.tenant_id=?", "g.goal_origin=1", "g.goal_id>?"}
	args := []any{req.TenantId, afterGoal}
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
	addSet("coalesce(c.goal_closure_state,'OPEN')", states)
	for _, b := range []struct {
		column, op string
		value      *time.Time
	}{{"g.created_at_ns", ">=", req.CreatedAfter}, {"g.created_at_ns", "<=", req.CreatedBefore}, {"coalesce(c.goal_closed_at_ns,g.created_at_ns)", ">=", req.UpdatedAfter}, {"coalesce(c.goal_closed_at_ns,g.created_at_ns)", "<=", req.UpdatedBefore}} {
		if b.value != nil {
			where = append(where, b.column+b.op+"?")
			args = append(args, b.value.UnixNano())
		}
	}
	// Match primary membership with EXISTS so several matching members cannot duplicate goals.
	if f := req.Candidate; f != nil {
		sub := []string{"j.tenant_id=g.tenant_id", "j.goal_id=g.goal_id"}
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
					return "", nil, fmt.Errorf("%w: invalid metadata value", jobdb.ErrInvalidDependency)
				}
				choices = append(choices, "(replace(json_type(j.metadata,?),'integer','real')=replace(json_type(?,'$'),'integer','real') AND json_extract(j.metadata,?) IS json_extract(?,'$'))")
				args = append(args, path, string(raw), path, string(raw))
			}
			sub = append(sub, "("+strings.Join(choices, " OR ")+")")
		}
		where = append(where, "EXISTS (SELECT 1 FROM jobdb_jobs j WHERE "+strings.Join(sub, " AND ")+")")
	}
	args = append(args, size+1)
	return "SELECT g.goal_id FROM jobdb_jobs g LEFT JOIN jobdb_jobs c ON c.tenant_id=g.tenant_id AND c.goal_id=g.goal_id AND c.goal_closure_state IS NOT NULL WHERE " + strings.Join(where, " AND ") + " ORDER BY g.goal_id LIMIT ?", args, nil
}
