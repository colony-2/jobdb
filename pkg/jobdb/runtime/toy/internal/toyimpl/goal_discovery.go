package toyimpl

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
)

func (r *Runtime) ListGoals(ctx context.Context, req jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	out := jobdb.ListGoalsResponse{Goals: []jobdb.GoalSummary{}}
	if err := req.Validate(); err != nil {
		return out, err
	}
	if ctx != nil && ctx.Err() != nil {
		return out, ctx.Err()
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
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	e := r.engine
	e.initGoalsLocked()
	for k := range e.goals.GoalRows {
		if k.TenantId != req.TenantId || k.GoalID <= after[0] {
			continue
		}
		g, err := e.goals.Goal(k)
		if err != nil {
			return out, err
		}
		if len(req.GoalIDs) > 0 && !slices.Contains(req.GoalIDs, k.GoalID) {
			continue
		}
		if len(req.States) > 0 && !slices.Contains(req.States, g.State) {
			continue
		}
		if req.CreatedAfter != nil && g.CreatedAt.Before(*req.CreatedAfter) || req.CreatedBefore != nil && g.CreatedAt.After(*req.CreatedBefore) || req.UpdatedAfter != nil && g.UpdatedAt.Before(*req.UpdatedAfter) || req.UpdatedBefore != nil && g.UpdatedAt.After(*req.UpdatedBefore) {
			continue
		}
		members := map[string]bool{}
		for key, rec := range e.jobRecords {
			if key.TenantId == req.TenantId && rec.goalID == k.GoalID {
				members[key.JobId] = true
			}
		}
		for pair := range e.goals.Extras {
			if pair.Goal == k {
				members[pair.JobID] = true
			}
		}
		row := jobdb.GoalSummary{GoalInfo: g}
		matched := req.Candidate == nil
		for id := range members {
			row.CandidateCount++
			rec := e.jobRecords[jobdb.JobKey{TenantId: req.TenantId, JobId: id}]
			if rec == nil {
				row.UnavailableCandidateCount++
				continue
			}
			if goalJob(jobdb.JobKey{TenantId: req.TenantId, JobId: id}, rec).Terminal {
				row.TerminalCandidateCount++
			} else {
				row.NonTerminalCandidateCount++
			}
			if f := req.Candidate; f != nil {
				if len(f.JobIDs) > 0 && !slices.Contains(f.JobIDs, id) {
					continue
				}
				if len(f.JobTypes) > 0 && !slices.Contains(f.JobTypes, rec.jobType) {
					continue
				}
				predicates, err := normalizeMetadataPredicates(f.MetadataEquals)
				if err != nil {
					return out, err
				}
				ok, err := metadataMatches(rec.metadata, predicates)
				if err != nil {
					return out, err
				}
				if ok {
					matched = true
				}
			}
		}
		if matched {
			out.Goals = append(out.Goals, row)
		}
	}
	sort.Slice(out.Goals, func(i, j int) bool { return out.Goals[i].GoalKey.GoalID < out.Goals[j].GoalKey.GoalID })
	if len(out.Goals) > size {
		out.Goals = out.Goals[:size]
		out.NextPageToken = jobdb.EncodeDiscoveryPage(scope, out.Goals[size-1].GoalKey.GoalID)
	}
	return out, nil
}
func (r *Runtime) ListDependencyWaits(ctx context.Context, req jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	out := jobdb.ListDependencyWaitsResponse{Waits: []jobdb.DependencyWaitInfo{}}
	if err := req.Validate(); err != nil {
		return out, err
	}
	if ctx != nil && ctx.Err() != nil {
		return out, ctx.Err()
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
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	e := r.engine
	e.initGoalsLocked()
	engine := goalstate.Engine{Store: e.goals, Now: time.Now().UTC()}
	for key := range e.goals.WaitRows {
		if key.JobKey.TenantId != req.TenantId || req.ConsumerJobID != "" && key.JobKey.JobId != req.ConsumerJobID {
			continue
		}
		if key.JobKey.JobId < after[0] || key.JobKey.JobId == after[0] && key.WaitID <= after[1] {
			continue
		}
		w, err := e.goals.Wait(key)
		if err != nil {
			return out, err
		}
		w, err = engine.Evaluate(w)
		if err != nil {
			return out, err
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
	}
	sort.Slice(out.Waits, func(i, j int) bool {
		a, b := out.Waits[i], out.Waits[j]
		if a.JobKey.JobId != b.JobKey.JobId {
			return a.JobKey.JobId < b.JobKey.JobId
		}
		return a.WaitID < b.WaitID
	})
	if len(out.Waits) > size {
		out.Waits = out.Waits[:size]
		last := out.Waits[size-1]
		out.NextPageToken = jobdb.EncodeDiscoveryPage(scope, last.JobKey.JobId, last.WaitID)
	}
	return out, nil
}
func (e *ToyEngine) blockingWaitLocked(key jobdb.JobKey, rec *jobRecord, now time.Time) (*jobdb.DependencyWaitInfo, error) {
	if rec.dependencyWaitID == "" || rec.archived != nil || rec.cancelled {
		return nil, nil
	}
	e.initGoalsLocked()
	w, err := e.goals.Wait(jobdb.DependencyWaitKey{JobKey: key, WaitID: rec.dependencyWaitID})
	if err != nil {
		return nil, err
	}
	w, err = (goalstate.Engine{Store: e.goals, Now: now}).Evaluate(w)
	if err != nil {
		return nil, err
	}
	if w.State != jobdb.DependencyPending {
		return nil, nil
	}
	return &w, nil
}
