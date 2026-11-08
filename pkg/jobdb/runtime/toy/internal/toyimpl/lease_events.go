package toyimpl

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	"slices"
	"sort"
	"strconv"
	"time"
)

// Caller holds engine then record lock; events contain no mutable job pointers.
func (e *ToyEngine) appendLeaseEventLocked(r *jobRecord, now time.Time, source jobdb.LeaseAcquisitionSource, previousID string, previousExpiry *time.Time) {
	e.leaseEventSequence++
	c := e.cellsLocked(r.key.TenantId).Cells[r.cell.CellID]
	v := jobdb.LeaseEvent{EventID: strconv.FormatInt(e.leaseEventSequence, 10), EventType: jobdb.LeaseEventAcquired, OccurredAt: now, JobKey: r.key, Cell: c.Name, CellID: c.ID, CellRepo: c.Repo, GoalID: r.goalID, JobType: r.jobType, Route: r.route, LeaseID: r.leaseID, WorkerID: r.leaseWorkerID, LeaseExpiresAt: r.leaseExpiresAt, AcquisitionSource: source, PreviousLeaseID: previousID}
	if previousExpiry != nil {
		t := *previousExpiry
		v.PreviousLeaseExpiresAt = &t
		v.EventType = jobdb.LeaseEventRenewed
		v.AcquisitionSource = ""
		v.PreviousLeaseID = ""
	}
	e.leaseEvents = append(e.leaseEvents, v)
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
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	s := r.engine.cells[req.TenantID]
	if s == nil {
		s = &cellstate.State{Cells: map[string]jobdb.Cell{}}
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
	events := []jobdb.LeaseEvent{}
	match := func(values []string, v string) bool { return len(values) == 0 || slices.Contains(values, v) }
	for _, v := range r.engine.leaseEvents {
		if v.JobKey.TenantId != req.TenantID || !slices.Contains(ids, v.CellID) || !match(req.JobIDs, v.JobKey.JobId) || !match(req.WorkerIDs, v.WorkerID) || !match(req.LeaseIDs, v.LeaseID) || !match(req.JobTypes, v.JobType) {
			continue
		}
		if len(req.EventTypes) > 0 && !slices.Contains(req.EventTypes, v.EventType) {
			continue
		}
		if len(req.Routes) > 0 && !slices.Contains(req.Routes, v.Route) {
			continue
		}
		if !req.IncludeHidden {
			if rec := r.engine.jobRecords[v.JobKey]; rec != nil {
				rec.mu.Lock()
				hidden := rec.hidden
				rec.mu.Unlock()
				if hidden {
					continue
				}
			}
		}
		if len(req.GoalIDs) > 0 && !slices.Contains(req.GoalIDs, v.GoalID) {
			found := false
			if r.engine.goals != nil {
				for _, g := range req.GoalIDs {
					if _, ok := r.engine.goals.Extras[goalstate.CandidateKey{Goal: jobdb.GoalKey{TenantId: req.TenantID, GoalID: g}, JobID: v.JobKey.JobId}]; ok {
						found = true
						break
					}
				}
			}
			if !found {
				continue
			}
		}
		if req.OccurredAfter != nil && v.OccurredAt.Before(*req.OccurredAfter) {
			continue
		}
		if req.OccurredBefore != nil && !v.OccurredAt.Before(*req.OccurredBefore) {
			continue
		}
		id, _ := strconv.ParseInt(v.EventID, 10, 64)
		if pos != nil && (v.OccurredAt.After(pos.Time) || (v.OccurredAt.Equal(pos.Time) && id >= pos.ID)) {
			continue
		}
		events = append(events, cellstate.Clone(v))
	}
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.OccurredAt.Equal(b.OccurredAt) {
			ai, _ := strconv.ParseInt(a.EventID, 10, 64)
			bi, _ := strconv.ParseInt(b.EventID, 10, 64)
			return ai > bi
		}
		return a.OccurredAt.After(b.OccurredAt)
	})
	return cellstate.LeaseEventPage(events, req.PageSize, s.CursorKey, binding), nil
}
