package toyimpl

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/segmentio/ksuid"
)

type preparedGoalJob struct {
	Request goalstate.Preparation
	GoalID  string
}

func (e *ToyEngine) initGoalsLocked() {
	if e.goals != nil {
		return
	}
	e.preparedGoals = map[jobdb.JobKey]preparedGoalJob{}
	e.goalRequests = map[jobdb.GoalKey]jobdb.JobKey{}
	e.goals = goalstate.NewMemory(func(k jobdb.JobKey) (goalstate.Job, error) {
		r := e.jobRecords[k]
		if r == nil {
			return goalstate.Job{}, jobdb.ErrJobNotFound
		}
		return goalJob(k, r), nil
	}, func(k jobdb.GoalKey) []goalstate.Job {
		out := []goalstate.Job{}
		for key, r := range e.jobRecords {
			if key.TenantId == k.TenantId && r.goalID == k.GoalID {
				out = append(out, goalJob(key, r))
			}
		}
		return out
	})
}
func goalJob(k jobdb.JobKey, r *jobRecord) goalstate.Job {
	return goalstate.Job{Key: k, GoalID: r.goalID, Terminal: r.archived != nil || r.cancelled, Success: r.archived != nil && (r.completionStatus == "success" || r.completionStatus == "succeeded"), CreatedAt: r.createdAt}
}
func (r *Runtime) PrepareJob(ctx context.Context, req goalstate.Preparation) (goalstate.Prepared, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return goalstate.Prepared{}, err
		}
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	e := r.engine
	e.initGoalsLocked()
	if req.RequestID != "" {
		if key, ok := e.goalRequests[jobdb.GoalKey{TenantId: req.Key.TenantId, GoalID: req.RequestID}]; ok {
			if req.Key.JobId != "" && req.Key != key {
				return goalstate.Prepared{}, jobdb.NewExistingJobMismatchError("destination differs")
			}
			req.Key = key
		}
	}
	target, err := e.prepareCellLocked(ctx, req)
	if err != nil {
		return goalstate.Prepared{}, err
	}
	req.Target = target
	key := req.Key
	requestKey := jobdb.GoalKey{TenantId: key.TenantId, GoalID: req.RequestID}
	if req.RequestID != "" {
		if prior, ok := e.goalRequests[requestKey]; ok {
			if key.JobId != "" && key != prior {
				return goalstate.Prepared{}, jobdb.NewExistingJobMismatchError("destination differs")
			}
			key = prior
		}
	}
	if old, ok := e.preparedGoals[key]; ok {
		if old.Request.Target.CellRepo != target.CellRepo || old.Request.Target.CellID != target.CellID {
			return goalstate.Prepared{}, jobdb.NewExistingJobMismatchError("cell differs")
		}
		if old.Request.Descriptor != req.Descriptor || old.Request.RequestID != req.RequestID {
			return goalstate.Prepared{}, goalstate.PreparationMismatch(old.Request.Descriptor, req.Descriptor)
		}
		return goalstate.Prepared{Target: target, Key: key, GoalID: old.GoalID, Published: e.jobRecords[key] != nil}, nil
	}
	if key.JobId == "" {
		var err error
		key, err = e.idGenerator(key.TenantId)
		if err != nil {
			return goalstate.Prepared{}, err
		}
	}
	if err := key.Validate(); err != nil {
		return goalstate.Prepared{}, err
	}
	goalID := ksuid.New().String()
	if req.ShareGoal {
		source := e.jobRecords[jobdb.JobKey{TenantId: key.TenantId, JobId: req.PriorJobID}]
		if source == nil {
			return goalstate.Prepared{}, jobdb.ErrJobNotFound
		}
		goalID = source.goalID
		g, err := e.goals.Goal(jobdb.GoalKey{TenantId: key.TenantId, GoalID: goalID})
		if err != nil {
			return goalstate.Prepared{}, err
		}
		if g.State != jobdb.GoalOpen {
			return goalstate.Prepared{}, jobdb.ErrGoalConflict
		}
	}
	req.Key = key
	e.preparedGoals[key] = preparedGoalJob{Request: req, GoalID: goalID}
	if req.RequestID != "" {
		e.goalRequests[requestKey] = key
	}
	return goalstate.Prepared{Target: target, Key: key, GoalID: goalID}, nil
}

// publishGoalLocked is called with the new job provisionally inserted. It
// commits a copy of goal state only after all validations succeed.
func (e *ToyEngine) publishGoalLocked(ctx context.Context, key jobdb.JobKey, record *jobRecord) error {
	e.initGoalsLocked()
	p, ok := e.preparedGoals[key]
	if !ok {
		return jobdb.ErrInvalidCell
	}
	target, cells, err := e.publishCellLocked(ctx, key)
	if err != nil {
		return err
	}
	record.cell = target
	state := e.goals.Clone()
	gk := jobdb.GoalKey{TenantId: key.TenantId, GoalID: p.GoalID}
	record.goalID = p.GoalID
	record.key = key
	if p.Request.ShareGoal {
		g, err := state.Goal(gk)
		if err != nil {
			return err
		}
		if g.State != jobdb.GoalOpen {
			return jobdb.ErrGoalConflict
		}
	} else {
		state.SaveGoal(jobdb.GoalInfo{Cell: record.cell.Cell, CellID: record.cell.CellID, CellRepo: record.cell.CellRepo, GoalKey: gk, State: jobdb.GoalOpen, Revision: 1, CreatedAt: record.createdAt, UpdatedAt: record.createdAt})
	}
	engine := goalstate.Engine{Store: state, Now: time.Now().UTC()}
	if len(p.Request.Dependencies) > 0 {
		if _, err := engine.Register(key, jobdb.DependencyWait{WaitID: "submission", Dependencies: p.Request.Dependencies}, true); err != nil {
			return err
		}
		record.dependencyWaitID = "submission"
	}
	if err := engine.CheckCycle(key.TenantId, jobdb.DependencyTarget{GoalID: p.GoalID}); err != nil {
		return err
	}
	e.goals = state
	e.cells[key.TenantId] = cells
	return nil
}
func (r *Runtime) GetGoal(ctx context.Context, k jobdb.GoalKey) (jobdb.GoalInfo, error) {
	if err := k.Validate(); err != nil {
		return jobdb.GoalInfo{}, err
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	r.engine.initGoalsLocked()
	return r.engine.goals.Goal(k)
}
func (r *Runtime) AddGoalCandidate(ctx context.Context, req jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	r.engine.initGoalsLocked()
	s := r.engine.goals.Clone()
	out, err := (goalstate.Engine{Store: s, Now: time.Now().UTC()}).Add(req)
	if err == nil {
		r.engine.goals = s
	}
	return out, err
}
func (r *Runtime) FailGoal(ctx context.Context, req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	r.engine.initGoalsLocked()
	s := r.engine.goals.Clone()
	out, err := (goalstate.Engine{Store: s, Now: time.Now().UTC()}).Fail(req)
	if err == nil {
		r.engine.goals = s
	}
	return out, err
}
func (r *Runtime) GetDependencyWait(ctx context.Context, key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	e := r.engine
	e.initGoalsLocked()
	if e.jobRecords[key.JobKey] == nil {
		return jobdb.DependencyWaitInfo{}, jobdb.ErrJobNotFound
	}
	w, err := e.goals.Wait(key)
	if err != nil {
		return w, err
	}
	return (goalstate.Engine{Store: e.goals, Now: time.Now().UTC()}).Evaluate(w)
}
func (e *ToyEngine) otherGoalsLocked(key jobdb.JobKey) []string {
	e.initGoalsLocked()
	out := []string{}
	for k := range e.goals.Extras {
		if k.Goal.TenantId == key.TenantId && k.JobID == key.JobId {
			out = append(out, k.Goal.GoalID)
		}
	}
	sort.Strings(out)
	return out
}
func (e *ToyEngine) dependencyReadyLocked(record *jobRecord, now time.Time) (bool, error) {
	if record.dependencyWaitID == "" {
		return true, nil
	}
	e.initGoalsLocked()
	w, err := e.goals.Wait(jobdb.DependencyWaitKey{JobKey: record.key, WaitID: record.dependencyWaitID})
	if err != nil {
		return false, fmt.Errorf("wait for job %s: %w", record.key, err)
	}
	return w.State != jobdb.DependencyPending || (w.Deadline != nil && !now.Before(*w.Deadline)), nil
}

func (r *Runtime) DiscardJobPreparation(ctx context.Context, key jobdb.JobKey) error {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	e := r.engine
	if e.jobRecords[key] != nil {
		return nil
	}
	p, ok := e.preparedGoals[key]
	if ok {
		delete(e.preparedGoals, key)
		if p.Request.RequestID != "" {
			delete(e.goalRequests, jobdb.GoalKey{TenantId: key.TenantId, GoalID: p.Request.RequestID})
		}
	}
	return nil
}
