// Package goalstate implements goal and wait transitions inside a caller-owned
// scheduler transaction. Store methods must return detached values and all
// mutations in an operation must roll back together on error.
package goalstate

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type Job struct {
	Key       jobdb.JobKey
	GoalID    string
	Terminal  bool
	Success   bool
	CreatedAt time.Time
}

type Store interface {
	Job(jobdb.JobKey) (Job, error)
	Goal(jobdb.GoalKey) (jobdb.GoalInfo, error)
	SaveGoal(jobdb.GoalInfo) error
	Candidates(jobdb.GoalKey) ([]Job, error)
	Goals(jobdb.JobKey) ([]jobdb.GoalKey, error)
	Candidate(jobdb.GoalKey, string) (time.Time, bool, error)
	InsertCandidate(jobdb.GoalKey, string, time.Time) error
	Wait(jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error)
	SaveWait(jobdb.DependencyWaitInfo) error
	ConsumerWaits(jobdb.JobKey) ([]jobdb.DependencyWaitInfo, error)
	TargetWaits(string, jobdb.DependencyTarget) ([]jobdb.DependencyWaitInfo, error)
}

type Engine struct {
	Store Store
	Now   time.Time
}

func (e Engine) Add(req jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	if err := req.GoalKey.Validate(); err != nil {
		return jobdb.GoalCandidateInfo{}, err
	}
	if req.JobID == "" {
		return jobdb.GoalCandidateInfo{}, jobdb.ErrInvalidDependency
	}
	g, err := e.Store.Goal(req.GoalKey)
	if err != nil {
		return jobdb.GoalCandidateInfo{}, err
	}
	j, err := e.Store.Job(jobdb.JobKey{TenantId: req.GoalKey.TenantId, JobId: req.JobID})
	if err != nil {
		return jobdb.GoalCandidateInfo{}, err
	}
	at, exists, err := e.Store.Candidate(req.GoalKey, req.JobID)
	if err != nil {
		return jobdb.GoalCandidateInfo{}, err
	}
	if !exists {
		if g.State != jobdb.GoalOpen {
			return jobdb.GoalCandidateInfo{}, jobdb.ErrGoalConflict
		}
		at = e.Now
		if err = e.Store.InsertCandidate(req.GoalKey, req.JobID, at); err != nil {
			return jobdb.GoalCandidateInfo{}, err
		}
		if j.Success {
			if err = e.resolve(&g, j.Key.JobId); err != nil {
				return jobdb.GoalCandidateInfo{}, err
			}
		} else if !j.Terminal {
			if err = e.CheckCycle(j.Key.TenantId, jobdb.DependencyTarget{GoalID: g.GoalKey.GoalID}); err != nil {
				return jobdb.GoalCandidateInfo{}, err
			}
		}
	}
	return jobdb.GoalCandidateInfo{GoalKey: g.GoalKey, JobID: req.JobID, AddedAt: at, GoalState: g.State, SuccessfulJobID: g.SuccessfulJobID}, nil
}
func (e Engine) Fail(req jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	if err := req.Validate(); err != nil {
		return jobdb.GoalInfo{}, err
	}
	g, err := e.Store.Goal(req.GoalKey)
	if err != nil {
		return g, err
	}
	if g.State == jobdb.GoalFailed && g.Failure != nil && g.Failure.Message == req.Message {
		return g, nil
	}
	if g.State != jobdb.GoalOpen {
		return jobdb.GoalInfo{}, jobdb.ErrGoalConflict
	}
	jobs, err := e.Store.Candidates(req.GoalKey)
	if err != nil {
		return g, err
	}
	f := &jobdb.GoalFailure{Code: "goal_failed", GoalID: req.GoalKey.GoalID, Message: req.Message, FailedAt: e.Now, TerminalJobIDs: []string{}, NonTerminalJobIDs: []string{}}
	for _, j := range jobs {
		if j.Terminal {
			f.TerminalJobIDs = append(f.TerminalJobIDs, j.Key.JobId)
		} else {
			f.NonTerminalJobIDs = append(f.NonTerminalJobIDs, j.Key.JobId)
		}
	}
	sort.Strings(f.TerminalJobIDs)
	sort.Strings(f.NonTerminalJobIDs)
	g.State = jobdb.GoalFailed
	g.Revision = 2
	g.UpdatedAt = e.Now
	g.ClosedAt = &e.Now
	g.Failure = f
	if err = e.Store.SaveGoal(g); err != nil {
		return g, err
	}
	return g, e.SettleTarget(req.GoalKey.TenantId, jobdb.DependencyTarget{GoalID: req.GoalKey.GoalID})
}
func (e Engine) resolve(g *jobdb.GoalInfo, jobID string) error {
	if g.State != jobdb.GoalOpen {
		return nil
	}
	g.State = jobdb.GoalSucceeded
	g.SuccessfulJobID = jobID
	g.Revision = 2
	g.UpdatedAt = e.Now
	g.ClosedAt = &e.Now
	if err := e.Store.SaveGoal(*g); err != nil {
		return err
	}
	return e.SettleTarget(g.GoalKey.TenantId, jobdb.DependencyTarget{GoalID: g.GoalKey.GoalID})
}

// Complete runs after the job's terminal scheduler outcome has been stored.
func (e Engine) Complete(key jobdb.JobKey) error {
	j, err := e.Store.Job(key)
	if err != nil {
		return err
	}
	if j.Success {
		goals, err := e.Store.Goals(key)
		if err != nil {
			return err
		}
		sort.Slice(goals, func(i, j int) bool { return goals[i].GoalID < goals[j].GoalID })
		for _, key := range goals {
			g, err := e.Store.Goal(key)
			if err != nil {
				return err
			}
			if err = e.resolve(&g, j.Key.JobId); err != nil {
				return err
			}
		}
	}
	if err = e.CloseConsumer(key); err != nil {
		return err
	}
	return e.SettleTarget(key.TenantId, jobdb.DependencyTarget{JobID: key.JobId})
}
func (e Engine) CloseConsumer(key jobdb.JobKey) error {
	waits, err := e.Store.ConsumerWaits(key)
	if err != nil {
		return err
	}
	for _, w := range waits {
		if w.State == jobdb.DependencyPending {
			var err error
			w, err = e.Evaluate(w)
			if err != nil {
				return err
			}
			if w.State == jobdb.DependencyPending {
				w.State = jobdb.DependencyCancelled
			}

			closePending(&w)
			if err = e.Store.SaveWait(w); err != nil {
				return err
			}
		}
	}
	return nil
}
func (e Engine) Register(key jobdb.JobKey, req jobdb.DependencyWait, submission bool) (jobdb.DependencyWaitInfo, error) {
	if err := jobdb.ValidateDependencyWait(&req, submission); err != nil {
		return jobdb.DependencyWaitInfo{}, err
	}
	if _, err := e.Store.Job(key); err != nil {
		return jobdb.DependencyWaitInfo{}, err
	}
	wk := jobdb.DependencyWaitKey{JobKey: key, WaitID: req.WaitID}
	old, err := e.Store.Wait(wk)
	if err == nil {
		if !sameDescriptor(old, req) {
			return old, jobdb.ErrDependencyWaitConflict
		}
		evaluated, err := e.Evaluate(old)
		if err != nil {
			return old, err
		}
		return evaluated, e.Store.SaveWait(evaluated)
	}
	if !errors.Is(err, jobdb.ErrDependencyWaitNotFound) {
		return old, err
	}
	w := jobdb.DependencyWaitInfo{JobKey: key, WaitID: req.WaitID, Deadline: req.Deadline, State: jobdb.DependencyPending, Dependencies: make([]jobdb.DependencyResolution, len(req.Dependencies))}
	for i, d := range req.Dependencies {
		if d.Target.JobID == key.JobId {
			return w, jobdb.ErrDependencyCycle
		}
		if d.Target.JobID != "" {
			if _, err = e.Store.Job(jobdb.JobKey{TenantId: key.TenantId, JobId: d.Target.JobID}); err != nil {
				return w, err
			}
		} else {
			if _, err = e.Store.Goal(jobdb.GoalKey{TenantId: key.TenantId, GoalID: d.Target.GoalID}); err != nil {
				return w, err
			}
		}
		w.Dependencies[i] = jobdb.DependencyResolution{Target: d.Target, Condition: d.Condition, State: jobdb.DependencyPending}
	}
	w, err = e.Evaluate(w)
	if err != nil {
		return w, err
	}
	if err = e.Store.SaveWait(w); err != nil {
		return w, err
	}
	if err = e.CheckCycle(key.TenantId, jobdb.DependencyTarget{JobID: key.JobId}); err != nil {
		return w, err
	}
	return w, nil
}
func sameDescriptor(w jobdb.DependencyWaitInfo, r jobdb.DependencyWait) bool {
	if !reflect.DeepEqual(w.Deadline, r.Deadline) || len(w.Dependencies) != len(r.Dependencies) {
		return false
	}
	for i, d := range r.Dependencies {
		if w.Dependencies[i].Target != d.Target || w.Dependencies[i].Condition != d.Condition {
			return false
		}
	}
	return true
}
func closePending(w *jobdb.DependencyWaitInfo) {
	for i := range w.Dependencies {
		if w.Dependencies[i].State == jobdb.DependencyPending {
			w.Dependencies[i].State = jobdb.DependencyCancelled
			w.Dependencies[i].BlockedReason = ""
		}
	}
}

// Evaluate projects timeouts without requiring a write on inspection. Every
// mutation uses this same rule before persisting a new resolution.
func (e Engine) Evaluate(w jobdb.DependencyWaitInfo) (jobdb.DependencyWaitInfo, error) {
	if w.State != jobdb.DependencyPending {
		return w, nil
	}
	if w.Deadline != nil && !e.Now.Before(*w.Deadline) {
		w.State = jobdb.DependencyTimedOut
		closePending(&w)
		return w, nil
	}
	all := true
	for i := range w.Dependencies {
		d := &w.Dependencies[i]
		if d.State != jobdb.DependencyPending {
			continue
		}
		if d.Target.GoalID != "" {
			g, err := e.Store.Goal(jobdb.GoalKey{TenantId: w.JobKey.TenantId, GoalID: d.Target.GoalID})
			if err != nil {
				return w, err
			}
			switch g.State {
			case jobdb.GoalSucceeded:
				d.State = jobdb.DependencySatisfied
				d.SatisfiedByJobID = g.SuccessfulJobID
			case jobdb.GoalFailed:
				d.State = jobdb.DependencyFailed
				d.Failure = g.Failure
			default:
				d.BlockedReason = "awaiting_candidate"
				jobs, err := e.Store.Candidates(g.GoalKey)
				if err != nil {
					return w, err
				}
				for _, j := range jobs {
					if !j.Terminal {
						d.BlockedReason = "waiting_for_execution"
						break
					}
				}
			}
		} else {
			j, err := e.Store.Job(jobdb.JobKey{TenantId: w.JobKey.TenantId, JobId: d.Target.JobID})
			if err != nil {
				return w, err
			}
			if j.Terminal {
				if j.Success || d.Condition == jobdb.JobPrereqComplete {
					d.State = jobdb.DependencySatisfied
					d.SatisfiedByJobID = j.Key.JobId
				} else {
					d.State = jobdb.DependencyFailed
					d.Failure = &jobdb.GoalFailure{Code: "job_dependency_failed", JobID: j.Key.JobId, Message: fmt.Sprintf("prerequisite job %s did not succeed", j.Key.JobId), FailedAt: e.Now}
				}
			}
		}
		if d.State == jobdb.DependencyFailed {
			w.State = jobdb.DependencyFailed
			idx := i
			w.FailedDependencyIndex = &idx
			w.Failure = d.Failure
			closePending(&w)
			return w, nil
		}
		if d.State == jobdb.DependencyPending {
			all = false
		} else {
			d.BlockedReason = ""
		}
	}
	if all {
		w.State = jobdb.DependencySatisfied
	}
	return w, nil
}
func (e Engine) SettleTarget(tenant string, target jobdb.DependencyTarget) error {
	waits, err := e.Store.TargetWaits(tenant, target)
	if err != nil {
		return err
	}
	for _, w := range waits {
		w, err = e.Evaluate(w)
		if err != nil {
			return err
		}
		if err = e.Store.SaveWait(w); err != nil {
			return err
		}
	}
	return nil
}

// CheckCycle deliberately rejects directed cycles even if an independent
// alternative candidate could break them. Only unresolved edges participate.
func (e Engine) CheckCycle(tenant string, start jobdb.DependencyTarget) error {
	active := map[jobdb.DependencyTarget]bool{}
	done := map[jobdb.DependencyTarget]bool{}
	var visit func(jobdb.DependencyTarget) error
	visit = func(n jobdb.DependencyTarget) error {
		if active[n] {
			return jobdb.ErrDependencyCycle
		}
		if done[n] {
			return nil
		}
		active[n] = true
		if n.GoalID != "" {
			g, err := e.Store.Goal(jobdb.GoalKey{TenantId: tenant, GoalID: n.GoalID})
			if err != nil {
				return err
			}
			if g.State == jobdb.GoalOpen {
				js, err := e.Store.Candidates(g.GoalKey)
				if err != nil {
					return err
				}
				for _, j := range js {
					if !j.Terminal {
						if err = visit(jobdb.DependencyTarget{JobID: j.Key.JobId}); err != nil {
							return err
						}
					}
				}
			}
		} else {
			key := jobdb.JobKey{TenantId: tenant, JobId: n.JobID}
			j, err := e.Store.Job(key)
			if err != nil {
				return err
			}
			if !j.Terminal {
				ws, err := e.Store.ConsumerWaits(key)
				if err != nil {
					return err
				}
				for _, w := range ws {
					if w.State != jobdb.DependencyPending || (w.Deadline != nil && !e.Now.Before(*w.Deadline)) {
						continue
					}
					for _, d := range w.Dependencies {
						if d.State == jobdb.DependencyPending {
							if err = visit(d.Target); err != nil {
								return err
							}
						}
					}
				}
			}
		}
		active[n] = false
		done[n] = true
		return nil
	}
	return visit(start)
}
