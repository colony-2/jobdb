package workflow

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

func goalDependencies(ids []string) []jobdb.Dependency {
	deps := make([]jobdb.Dependency, len(ids))
	for i, id := range ids {
		deps[i] = jobdb.Dependency{Target: jobdb.DependencyTarget{GoalID: id}, Condition: jobdb.JobPrereqSuccess}
	}
	return deps
}
func (c *executionContext) AwaitGoals(ids ...string) ([]jobdb.DependencyResolution, error) {
	return c.AwaitDependencies(goalDependencies(ids)...)
}
func (c *executionContext) AwaitDependencies(deps ...jobdb.Dependency) ([]jobdb.DependencyResolution, error) {
	if err := jobdb.ValidateDependencies(deps); err != nil {
		return nil, err
	}
	if len(deps) == 0 {
		return []jobdb.DependencyResolution{}, nil
	}
	id, err := c.occurrence("join", "dependencies")
	if err != nil {
		return nil, err
	}
	defer c.calls.Unlock()
	descriptor, _ := json.Marshal(deps)
	rec, err := c.event("join", id, func() *jobdb.ExecutionRecord { return &jobdb.ExecutionRecord{Descriptor: descriptor} })
	if err != nil {
		return nil, err
	}
	if err = c.match(rec, descriptor); err != nil {
		return nil, err
	}
	if c.run.peek() != nil {
		result, err := c.event("join_result", id, func() *jobdb.ExecutionRecord { return nil })
		if err != nil {
			return nil, err
		}
		return c.dependencyResult(result)
	}
	if c.run.r.replay {
		return nil, ReplayCacheMissError{JobKey: c.GetJobKey(), Ordinal: c.run.nextOrdinal(), Reason: ReplayCacheMissAwaitJobsPending}
	}
	waitID := fmt.Sprintf("join-%x", sha256.Sum256([]byte(id)))
	wait, err := c.run.r.runtime.GetDependencyWait(c.ctx, jobdb.DependencyWaitKey{JobKey: c.GetJobKey(), WaitID: waitID})
	if err == nil && wait.State != jobdb.DependencyPending {
		result, err := c.finishBody("join_result", id, NewTaskDataOrPanic(wait), nil, nil)
		if err != nil {
			return nil, err
		}
		return c.dependencyResult(result)
	}
	if err != nil && !errors.Is(err, jobdb.ErrDependencyWaitNotFound) {
		return nil, err
	}
	return nil, c.suspendForDependencies(jobdb.DependencyWait{WaitID: waitID, Dependencies: deps, Deadline: nonzeroTime(c.deadline)})
}
func (c *executionContext) dependencyResult(rec jobdb.ExecutionRecord) ([]jobdb.DependencyResolution, error) {
	data, err := c.run.resultData(rec)
	if err != nil {
		return nil, err
	}
	raw, err := data.GetData()
	if err != nil {
		return nil, err
	}
	var wait jobdb.DependencyWaitInfo
	if err = json.Unmarshal(raw, &wait); err != nil {
		return nil, err
	}
	return wait.Dependencies, wait.Err()
}
func (c *executionContext) suspendForDependencies(wait jobdb.DependencyWait) error {
	if err := c.checkpointPath(); err != nil {
		return err
	}
	err := c.run.r.lease.Reschedule(c.ctx, RescheduleExecutionRequest{NextRoute: Route{JobType: c.run.r.worker.JobWorker.Name()}, DependencyWait: &wait})
	if err != nil {
		return c.run.control(err)
	}
	return c.run.suspend()
}

// Tasks re-enter on recovery. Their invocation ID and descriptor give the wait a
// stable identity across activations; the durable task result records its outcome.
func (c *executionContext) awaitTaskDependencies(deps ...jobdb.Dependency) ([]jobdb.DependencyResolution, error) {
	if err := jobdb.ValidateDependencies(deps); err != nil {
		return nil, err
	}
	if len(deps) == 0 {
		return []jobdb.DependencyResolution{}, nil
	}
	raw, _ := json.Marshal(deps)
	id := fmt.Sprintf("task-join-%x", sha256.Sum256(append([]byte(c.id), raw...)))
	c.run.mu.Lock()
	cached, ok := c.run.dependencyBindings[id]
	c.run.mu.Unlock()
	if ok {
		return cached.Dependencies, cached.Err()
	}

	wait, err := c.run.r.runtime.GetDependencyWait(c.ctx, jobdb.DependencyWaitKey{JobKey: c.GetJobKey(), WaitID: id})
	if err == nil && wait.State != jobdb.DependencyPending {
		if err := c.recordTaskDependency(id, wait); err != nil {
			return nil, err
		}
		return wait.Dependencies, wait.Err()
	}
	if err != nil && !errors.Is(err, jobdb.ErrDependencyWaitNotFound) {
		return nil, err
	}
	if c.run.r.replay {
		return nil, ReplayCacheMissError{JobKey: c.GetJobKey(), Ordinal: c.run.nextOrdinal(), Reason: ReplayCacheMissAwaitJobsPending}
	}
	return nil, c.suspendForDependencies(jobdb.DependencyWait{WaitID: id, Dependencies: deps, Deadline: nonzeroTime(c.deadline)})
}

// Admission snapshots structured submission dependencies once in history.
// Replay consumes that snapshot without consulting mutable scheduler state.
func (c *executionContext) goalAdmission() (error, error) {
	var wait jobdb.DependencyWaitInfo
	next := c.run.peek()
	recorded := next != nil && next.record.Kind == "admission" && next.record.ID == "$goal-dependencies"
	if !recorded {
		if c.run.r.replay || next != nil || c.run.restart != nil {
			return nil, nil
		}
		var err error
		wait, err = c.run.r.runtime.GetDependencyWait(c.ctx, jobdb.DependencyWaitKey{JobKey: c.GetJobKey(), WaitID: "submission"})
		if errors.Is(err, jobdb.ErrDependencyWaitNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if wait.State == jobdb.DependencyPending {
			return nil, fmt.Errorf("job leased with pending submission dependencies")
		}
	}
	rec, err := c.event("admission", "$goal-dependencies", func() *jobdb.ExecutionRecord {
		record, _ := recordResult(c.ctx, NewTaskDataOrPanic(wait), nil)
		return record
	})
	if err != nil {
		return nil, err
	}
	_, failure := c.dependencyResult(rec)
	return failure, nil
}

// Record the full binding before task code observes it. These records are
// provenance, skipped by normal task-result replay and reused after a crash.
func (c *executionContext) recordTaskDependency(id string, wait jobdb.DependencyWaitInfo) error {
	if err := c.checkpointPath(); err != nil {
		return err
	}
	e := c.run
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.dependencyBindings[id]; exists {
		return nil
	}
	if err := c.liveLocked(); err != nil {
		return err
	}
	if err := e.activateLocked(); err != nil {
		return err
	}
	payload, err := json.Marshal(wait)
	if err != nil {
		return err
	}
	if err = e.appendLocked(&jobdb.ExecutionRecord{Kind: "dependency_binding", ID: id, Payload: payload}); err != nil {
		return err
	}
	e.dependencyBindings[id] = wait
	return nil
}
