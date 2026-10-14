package workflow

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type taskDispatchState struct {
	taskType    string
	alternateAt *time.Time
}

func (c *executionContext) externalTask(call TaskCall, id string, input *jobdb.ExecutionSnapshot, deadline, total time.Time, state *taskDispatchState) (jobdb.ExecutionRecord, error) {
	rec, err := c.event("wait", id, func() *jobdb.ExecutionRecord {
		if call.Alternate != nil && state.alternateAt == nil {
			at := time.Now().UTC().Add(call.Alternate.After)
			state.alternateAt = &at
		}
		record := &jobdb.ExecutionRecord{Input: input, Deadline: deadline, TotalDeadline: total, Descriptor: json.RawMessage(fmt.Sprintf("%q", call.TaskType))}
		if c.run.initialVersions.ExecutionPatternVersion >= 2 {
			record.ExecutionTaskType, record.AlternateAt = state.taskType, state.alternateAt
		}
		return record
	})
	if err != nil {
		return jobdb.ExecutionRecord{}, err
	}
	if rec.ExecutionTaskType != "" {
		state.taskType = rec.ExecutionTaskType
	}
	state.alternateAt = rec.AlternateAt
	// The persisted wait is the generation; rescheduling must retain its due time.
	ordinal := c.run.nextOrdinal() - 1
	for _, event := range c.run.events {
		if event.record.Kind == "wait" && event.record.ID == id {
			ordinal = event.ordinal
		}
	}
	outputPositions := map[int64]bool{ordinal + 1: true}
	for _, event := range c.run.events {
		if event.ordinal > ordinal && event.record.Kind == "activation" {
			// A task may yield and later re-enter external waiting. Its pending
			// output then follows the activation that ran the suspended worker.
			outputPositions[event.ordinal+1] = true
		}
	}
	dispatched := false
	for next := c.run.peek(); next != nil && next.record.Kind == "dispatch" && next.record.ID == id; next = c.run.peek() {
		dispatch, err := c.event("dispatch", id, func() *jobdb.ExecutionRecord { return nil })
		if err != nil {
			return jobdb.ExecutionRecord{}, err
		}
		if !allowedExecutionType(call, dispatch.ExecutionTaskType) || (state.taskType != call.TaskType && state.taskType != dispatch.ExecutionTaskType) {
			return jobdb.ExecutionRecord{}, c.run.control(fmt.Errorf("%w: invalid recorded task dispatch", ErrWorkflowNotDeterministic))
		}
		state.taskType, dispatched = dispatch.ExecutionTaskType, true
		outputPositions[next.ordinal+1] = true
	}
	if !allowedExecutionType(call, state.taskType) {
		return jobdb.ExecutionRecord{}, c.run.control(fmt.Errorf("%w: invalid waiting task type", ErrWorkflowNotDeterministic))
	}
	if next := c.run.peek(); next != nil {
		return c.event("result", id, func() *jobdb.ExecutionRecord { return nil })
	}
	if c.run.r.replay {
		return jobdb.ExecutionRecord{}, ReplayCacheMissError{JobKey: c.GetJobKey(), Ordinal: c.run.nextOrdinal(), Reason: ReplayCacheMissTaskResultMissing}
	}
	raw, _ := json.Marshal(rec)
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	lease := c.run.r.lease
	route := lease.Route()
	wait := lease.ExecutionState().TaskWait
	matchingWait := wait != nil && wait.InputOrdinal == ordinal && outputPositions[wait.OutputOrdinal] && wait.InputHash == hash && wait.ResumeJobType == c.run.r.worker.JobWorker.Name()
	execute := false
	if matchingWait && route.TaskType != "" {
		if route.JobType != c.run.r.worker.JobWorker.Name() || !allowedExecutionType(call, route.TaskType) || (state.taskType != call.TaskType && state.taskType != route.TaskType) {
			return jobdb.ExecutionRecord{}, c.run.control(fmt.Errorf("%w: acquired task route differs from declared invocation", ErrWorkflowNotDeterministic))
		}
		if route.TaskType != call.TaskType && (state.alternateAt == nil || time.Now().Before(*state.alternateAt)) {
			return jobdb.ExecutionRecord{}, c.run.control(fmt.Errorf("%w: alternate acquired before eligibility", ErrWorkflowNotDeterministic))
		}
		// The scheduler's committed route is authoritative, including a B claim
		// whose worker stopped before it could append its dispatch record.
		if _, ok := c.run.r.worker.TaskWorkers[route.TaskType]; ok {
			if !dispatched || state.taskType != route.TaskType {
				if c.run.initialVersions.ExecutionPatternVersion >= 2 {
					_, err = c.event("dispatch", id, func() *jobdb.ExecutionRecord { return &jobdb.ExecutionRecord{ExecutionTaskType: route.TaskType} })
					if err != nil {
						return jobdb.ExecutionRecord{}, err
					}
				}
			}
			state.taskType, execute = route.TaskType, true
		}
	} else if dispatched && wait == nil {
		// A task-level yield/child wait resumes orchestration on the job route.
		// Its recorded implementation selection survives that suspension.
		_, execute = c.run.r.worker.TaskWorkers[state.taskType]
	}
	// Persist a committed B selection even when this attempt expired before
	// pickup: a retry must not return to A. Neither callback may run past budget.
	if !rec.Deadline.IsZero() && !time.Now().Before(rec.Deadline) {
		scope, retryable := TimeoutScopeInvocation, true
		if !rec.TotalDeadline.IsZero() && !rec.Deadline.Before(rec.TotalDeadline) {
			scope, retryable = TimeoutScopeTotal, false
		}
		return c.finishBody("result", id, nil, NewTimeoutError("task", 0, scope, nil, retryable), rec.Input)
	}
	if execute {
		child := c.child(id, rec.Deadline, total)
		worker := c.run.r.worker.TaskWorkers[state.taskType]
		out, taskErr := child.invokeTask(worker, snapshotData(rec.Input))
		child.close()
		return c.finishBody("result", id, out, taskErr, rec.Input, call.Policy.Retry)
	}
	req := RescheduleExecutionRequest{NextRoute: Route{JobType: c.run.r.worker.JobWorker.Name(), TaskType: state.taskType}, TaskWait: &TaskWait{InputOrdinal: ordinal, OutputOrdinal: c.run.nextOrdinal(), InputHash: hash, ResumeJobType: c.run.r.worker.JobWorker.Name()}}
	wakeAt := rec.Deadline
	wakeRoute := Route{JobType: c.run.r.worker.JobWorker.Name()}
	if call.Alternate != nil && state.taskType == call.TaskType && state.alternateAt != nil && (wakeAt.IsZero() || state.alternateAt.Before(wakeAt)) {
		wakeAt, wakeRoute = *state.alternateAt, Route{JobType: c.run.r.worker.JobWorker.Name(), TaskType: call.Alternate.TaskType}
	}
	if !wakeAt.IsZero() {
		delay := max(time.Until(wakeAt), 0)
		req.AlternateRoute, req.AlternateAfter = &wakeRoute, &delay
	}
	if err := lease.Reschedule(c.ctx, req); err != nil {
		return jobdb.ExecutionRecord{}, c.run.control(err)
	}
	return jobdb.ExecutionRecord{}, c.run.suspend()
}

func allowedExecutionType(call TaskCall, taskType string) bool {
	return taskType == call.TaskType || (call.Alternate != nil && taskType == call.Alternate.TaskType)
}
