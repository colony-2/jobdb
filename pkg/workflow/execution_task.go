package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type executionTaskDescriptor struct {
	Call   TaskCall        `json:"call"`
	Policy executionPolicy `json:"policy"`
}

func (c *executionContext) DoTask(call TaskCall, build func(context.Context) (TaskData, error)) (TaskData, error) {
	id, err := c.occurrence("task", call.Site)
	if err != nil {
		return nil, err
	}
	defer c.calls.Unlock()
	if call.TaskType == "" || call.ContractVersion == 0 || build == nil {
		return nil, fmt.Errorf("task type, contract version and input builder are required")
	}
	if call.Alternate != nil {
		alternate := *call.Alternate
		call.Alternate = &alternate
		if alternate.TaskType == "" || alternate.TaskType == call.TaskType || alternate.After < 0 {
			return nil, fmt.Errorf("alternate requires a different nonempty task type and nonnegative delay")
		}
		if c.run.initialVersions.ExecutionPatternVersion < 2 {
			return nil, c.run.control(fmt.Errorf("%w: task alternates require execution version 2", ErrWorkflowNotDeterministic))
		}
	}
	call.Command, err = canonicalJSON(call.Command)
	if err != nil {
		return nil, err
	}
	p, err := makeExecutionPolicy(ScopeOptions{TotalTimeout: durationPtrToDuration(call.Policy.TotalTimeout), AttemptTimeout: durationPtrToDuration(call.Policy.InvocationTimeout), Retry: call.Policy.Retry, Catch: call.Catch})
	if err != nil {
		return nil, err
	}
	descriptor, _ := json.Marshal(executionTaskDescriptor{call, p})
	if err := c.applyRestart(id, ScopeOptions{}); err != nil {
		return nil, err
	}
	entry, err := c.event("task", id, func() *jobdb.ExecutionRecord {
		return &jobdb.ExecutionRecord{Descriptor: descriptor, TotalDeadline: deadlineFrom(time.Now().UTC(), p.TotalTimeout)}
	})
	if err != nil {
		return nil, err
	}
	var recorded executionTaskDescriptor
	if err := json.Unmarshal(entry.Descriptor, &recorded); err != nil {
		return nil, c.run.control(fmt.Errorf("%w: invalid task policy for %s: %v", ErrWorkflowNotDeterministic, id, err))
	}
	call.Policy = recorded.Call.Policy
	p = p.withRecordedLifecycle(recorded.Policy)
	descriptor, _ = json.Marshal(executionTaskDescriptor{call, p})
	if err = c.match(entry, descriptor); err != nil {
		return nil, err
	}
	total := earlier(entry.TotalDeadline, c.deadline)
	var input *jobdb.ExecutionSnapshot
	dispatch := taskDispatchState{taskType: call.TaskType}
	for attempt := 1; ; attempt++ {
		aid := fmt.Sprintf("%s/attempt:%d", id, attempt)
		start, err := c.event("attempt", aid, func() *jobdb.ExecutionRecord {
			return &jobdb.ExecutionRecord{Attempt: attempt, Deadline: earlier(deadlineFrom(time.Now().UTC(), p.AttemptTimeout), total), TotalDeadline: total}
		})
		if err != nil {
			return nil, err
		}
		var rec jobdb.ExecutionRecord
		if next := c.run.peek(); next != nil && next.record.Kind == "result" && next.record.ID == aid {
			rec, err = c.event("result", aid, func() *jobdb.ExecutionRecord { return nil })
		} else if next := c.run.peek(); next != nil && next.record.Kind == "wait" && next.record.ID == aid {
			rec, err = c.externalTask(call, aid, next.record.Input, start.Deadline, total, &dispatch)
			if c.run.r.replay && isReplayCacheMiss(err) {
				return nil, c.run.unfinishedTask(call, aid, attempt, start.At, next.record.Input)
			}
		} else if next := c.run.peek(); next != nil {
			_, err = c.event("result", aid, func() *jobdb.ExecutionRecord { return nil })
			return nil, err
		} else if c.run.r.replay {
			if input == nil {
				// Input construction is replay-safe orchestration. Completed and
				// externally waiting tasks use their stored snapshot instead.
				out, buildErr := c.invoke(func() (TaskData, error) { return build(c.ctx) })
				if buildErr != nil {
					return nil, c.run.control(buildErr)
				}
				input, err = executionSnapshot(c.ctx, out)
				if err != nil {
					return nil, c.run.control(err)
				}
			}
			return nil, c.run.unfinishedTask(call, aid, attempt, start.At, input)
		} else {
			// A local invocation with no committed outcome starts a fresh timer even
			// when a prior scheduler handoff persisted its path metadata.
			start.At = time.Now().UTC()
			start.Deadline = earlier(deadlineFrom(start.At, p.AttemptTimeout), total)
			child := c.child(aid, start.Deadline, total)
			var out TaskData
			var taskErr error
			if input == nil {
				out, taskErr = child.invoke(func() (TaskData, error) { return build(child.ctx) })
				if taskErr == nil {
					input, taskErr = executionSnapshot(child.ctx, out)
				}
			}
			worker, local := c.run.r.worker.TaskWorkers[dispatch.taskType]
			if taskErr == nil && !local {
				child.close()
				rec, err = c.externalTask(call, aid, input, start.Deadline, total, &dispatch)
			} else {
				if taskErr == nil {
					workerInput := c.run.snapshotData(input)
					out, taskErr = child.invokeTask(worker, workerInput)
				}
				child.close()
				rec, err = c.finishBody("result", aid, out, taskErr, input, p.Retry)
			}
		}
		if err != nil {
			return nil, err
		}
		c.run.emitTask(call, aid, attempt, start.At, rec)
		if rec.Input != nil {
			input = rec.Input
		}
		out, taskErr := c.run.resultData(rec)
		out, taskErr, retry, err := c.handleFailure(aid, total, p, call.Catch, attempt, out, taskErr, rec.Retryable)
		if err != nil {
			return nil, err
		}
		if !retry {
			return out, taskErr
		}
	}
}

func (c *executionContext) invokeTask(worker TaskWorker, input TaskData) (TaskData, error) {
	// On recovery, activation can be the next append. Commit it before exposing
	// the result ordinal to the worker; TaskContext's lease/wait actions don't
	// append execution records while Run continues in this activation.
	c.run.mu.Lock()
	err := c.liveLocked()
	if err == nil {
		err = c.run.activateLocked()
	}
	c.run.mu.Unlock()
	if err != nil {
		return nil, err
	}
	tc := c.taskContext()
	return c.invoke(func() (TaskData, error) { return worker.Run(tc, input) })
}

func (c *executionContext) taskContext() TaskContext {
	tc := newTaskContextWithLeaseActions(c.GetJobKey(), c.run.nextOrdinal(), c.Logger(), func(at time.Time) error { return c.waitUntil(at, c.deadline) }, c.awaitTaskJobs, c.SubmitJob, c.SubmitRestartJob, c.ClientPayload, c.ClientPayloadRevision, c.Yield, c.ResolveSecrets, c.ResolveSecretsFresh, c.GetConfiguration)
	tc.executionContext = c.ctx
	tc.goalID = c.run.r.goalID
	tc.createDelegation = c.createTaskDelegation
	// c.id ends with the engine-owned task attempt suffix. Consumer call sites
	// are JSON-quoted earlier in the identity and cannot replace this suffix.
	if pos := strings.LastIndex(c.id, "/attempt:"); pos >= 0 {
		tc.awaitDependencies = c.awaitTaskDependencies
		tc.InvocationID = c.id[:pos]
		tc.AttemptNumber, _ = strconv.Atoi(c.id[pos+len("/attempt:"):])
	}
	return tc
}
func (c *executionContext) Context() context.Context { return c.ctx }
func (c *executionContext) GetJobKey() JobKey        { return c.run.r.jobKey }
func (c *executionContext) Logger() *slog.Logger     { return c.run.r.logger }
func (c *executionContext) cacheOwner() *workerRunner {
	for p := c; p != nil; p = p.parent {
		if p.cache != nil {
			return p.cache
		}
	}
	return c.run.r
}
func (c *executionContext) GetConfiguration(ctx context.Context) (ConfigurationSnapshot, error) {
	if err := context.Cause(c.ctx); err != nil {
		return ConfigurationSnapshot{}, err
	}
	return c.cacheOwner().GetConfiguration(ctx)
}
func (c *executionContext) ResolveSecrets(ctx context.Context, names []string) (SecretResolveResult, error) {
	if err := context.Cause(c.ctx); err != nil {
		return SecretResolveResult{}, err
	}
	callCtx, cancel := invocationCallContext(ctx, c.ctx)
	defer cancel()
	return c.cacheOwner().ResolveSecrets(callCtx, names)
}
func (c *executionContext) ResolveSecretsFresh(ctx context.Context, names []string) (SecretResolveResult, error) {
	callCtx, cancel := invocationCallContext(ctx, c.ctx)
	defer cancel()
	return c.cacheOwner().ResolveSecretsFresh(callCtx, names)
}
func (c *executionContext) ClientPayload() json.RawMessage { return c.run.r.ClientPayload() }
func (c *executionContext) ClientPayloadRevision() int64   { return c.run.r.ClientPayloadRevision() }
func (c *executionContext) SubmitJob(ctx context.Context, req SubmitJob) (JobHandle, error) {
	if err := context.Cause(c.ctx); err != nil {
		return JobHandle{}, err
	}
	if c.run.r.replay {
		return JobHandle{}, ErrReplayShouldNeverMutate
	}
	callCtx, cancel := invocationCallContext(ctx, c.ctx)
	defer cancel()
	return c.run.r.SubmitJob(callCtx, req)
}
func (c *executionContext) AwaitDuration(d Duration) error {
	id, err := c.occurrence("sleep", "wait")
	if err != nil {
		return err
	}
	defer c.calls.Unlock()
	descriptor, _ := json.Marshal(d)
	rec, err := c.event("sleep", id, func() *jobdb.ExecutionRecord {
		return &jobdb.ExecutionRecord{WakeAt: time.Now().UTC().Add(time.Duration(d)), Descriptor: descriptor}
	})
	if err == nil {
		err = c.match(rec, descriptor)
	}
	if err != nil {
		return err
	}
	return c.waitUntil(rec.WakeAt, c.deadline)
}
func (c *executionContext) AwaitJobs(ids ...string) error {
	id, err := c.occurrence("join", "jobs")
	if err != nil {
		return err
	}
	defer c.calls.Unlock()
	desc, _ := json.Marshal(ids)
	rec, err := c.event("join", id, func() *jobdb.ExecutionRecord { return &jobdb.ExecutionRecord{Descriptor: desc} })
	if err != nil {
		return err
	}
	if err = c.match(rec, desc); err != nil {
		return err
	}
	if c.run.peek() != nil {
		result, err := c.event("join_result", id, func() *jobdb.ExecutionRecord { return nil })
		if err != nil {
			return err
		}
		_, err = c.run.resultData(result)
		return err
	}
	if c.run.r.replay {
		return ReplayCacheMissError{JobKey: c.GetJobKey(), Ordinal: c.run.nextOrdinal(), Reason: ReplayCacheMissAwaitJobsPending}
	}
	done, waitErr := c.run.r.awaitJobsComplete(c.ctx, ids)
	if done || waitErr != nil {
		rec, err := c.finishBody("join_result", id, NewTaskDataOrPanic(true), waitErr, nil)
		if err != nil {
			return err
		}
		_, err = c.run.resultData(rec)
		return err
	}
	return c.suspendForJobs(ids)
}
func (c *executionContext) awaitTaskJobs(ids ...string) error {
	if err := context.Cause(c.ctx); err != nil {
		return err
	}
	done, err := c.run.r.awaitJobsComplete(c.ctx, ids)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	return c.suspendForJobs(ids)
}
func (c *executionContext) suspendForJobs(ids []string) error {
	if err := c.checkpointPath(); err != nil {
		return err
	}
	err := c.run.r.lease.Reschedule(c.ctx, RescheduleExecutionRequest{NextRoute: Route{JobType: c.run.r.worker.JobWorker.Name()}, WaitForJobIDs: ids, WaitUntil: nonzeroTime(c.deadline)})
	if err != nil {
		return c.run.control(err)
	}
	return c.run.suspend()
}
func nonzeroTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (c *executionContext) SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error) {
	if err := context.Cause(c.ctx); err != nil {
		return JobHandle{}, err
	}
	if c.run.r.replay {
		return JobHandle{}, ErrReplayShouldNeverMutate
	}
	callCtx, cancel := invocationCallContext(ctx, c.ctx)
	defer cancel()
	return c.run.r.SubmitRestartJob(callCtx, req)
}
func (c *executionContext) Yield(ctx context.Context, req YieldRequest) error {
	if err := context.Cause(c.ctx); err != nil {
		return err
	}
	if c.run.r.replay {
		return ErrReplayShouldNeverMutate
	}
	callCtx, cancel := invocationCallContext(ctx, c.ctx)
	defer cancel()
	if err := callCtx.Err(); err != nil {
		return c.run.control(err)
	}
	handoff, err := prepareHandoff(req)
	if err != nil {
		return err
	}
	if err := c.checkpointPath(); err != nil {
		return err
	}
	if err := c.run.r.lease.Reschedule(callCtx, req.Reschedule); err != nil {
		return c.run.control(err)
	}
	if !c.run.r.handoff.confirm(handoff) {
		return context.Canceled
	}
	return c.run.suspend()
}

func (c *executionContext) Job() JobHandle {
	return JobHandle{JobKey: c.GetJobKey(), GoalID: c.run.r.goalID}
}
