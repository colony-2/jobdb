package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type executionPolicy struct {
	TotalTimeout   time.Duration `json:"total_timeout"`
	AttemptTimeout time.Duration `json:"attempt_timeout"`
	Retry          RetryPolicy   `json:"retry"`
	Rules          []string      `json:"rules,omitempty"`
	Restart        bool          `json:"restart,omitempty"`
}

func makeExecutionPolicy(opts ScopeOptions) (executionPolicy, error) {
	p := executionPolicy{TotalTimeout: opts.TotalTimeout, AttemptTimeout: opts.AttemptTimeout, Retry: normalizeRetryPolicy(opts.Retry), Restart: opts.OnRestart != nil}
	if p.TotalTimeout < 0 || p.AttemptTimeout < 0 || opts.Retry.MaximumAttempts < 0 || opts.Retry.InitialInterval < 0 || opts.Retry.MaximumInterval < 0 || opts.Retry.BackoffCoefficient < 0 || math.IsNaN(opts.Retry.BackoffCoefficient) || math.IsInf(opts.Retry.BackoffCoefficient, 0) {
		return p, fmt.Errorf("invalid execution policy")
	}
	names := map[string]bool{}
	for _, r := range opts.Catch {
		if r.ID == "" || names[r.ID] || r.Match == nil || r.Handle == nil {
			return p, fmt.Errorf("catch rules require unique ids, predicates and handlers")
		}
		names[r.ID] = true
		p.Rules = append(p.Rules, r.ID)
	}
	return p, nil
}
func deadlineFrom(at time.Time, limit time.Duration) time.Time {
	if limit <= 0 {
		return time.Time{}
	}
	return at.Add(limit)
}
func earlier(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}
func (c *executionContext) match(rec jobdb.ExecutionRecord, descriptor json.RawMessage) error {
	if !sameExecutionJSON(rec.Descriptor, descriptor) {
		return c.run.control(fmt.Errorf("%w: logical descriptor changed for %s", ErrWorkflowNotDeterministic, rec.ID))
	}
	return nil
}

func (c *executionContext) invoke(fn func() (TaskData, error)) (TaskData, error) {
	// Timers start only at the live edge, never while consuming completed history.
	e := c.run
	e.mu.Lock()
	if !e.r.replay && e.peekLocked() == nil {
		if err := c.liveLocked(); err != nil {
			e.mu.Unlock()
			return nil, err
		}
	}
	e.mu.Unlock()
	result := make(chan taskResult, 1)
	go func() {
		var out TaskData
		var err error
		returned := false
		defer func() {
			if p := recover(); p != nil {
				err = panicToAppError(p)
			}
			if !returned && err == nil {
				err = panicToAppError("callback exited without returning")
			}
			result <- taskResult{out, err}
		}()
		out, err = fn()
		returned = true
	}()
	select {
	case <-c.ctx.Done():
		return nil, context.Cause(c.ctx)
	case r := <-result:
		e.mu.Lock()
		var liveErr error
		if !e.r.replay && e.peekLocked() == nil {
			liveErr = c.liveLocked()
		}
		e.mu.Unlock()
		if liveErr != nil {
			return nil, liveErr
		}
		if err := context.Cause(c.ctx); err != nil {
			return nil, err
		}
		return r.output, r.err
	}
}
func (c *executionContext) finishBody(kind, id string, out TaskData, bodyErr error, input *jobdb.ExecutionSnapshot, retryPolicies ...RetryPolicy) (jobdb.ExecutionRecord, error) {
	return c.finishOutcome(kind, id, out, bodyErr, input, false, retryPolicies...)
}
func (c *executionContext) finishOutcome(kind, id string, out TaskData, bodyErr error, input *jobdb.ExecutionSnapshot, deferred bool, retryPolicies ...RetryPolicy) (jobdb.ExecutionRecord, error) {
	if bodyErr != nil && isExecutionControl(bodyErr) && !errors.Is(bodyErr, errRecordedBoundary) {
		return jobdb.ExecutionRecord{}, bodyErr
	}
	if next := c.run.peek(); next != nil && next.record.Kind == kind && next.record.ID == id {
		return c.event(kind, id, func() *jobdb.ExecutionRecord { return nil })
	}
	if errors.Is(bodyErr, errRecordedBoundary) {
		if c.run.peek() != nil {
			// An ancestor may have timed out before this nested scope committed
			// an outcome. Keep unwinding to that recorded boundary; event still
			// rejects unrelated identities or non-timeout records.
			return c.event(kind, id, func() *jobdb.ExecutionRecord { return nil })
		}
		return jobdb.ExecutionRecord{}, c.run.control(fmt.Errorf("%w: unexpected interrupted boundary %s", ErrWorkflowNotDeterministic, id))
	}
	rec, err := recordResult(c.ctx, out, bodyErr)
	if err != nil {
		return jobdb.ExecutionRecord{}, err
	}
	rec.Input = input
	if bodyErr != nil && len(retryPolicies) > 0 {
		retryable := isRetryable(bodyErr, retryPolicies[0])
		rec.Retryable = &retryable
	}
	return c.recordEvent(kind, id, func() *jobdb.ExecutionRecord { return rec }, deferred)
}
func (c *executionContext) DoScope(site string, opts ScopeOptions, body func(ExecutionContext) (JobData, error)) (JobData, error) {
	id, err := c.occurrence("scope", site)
	if err != nil {
		return nil, err
	}
	defer c.calls.Unlock()
	if body == nil {
		return nil, fmt.Errorf("scope body is required")
	}
	policy, err := makeExecutionPolicy(opts)
	if err != nil {
		return nil, err
	}
	descriptor, _ := json.Marshal(policy)
	if err := c.applyRestart(id, opts); err != nil {
		return nil, err
	}
	// Pure organizational scopes have no declaration to persist. Identity still
	// includes their deterministic occurrence path.
	significant := site == "$job" || policy.TotalTimeout > 0 || policy.AttemptTimeout > 0 || policy.Retry.MaximumAttempts > 1 || len(policy.Rules) > 0 || policy.Restart
	anchor := c.run.progressTime()
	entry := jobdb.ExecutionRecord{At: anchor, Descriptor: descriptor, TotalDeadline: deadlineFrom(anchor, policy.TotalTimeout)}
	if significant {
		entry, err = c.event("scope", id, func() *jobdb.ExecutionRecord { return &entry })
		if err != nil {
			return nil, err
		}
		if err = c.match(entry, descriptor); err != nil {
			return nil, err
		}
	}
	total := earlier(entry.TotalDeadline, c.deadline)
	for attempt := 1; ; attempt++ {
		aid := fmt.Sprintf("%s/attempt:%d", id, attempt)
		at := c.run.progressTime()
		start := jobdb.ExecutionRecord{At: at, Attempt: attempt, Deadline: earlier(deadlineFrom(at, policy.AttemptTimeout), total), TotalDeadline: total}
		if significant {
			start, err = c.event("attempt", aid, func() *jobdb.ExecutionRecord { return &start })
			if err != nil {
				return nil, err
			}
		}
		child := c.child(aid, start.Deadline, total)
		child.policy = policy
		child.attempt = attempt
		if site == "$job" {
			// Use a distinct cache owner per whole-job attempt, including on recovery.
			// Scope/task retries below the job share that owner.
			child.cache = &workerRunner{ctx: child.ctx, lease: c.run.r.lease, runtime: c.run.r.runtime, replay: c.run.r.replay, jobKey: c.run.r.jobKey}
		}
		if site == "$job" {
			c.run.observerMu.Lock()
			c.run.r.emitJobStart(attempt, c.run.snapshotData(c.run.jobInput), start.At)
			c.run.observerMu.Unlock()
		}
		out, bodyErr := child.invoke(func() (TaskData, error) {
			if restart := c.run.restarts[id]; restart != nil && opts.OnRestart != nil {
				if err := opts.OnRestart(append(json.RawMessage(nil), restart.Payload...)); err != nil {
					return nil, err
				}
			}
			return body(child)
		})
		child.close()
		if budget, ok := c.run.continuationBudget(aid); ok {
			total = budget.TotalDeadline
		}
		// Normal scope completion is reconstructed. A recorded exceptional result
		// is consumed before consulting live clocks or recomputing a branch.
		next := c.run.peek()
		recorded := next != nil && next.record.Kind == "result" && next.record.ID == aid
		var retryable *bool
		var rec jobdb.ExecutionRecord
		if site == "$job" || bodyErr != nil || recorded {
			rec, err = c.finishOutcome("result", aid, out, bodyErr, nil, site != "$job", policy.Retry)
			if err != nil {
				return nil, err
			}
			out, bodyErr = c.run.resultData(rec)
			retryable = rec.Retryable
		}
		if site == "$job" {
			c.run.observerMu.Lock()
			c.run.r.emitJobEnd(attempt, out, bodyErr, rec.At)
			c.run.observerMu.Unlock()
		}
		out, bodyErr, retry, err := c.handleFailure(aid, total, policy, opts.Catch, attempt, out, bodyErr, retryable)
		if err != nil {
			return nil, err
		}
		if !retry {
			return out, bodyErr
		}
	}
}
func (c *executionContext) handleFailure(id string, total time.Time, p executionPolicy, rules []CatchRule, attempt int, out TaskData, bodyErr error, retryable *bool) (TaskData, error, bool, error) {
	if budget, ok := c.run.continuationBudget(id); ok {
		total = budget.TotalDeadline
		p.Retry.MaximumAttempts += int32(budget.Attempt - 1)
	}
	if bodyErr == nil {
		return out, nil, false, nil
	}
	if len(rules) == 0 && p.Retry.MaximumAttempts <= 1 {
		return out, bodyErr, false, nil
	}
	disposition := CatchContinue
	if bodyErr != nil {
		selectionValue := &jobdb.ExecutionRecord{}
		// A missing catch/retry decision is a control boundary, not permission
		// to reevaluate predicates or live clocks during inspection.
		if c.run.r.replay && c.run.peek() == nil {
			return nil, nil, false, ReplayCacheMissError{JobKey: c.GetJobKey(), Ordinal: c.run.nextOrdinal(), Reason: ReplayCacheMissAwaitNotReady}
		}
		if c.run.peek() == nil && (total.IsZero() || time.Now().Before(total)) {
			failure := bodyErr
			matcher := c.child(id+"/matching", earlier(total, c.deadline), total)
			value, matchErr := matcher.invoke(func() (TaskData, error) {
				for _, rule := range rules {
					if rule.Match(failure) {
						return NewTaskData(rule.ID)
					}
				}
				return NewTaskData("")
			})
			matcher.close()
			if isExecutionControl(matchErr) {
				return nil, nil, false, matchErr
			}
			if matchErr != nil {
				selectionValue, _ = recordResult(c.ctx, nil, matchErr)
			} else if value != nil {
				raw, err := value.GetData()
				if err != nil {
					return nil, nil, false, err
				}
				if err = json.Unmarshal(raw, &selectionValue.Rule); err != nil {
					return nil, nil, false, err
				}
			}
		}
		selection, err := c.event("catch", id, func() *jobdb.ExecutionRecord { return selectionValue })
		if err != nil {
			return nil, nil, false, err
		}
		if selection.ErrorKind != "" {
			_, bodyErr = c.run.resultData(selection)
			disposition = CatchPropagate
		}
		if selection.Rule != "" {
			var selected *CatchRule
			for i := range rules {
				if rules[i].ID == selection.Rule {
					selected = &rules[i]
					break
				}
			}
			if selected == nil {
				return nil, nil, false, c.run.control(fmt.Errorf("%w: recorded catch %s unavailable", ErrWorkflowNotDeterministic, selection.Rule))
			}
			failure := bodyErr
			bodyRetryable := retryable
			handler := c.child(id+"/catch:"+selection.Rule, earlier(total, c.deadline), total)
			var decision CatchResult
			var handlerRetryable *bool
			handlerOutput, handlerErr := handler.invoke(func() (TaskData, error) {
				result, err := selected.Handle(handler, failure)
				if err != nil {
					return nil, err
				}
				preserveRetry := result.Err == nil
				if result.Disposition != CatchHandled && result.Err == nil {
					result.Err = failure
				}
				record, err := recordResult(handler.ctx, result.Output, result.Err)
				if err != nil {
					return nil, err
				}
				record.Disposition = string(result.Disposition)
				if result.Err != nil {
					allowed := isRetryable(result.Err, p.Retry)
					record.Retryable = &allowed
					if preserveRetry && bodyRetryable != nil {
						record.Retryable = bodyRetryable
					}
				}
				return NewTaskData(record)
			})
			if handlerErr == nil && handlerOutput != nil {
				raw, err := handlerOutput.GetData()
				if err != nil {
					return nil, nil, false, err
				}
				var record jobdb.ExecutionRecord
				if err = json.Unmarshal(raw, &record); err != nil {
					return nil, nil, false, err
				}
				handlerRetryable = record.Retryable
				decision.Disposition = CatchDisposition(record.Disposition)
				decision.Output, decision.Err = c.run.resultData(record)
			}
			handler.close()
			if isExecutionControl(handlerErr) && !errors.Is(handlerErr, errRecordedBoundary) {
				return nil, nil, false, handlerErr
			}
			var hrec jobdb.ExecutionRecord
			if next := c.run.peek(); next != nil && next.record.Kind == "handled" && next.record.ID == id {
				hrec, err = c.event("handled", id, func() *jobdb.ExecutionRecord { return nil })
			} else {
				if handlerErr != nil {
					decision = CatchResult{Disposition: CatchPropagate, Err: handlerErr}
				}
				if decision.Disposition != CatchHandled && decision.Disposition != CatchContinue && decision.Disposition != CatchPropagate {
					return nil, nil, false, fmt.Errorf("invalid catch disposition %q", decision.Disposition)
				}
				if decision.Disposition != CatchHandled && decision.Err == nil {
					decision.Err = bodyErr
				}
				record, e := recordResult(c.ctx, decision.Output, decision.Err)
				if e != nil {
					return nil, nil, false, e
				}
				record.Disposition = string(decision.Disposition)
				record.Retryable = handlerRetryable
				hrec, err = c.event("handled", id, func() *jobdb.ExecutionRecord { return record })
			}
			if err != nil {
				return nil, nil, false, err
			}
			out, bodyErr = c.run.resultData(hrec)
			disposition = CatchDisposition(hrec.Disposition)
			retryable = hrec.Retryable
		}
	}
	decision, err := c.event("decision", id, func() *jobdb.ExecutionRecord {
		allowed := isRetryable(bodyErr, p.Retry)
		if retryable != nil {
			allowed = *retryable
		}
		retry := bodyErr != nil && disposition != CatchPropagate && attempt < int(p.Retry.MaximumAttempts) && allowed && (total.IsZero() || time.Now().Before(total))
		rec := &jobdb.ExecutionRecord{Retry: retry, Disposition: string(disposition)}
		if retry {
			rec.WakeAt = time.Now().UTC().Add(computeBackoff(p.Retry, attempt))
		}
		return rec
	})
	if err != nil {
		return nil, nil, false, err
	}
	if decision.Retry {
		if err := c.waitUntil(decision.WakeAt, total); err != nil {
			return nil, nil, false, err
		}
	}
	return out, bodyErr, decision.Retry, nil
}
func (c *executionContext) waitUntil(wake, total time.Time) error {
	// If the next attempt is already recorded, its backoff was satisfied.
	if c.run.peek() != nil {
		return nil
	}
	if c.run.r.replay {
		// A recorded backoff may already have elapsed while the next attempt's
		// declarations are still deferred. Do not apply execution deadlines or
		// sleep here; the next task remains logically waiting until the wake time.
		if !wake.After(time.Now()) {
			return nil
		}
		return ReplayCacheMissError{JobKey: c.GetJobKey(), Ordinal: c.run.nextOrdinal(), Reason: ReplayCacheMissAwaitNotReady}
	}
	e := c.run
	e.mu.Lock()
	err := c.liveLocked()
	e.mu.Unlock()
	if err != nil {
		return err
	}
	wake = earlier(wake, earlier(total, c.deadline))
	if !wake.After(time.Now()) {
		return nil
	}
	threshold := e.r.awaitThreshold
	if threshold <= 0 {
		threshold = 5 * time.Minute
	}
	if time.Until(wake) > threshold {
		if err := c.checkpointPath(); err != nil {
			return err
		}
		err = e.r.lease.Reschedule(c.ctx, RescheduleExecutionRequest{NextRoute: Route{JobType: e.r.worker.JobWorker.Name()}, WaitUntil: &wake})
		if err != nil {
			return e.control(err)
		}
		return c.run.suspend()
	}
	t := time.NewTimer(time.Until(wake))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-c.ctx.Done():
		return context.Cause(c.ctx)
	}
}
