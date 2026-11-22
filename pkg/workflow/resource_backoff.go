package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
)

// ResourceBackoffOption describes one desired policy value. Options are sealed,
// validated together, and duplicate values reject even when equal.
type ResourceBackoffOption struct {
	kind     uint8
	duration time.Duration
	factor   float64
	budget   int64
}

func WithInitialWait(v time.Duration) ResourceBackoffOption {
	return ResourceBackoffOption{kind: 1, duration: v}
}
func WithBackoffGrowth(v float64) ResourceBackoffOption {
	return ResourceBackoffOption{kind: 2, factor: v}
}
func WithMaxWait(v time.Duration) ResourceBackoffOption {
	return ResourceBackoffOption{kind: 3, duration: v}
}
func WithBackoffJitter(v float64) ResourceBackoffOption {
	return ResourceBackoffOption{kind: 4, factor: v}
}
func WithCrashBudget(v int64) ResourceBackoffOption { return ResourceBackoffOption{kind: 5, budget: v} }

// EAGAINWait returns a safe-restart signal. Return it immediately after joining
// application activity. It never sleeps and never acknowledges a suspension.
// Manual/offline contexts reject; required context interfaces stay unchanged.
func EAGAINWait(ctx any, options ...ResourceBackoffOption) error {
	p := jobdb.DefaultResourceBackoffPolicy()
	seen := uint8(0)
	for _, o := range options {
		if o.kind < 1 || o.kind > 5 || seen&(1<<o.kind) != 0 {
			return resourceInfrastructure(jobdb.ErrInvalidResourceBackoff)
		}
		seen |= 1 << o.kind
		switch o.kind {
		case 1:
			p.Initial = o.duration
		case 2:
			p.Growth = o.factor
		case 3:
			p.Maximum = o.duration
		case 4:
			p.Jitter = o.factor
		case 5:
			v := o.budget
			p.CrashAllowance = &v
		}
	}
	if err := p.Validate(); err != nil {
		return resourceInfrastructure(err)
	}
	adapter, ok := ctx.(interface {
		resourceWait(jobdb.ResourceBackoffPolicy) error
	})
	if !ok {
		return resourceInfrastructure(&jobdb.ResourceBackoffUnsupportedError{Feature: "execution context"})
	}
	return adapter.resourceWait(p)
}
func resourceInfrastructure(err error) error {
	return jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureRuntimeIO, "resource_backoff", nil, nil)
}

type resourceWaitSignal struct {
	policy   jobdb.ResourceBackoffPolicy
	owner    *executionContext
	input    *jobdb.ExecutionSnapshot
	id       string
	taskType string
}

func (*resourceWaitSignal) Error() string { return "resource temporarily unavailable" }
func (c *executionContext) resourceWait(p jobdb.ResourceBackoffPolicy) error {
	if c == nil || c.run == nil || c.run.r.replay {
		return resourceInfrastructure(&jobdb.ResourceBackoffUnsupportedError{Feature: "live execution context"})
	}
	if err := c.run.r.lease.ExecutionState().ResourceBackoff.Validate(false); err != nil {
		return resourceInfrastructure(err)
	}
	if _, ok := c.run.r.lease.(jobdb.ResourceBackoffLease); !ok {
		return resourceInfrastructure(&jobdb.ResourceBackoffUnsupportedError{Feature: "lease port"})
	}
	return &resourceWaitSignal{policy: p, owner: c}
}
func (tc TaskContext) resourceWait(p jobdb.ResourceBackoffPolicy) error {
	if tc.resourceContext == nil {
		return resourceInfrastructure(&jobdb.ResourceBackoffUnsupportedError{Feature: "task context"})
	}
	return tc.resourceContext.resourceWait(p)
}

// CrashBudgetRemaining returns an acquired snapshot; false means unknown.
func CrashBudgetRemaining(ctx any) (int64, bool) {
	if a, ok := ctx.(interface{ crashBudgetRemaining() (int64, bool) }); ok {
		return a.crashBudgetRemaining()
	}
	return 0, false
}
func (c *executionContext) crashBudgetRemaining() (int64, bool) {
	b := c.run.r.lease.ExecutionState().CrashBudgetRemaining
	if b == nil {
		return 0, false
	}
	return *b, true
}
func (tc TaskContext) crashBudgetRemaining() (int64, bool) {
	if tc.resourceContext == nil {
		return 0, false
	}
	return tc.resourceContext.crashBudgetRemaining()
}

func (c *executionContext) interceptResource(id string, input *jobdb.ExecutionSnapshot, err error) error {
	var signal *resourceWaitSignal
	if !errors.As(err, &signal) {
		return err
	}
	// Cancellation, deadlines and infrastructure faults were checked before this
	// boundary. A joined cancellation cannot be reclassified as a resource wait.
	var timeout *jobdb.TimeoutError
	if isExecutionControl(err) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) {
		return err
	}
	if signal.owner == nil || signal.owner.run != c.run || signal.owner.id != id {
		return resourceInfrastructure(fmt.Errorf("resource marker belongs to a different invocation"))
	}
	e := c.run
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.resourcePending == nil {
		signal.id = id
		signal.input = input
		signal.taskType = signal.owner.resourceTaskType
		e.resourcePending = signal
		for scope := range e.active {
			scope.cancel(errResourcePreparing)
		}
	}
	return errResourcePreparing
}

var errResourcePreparing = errors.New("resource suspension preparing")

func (e *executionRun) commitResource(ctx context.Context) error {
	// No engine callback may survive the release. A worker ignoring cancellation
	// makes this an infrastructure failure while the lease remains unreleased.
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		e.mu.Lock()
		quiet := len(e.active) == 0 && e.taskCallbacks == 0
		e.mu.Unlock()
		if quiet {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return resourceInfrastructure(fmt.Errorf("resource suspension could not quiesce callbacks"))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := e.r.guard.failure(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fatal != nil {
		return e.fatal
	}
	signal := e.resourcePending
	c := signal.owner
	// Timers retain their original values through recovery, including an expired
	// attempt whose timer delivery raced this boundary.
	if !c.deadline.IsZero() && !time.Now().Before(c.deadline) {
		return resourceInfrastructure(context.DeadlineExceeded)
	}
	rec := jobdb.ExecutionRecord{Kind: "resource_checkpoint", ID: signal.id, Input: signal.input, Deadline: c.deadline, TotalDeadline: c.totalDeadline, Resource: &jobdb.ResourceCheckpoint{Version: 1, Invocation: signal.id, Attempt: 1, Policy: signal.policy, ExecutionTaskType: signal.taskType, PayloadRevision: e.r.lease.ClientPayloadRevision()}}
	if c.attempt > 0 {
		rec.Resource.Attempt = c.attempt
	}
	var names []string
	for name := range e.resourceFrames {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		rec.Resource.Frames = append(rec.Resource.Frames, e.resourceFrames[name])
	}
	// Task attempts include their current recipe attempt in the canonical path.
	if err := e.activateLocked(); err != nil {
		return err
	}
	ordinal := e.ordinal
	if err := e.appendLocked(&rec); err != nil {
		return resourceInfrastructure(err)
	}
	raw, _ := json.Marshal(rec)
	state := e.r.lease.ExecutionState()
	route := e.r.lease.Route()
	task := state.TaskWait
	if task != nil {
		copy := *task
		copy.OutputOrdinal = e.ordinal
		task = &copy
	}
	resume := jobdb.ResourceResume{Version: 1, CheckpointOrdinal: ordinal, CheckpointHash: jobdb.ResourceCheckpointHash(raw), Invocation: signal.id, Attempt: rec.Resource.Attempt, Route: route, TaskWait: task, PayloadRevision: e.r.lease.ClientPayloadRevision()}
	req := jobdb.ResourceBackoffRequest{Version: 1, RequestID: ksuid.New().String(), Invocation: signal.id, Attempt: resume.Attempt, Reason: jobdb.ResourceBackoffReason, Policy: signal.policy, Resume: resume}
	port, ok := e.r.lease.(jobdb.ResourceBackoffLease)
	if !ok {
		return resourceInfrastructure(jobdb.ErrResourceBackoffUnsupported)
	}
	receipt, err := port.RescheduleResourceBackoff(ctx, req)
	if err != nil {
		return resourceInfrastructure(err)
	}
	if receipt.Version != 1 || receipt.RequestID != req.RequestID || receipt.LeaseID != e.r.lease.LeaseID() || receipt.JobKey != e.r.jobKey || receipt.Count < 1 {
		return resourceInfrastructure(fmt.Errorf("invalid resource acknowledgment"))
	}
	e.r.lease.StopKeepAlive()
	e.suspended = true
	return nil
}
