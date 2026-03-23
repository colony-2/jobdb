# SWF Direct Job Execution Proposal

## Recommendation

Use a two-step direct API:

- `GetJobForRun(...)` acquires and prepares a runnable view of one specific job
- `JobRunnable.Run(listener)` executes one leased run if a lease was acquired
- the listener receives job and task lifecycle events asynchronously

I would keep this in `swf`, not on `WorkflowRuntime`, and I would not require constructing an `SWFEngine`.

## Proposed Shape

```go
type JobRunStatus string

const (
	JobRunNotLeaseable JobRunStatus = "NOT_LEASEABLE"
	JobRunCompleted    JobRunStatus = "COMPLETED"
	JobRunFailed       JobRunStatus = "FAILED"
	JobRunSuspended    JobRunStatus = "SUSPENDED"
)

type GetJobForRunRequest struct {
	JobKey        JobKey
	JobWorker     JobWorker
	TaskWorkers   []TaskWorker
	WorkerID      string
	LeaseDuration time.Duration
	Logger        *slog.Logger
	AwaitThreshold time.Duration
}

type JobRunOutcome struct {
	Status        JobRunStatus
	LeaseAcquired bool
	Output        JobData
	JobError      error
	JobStatus         *JobStatus
	NextNeed          *string
	WaitForJobIDs     []string
	MissingCapability *string
}

type JobRunListener interface {
	OnJobStart(JobStartEvent)
	OnTaskStart(TaskStartEvent)
	OnTaskEnd(TaskEndEvent)
	OnJobEnd(JobEndEvent)
}

type JobRunnable struct { ... }

func GetJobForRun(ctx context.Context, runtime WorkflowRuntime, req GetJobForRunRequest) (*JobRunnable, error)

func (r *JobRunnable) LeaseAcquired() bool
func (r *JobRunnable) Outcome() (JobRunOutcome, bool)
func (r *JobRunnable) Run(listener JobRunListener) (JobRunOutcome, error)
```

## Semantics

`GetJobForRun(...)` does the cheap upfront work:

- validates workers
- derives capabilities
- tries `runtime.GetJobLease(...)`
- if no lease is available, snapshots the current job state into a cached outcome

`Run(listener)` then behaves like this:

- if the runnable already has a cached outcome, return it immediately
- if it holds a lease, execute one normal `workerRunner` lease attempt
- classify the resulting state as `COMPLETED`, `FAILED`, or `SUSPENDED`

The important point is still the same:
this represents **one leased run**, not “drive the job to terminal completion across every future wake-up”.

## Why The Split Is Better

This shape is better than an immediate `RunJobIfLeaseable(...)` helper because it cleanly separates:

- acquisition / inspection
- execution
- optional observation

That lets a caller:

- ask “did I actually acquire the lease?”
- inspect a cached outcome before deciding whether to run
- attach a listener only when they want execution-time visibility

## Listener Behavior

The listener should not slow down real execution.

So the listener callbacks should run off the main execution path:

- `workerRunner` continues immediately after publishing events
- listener panics should not break the job run
- callers may see listener callbacks continue briefly after `Run(...)` returns

That tradeoff is correct here.
The listener is for observation, not control flow.

## Why Not Put It On `WorkflowRuntime`

`WorkflowRuntime` should stay focused on:

- persistence
- leasing
- artifact access

Actually running a job with workers is SWF-level behavior.
It depends on:

- `JobWorker`
- `TaskWorker`
- `workerRunner`
- retry / await / reschedule semantics

So this belongs in `swf`, above the runtime boundary.

## Relation To Existing Code

This maps directly onto code that already exists:

- `workerEngine.runLease(...)` already executes one claimed lease
- `newWorkerRunner(...)` already emits the lifecycle events the listener wants
- `ReplayObserver` already defines the right event vocabulary

So the direct API is mostly packaging existing execution behavior into a cleaner public seam.

## Bottom Line

Recommended direct API:

```diff
+func GetJobForRun(ctx context.Context, runtime WorkflowRuntime, req GetJobForRunRequest) (*JobRunnable, error)
+
+func (r *JobRunnable) LeaseAcquired() bool
+func (r *JobRunnable) Outcome() (JobRunOutcome, bool)
+func (r *JobRunnable) Run(listener JobRunListener) (JobRunOutcome, error)
```

That keeps:

- `WorkflowRuntime` low level
- `SWFEngine` for managed loops
- a focused direct path for “claim this known job and run it with these workers”
- listener-based execution visibility without blocking real job progress
