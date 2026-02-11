# Replay Run API (Draft)

## Summary
Add a **replay run** API that exercises the existing job/task execution flow but **only consumes cached results**. The design must **share the same runner code path** as a real run, with a single behavioral difference: what to do when a cached task/job attempt is missing. This should work for both the **toy** and **real** engines.

## Goals
- Maximize code sharing with real runs.
- Maintain current error semantics (timeouts, determinism errors, app/system errors).
- Respect job and task retries in replay timelines.
- Support an observer for job/task attempt start/failure/retry events.
- Use the same return path and types as real runs.

## Non-Goals
- No new persistence: replay is read-only.
- No changes to existing job/task execution semantics beyond cache-miss behavior.

## API Shape (Go)

### New Types
```go
// ReplayRunRequest describes a cache-only job replay.
type ReplayRunRequest struct {
    JobKey   swf.JobKey
    Observer ReattemptObserver // optional
}

// ReattemptObserver receives retry boundary events.
// Always present; default implementation is a noop.
type ReattemptObserver interface {
    OnTaskReattemptBoundary(event TaskReattemptBoundary)
    OnJobReattemptBoundary(event JobReattemptBoundary)
}

// ReplayCacheMissError is returned when a required cached result is missing.
type ReplayCacheMissError struct {
    JobKey   swf.JobKey
    TaskType string
    Ordinal  int64
    Attempt  int
    Reason   ReplayCacheMissReason
}

func (e ReplayCacheMissError) Error() string

type ReplayCacheMissReason string

const (
    ReplayCacheMissTaskResultMissing ReplayCacheMissReason = "task_result_missing"
    ReplayCacheMissJobResultMissing  ReplayCacheMissReason = "job_result_missing"
    ReplayCacheMissAwaitNotReady     ReplayCacheMissReason = "await_not_ready"
    ReplayCacheMissAwaitJobsPending  ReplayCacheMissReason = "await_jobs_pending"
)

type TaskReattemptBoundary struct {
    JobKey   swf.JobKey
    TaskType string
    PreviousAttemptOrdinal int64
    PreviousAttemptNumber  int
    PreviousAttemptError   error
    NextAttemptOrdinal     int64
    NextAttemptNumber      int
}

type JobReattemptBoundary struct {
    JobKey                swf.JobKey
    PreviousAttemptOrdinal int64
    PreviousAttemptNumber  int
    PreviousAttemptError   error
    NextAttemptOrdinal     int64
    NextAttemptNumber      int
}
```

### New Engine Method
```go
// ReplayJobRun executes the job worker using cached task/job results only.
// Returns the same output/error shape as a real run (JobWorker.Run).
ReplayJobRun(ctx context.Context, req ReplayRunRequest) (swf.JobData, error)
```

### Engine Interface
```go
type jobRunApi interface {
    StartJob(...)
    RestartJob(...)
    CancelJob(...)
    CheckJobStatus(...)
    GetJobResult(...)
    GetJobRun(...)

    ReplayJobRun(ctx context.Context, req ReplayRunRequest) (swf.JobData, error)
}
```

## Key Design: Shared Runner With a Pluggable Cache Miss Policy (Aligned to Current Runner)

**Principle:** The runner should do the same work for real and replay runs, and only diverge on **cache misses**.

### Proposed Abstraction
Introduce a single **runner backend** interface that handles chapter IO, job outcome lookup, and awaits. This keeps the main execution code oblivious to replay vs real mode and avoids conditional duplication.

```go
// RunnerBackend abstracts all external interactions required by the runner.
// The runner uses it for chapter retrieval/persistence and await behavior.
type RunnerBackend interface {
    GetChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
    SaveChapter(ctx context.Context, key story.Key, chap story.Chapter) error
    GetJobAttemptOutcome(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
    AwaitUntil(ctx context.Context, wakeAt time.Time, info AwaitInfo) error
    AwaitJobs(ctx context.Context, jobIds []string, info AwaitInfo) error
}

// DefaultRunnerBackend (real run):
// - GetChapter returns (nil, nil) on not-found (mirrors existing behavior).
// - SaveChapter persists as normal.
// - GetJobAttemptOutcome behaves the same as GetChapter.
// - AwaitUntil/AwaitJobs use existing waiting behavior.

// ReplayRunnerBackend (replay run):
// - GetChapter returns a typed miss error on not-found.
// - SaveChapter always returns ReplayShouldNeverMutate().
// - GetJobAttemptOutcome returns a miss to force the replay to rely on executed job logic.
// - AwaitUntil/AwaitJobs return cache-miss errors rather than waiting.
```

Define miss and mutation errors:
```go
var ErrReplayShouldNeverMutate = errors.New("replay run should never mutate state")

type ReplayCacheMissError struct { ... } // as defined above, with ReplayCacheMissReason
```

### Runner Wiring (Concrete)
Add fields to `impl.runner`:
```go
backend            RunnerBackend
reattemptObserver  ReattemptObserver // always present; default is noop
```

Construction sites:
- `impl.swfEngineImpl.runSomething(...)` (real engine) uses `DefaultChapterStore` and `noopReattemptObserver`.
- `ReplayJobRun(...)` (real engine) uses `ReplayChapterStore` and a user-provided `ReattemptObserver` (or noop).
- The toy engine constructs its runner the same way with its local chapter store implementation.

### Runner Integration Points (Aligned to Current Code)
1. **Task path** (`impl.runner.DoTask`):
   - Replace direct `strata.Chapter(...)` calls with `backend.GetChapter(...)`.
   - In real runs, not-found returns `(nil, nil)` and the flow continues unchanged.
   - In replay runs, not-found returns `ReplayCacheMissError{Reason: ReplayCacheMissTaskResultMissing}` and bubbles out immediately.

2. **Job result path** (`impl.runner.checkCachedJobResult` + `impl.runner.DoJob`):
   - Replace direct `strata.Chapter(...)` calls with `backend.GetChapter(...)`.
   - Use `backend.GetJobAttemptOutcome(...)` to allow replay to ignore cached job outcomes if desired.
   - In replay runs, missing job attempt outcome bubbles as `ReplayCacheMissError{Reason: ReplayCacheMissJobResultMissing}`.

3. **Persistence** (`saveJobChapter` and any other write sites):
   - Replace direct `strata.SaveChapter(...)` with `backend.SaveChapter(...)`.
   - In replay runs, SaveChapter always returns `ErrReplayShouldNeverMutate`.

### Persistence Strategy
- Real run: unchanged behavior, using `DefaultChapterStore`.
- Replay:
  - Any mutation attempts error via `ErrReplayShouldNeverMutate` (should not happen in correct replay flows).
  - `lease.Complete` remains a real-run concern; replay mode should not complete or reschedule leases.

### Observer Wiring (Reduced Surface)
- The system **always** has a `ReattemptObserver` (default is noop).
- Replay uses the user-provided observer; real runs use noop unless explicitly wired.
- Emit `OnTaskReattemptBoundary` when a task attempt fails and the replay will retry.
- Emit `OnJobReattemptBoundary` when a job attempt fails and the replay will retry.
- Cache-miss errors are returned to the caller; no observer hook is required.

## Behavior

### Cache-Only Task Execution
- Cached task attempt found: decode and return as usual.
- Cache miss: if replay policy, return `ReplayCacheMissError`.
- Input hash mismatch: return the same determinism errors as real run.

### Await/Timer Behavior (No Waiting in Replay)
Replay must never sleep or block on timers/awaits. Any awaited time or dependency that is not already satisfied should return a cache-miss error immediately.

This is handled by `RunnerBackend.AwaitUntil` / `RunnerBackend.AwaitJobs`:
- **DefaultRunnerBackend (real run)**: uses existing wait/sleep behavior.
- **ReplayRunnerBackend (replay run)**:
  - If `wakeAt` is in the future, return `ReplayCacheMissError{Reason: ReplayCacheMissAwaitNotReady}`.
  - If waiting on jobs and any are not complete, return `ReplayCacheMissError{Reason: ReplayCacheMissAwaitJobsPending}`.
  - If the await is already satisfied, return nil immediately.

```go
type AwaitInfo struct {
    JobKey   swf.JobKey
    TaskType string // empty for job-level awaits
    Ordinal  int64
    Attempt  int
}
```

### No Short-Circuiting (Replay Always Executes the Job Path)
- Replay must always invoke `JobWorker.Run` and traverse the workflow code path, even if the job is already complete.
- A cached job attempt outcome **must not** short-circuit the replay run.
- Cached job outcomes are only used for **validation** (e.g., determinism/timeout consistency) and to determine whether a cache miss should be raised at the job-attempt boundary.
- Real runs keep existing short-circuit behavior (e.g., cached job attempt outcomes can terminate without re-execution).

### Job Outcome Short-Circuiting: Proposed Option
If we need to guarantee that cached job outcomes never terminate replay (while still using shared code), use the optional `JobOutcomeStore` hook:
- **Real run**: `GetJobAttemptOutcome` behaves like `GetChapter` (cached outcome can short-circuit to avoid double-save).\n- **Replay run**: `GetJobAttemptOutcome` returns a cache miss to force the runner to rely on the executed job path.\n- This keeps the core flow shared and avoids explicit replay conditionals in the runner.

### Job Attempts & Retries
- Job retries are driven by existing `RunPolicy.Retry` logic.
- Replay uses the same retry decision process but consumes cached attempt outcomes.

### Timeouts and Failures
- If cached chapters contain timeouts/errors, return them exactly as real run.
- If a timeout would be reached based on persisted timestamps/run policy, return the same timeout error type.

## Toy + Real Engine Compatibility
- The toy engine should implement the same `ReplayJobRun` API.
- The toy runner can reuse the same cache-miss policy abstraction, even if its "cache" is in-memory.
- Real engine uses Strata chapters as cache; toy engine uses its local chapter store.

## Testing Plan
- Replay success with full cached task/job attempt data.
- Replay cache miss on a missing task attempt.
- Replay cache miss on missing job attempt outcome.
- Replay determinism error on input hash mismatch.
- Replay respects retry policy and emits observer events.
- Replay does not persist new chapters.
