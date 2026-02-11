# Phase 2: Replay APIs + ReplayRunnerBackend

## Goal
Add replay APIs that execute workflows through the standard runner but use a **ReplayRunnerBackend** to enforce cache-only semantics. Replay should never mutate state or wait; missing data should return cache-miss errors.

## Scope
- Add `ReplayJobRun` API.
- Add `ReplayRunnerBackend` implementing `RunnerBackend`.
- Add replay cache-miss errors and reasons.
- Add reattempt observer plumbing (default noop; replay uses user-provided).

## Replay API (Go)
```go
type ReplayRunRequest struct {
    JobKey   swf.JobKey
    Observer ReattemptObserver // optional
}

func (e *swfEngineImpl) ReplayJobRun(ctx context.Context, req ReplayRunRequest) (swf.JobData, error)
```

## Replay Errors
```go
type ReplayCacheMissReason string
const (
    ReplayCacheMissTaskResultMissing ReplayCacheMissReason = "task_result_missing"
    ReplayCacheMissJobResultMissing  ReplayCacheMissReason = "job_result_missing"
    ReplayCacheMissAwaitNotReady     ReplayCacheMissReason = "await_not_ready"
    ReplayCacheMissAwaitJobsPending  ReplayCacheMissReason = "await_jobs_pending"
)

type ReplayCacheMissError struct {
    JobKey   swf.JobKey
    TaskType string
    Ordinal  int64
    Attempt  int
    Reason   ReplayCacheMissReason
}
```

## ReplayRunnerBackend Behavior
- `GetChapter`: if missing, return `ReplayCacheMissError{Reason: ReplayCacheMissTaskResultMissing}`.
- `GetJobAttemptOutcome`: if missing, return `ReplayCacheMissError{Reason: ReplayCacheMissJobResultMissing}`.
- `SaveChapter`: always return `ErrReplayShouldNeverMutate`.
- `AwaitUntil`: if `wakeAt` is in the future, return `ReplayCacheMissError{Reason: ReplayCacheMissAwaitNotReady}`; if already satisfied, return nil immediately.
- `AwaitJobs`: return `(false, ReplayCacheMissError{Reason: ReplayCacheMissAwaitJobsPending})` if any job is not complete; otherwise return `(false, nil)`.
## Replay Lease Behavior
- Replay uses a noop internal SWF `lease` implementation that never mutates state.
- Optionally return `ErrReplayShouldNeverMutate` on mutation attempts for easier detection.
- The noop lease must implement all methods required by the internal lease interface (`KeepAlive`, `StopKeepAlive`, `Complete`, `Reschedule`, `NextNeed`, `Payload`).

## No Short-Circuiting in Replay
- Replay must always execute the job worker; cached job outcomes should not end the run.
- `ReplayRunnerBackend.GetJobAttemptOutcome` can be implemented to always report missing, forcing the runner to rely on executed job logic while still validating determinism via task caches.

## Observer (Retry Boundaries)
```go
type ReattemptObserver interface {
    OnTaskReattemptBoundary(event TaskReattemptBoundary)
    OnJobReattemptBoundary(event JobReattemptBoundary)
}
```
- Replay uses the caller-provided observer.
- Default observer is noop for real runs.

## Acceptance Criteria
- Replay path exercises the same runner code.
- No state mutation in replay.
- Replay returns cache-miss errors instead of waiting.
- Retry boundary observer events fire in replay.
- Existing tests still pass; add replay-specific tests.
