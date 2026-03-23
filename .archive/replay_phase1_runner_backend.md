# Phase 1: Introduce RunnerBackend (No Behavior Change)

## Goal
Refactor the runner to depend on a unified `RunnerBackend` interface for chapter IO, job outcome lookup, and awaits. This phase must **not** change behavior. All existing tests should pass after this refactor.

## Scope
- Add `RunnerBackend` interface and default implementation that wraps the current Strata/PGWF logic.
- Update `impl.runner` to use `RunnerBackend` for chapter reads/writes and await handling.
- Keep existing caching/short-circuit behavior unchanged.

## Non-Goals
- No replay APIs.
- No new cache-miss semantics.
- No new errors.
- No behavioral changes to timeouts, retries, or determinism handling.

## Proposed Interface
```go
// RunnerBackend abstracts external interactions used by runner.
type RunnerBackend interface {
    GetChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
    SaveChapter(ctx context.Context, key story.Key, chap story.Chapter) error
    GetJobAttemptOutcome(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
    AwaitUntil(ctx context.Context, wakeAt time.Time, info AwaitInfo) error
    // returns (rescheduled, error)
    AwaitJobs(ctx context.Context, jobIds []string, info AwaitInfo) (bool, error)
}

// AwaitInfo provides context for awaits.
type AwaitInfo struct {
    JobKey   swf.JobKey
    TaskType string // empty for job-level awaits
    Ordinal  int64
    Attempt  int
}
```

## Default Implementation (Real Runs)
`DefaultRunnerBackend` should preserve current behavior:
- `GetChapter`: same as `strata.Chapter(...)` (including `core.ErrNotFound` handling).
- `SaveChapter`: same as `strata.SaveChapter(...)`.
- `GetJobAttemptOutcome`: same as `GetChapter` (no special logic).
- `AwaitUntil`: same as current `runner.awaitUntil` logic (sleep/reschedule as today).
- `AwaitJobs`: same as current `runner.rescheduleAwaitJobs` / `TaskContext.AwaitJobs` behavior.

## Lease Abstraction (Noop-Friendly)
Instead of adding lease methods to `RunnerBackend`, introduce an **internal SWF lease interface** and pass it into the runner. The API should exclude DB arguments; the adapter can carry any DB references internally.
```go
// swf/internal lease interface used by the runner.
// It should cover all pgwf.Lease methods currently used in runner code.
type Lease interface {
    KeepAlive(ctx context.Context) error
    StopKeepAlive()
    Complete(ctx context.Context) error
    Reschedule(ctx context.Context, deps pgwf.JobDependencies, payload any) error
    NextNeed() pgwf.Capability
    Payload() []byte
}
```

- **Real runs**: wrap `*pgwf.Lease` with an adapter that captures `udb` internally and implements the interface without DB args.
- **Replay runs**: pass a noop lease that never mutates state (returns nil or a typed replay mutation error).
- This keeps pgwf concerns out of the runner without expanding `RunnerBackend`.

Example adapter sketch:
```go
// swf/internal/lease_adapter.go (example)
type pgwfLeaseAdapter struct {
    lease *pgwf.Lease
    udb   *sql.DB
}

func (l *pgwfLeaseAdapter) KeepAlive(ctx context.Context) error {
    if l.lease == nil || l.udb == nil {
        return nil
    }
    return l.lease.WithKeepAlive(l.udb)
}

func (l *pgwfLeaseAdapter) StopKeepAlive() {
    if l.lease == nil {
        return
    }
    // call through to pgwf Lease stop method
    if stopper, ok := any(l.lease).(interface{ StopKeepAlive() }); ok {
        stopper.StopKeepAlive()
        return
    }
    stopKeepAliveFallback(l.lease)
}

func (l *pgwfLeaseAdapter) Complete(ctx context.Context) error {
    if l.lease == nil || l.udb == nil {
        return nil
    }
    return l.lease.Complete(ctx, l.udb)
}

func (l *pgwfLeaseAdapter) Reschedule(ctx context.Context, deps pgwf.JobDependencies, payload any) error {
    if l.lease == nil || l.udb == nil {
        return nil
    }
    return l.lease.Reschedule(ctx, l.udb, deps, payload)
}

func (l *pgwfLeaseAdapter) NextNeed() pgwf.Capability {
    if l.lease == nil {
        return \"\"
    }
    return l.lease.NextNeed()
}

func (l *pgwfLeaseAdapter) Payload() []byte {
    if l.lease == nil {
        return nil
    }
    return l.lease.Payload()
}
```

## Code Integration Points
1. **Runner Fields**
   - Add `backend RunnerBackend` to `impl.runner`.
   - Add `awaitInfo` helpers to populate `AwaitInfo` for job/task contexts.

2. **Chapter Access**
   - Replace direct `strata.Chapter(...)` calls in `impl.runner` with `backend.GetChapter(...)`.
   - Replace `saveJobChapter` + any direct `SaveChapter` calls with `backend.SaveChapter(...)`.
   - Replace cached job outcome reads in `checkCachedJobResult` with `backend.GetJobAttemptOutcome(...)`.

3. **Await Handling**
   - `runner.awaitUntil` should delegate to `backend.AwaitUntil(...)` instead of sleeping directly.
   - `TaskContext.AwaitJobs` should call `backend.AwaitJobs(...)` through runner wiring.

4. **Construction Sites**
   - `impl.swfEngineImpl.runSomething(...)` should create a `DefaultRunnerBackend` and pass it to runner.
- The toy engine should do the same with its local story/chapter store.

5. **Lease/PGWF Calls**
   - Replace direct `lease.WithKeepAlive`, `lease.Complete`, and `lease.Reschedule` calls with the internal `lease` interface.
   - Ensure runner does not import or invoke pgwf directly after refactor.
   - Replace `stopLeaseKeepAlive(*pgwf.Lease)` usage with `lease.StopKeepAlive()`.

## Solutions for Remaining Gaps
To ensure replay can avoid pauses and mutations, and that the runner has no access to mutating objects:

1. **Remove `engine` from `runner`**\n
   - Replace all `r.engine.*` accesses with `backend` + `lease`.\n
   - `runner` should only keep: `backend`, `lease`, `worker`, `logger`, and local state.\n

2. **Chapter access via backend**\n
   - `taskTotalDeadline`: use `backend.GetChapter`.\n
   - `DoTask`: use `backend.GetChapter` and `backend.SaveChapter`.\n
   - `checkCachedJobResult`: use `backend.GetJobAttemptOutcome`.\n
   - `DoJob` cached result checks: use `backend.GetChapter`.\n

3. **Await logic via backend**\n
   - Replace `r.engine.AwaitUntil` with `backend.AwaitUntil`.\n
   - Ensure `TaskContext.AwaitJobs` dispatches to `backend.AwaitJobs`.\n
   - All sleeps/backoff should go through `awaitUntil` so replay can intercept.\n

4. **Lease-only PGWF operations**\n
   - `r.lease.Reschedule` in `DoTask` and `rescheduleAwaitJobs` should be via internal `lease`.\n
   - `lease.WithKeepAlive`, `lease.Complete`, `stopLeaseKeepAlive` should be replaced with `lease.KeepAlive`, `lease.Complete`, `lease.StopKeepAlive`.\n
   - Change `runner.lease` type to the internal interface, not `*pgwf.Lease`.\n

5. **No direct DB access in runner**\n
   - Runner should not receive `udb` or `*sql.DB`.\n
   - DB access (for lease keepalive/reschedule) stays in the lease adapter.\n

6. **Construction wiring**\n
   - `runSomething(...)`: build `DefaultRunnerBackend` and a `lease` adapter from `*pgwf.Lease`.\n
   - Replay (phase 2): build `ReplayRunnerBackend` and pass noop `lease`.\n

## Acceptance Criteria
- All existing tests pass without modification.
- Behavior is unchanged: cached task/job results still short-circuit as today.
- No new exported APIs; all changes are internal.

## Follow-Up
Phase 2 introduces replay APIs + a replay backend that changes cache-miss semantics and await behavior.
