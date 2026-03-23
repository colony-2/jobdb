# Proposal: In-Task Checkpoints and Resume

## Summary
Add first-class **task checkpoints** so a single task attempt can persist intermediate state as Strata chapters and resume from that state after recycle/crash.

This keeps the current durable model (append-only chapters + deterministic replay), but changes task attempts from:

- current: `TaskAttemptOutcome`
- proposed: `TaskCheckpoint*` then `TaskAttemptOutcome`

The final task output remains unchanged (`TaskData` with data + artifacts). Checkpoints use the same shape.

## Why This Fits Current SWF
Current SWF behavior (from `impl/runner.go`, `impl/envelope.go`, `impl/job_run_details.go`) is:

1. One task attempt maps to one terminal chapter (`TaskAttemptOutcome`).
1. Cache/replay logic expects deterministic input hash per ordinal.
1. `TaskContext` has await APIs but no persistence API for in-task progress.

This proposal extends those assumptions instead of replacing them:

1. Checkpoints are just additional typed chapters in the same envelope format.
1. Input hash validation remains the source of determinism.
1. Existing tasks/workers continue to run with no code changes.

## Public API Changes

### 1) TaskContext write API
```go
type TaskContext struct {
    JobKey JobKey
    Step   int64
    Logger *slog.Logger

    // new (runner-wired)
    writeCheckpoint func(TaskData) error
}

func (tc TaskContext) WriteCheckpoint(checkpoint TaskData) error
```

Semantics:
- `WriteCheckpoint` persists a checkpoint for the **current task attempt**.
- Checkpoint payload/artifacts are stored like a normal task output.
- The call is idempotent across retries/replays (details below).

### 2) Optional resume API for tasks
Keep `TaskWorker` unchanged, and add an optional interface:

```go
type ResumableTaskWorker interface {
    Resume(ctx TaskContext, input TaskData, checkpoints []TaskCheckpoint) (TaskData, error)
}

type TaskCheckpoint struct {
    Ordinal   int64
    Index     int
    CreatedAt time.Time
    Data      TaskData
}
```

Runner behavior:
- no checkpoints: call `Run(...)`
- checkpoints + worker implements `ResumableTaskWorker`: call `Resume(...)`
- checkpoints + worker does not implement resume: call `Run(...)` (backward-compatible fallback)

## Chapter Model Changes

### New chapter type
Add:
- `TaskCheckpoint`

`chapter_type` allowed values become:
- `JobStart`
- `JobAttemptOutcome`
- `TaskAttemptOutcome`
- `TaskCheckpoint`
- `RestartExtra`

### Metadata additions
Add to chapter meta:
- `checkpoint_index` (int, only for `TaskCheckpoint`)

All checkpoint chapters still carry:
- `input_hash` (same hash as the task attempt input)
- `attempt`
- `task_type`
- `worker_id`
- `created_at`

### Task attempt chain shape
For each task attempt at base ordinal `O`:

1. zero or more checkpoint chapters at `O..O+N-1`
1. one terminal outcome chapter (`TaskAttemptOutcome`) at `O+N`

No outcome means an incomplete attempt (e.g., crash/recycle mid-task).

## Runner Algorithm Changes (`DoTask`)

### 1) Scan a task-attempt chain
Before executing a task, runner scans from `storyCounter` forward:

1. collect contiguous `TaskCheckpoint` chapters for the same task attempt
1. stop on terminal chapter (`TaskAttemptOutcome` or `RestartExtra`)
1. validate `input_hash` on every scanned chapter

If terminal chapter exists, current cache-first behavior applies.

If only checkpoints exist (no terminal), runner treats this as a resumable partial attempt.

### 2) Execute worker with checkpoint context
Build a checkpoint-aware `TaskContext`:

- `WriteCheckpoint` call number `i` maps to checkpoint index `i`
- if checkpoint `i` already exists, validate it matches and no-op
- if checkpoint `i` does not exist, append it as a new chapter
- mismatch returns deterministic error (`ErrWorkflowNotDeterministic` wrapped by a specific checkpoint mismatch error type)

This keeps both paths safe:
- resumable workers can append from `len(existing)`
- legacy workers can rerun from scratch and re-emit the same checkpoints

### 3) Save final task outcome
Task outcome chapter is written immediately after the last checkpoint index observed/emitted in this execution.

`storyCounter` advances to first unused ordinal after the terminal chapter.

## Replay Behavior
Replay remains cache-only:

1. full chain with terminal chapter: replay succeeds from cache
1. checkpoint-only chain (no terminal): replay returns cache miss (`task_result_missing` is acceptable; optional new reason can be added)
1. replay never writes checkpoints or outcomes (`ErrReplayShouldNeverMutate`)

## Restart Behavior
Current restart boundary validation only guards retry-chain boundaries (`attempt > 1`).

With checkpoints, extend boundary rule:

1. `LastStepToKeep + 1` may be:
   - `TaskAttemptOutcome` (attempt 1), or
   - `TaskCheckpoint` with `checkpoint_index == 0` (start of attempt)
1. reject if `LastStepToKeep + 1` is `TaskCheckpoint` with `checkpoint_index > 0` (cuts through checkpoint chain)

This preserves clean restart points and avoids slicing into a partially completed attempt.

## GetJobRun / Read Model Changes
Extend task attempt view to include checkpoints:

```go
type TaskAttempt struct {
    // existing fields...
    Checkpoints []TaskCheckpointView `json:"checkpoints,omitempty"`
}

type TaskCheckpointView struct {
    Ordinal   int64
    Index     int
    CreatedAt time.Time
    Output    *TaskIO
}
```

Rules:
- checkpoint chapters attach to the containing task attempt (not separate task runs)
- include data/artifacts controlled by existing include flags
- existing callers remain compatible (additive field)

## ToyEngine Parity
Toy engine should support the same API/semantics:

1. `WriteCheckpoint` stores in-memory checkpoint chapters
1. replay of toy jobs understands checkpoint chains
1. optional resume interface is called when checkpoints exist
1. restart boundary validation mirrors durable engine behavior

## Error Model
Add deterministic checkpoint mismatch error:

```go
type TaskCheckpointMismatchError struct {
    TaskType         string
    AttemptBase      int64
    CheckpointIndex  int
    CachedHash       string
    ComputedHash     string
}

func (e TaskCheckpointMismatchError) Unwrap() error { return ErrWorkflowNotDeterministic }
```

Return when an existing checkpoint at index `i` differs from what workflow code produced for the same call position.

## Backward Compatibility
- No breaking change to `TaskWorker`.
- Tasks that never call `WriteCheckpoint` are unchanged.
- Existing stories without checkpoints remain valid.
- `GetJobRun` is additive.

## Rollout Plan

1. **Phase 1: Core runtime + envelope**
   - Add `TaskContext.WriteCheckpoint`
   - Add `TaskCheckpoint` chapter type
   - Implement checkpoint-chain scan + write in `DoTask`
   - Add deterministic mismatch error

1. **Phase 2: Resume API + parity**
   - Add `ResumableTaskWorker`
   - Wire resume dispatch in durable runner + toy engine
   - Add replay cache-miss handling for incomplete chains

1. **Phase 3: Read APIs + restart**
   - Add checkpoint fields to `GetJobRun` response
   - Extend restart boundary validation for checkpoint chains
   - Update docs/migration guides

## Test Matrix (Must-Have)

1. Task writes checkpoints, then succeeds: chain persists and output is unchanged.
1. Crash/recycle after checkpoint, before outcome: next run resumes and appends outcome.
1. Non-resumable task rerun with existing checkpoints: deterministic no-op writes for matching checkpoints.
1. Checkpoint mismatch on rerun: deterministic error.
1. Replay with complete chain: succeeds.
1. Replay with incomplete chain: cache miss.
1. `GetJobRun` includes checkpoints under task attempts.
1. Restart rejects mid-checkpoint-chain cut.

## Open Questions

1. Should checkpoint chapter payloads allow non-`App` payload kinds, or only successful intermediate states?
1. Do we want a dedicated replay miss reason for incomplete checkpoint chains (`task_checkpoint_pending`)?
1. Should external `TaskHandle` API get checkpoint support in a later phase, or remain job-worker local only?
