# Bug: External task completion uses wrong input hash causing non-determinism errors

## Summary

When a task is completed externally via `TaskHandle.Finish()`, the input hash is incorrectly computed from the previous chapter's output data instead of the actual input that was passed to `DoTask()`. This causes "workflow was not deterministic" errors when the workflow replays after recycling.

## Reproduction

1. Create a workflow with a task that requires external completion (unheld task)
2. Start the workflow and wait for the task to be in "waiting" state
3. Complete the task externally via `TaskHandle.Finish()`
4. Wait for the engine's `AwaitRecycleThreshold` to expire (causing workflow replay)
5. Observe the error: `workflow was not deterministic: ordinal N task <taskType>`

Test case: `TestSimpleInputRealEngineWaitRestart` in `server/recipe-input/pkg/input/activity_test.go`

## Debug Log Output

```
# First run - DoTask computes hash from full input (462 bytes)
computed task input hash taskType=input:collect_user_input inputHash=f35587d3... dataLength=462

# External completion - Finish computes hash from previous chapter (204 bytes)
computed external task input hash taskType=input:collect_user_input inputHash=2427469381... dataLength=204

# Replay - DoTask computes same hash as first run
computed task input hash taskType=input:collect_user_input inputHash=f35587d3... dataLength=462

# Hash mismatch!
checking cached task result cachedInputHash=2427469381... computedInputHash=f35587d3... hashMatch=false
task input hash mismatch
job worker run failed error="workflow was not deterministic: ordinal 2 task input:collect_user_input"
```

## Root Cause

### Current behavior

In `runner.go DoTask()`:
- Computes `inputHash` from the `data` parameter (full task input including context)
- When task needs external completion, stores only `InputStep` ordinal in `taskWait`

```go
// runner.go:315-327
err = r.lease.Reschedule(context.TODO(), r.engine.udb, pgwf.JobDependencies{
    NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
}, jobPayload{
    TaskWait: &taskWait{
        InputStep:  inputOrdinal,  // Points to previous chapter
        OutputStep: ordinal,
        Next:       r.worker.JobWorker.Name(),
        // InputHash is NOT stored!
    },
})
```

In `task.go Finish()`:
- Loads chapter at `InputStep` ordinal (previous task's OUTPUT)
- Computes hash from that chapter's data
- This hash differs from the original because previous chapter contains only the task output, not the full input with context

```go
// task.go:81-91
if h.inputChapter != nil {
    inputTD, err := chapterToTaskData(h.inputChapter)  // Gets previous task's OUTPUT
    inputHash, err = computeInputHash(ctx, inputTD)     // Wrong hash!
}
```

### Why the hashes differ

| Source | Data | Length |
|--------|------|--------|
| `DoTask` input | `ActivityInvocationRequest{Input: InputForm{...}, GitTaskContext: {...}}` | 462 bytes |
| Previous chapter | `InputForm{...}` (just the form output) | 204 bytes |

The `GitTaskContext` is included in the task input but is not part of the previous task's output.

## Proposed Fix

### 1. Add `InputHash` to `taskWait` struct

```go
// engine.go
type taskWait struct {
    InputStep  int64  `json:"in"`
    OutputStep int64  `json:"out"`
    Next       string `json:"next"`
    InputHash  string `json:"input_hash,omitempty"`  // NEW
}
```

### 2. Store the hash when rescheduling

```go
// runner.go ~line 315
err = r.lease.Reschedule(context.TODO(), r.engine.udb, pgwf.JobDependencies{
    NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
}, jobPayload{
    RunPolicy: r.jobPolicy,
    TaskWait: &taskWait{
        InputStep:  inputOrdinal,
        OutputStep: ordinal,
        Next:       r.worker.JobWorker.Name(),
        InputHash:  inputHash,  // NEW - store the computed hash
    },
})
```

### 3. Use stored hash in Finish

```go
// task.go Finish() ~line 81
var inputHash string
tw, err := extractTaskWaitFromRaw(h.payload)
if err == nil && tw.InputHash != "" {
    inputHash = tw.InputHash
} else if h.inputChapter != nil {
    // Fallback for backwards compatibility
    inputTD, err := chapterToTaskData(h.inputChapter)
    if err != nil {
        return err
    }
    inputHash, err = computeInputHash(ctx, inputTD)
    if err != nil {
        return err
    }
}
```

## Impact

Any workflow using external task completion (unheld tasks) will fail with non-determinism errors after the workflow is recycled/replayed, if the task input contains additional context beyond what the previous task output.

## Version

```
github.com/colony-2/swf-go v0.0.0-20260114010821-1186138cd156
```
