# SWF Specification: Store Artifacts on Task Error

**Status**: Proposed
**Date**: 2026-01-08
**Author**: System

## Problem Statement

Currently, when a task execution errors in SWF, any artifacts that were attached or produced during the task execution are **not stored**. The implementation in `pkg/swf/impl/runner.go` only extracts and stores artifacts from the task output on the **success path**:

```go
// Current behavior in runner.go:405-414
if taskErr != nil {
    // ERROR path: only extract error payload
    payload, payloadKind, tdErr = errorPayloadFromError(taskErr, inputRef)
    // artifacts remains []swf.Artifact{} - empty!
} else {
    // SUCCESS path: extract data and artifacts
    dataBytes, err := output.GetData()
    payload = dataBytes
    artifacts, err = output.GetArtifacts()  // <-- Only called on success
}
```

This means that valuable debugging information, partial results, or diagnostic artifacts produced before or during the error are lost. These artifacts could be critical for:
- **Debugging**: Understanding what state the task was in when it failed
- **Partial results**: Preserving work that was completed before the error
- **Error diagnostics**: Logs, screenshots, or other diagnostic data generated during failure
- **Forensics**: Investigating production issues after the fact

## Desired Behavior

### Core Requirement

**Artifacts should be stored in the chapter regardless of whether the task succeeds or fails.**

If a task produces or attaches artifacts during execution, these artifacts should be:
1. Extracted from the task output or error
2. Stored in the chapter alongside the error payload
3. Available for retrieval via the workflow history
4. Subject to the same cleanup lifecycle as success-case artifacts

### User Experience

From a workflow developer's perspective:

```go
// Task code that may error but still produces artifacts
func MyTask(ctx context.Context, input swf.TaskInput) (swf.TaskOutput, error) {
    output := swf.NewTaskOutput()

    // Attach diagnostic artifacts
    logFile := createLogFile()
    output.AddArtifact("debug.log", logFile)

    // Do work that might fail
    result, err := riskyOperation()
    if err != nil {
        // Even though we're returning an error, the debug.log artifact
        // should still be stored and available for debugging
        return output, fmt.Errorf("operation failed: %w", err)
    }

    output.SetData(result)
    return output, nil
}

// Later, when retrieving workflow history
func DebugWorkflow(ctx context.Context, wfKey swf.WorkflowKey) {
    history, _ := engine.GetWorkflowHistory(ctx, wfKey)
    for _, chapter := range history.Chapters {
        // Should be able to retrieve artifacts even from failed tasks
        artifacts := chapter.GetArtifacts()
        if chapter.PayloadKind == "AppError" {
            // Access diagnostic artifacts from the failed task
            for _, art := range artifacts {
                fmt.Printf("Error diagnostics: %s\n", art.Name())
            }
        }
    }
}
```

## Technical Design

### Implementation Changes

#### 1. Extract Artifacts on Error Path

**Location**: `pkg/swf/impl/runner.go:405-414`

**Current**:
```go
if taskErr != nil {
    payload, payloadKind, tdErr = errorPayloadFromError(taskErr, inputRef)
    if tdErr != nil {
        return nil, tdErr
    }
    // artifacts is empty []
} else {
    dataBytes, err := output.GetData()
    payload = dataBytes
    artifacts, err = output.GetArtifacts()  // <-- Only on success
}
```

**Proposed**:
```go
if taskErr != nil {
    payload, payloadKind, tdErr = errorPayloadFromError(taskErr, inputRef)
    if tdErr != nil {
        return nil, tdErr
    }
    // NEW: Extract artifacts even on error
    if output != nil {
        artifacts, err = output.GetArtifacts()
        if err != nil {
            r.logger.Warn("Failed to extract artifacts from error case",
                         "error", err)
            artifacts = []swf.Artifact{}
        }
    }
} else {
    dataBytes, err := output.GetData()
    payload = dataBytes
    artifacts, err = output.GetArtifacts()
}
```

#### 2. Cleanup Artifacts on Error Path

**Location**: `pkg/swf/impl/runner.go:460-463`

**Current**:
```go
// Max attempts or non-retryable - cleanup input artifacts and return error
inputArtifacts, _ := data.GetArtifacts()
cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
return nil, originalErr
```

**Proposed**:
```go
// Max attempts or non-retryable - cleanup both input and output artifacts
inputArtifacts, _ := data.GetArtifacts()
cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
// NEW: Also cleanup output artifacts on final error
cleanupArtifacts(context.TODO(), artifacts, r.logger)
return nil, originalErr
```

**Rationale**: Since we're now storing artifacts on error, we should clean them up after storage, just like we do on the success path (line 438).

#### 3. Handle Retry Attempts

When a task fails but will be retried, the artifacts from the failed attempt should:
- **Be stored** in the chapter for that attempt (for debugging)
- **Be cleaned up** after storage (to avoid accumulation)
- **Not interfere** with subsequent retry attempts

**Location**: `pkg/swf/impl/runner.go:447-458`

The current retry logic already saves the chapter before retrying (line 432). We need to ensure artifacts from failed attempts are cleaned up:

```go
// After SaveChapter but before retry
if retryable && attempt < maxAttempts {
    // NEW: Cleanup artifacts from failed attempt before retry
    cleanupArtifacts(context.TODO(), artifacts, r.logger)

    // Existing backoff logic
    sleep := backoff(minBackoff, maxBackoff, attempt)
    time.Sleep(sleep)
    continue
}
```

### Storage Layer

**No changes required** in the storage layer. The chapter creation logic in `payloadToChapter()` (`pkg/swf/impl/engine.go:605-609`) already handles artifacts unconditionally:

```go
chapBuilder := story.NewChapter().WithOrdinal(ordinal).WithBytes(envBytes)
for _, v := range artifacts {
    chapBuilder.AddArtifact(swf.ToStrataArtifact(v))  // Already unconditional
}
return chapBuilder, nil
```

### Error Handling Interface

Tasks that want to provide artifacts on error should return both:
1. A `TaskOutput` with artifacts attached
2. An error

**Current Interface**: The `Task` function signature already supports this:
```go
type Task func(context.Context, TaskInput) (TaskOutput, error)
```

**Important**: The runner must **not assume** that `output == nil` when `error != nil`. Tasks may return both a non-nil output (with artifacts) and an error.

## Edge Cases and Behavior

### 1. Output is nil on Error
If a task returns `error` with `output == nil`, the artifacts list remains empty. This is safe and expected.

### 2. GetArtifacts() Errors
If extracting artifacts from the output fails during error handling:
- Log a warning
- Set artifacts to empty slice `[]`
- Continue with error processing
- **Do not** fail the entire error handling path

### 3. Artifact Cleanup Failures
Cleanup errors should not prevent workflow progress (already implemented in lines 469-476):
```go
if cleanupErr != nil {
    r.logger.Warn("artifact cleanup failed", "error", cleanupErr,
                 "taskType", taskType, "ordinal", ordinal)
}
```

### 4. Retries and Artifact Accumulation
Each retry attempt should:
- Store its own artifacts (for that attempt's chapter)
- Clean up artifacts after storage
- Start fresh with new artifacts on retry

### 5. System Errors vs Application Errors
The change applies to both:
- **AppError**: Task returned an error (`taskErr != nil`)
- **SystemError**: Runner/framework errors (deserialization, etc.)

For system errors, there may be no `output` to extract artifacts from. The nil check handles this safely.

### 6. Artifact Size Considerations
Storing artifacts on error increases storage:
- Failed tasks now store artifacts (previously didn't)
- Retry attempts each store their artifacts (previously only final success did)

**Mitigation**: Existing cleanup mechanisms ensure artifacts are cleaned up after storage. The actual stored data is already cleaned up by the cleanup job (see `artifact_cleanup_integration_test.go`).

## Backward Compatibility

### Breaking Changes: None

This change is **backward compatible**:

1. **Existing workflows**: Tasks that don't attach artifacts on error will continue to work (empty artifacts list)
2. **Storage format**: Chapter format already supports artifacts on any payload kind
3. **Retrieval**: Existing code reading chapters will see artifacts if present, empty list if not
4. **API surface**: No changes to public interfaces

### Migration: Not Required

- No migration needed for existing workflows or data
- New behavior takes effect immediately for new task executions
- Old chapters without error artifacts remain valid

## Testing Requirements

### Unit Tests

1. **Test artifact storage on task error**
   ```go
   func TestDoTask_StoresArtifactsOnError(t *testing.T)
   ```
   - Task returns error with artifacts attached
   - Verify artifacts are stored in chapter
   - Verify artifacts are cleaned up

2. **Test artifact storage on retried errors**
   ```go
   func TestDoTask_StoresArtifactsOnEachRetry(t *testing.T)
   ```
   - Task fails multiple times with artifacts
   - Verify each attempt's artifacts are stored
   - Verify each attempt's artifacts are cleaned up

3. **Test nil output on error**
   ```go
   func TestDoTask_NilOutputOnError(t *testing.T)
   ```
   - Task returns `nil, error`
   - Verify no panic, empty artifacts stored

4. **Test GetArtifacts error during error handling**
   ```go
   func TestDoTask_GetArtifactsFailsDuringError(t *testing.T)
   ```
   - Mock `GetArtifacts()` to return error
   - Verify warning logged, error handling continues

### Integration Tests

1. **End-to-end workflow with error artifacts**
   ```go
   func TestWorkflow_ErrorArtifactsStoredAndRetrievable(t *testing.T)
   ```
   - Run workflow with task that errors with artifacts
   - Retrieve workflow history
   - Verify artifacts are present in failed task's chapter
   - Verify artifacts have been cleaned up

2. **Artifact cleanup with errors**
   ```go
   func TestArtifactCleanup_CleansErrorArtifacts(t *testing.T)
   ```
   - Task errors with artifacts
   - Run cleanup job
   - Verify artifacts are marked for cleanup and reclaimed

### Manual Testing

1. Create a workflow with a task that produces diagnostic logs and then fails
2. Execute the workflow
3. Retrieve the workflow history
4. Verify:
   - Error chapter contains the artifacts
   - Artifacts can be retrieved and read
   - Artifacts are cleaned up after storage

## Rollout Plan

### Phase 1: Implementation
- Implement changes in `runner.go` as specified
- Add unit tests covering all edge cases

### Phase 2: Integration Testing
- Add integration tests for error artifact storage and cleanup
- Verify behavior with various task types

### Phase 3: Deployment
- Deploy to test environment
- Run test workflows with error artifacts
- Monitor logs for any cleanup warnings or failures

### Phase 4: Production
- Deploy to production
- Monitor artifact storage and cleanup metrics
- No migration required

## Success Metrics

- **Correctness**: Artifacts from failed tasks are stored and retrievable
- **Cleanup**: Artifacts from failed tasks are cleaned up (no storage leaks)
- **Performance**: No significant performance regression in task execution
- **Logging**: No increase in artifact cleanup warnings/errors

## Open Questions

1. **Artifact size limits**: Should we enforce size limits on error artifacts to prevent storage abuse?
   - *Recommendation*: Use existing artifact size limits; no special handling needed

2. **Selective artifact storage**: Should tasks be able to specify "store on error only" artifacts?
   - *Recommendation*: Not for initial implementation; can add later if needed

3. **Retention policy**: Should error artifacts have different retention than success artifacts?
   - *Recommendation*: No; use same retention policy for simplicity

## References

- Current implementation: `pkg/swf/impl/runner.go:189-465`
- Artifact cleanup: `pkg/swf/artifact_cleanup_integration_test.go`
- Chapter storage: `pkg/swf/impl/engine.go:563-609`
