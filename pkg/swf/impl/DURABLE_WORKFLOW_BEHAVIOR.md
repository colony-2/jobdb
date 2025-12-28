# Durable Workflow Behavior Specification

This document describes the expected behavior of the SWF (Simple Workflow) durable execution engine, focusing on job and task execution, retry logic, and crash recovery guarantees.

## Core Principles

### 1. Durability and Crash Recovery

The workflow engine must be **crash-resistant** and **resumable**:
- Jobs and tasks can be interrupted at any time (process crash, restart, etc.)
- Upon restart, execution resumes from where it left off
- Completed work is NEVER re-executed
- The system achieves this through the **cache-first** execution pattern

### 2. Cache-First Execution

**CRITICAL**: Before executing any job or task, ALWAYS check if a cached result already exists.

```
for each attempt:
    1. Check if result exists in cache (Strata chapter at current ordinal)
    2. If cached result found:
       - If success: return immediately (NO EXECUTION)
       - If retryable error: apply backoff and retry
       - If non-retryable error: fail immediately
    3. If NO cached result:
       - Execute the job/task worker
       - Save result to cache
       - Decide whether to retry
```

**Why this matters**: Without cache-first execution, jobs would re-execute on every restart, violating durability guarantees.

### 3. Write-Once Chapters

Chapters in Strata are **immutable** once saved:
- Each execution attempt gets a **unique ordinal**
- You CANNOT overwrite a chapter at an existing ordinal
- Retries must use a NEW ordinal

**Example ordinal sequence**:
```
Ordinal 0: Job input data
Ordinal 1: First attempt result (success or failure)
Ordinal 2: Second attempt result (if first failed and retryable)
Ordinal 3: Third attempt result (if second failed and retryable)
...
```

## Job Execution Model (`DoJob`)

### Execution Flow

```go
func (r *runner) DoJob(ctx context.Context, lease *pgwf.Lease) {
    // 1. Load chapter 0 (input) and merge run policy
    inputData, env0, err := r.loadInitialChapterAndPolicy()

    // 2. Setup execution config (retry policy, timeouts, deadlines)
    cfg, err := r.setupJobExecutionConfig(ctx, inputData, env0)

    attempt := 1

    // 3. Main retry loop - each attempt gets a new ordinal
    for {
        // 4. CACHE-FIRST: Check if we already have a cached result
        cachedJobOrdinal := r.storyCounter
        cachedOutput, nextAttempt, cached, terminal, err :=
            r.checkCachedJobResult(ctx, key, cachedJobOrdinal, ...)

        if cached {
            r.storyCounter++  // Move past cached result
            if terminal {
                // Terminal state (success or non-retryable error)
                // Return immediately - NO EXECUTION!
                _ = lease.Complete(ctx, r.engine.udb)
                return
            }
            // Retryable error - continue to retry with backoff
            attempt = nextAttempt
            continue
        }

        // 5. No cached result - execute job worker
        resultCh := r.executeJobWorkerAsync(inputData)
        output, jobErr := r.waitForJobResultWithDeadline(resultCh, ...)

        // 6. Save result at NEW ordinal
        jobResultOrdinal := r.storyCounter
        r.storyCounter++
        r.saveJobChapter(key, payload, artifacts, jobResultOrdinal, ...)

        // 7. Check if should retry
        retryable := isRetryable(jobErr, cfg.retryCfg)
        if jobErr == nil || !retryable || attempt >= cfg.maxAttempts {
            // Terminal state - complete lease and return
            _ = lease.Complete(ctx, r.engine.udb)
            return
        }

        // 8. Apply backoff and retry
        attempt++
        backoff := calculateBackoff(attempt, cfg.retryCfg)
        select {
        case <-time.After(backoff):
            continue  // Next iteration with NEW ordinal
        case <-ctx.Done():
            return
        }
    }
}
```

### Key Behaviors

1. **Ordinal Management**:
   - `storyCounter` tracks current ordinal
   - Incremented for each chapter (cached or newly saved)
   - Each retry attempt uses a new ordinal

2. **Lease Completion**:
   - Lease is ONLY completed when job reaches terminal state
   - Terminal states: success, non-retryable error, max retries exceeded
   - If job crashes mid-execution, lease expires and job can be re-leased

3. **Retry Decision**:
   - Retryable errors: Apply backoff and retry with new ordinal
   - Non-retryable errors: Fail immediately
   - Max attempts exceeded: Fail with last error

## Task Execution Model (`DoTask`)

Tasks follow the **same pattern** as jobs but at a finer granularity:

```go
func (r *runner) DoTask(policy swf.RunPolicy, taskType string, data swf.TaskData) (swf.TaskData, error) {
    // Setup retry config, timeouts, etc.
    retryCfg := mergeRetryPolicy(policy.RetryPolicy)
    maxAttempts := retryCfg.MaxAttempts
    attempt := 1

    // Main retry loop - each attempt gets a new ordinal
    for {
        // Get ordinal for this attempt
        ordinal := r.storyCounter
        r.storyCounter++

        // CACHE-FIRST: Check if we already have a result at this ordinal
        chap, err := r.engine.strata.Chapter(ctx, key, ordinal)
        if err == nil {
            // Cached result exists
            td, payloadErr := envelopeToTaskData(env, chap.Artifacts())
            if payloadErr == nil {
                // Cached success - return immediately
                return td, nil
            }

            // Cached error - check if retryable
            retryable := isRetryable(payloadErr, retryCfg)
            if !retryable || priorAttempt >= maxAttempts {
                return nil, payloadErr
            }

            // Wait backoff and continue to next iteration (new ordinal)
            attempt = priorAttempt + 1
            backoff := calculateBackoff(attempt, retryCfg)
            select {
            case <-time.After(backoff):
                continue
            case <-ctx.Done():
                return nil, ctx.Err()
            }
        }

        // No cached result - execute task
        output, originalErr := r.executeTaskWorker(ctx, taskType, data, ...)

        // Save result at this ordinal
        err = r.engine.strata.SaveChapter(context.TODO(), key, chap)

        if originalErr == nil {
            return output, nil
        }

        retryable := isRetryable(originalErr, retryCfg)
        if retryable && attempt < maxAttempts {
            attempt++
            backoff := calculateBackoff(attempt, retryCfg)
            select {
            case <-time.After(backoff):
                continue  // Next iteration (new ordinal)
            case <-ctx.Done():
                return nil, ctx.Err()
            }
        }

        return nil, originalErr
    }
}
```

### Jobs vs Tasks

| Aspect | Jobs | Tasks |
|--------|------|-------|
| Scope | Top-level workflow unit | Sub-unit called by jobs |
| Lease | Managed via pgwf.Lease | No lease (part of job) |
| Ordinals | Start from 0 (input) | Continue from job's counter |
| Story Key | Based on JobKey | Based on parent job + task index |
| Execution | Via JobWorker.Run() | Via TaskWorker.Run() |
| Result | JobData | TaskData |

## Ordinal Sequences

### Example 1: Job Success on First Attempt

```
Ordinal 0: Job input {"workflow": "checkout"}
Ordinal 1: Job result {"status": "completed"} [SUCCESS]
→ Lease completed, job done
```

### Example 2: Job Retry - First Fails, Second Succeeds

```
Ordinal 0: Job input {"amount": 100}
Ordinal 1: Job error "timeout" [RETRYABLE, attempt=1]
→ Wait backoff 1s
Ordinal 2: Job result {"receipt": "abc123"} [SUCCESS, attempt=2]
→ Lease completed, job done
```

### Example 3: Job Max Retries Exhausted

```
Ordinal 0: Job input {"endpoint": "api.example.com"}
Ordinal 1: Job error "connection refused" [RETRYABLE, attempt=1]
→ Wait backoff 1s
Ordinal 2: Job error "connection refused" [RETRYABLE, attempt=2]
→ Wait backoff 2s
Ordinal 3: Job error "connection refused" [TERMINAL, attempt=3, max=3]
→ Lease completed with failure
```

### Example 4: Job with Multiple Tasks

```
Ordinal 0: Job input
Ordinal 1: Task "validate-user" result [SUCCESS]
Ordinal 2: Task "charge-card" error "insufficient funds" [RETRYABLE]
→ Wait backoff 1s
Ordinal 3: Task "charge-card" result [SUCCESS]
Ordinal 4: Task "send-email" result [SUCCESS]
Ordinal 5: Job result [SUCCESS]
→ Lease completed
```

### Example 5: Crash Recovery with Cache

```
Initial execution:
Ordinal 0: Job input
Ordinal 1: Task "fetch-data" result [SUCCESS]
Ordinal 2: Task "process-data" result [SUCCESS]
→ CRASH (before job completes)

After restart:
Ordinal 0: Job input [CACHED]
Ordinal 1: Task "fetch-data" [CACHED, return immediately]
Ordinal 2: Task "process-data" [CACHED, return immediately]
Ordinal 3: Job result [SUCCESS]
→ Lease completed

CRITICAL: Tasks at ordinal 1 and 2 are NOT re-executed!
```

## Retry Policies

### Retry Configuration

```go
type RetryPolicy struct {
    MaxAttempts         int           // Maximum number of attempts (including first)
    InitialInterval     time.Duration // Initial backoff duration
    BackoffCoefficient  float64       // Multiplier for exponential backoff
    MaxInterval         time.Duration // Maximum backoff duration
    RetryableErrorTypes []string      // Error types that trigger retry
    NonRetryableErrorTypes []string   // Error types that never retry
}
```

### Retry Decision Logic

```go
func isRetryable(err error, policy RetryPolicy) bool {
    if err == nil {
        return false
    }

    // Check non-retryable error types first
    for _, errType := range policy.NonRetryableErrorTypes {
        if errors.As(err, reflect.TypeOf(errType)) {
            return false
        }
    }

    // Check retryable error types (if specified)
    if len(policy.RetryableErrorTypes) > 0 {
        for _, errType := range policy.RetryableErrorTypes {
            if errors.As(err, reflect.TypeOf(errType)) {
                return true
            }
        }
        return false  // Not in retryable list
    }

    // Default: most errors are retryable unless explicitly non-retryable
    return true
}
```

### Backoff Calculation

```go
func calculateBackoff(attempt int, policy RetryPolicy) time.Duration {
    // Exponential backoff: InitialInterval * (BackoffCoefficient ^ (attempt-1))
    backoff := policy.InitialInterval
    for i := 1; i < attempt; i++ {
        backoff = time.Duration(float64(backoff) * policy.BackoffCoefficient)
    }

    // Cap at MaxInterval
    if backoff > policy.MaxInterval {
        backoff = policy.MaxInterval
    }

    return backoff
}
```

### Example Backoff Sequence

With `InitialInterval=1s`, `BackoffCoefficient=2.0`, `MaxInterval=60s`:

```
Attempt 1: Execute immediately
Attempt 2: Wait 1s   (1 * 2^0)
Attempt 3: Wait 2s   (1 * 2^1)
Attempt 4: Wait 4s   (1 * 2^2)
Attempt 5: Wait 8s   (1 * 2^3)
...
Attempt N: Wait 60s  (capped at MaxInterval)
```

## Timeout Handling

### Two-Level Timeouts

1. **Invocation Timeout**: Per-attempt timeout
2. **Total Timeout**: Overall job/task timeout across all attempts

```go
type RunPolicy struct {
    InvocationTimeout time.Duration  // Timeout per attempt
    TotalTimeout      time.Duration  // Timeout for all attempts combined
}
```

### Timeout Behavior

```go
func (r *runner) waitForJobResultWithDeadline(...) (swf.JobData, error) {
    // Calculate deadlines
    invocationDeadline := time.Now().Add(invocationTimeout)

    select {
    case result := <-resultCh:
        return result.output, result.err

    case <-time.After(time.Until(invocationDeadline)):
        // Invocation timeout - retryable error
        return nil, &TimeoutError{Type: "invocation"}

    case <-time.After(time.Until(totalDeadline)):
        // Total timeout - non-retryable error
        return nil, &TimeoutError{Type: "total"}
    }
}
```

### Timeout Example

```
TotalTimeout=10s, InvocationTimeout=3s, MaxAttempts=5

Attempt 1: Execute at t=0s, timeout at t=3s [RETRYABLE]
Attempt 2: Execute at t=4s, timeout at t=7s [RETRYABLE]
Attempt 3: Execute at t=8s, timeout at t=10s (total deadline) [NON-RETRYABLE]
→ Job fails with total timeout
```

## Input Hash Validation

### Purpose

Input hash ensures **deterministic replay**:
- Verifies cached results match current input
- Detects if job was restarted with different input
- Prevents using stale cached results

### Implementation

```go
func (r *runner) checkCachedJobResult(...) (...) {
    chap, err := r.engine.strata.Chapter(ctx, key, ordinal)
    if err != nil {
        return nil, 0, false, false, nil  // No cached result
    }

    env, err := envelopeFromChapter(chap)

    // Validate input hash matches
    if env.InputHash != inputHash {
        return nil, 0, false, false,
            fmt.Errorf("input hash mismatch: expected %s, got %s",
                inputHash, env.InputHash)
    }

    // Return cached result
    return output, env.Attempt, true, terminal, err
}
```

### Input Hash Mismatch Example

```
Original execution:
Input: {"userId": 123}
InputHash: "abc123"
Ordinal 1: Result cached

Restart with different input:
Input: {"userId": 456}
InputHash: "def456"
Ordinal 1: Hash mismatch!
→ Error: Cannot resume - input changed
```

## Testing Strategy

### Critical Test Cases

1. **Cache Hit on Restart** (`TestJobRestartUsesCache`, `TestTaskRestartUsesCache`)
   - Execute job/task, save result
   - Restart without completing lease
   - Verify NO re-execution (cached result used)
   - **This test would FAIL with buggy code!**

2. **Retry with Failures** (`TestJobRetryWithFailures`, `TestTaskRetryWithFailures`)
   - First attempt fails with retryable error
   - Second attempt succeeds
   - Verify two chapters saved (ordinals 1 and 2)
   - Verify backoff applied

3. **Max Retries Exhausted** (`TestJobMaxRetriesExhausted`, `TestTaskMaxRetriesExhausted`)
   - All attempts fail with retryable error
   - Verify all attempts executed (up to MaxAttempts)
   - Verify all chapters saved
   - Verify job/task fails after last attempt

4. **Non-Retryable Error** (`TestJobNonRetryableError`, `TestTaskNonRetryableError`)
   - First attempt fails with non-retryable error
   - Verify NO retry (only one chapter saved)
   - Verify immediate failure

5. **Ordinal Determinism** (`TestJobOrdinalDeterminism`)
   - Execute job with multiple tasks
   - Restart and verify same ordinal sequence
   - Ensures deterministic replay

### Test Helper Pattern

```go
type countingTaskWorker struct {
    counter *int32  // Shared counter to track executions
}

func (w *countingTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
    atomic.AddInt32(w.counter, 1)
    return data, nil
}

func TestTaskRestartUsesCache(t *testing.T) {
    var executionCount int32
    worker := &countingTaskWorker{counter: &executionCount}

    // First execution
    engine.DoTask(ctx, policy, "test-task", input)
    assert.Equal(t, int32(1), executionCount)

    // Restart - should use cache
    engine2.DoTask(ctx, policy, "test-task", input)
    assert.Equal(t, int32(1), executionCount)  // STILL 1!
}
```

## Common Pitfalls

### ❌ WRONG: Execute then Check Cache

```go
// BUGGY CODE - DO NOT DO THIS!
for {
    // Execute first
    output, err := executeJobWorker(input)

    // Check cache after execution
    cached, _ := checkCache(ordinal)
    if cached {
        return cachedResult
    }

    saveResult(output, ordinal)
}
```

**Problem**: Job executes even when cached result exists!

### ✅ CORRECT: Check Cache then Execute

```go
// CORRECT CODE
for {
    // Check cache FIRST
    cached, result := checkCache(ordinal)
    if cached {
        return result  // NO EXECUTION!
    }

    // Execute only if no cache
    output, err := executeJobWorker(input)
    saveResult(output, ordinal)
}
```

### ❌ WRONG: Reuse Ordinal for Retry

```go
// BUGGY CODE - DO NOT DO THIS!
ordinal := r.storyCounter
for {
    saveResult(output, ordinal)  // Same ordinal every attempt!
    if shouldRetry {
        continue  // Will fail: chapter already exists!
    }
}
```

**Problem**: Strata error "chapter already exists" on retry!

### ✅ CORRECT: New Ordinal per Attempt

```go
// CORRECT CODE
for {
    ordinal := r.storyCounter
    r.storyCounter++  // Increment INSIDE loop

    saveResult(output, ordinal)  // New ordinal each attempt
    if shouldRetry {
        continue
    }
}
```

## Summary

The durable workflow engine ensures crash-resistant, resumable execution through:

1. **Cache-First Execution**: Always check for cached results before executing
2. **Write-Once Chapters**: Each attempt gets a unique ordinal; no overwrites
3. **Ordinal Management**: Sequential ordinals track execution history
4. **Retry Logic**: Retryable errors get new attempts with exponential backoff
5. **Timeout Handling**: Per-attempt and total timeouts with proper error classification
6. **Input Validation**: Hash checking prevents using stale cached results
7. **Lease Management**: Leases completed only at terminal states

**Golden Rule**: If the system crashes and restarts, it should pick up exactly where it left off, using cached results for completed work and continuing from the next uncompleted step.
