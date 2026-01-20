# Runner.Run() Bug Analysis and Specification

## Current Behavior (BUGGY)

The refactored `Run()` method has the following flow:

```
for each retry attempt:
1. Check if total timeout exceeded
2. Setup deadlines for this attempt
3. ❌ Execute job worker (ALWAYS runs, even if cached result exists)
4. Wait for job worker result
5. Validate timeouts after execution
6. Get ordinal and increment storyCounter
7. Check if cached result exists at this ordinal
8. If cached result exists and is terminal (success or non-retryable), complete lease and return
9. Otherwise, use the freshly executed result to prepare payload
10. Save the new chapter with fresh execution result
11. Complete lease in saveJobChapterAndComplete
12. If successful or non-retryable, return
13. If retryable and under max attempts, wait backoff and loop
```

## Critical Bugs

### Bug 1: Job Worker Executes Before Cache Check
**Location:** Lines 860-867

The job worker is executed BEFORE checking if a cached result exists. This means:
- ❌ On job restart, we re-execute the entire job worker even if a successful result is already saved
- ❌ Violates durability guarantee - we don't replay through cached results
- ❌ Wastes CPU and potentially causes duplicate side effects
- ❌ The fresh execution result is discarded if a terminal cached result exists

### Bug 2: Lease Completed Prematurely
**Location:** Line 903 (saveJobChapterAndComplete) and line 879

The lease is completed inside `saveJobChapterAndComplete`, which is called before we know if we need to retry. This means:
- ❌ Lease is completed even when we're about to retry (line 903)
- ❌ Lease is also completed when cached result is terminal (line 879)
- ✅ Only the second case is correct

### Bug 3: Fresh Execution Result Used When Cached Result Exists
**Location:** Lines 889-903

After finding a cached result that indicates we should NOT return (needs retry), we still use the fresh execution result to save a chapter:
- ❌ If cached result says "attempt 2, retry needed", we execute fresh (creating what should be attempt 3), but then save using attempt from cached result
- ❌ Mixing cached metadata with fresh execution results
- ❌ The fresh execution result should be discarded if cached result exists

### Bug 4: Ordinal Incremented After Execution
**Location:** Lines 869-870

The ordinal is obtained and storyCounter incremented AFTER job execution:
- ❌ We don't know which ordinal to check for cache until after wasted execution
- ❌ Should get ordinal first, check cache, then conditionally execute

## Correct Behavior (Based on DoTask Pattern)

The `DoTask` method (lines 188-431) implements the correct durable workflow pattern:

```
1. Get ordinal and increment storyCounter
2. ✅ Try to get cached chapter at ordinal
3. If cached result found:
   a. Validate input hash matches (determinism check)
   b. Check if total timeout exceeded
   c. Try to decode result
   d. ✅ If successful, return immediately (no execution!)
   e. If error and retryable and under max attempts:
      - Wait backoff if needed
      - Fall through to execution
   f. If error and non-retryable or max attempts reached:
      - Return error immediately (no execution!)
4. If no cached result found (or fell through from 3e):
   a. Check if worker capability exists locally
   b. Execute the worker
   c. Save result
   d. Handle retry logic
```

## Required Changes for Run() Method

The `Run()` method should follow this corrected flow:

```
for each retry attempt:
1. Check if total timeout exceeded
2. Get ordinal and increment storyCounter
3. ✅ Check if cached job result exists at ordinal
4. If cached result found:
   a. Validate input hash matches
   b. Check if total timeout exceeded
   c. Try to decode result
   d. ✅ If successful:
      - Complete lease
      - Return (DO NOT execute!)
   e. If error and non-retryable or max attempts reached:
      - Complete lease
      - Return (DO NOT execute!)
   f. If error and retryable and under max attempts:
      - Wait backoff if needed
      - Continue to step 10 (retry next iteration)
5. If no cached result found (or fell through from 4f):
   a. Setup deadlines for this attempt
   b. Execute job worker
   c. Wait for result with timeout
   d. Validate timeouts after execution
   e. Prepare payload from execution result
   f. Save chapter with execution result
   g. If successful or non-retryable:
      - Complete lease
      - Return
   h. If retryable and under max attempts:
      - Wait backoff
      - Continue to step 10 (retry next iteration)
6. If max attempts reached:
   - Complete lease
   - Return
```

## Key Principles for Durable Workflows

1. **Cache-First Execution**: Always check for cached results before executing
2. **Idempotent Restarts**: Job can restart at any time and pick up where it left off
3. **No Re-execution of Completed Steps**: Once a step succeeds, never re-execute it
4. **Retry Uses Same Pattern**: Retry attempts also check cache first
5. **Lease Completion Only at Terminal States**: Complete lease only when job is done (success, non-retryable failure, or max attempts reached)
6. **Ordinal Determines Cache Location**: Ordinal must be known before cache check

## Additional Issues

### Issue 1: handleCachedJobAttempt Design
The current `handleCachedJobAttempt` function returns `cachedJobResult{found, attempt, shouldReturn, backoff}`, but:
- It's called AFTER execution has already happened
- The `shouldReturn` flag indicates terminal states, but execution was wasted
- Should be called BEFORE execution to avoid wasted work

### Issue 2: Attempt Tracking Confusion
When a cached retryable failure is found:
- Cached result has `priorAttempt = N`
- Fresh execution creates what should be attempt `N+1`
- But we save using attempt from cached result, creating inconsistency

### Issue 3: Lease Completion Logic Split
Lease completion happens in two places:
- Inside `saveJobChapterAndComplete` (line 814)
- In the main loop when cached result is terminal (line 879)

This should be consolidated to a single location after determining the job is complete.

## Recommended Refactoring Strategy

1. Create a `checkCachedJobResult()` helper that returns `(cachedOutput swf.JobData, cached bool, terminal bool, shouldRetry bool, err error)`
2. Move cache check to happen BEFORE execution
3. Only execute if `!cached || shouldRetry`
4. Separate chapter saving from lease completion
5. Complete lease only at final return points
6. Follow the exact pattern used in `DoTask` for consistency

## Test Coverage Needed

The current tests pass because they don't verify durability properties:
- Need test: Job crashes after saving result, restarts, should NOT re-execute
- Need test: Job result already exists, should complete immediately without execution
- Need test: Job has retryable failure cached, should retry correctly
- Need test: Job storyCounter is restored correctly on restart
