# Debug Logging for Input Hash Tracking

## Overview
Added comprehensive slog debug logging throughout the workflow execution to track input hash computation and comparison. This will help diagnose the "workflow was not deterministic" errors you're experiencing.

**Key Feature:** The actual data being hashed is now logged, allowing you to see exactly what's different between executions.

## Logging Locations

### 0. Input Hash Computation - Raw Data (MOST IMPORTANT)
**File:** `/src/pkg/swf/impl/envelope.go` (computeInputHash function, ~line 115)

**Logs when:** Every time an input hash is computed (called by all other logging points)

**Log message:** `"computeInputHash: data being hashed"`

**Fields logged:**
- `hash` - The computed SHA256 hash
- `data` - **The actual JSON data being hashed** (as string)
- `dataLength` - Size in bytes
- `artifacts` - Array of artifact details with `name`, `id`, and `hash` for each
- `artifactCount` - Number of artifacts

**This is the key log that will show you exactly what's different!**

### 1. Task Worker - Input Hash Computation
**File:** `/src/pkg/swf/impl/runner.go` (DoTask function, ~line 200)

**Logs when:** A task input hash is computed

**Log message:** `"computed task input hash"`

**Fields logged:**
- `taskType` - The type of task being executed
- `inputHash` - The computed SHA256 hash of the input
- `dataLength` - Size of the input data in bytes
- `artifactCount` - Number of artifacts in the input

### 2. Task Worker - Cached Result Check
**File:** `/src/pkg/swf/impl/runner.go` (DoTask function, ~line 245)

**Logs when:** Checking a cached task result and comparing input hashes

**Log message:** `"checking cached task result"`

**Fields logged:**
- `taskType` - The type of task
- `ordinal` - The chapter ordinal being checked
- `cachedInputHash` - The hash stored in the cached chapter
- `computedInputHash` - The freshly computed hash
- `hashMatch` - Boolean indicating if hashes match

**Error log on mismatch:** `"task input hash mismatch"` (ERROR level)

### 3. Job Worker - Input Hash Computation
**File:** `/src/pkg/swf/impl/runner.go` (setupJobExecutionConfig function, ~line 691)

**Logs when:** Computing the job's input hash

**Log message:** `"computed job input hash"`

**Fields logged:**
- `inputHash` - The computed SHA256 hash
- `dataLength` - Size of the job input data
- `artifactCount` - Number of artifacts

### 4. Job Worker - Cached Job Result Check
**File:** `/src/pkg/swf/impl/runner.go` (checkCachedJobResult function, ~line 831)

**Logs when:** Checking a cached job result and comparing input hashes

**Log message:** `"checking cached job result"`

**Fields logged:**
- `ordinal` - The chapter ordinal being checked
- `cachedInputHash` - The hash from the cached result
- `computedInputHash` - The freshly computed hash
- `hashMatch` - Boolean indicating if hashes match

**Error log on mismatch:** `"job result input hash mismatch"` (ERROR level)

### 5. External Task Completion
**File:** `/src/pkg/swf/impl/task.go` (taskHandleImpl.Finish function, ~line 93)

**Logs when:** An external task is completed and the input hash is computed from the input chapter

**Log message:** `"computed external task input hash"`

**Fields logged:**
- `taskType` - The type of external task
- `jobId` - The job ID
- `inputOrdinal` - The ordinal of the input chapter
- `outputOrdinal` - The ordinal where the output will be written
- `inputHash` - The computed hash
- `dataLength` - Size of the input data
- `artifactCount` - Number of input artifacts

### 6. Async Child Spawning
**File:** `/src/pkg/swf/impl/runner.go` (spawnAsyncWithDeadlines function, ~line 580)

**Logs when:** Spawning an async child job

**Log message:** `"computed async child input hash"`

**Fields logged:**
- `childJobType` - The type of child job
- `ordinal` - The ordinal for the spawn metadata
- `inputHash` - The computed hash
- `dataLength` - Size of the child input data
- `artifactCount` - Number of artifacts

**When checking cached spawn:**

**Log message:** `"checking cached async child spawn"`

**Fields logged:**
- `childJobType`
- `ordinal`
- `cachedInputHash`
- `computedInputHash`
- `hashMatch`

**Error log on mismatch:** `"async child input hash mismatch"` (ERROR level)

## How to Use This Logging

### Enable Debug Logging
To see these debug messages, ensure your logger is configured to show DEBUG level logs. In your application setup:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug, // Enable debug level
}))
```

### Interpreting the Logs

When you reproduce the issue, look for:

1. **Raw data comparison** - Find the two `"computeInputHash: data being hashed"` logs for the same task:
   - One from the initial execution
   - One from the restart after external task completion
   - **Compare the `data` fields** - you'll see exactly what's different (timestamps, UUIDs, etc.)
2. **Hash mismatches** - Look for ERROR logs showing hash mismatches
3. **Artifact differences** - If artifacts are involved, compare the `artifacts` array between executions
4. **Ordinal progression** - Track which ordinals are being used and whether they match expectations

### Example Analysis Flow

For your specific error:
```
"ordinal 1 task input:generate_form" - workflow was not deterministic
```

Look for logs like:
1. First execution:
   ```json
   {"msg":"computeInputHash: data being hashed","hash":"047ce6...",
    "data":"{\"prompt\":\"...\",\"timestamp\":\"2026-01-13T15:02:14Z\"}"}
   ```
2. After restart:
   ```json
   {"msg":"computeInputHash: data being hashed","hash":"69d03e...",
    "data":"{\"prompt\":\"...\",\"timestamp\":\"2026-01-13T15:03:18Z\"}"}
   ```
3. **Compare the `data` fields** - in this example, the timestamp changed!

For the second error:
```
"ordinal 2 job result input hash mismatch"
```

This is a cascading effect - once the task input is non-deterministic, the job result validation also fails.

## Additional Notes

- All debug logs use structured logging with key-value pairs for easy parsing
- Error-level logs are emitted when hash mismatches are detected
- **NEW:** The actual data being hashed is now printed in the logs - this will immediately show you what's different!
- The `dataLength` and `artifactCount` fields can help identify if the input data itself is changing between runs
- If artifacts are involved, ensure their hashes (SHA256 of content) are stable across runs
- For large payloads, you may want to use `jq` or similar to pretty-print and diff the `data` fields

## Next Steps

1. Run your workflow with debug logging enabled
2. Capture the logs from both the initial run and the restart after external task completion
3. Look for the `"computeInputHash: data being hashed"` logs and compare the `data` fields
4. The difference will show you exactly what's non-deterministic in your code

## Quick Analysis with jq

To extract and compare the data being hashed:

```bash
# Extract first execution data for a specific task
grep 'computeInputHash: data being hashed' logs.json | \
  jq -r 'select(.taskType == "input:generate_form") | .data' | \
  head -1 > first_execution.json

# Extract second execution data for the same task
grep 'computeInputHash: data being hashed' logs.json | \
  jq -r 'select(.taskType == "input:generate_form") | .data' | \
  tail -1 > second_execution.json

# Pretty print and diff
diff <(jq . first_execution.json) <(jq . second_execution.json)
```

This will show you exactly which fields changed between executions!
