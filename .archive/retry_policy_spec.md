# Retry Policy & Deterministic Retries

This spec adds first-class retry behavior to SWF tasks and jobs via a `RunPolicy`, carrying retry state in Strata chapters so retries are deterministic and recoverable after crashes while leaving room for future run-time directives (e.g., affinity, max duration).

## Goals
- Allow callers to supply a `RetryPolicy` on `StartJob` and per-task `DoTask`.
- Retry on system errors and any error not explicitly marked non-retryable; panics are treated as retryable app errors.
- Persist retry attempts as additional chapters so state survives crashes and replays.
- Hide intermediate failures from callers; only surface an error after the policy is exhausted.
- Maintain deterministic behavior even with backoffs and process crashes.

## API Changes
- Introduce `RunPolicy` to bundle runtime directives; initial field: `Retry RetryPolicy` (existing struct with `InitialInterval`, `BackoffCoefficient`, `MaximumInterval`, `MaximumAttempts`, `NonRetryableErrorTypes`). Future fields (not yet implemented) may include affinity, max duration, etc.
- `StartJob` (and `RestartJob`) gains `RunPolicy RunPolicy` to define the job-level default.
- `JobContext.DoTask` signature becomes `DoTask(policy RunPolicy, taskType string, data TaskData) (TaskData, error)`.
  - Callers can pass the zero-value policy to inherit the job default; tasks can override retry policy via the `RunPolicy.Retry` field.
- Add a time-based await API on contexts: `AwaitDuration(waitFor Duration) error`, letting the engine decide whether to park in-memory or recycle and reschedule. Retries use this duration path. The runner records the await intent; the engine enforces it (sleep vs reschedule). (Job-ID awaits are handled separately in the async spec.)

## Retry Semantics
- Retry on:
  - Any system error.
  - Any other error unless it matches a configured non-retryable error type (string match against error type name) or an error implements `NonRetryable() bool` returning true.
  - Panics are wrapped into app errors → retryable unless marked non-retryable.
- Attempt numbering starts at 1; retry stops when `attempt >= MaximumAttempts` (default 1).
- Each failed attempt produces a chapter with the error payload and retry metadata.
- On exhausting attempts, return the last error to the caller (DoTask/Run).

## Chapter Metadata Additions
- Extend envelope meta with:
  - `attempt` (int).
  - `max_attempts` (int).
  - `next_attempt_at` (RFC3339, optional; when to try again).
  - `backoff_ms` (int, actual backoff chosen).
  - `retryable` (bool; whether this error would be retried if attempts remain).
  - `input_ref` (int64 ordinal of the input chapter; optional hash for extra guard).
  - Existing fields (`ordinal`, `task_type`, `worker_id`, `input_hash`, `created_at`) remain.
- These fields must be stable per attempt; they drive deterministic replay.

## Payload Additions
- Error payloads include an `input_ref` instead of duplicating input bytes:
  - Fields: `ordinal` (the input chapter ordinal), and optionally `hash`.
  - Input data remains in its original chapter; error chapters reference it to avoid duplication and recursion.
  - Artifacts stay on chapters as today.

## Backoff & Scheduling
- Backoff calculation uses the existing policy fields:
  - `backoff = min(MaximumInterval, InitialInterval * BackoffCoefficient^(attempt-1))`.
  - Optional deterministic jitter (derive from a stable hash) may be applied; store the actual `backoff_ms`.
- Record the retry attempt chapter before any wait.
- Request a time-based await from the engine for `backoff`:
  - The engine chooses whether to keep the runner alive (short waits) or recycle and reschedule with `not_before = now + backoff` (long waits), using the same await API.
- On wake-up (or replay), read the last chapter for the ordinal:
  - If attempts remain and now < `next_attempt_at`, re-apply the await deterministically.
  - If attempts remain and time has arrived, re-run the task with the same input.

## Determinism & Crash Recovery
- Input hash validation remains required on every attempt; cache hits with mismatched hash still throw deterministic errors.
- Each attempt’s chapter stores the chosen backoff and next-at time; replay does not recalc random values.
- If the process crashes while waiting, the next runner reads the last chapter, sees `next_attempt_at`, and either waits/reschedules or proceeds—behavior is identical to the original plan.
- Final surface error (after exhaustion) is the last attempt’s error; intermediate errors are hidden from the caller.

## Flow (DoTask)
1. Compute `input_hash`; load existing chapter:
   - On cache hit with success payload → return it.
   - On cache hit with error payload:
     - If attempts remain and retryable → schedule/execute retry per stored metadata.
     - If exhausted → return the error.
2. On miss or retry execution:
   - Run task with panic capture → error classification.
   - Determine retryability and next backoff.
   - Persist chapter with payload + retry metadata (including attempt number, backoff, next-at, input snapshot).
   - If retryable and attempts remain: reschedule for `next_attempt_at` (or run again immediately if zero backoff).
   - Otherwise return output or final error.

## Flow (StartJob/RestartJob)
- Initial chapter (ordinal 0) includes retry metadata (attempt 1, max from policy).
- If the job worker errors and is retryable with attempts remaining:
  - Persist chapter with retry metadata and reschedule job with not-before time.
  - Hide the error from the caller until attempts are exhausted.

## Non-Goals / Open Items
- Jitter source: derive from deterministic hash to avoid randomness.
- Policy overrides: task-level policy overrides job-level default; unspecified fields inherit defaults.
- Observability: consider logging attempt/next-at info for debugging; not part of the chapter schema.
