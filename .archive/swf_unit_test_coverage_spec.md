# SWF Unit Test Coverage Spec

Goal: raise confidence in SWF’s building blocks without leaning solely on the heavyweight integration suites. These unit tests should run without Postgres/Strata daemons by using fakes/stubs, and they should lock in deterministic behavior described in the retry and chapter metadata specs.

## Test Harness Notes
- Provide an in-memory Strata fake that satisfies the minimal client surface used in `impl/runner.go` and `impl/task.go` (`CreateStory`, `CloneStory`, `Story`, `Chapter`, `SaveChapter`, `GetLastChapter`), storing chapters by `{tenant, jobId, ordinal}`.
- Stub `pgwf.Lease`/`Reschedule`/`Complete`/`RescheduleUnheldJob` so assertions can inspect the next need/payload without touching a real DB.
- Use lightweight worker doubles (success, returns error, panic) with controllable outputs; artifacts can be fake objects exposing `Name`, `ID`, and `Sha256`.
- Keep sleeps tiny (`time.Millisecond`) when exercising backoff/await paths; avoid real wall-clock waits.

## Coverage by Area

### Foundations (`pkg/swf`)
- `Data` helper: `ToBytes` round-trips map and raw bytes; `Set` mutates deserialized state and invalidates serialized; `Get` errors on bad JSON; nil/empty data handling.
- `Duration` YAML marshal/unmarshal accepts valid strings, rejects invalid, and preserves `String()`; `JSONSchema` type/title stay stable.
- Error helpers: `IsAppError`/`IsSystemError` detect wrapped errors; `NonRetryable` interface short-circuits `isRetryable`.

### Retry & Policy Helpers (`impl/retry.go`)
- `normalizeRetryPolicy` defaults (`MaximumAttempts` to 1, `BackoffCoefficient` to 1) without overwriting explicit zeros where forbidden.
- `mergeRunPolicy`/`mergeRetryPolicy` honor overrides only when set, leaving unset fields from the base.
- `computeBackoff` respects backoff coefficient growth and caps at `MaximumInterval`; negative/zero intervals clamp to zero.
- `isRetryable` matrix: system errors retry, non-retryable interface returns false, `NonRetryableErrorTypes` name matching (value vs pointer) prevents retry, generic errors retry.
- `errorMatchesTypeName` matches both pointer and value type names and ignores nil unwrap chain.

### Envelope & Hashing (`impl/envelope.go`)
- `buildChapterEnvelope` rejects invalid JSON payloads and missing payload kinds; produces parseable JSON with meta fields echoed.
- `computeInputHash` uses payload bytes + artifact tuples sorted deterministically; different artifact order yields same hash, changed artifact hash/name/URI changes the hash; nil task data errors out.
- `errorPayloadFromError` classification: app error → `AppError` payload; system error → `SystemError`; generic error → `AppError` with message; `input_ref` injected when provided.
- `envelopeToTaskData` clones artifacts slice, preserves payload bytes, and rehydrates the correct error type per `payload_kind`; unsupported payload kind returns error but still wraps data in `EnvelopedTaskData`.
- `taskDataToChapter`/`payloadToChapter` enforce required payload/input hash and copy artifacts into the chapter builder; meta fields (attempt/max/backoff/retryable/inputRef/runPolicy) flow into the envelope.

### Engine Builder & Config (`pkg/swf/jobs.go`, `impl/engine.go`)
- `PlusWorkers` rejects invalid names, duplicate job workers, and duplicate task workers within a set; registers capabilities for job + each task.
- `Build` validation errors when any of strata URI/API key/Postgres DSN/tenant/worker set are missing; passes a non-nil logger; worker capability map includes both job and task capabilities.
- `StartJob`/`RestartJob` fail on missing data, propagate `computeInputHash` errors, and write initial chapter metadata (`attempt=1`, normalized `RunPolicy`, `input_hash`, payload kind `App`).
- `RestartJob` clone options carry `LastStepToKeep` and new chapter ordinal = `LastStepToKeep+1`.
- `CancelJob` delegates to pgwf with worker ID; ensure no panic on nil logger.

### Runner Task Path (`impl/runner.go` `DoTask`)
- Cache hit success: matching `input_hash` returns cached output without running worker; cache hit with decode failure or missing `input_hash` yields deterministic errors (`ErrWorkflowNotDeterministic`/`ErrMissingInputHash`).
- Cache hit error payload: honors stored `RunPolicy`/attempt/max_attempts/retryable flags; when attempts remain and retryable, waits for stored `next_attempt_at` before retrying; when exhausted or non-retryable, surfaces the cached error kind.
- Cache miss run: worker success saves chapter with meta (attempt/backoff/retryable/inputRef/runPolicy) and returns output; panic converts to `AppError` envelope; system vs app errors pick payload kind accordingly.
- Retry evaluation: `NonRetryableError` and `NonRetryableErrorTypes` stop retries; system errors always retry; `BackoffMillis` and `NextAttemptAt` derived from `computeBackoff`.
- Remote capability: when task worker absent locally, `Reschedule` invoked with `NextNeed` = `<job>:<task>` and payload (`taskWait`) encoding input/output ordinals and next hop; goroutine exits via `prematureCloseOut` after reschedule.
- InputRef hashing: `input_ref.hash` matches the hashed input for the ordinal being processed (floor at 0 for first task).

### Runner Job Path (`impl/runner.go` `Run`)
- Reads chapter 0, merges stored run policy, hashes input, and initializes `storyCounter=1`.
- Job worker success writes final chapter with metadata and completes lease; missing output produces `SystemError` envelope.
- Cached final chapter: matching `input_hash` completes lease without rerun; mismatched hash logs deterministic error.
- Job worker error paths: payload kind reflects app vs system error, retry loops honor policy/backoff/attempt fields; panic captured as app error.
- Lease interactions: `WithKeepAlive` invoked; `Complete` called on success or terminal failure; retries do not complete lease early.

### Task Handles & External Completion (`impl/task.go`)
- `chapterToTaskData` decodes envelope to task data and propagates envelope decoding errors.
- `TaskHandle.Data` fetches and caches the input chapter; errors bubble from Strata/fake.
- `TaskHandle.Finish` recomputes input hash from the cached input, writes output chapter at `outputOrdinal` with payload kind `App`, and reschedules via `RescheduleUnheldJob` with the next capability from payload; error propagation when hashing or SaveChapter fails.
- `FindTasksWaitingForCapability` filters `Job` rows by `NextNeed` and status, decodes `taskWait` payload into handles (input/output ordinals, next capability), and errors on invalid JSON.

### Engine Read APIs
- `CheckJobStatus` returns job ID + ordinal from last Strata chapter when present; falls back to 0 on Strata error.
- `GetJobResult` returns `ErrJobNotComplete` when the job isn’t archived; when archived, returns final payload or propagated app/system error based on envelope; artifacts preserved.

### Existing Coverage Acknowledgment
- Keep `basic_workflow_integration_test.go` and `error_workflow_integration_test.go` as end-to-end safety nets; `impl/envelope_test.go` already covers the happy-path error envelopes—extend, don’t duplicate.

