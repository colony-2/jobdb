# AwaitDuration Engine-Directed Waiting (Mini Spec)

## Goal
Replace `AwaitDuration`'s direct `time.Sleep` with an engine-directed wait so the engine can choose whether to keep the goroutine alive or recycle and reschedule, while keeping deterministic retry behavior. The engine may change its mind at any time (e.g., resource pressure) and send a recycle signal while a wait is in progress. Await calls should hand the engine an absolute wake time to avoid over-waiting on replay after crashes.

## Key Idea
`AwaitDuration` blocks on an engine-provided channel instead of sleeping. The engine either:
- Sends a `wake` signal after a timer for short waits, or
- Sends a `recycle` signal immediately after persisting retry metadata and rescheduling, causing the runner to exit via `prematureCloseOut()`.

## Runner Behavior
1) Compute `wakeAt = now + waitFor` (or recompute from stored `created_at` + policy backoff).
2) Obtain the runner’s await channel from the engine (per-runner, reused): `ch := engine.AwaitUntil(jobID, ordinal, attempt, wakeAt)`.
3) `select` on:
   - `wake` → return nil and continue.
   - `recycle` (can arrive anytime before wake) → call `prematureCloseOut()` to terminate the goroutine.
   - `ctx.Done()` → treat as recycle (return `ctx.Err()` or call `prematureCloseOut()`).
4) Determinism: runner uses stored `attempt` and `created_at` from chapter metadata plus the provided `RunPolicy` to recompute backoff; no recomputation of randomness.

## Engine Behavior
- Decide based on `wakeAt` vs. `now`, but retain the right to recycle later:
  - If keeping in-memory, start a timer to fire at `wakeAt` and send `wake` on the channel.
  - If rescheduling, record the prior attempt (with its completion time `created_at`) and recompute `wakeAt` from policy; set `not_before = wakeAt`, and send `recycle` so the runner exits.
  - If conditions change (e.g., resource pressure) while a wake is pending, cancel the timer and send `recycle` instead.
- If engine is shutting down or cannot honor a wake, prefer `recycle` to keep behavior deterministic on replay.
- Keep a single per-runner await channel (the goroutine can only wait on one thing at a time); the engine sends `wake` or `recycle` on it per await.

## Persistence/Storage Map (what’s new vs. already present)
- **Chapter metadata (Strata)**:
  - **Existing**: `attempt` (per attempt), `ordinal`, `task_type`, `worker_id`, `created_at` (completion time), `input_hash`, `input_ref` (ordinal + hash) for error payload reference.
  - **Derived on replay, not persisted**: backoff, `wakeAt`, `retryable` (from current error and policy), and `run_policy` (supplied at call sites).
  - **Remove/avoid**: do not persist `next_attempt_at`, `backoff_ms`, `max_attempts`, `retryable`, or `run_policy` in chapters—these are recalculated deterministically from `attempt`, `created_at`, and the provided `RunPolicy`.
- **Chapter payload (Strata)**: task/job payload or error payload; no extra await state beyond the meta above.
- **pgwf lease rows**: when recycling, set `not_before = wakeAt` (recomputed from `created_at` + policy-derived backoff) so the job is only made available after the intended wake time. No retry/backoff details are stored in pgwf beyond `not_before`.
- **In-memory (engine)**: timers, await channel, and any late recycle decisions; these are ephemeral and not persisted.

## Replay Expectations
- On replay, read the last chapter for the ordinal. Use its `attempt` and `created_at` plus the provided `RunPolicy` to recompute backoff and `wakeAt`; if `now < wakeAt`, await again (engine may recycle); otherwise run immediately. The next retry time is derived from the previous attempt’s completion time plus the policy-derived backoff, not from the current wall clock alone.

## Persistence & Metadata
- No extra retry/backoff metadata needs to be persisted beyond `attempt` and `created_at`; backoff/jitter is recomputed deterministically from `RunPolicy` and attempt count. Await is called with `wakeAt` (absolute time) so a replay does not over-wait after a crash.
- Jitter (if added) must be derived deterministically from stable inputs (e.g., hash of input/task/attempt) so recomputation matches the original.
- When recycling, ensure the lease is completed/canceled after writing the chapter and setting `not_before`.

## Edge Cases
- If the await channel cannot be produced, default to `recycle`.
- Context cancellation while waiting should behave like recycle.
- On replay, compare `now` against recomputed `wakeAt`; if `now < wakeAt`, await (engine may recycle); otherwise run immediately.
