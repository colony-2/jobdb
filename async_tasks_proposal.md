**Async Tasks Proposal (Draft)**

Goal: allow workflows to launch and await asynchronous child jobs (not individual tasks), where each async child runs as its own pgwf job. Support spawning from both job workers and task workers, with deterministic IDs and restart-friendly semantics. Child jobs can run whatever tasks they need internally.

---

- **Async job identity & journal**
  - When an async task is spawned, pre-record a journal entry with the intended async job ID before submission.
  - Deterministic ID scheme: `<parent-job-id>-<await-ordinal>`, where `await-ordinal` is the chapter ordinal at which we record the spawn.
  - Chapter payload at that ordinal includes the async invocation metadata (task type, inputs, any options), so replays can reconstruct the same async job ID and avoid duplicates.

- **Launch flow (spawn)**
  - New API on `JobContext` and `TaskContext`: `SpawnAsync(jobType string, data TaskData) (*Future, error)`.
  - Steps:
    1) Build async job ID as above; write a journal chapter at the current ordinal describing the spawn (id, task type, inputs).
    2) Submit a new pgwf child job with `JobType = jobType` (no prefix) and its own story initialized with the input data as chapter 0.
    3) Return a `Future` owned by the runner that wraps the async job ID and encapsulates await logic.
  - Spawns can occur inside job worker logic or inside task worker logic.

- **Await flow (hybrid)**
  - Future exposes `Await(ctx)`; the runner wires it to the same await path as `AwaitDuration` (see `await_duration_mini_spec.md`).
    - The runner blocks on the engine-provided await channel; the engine can send `wake` (child complete) or `recycle` at any time (with message types distinguishing duration vs. child completion).
    - On `recycle`, the future’s await calls `prematureCloseOut()` so the worker exits and the engine reschedules with the proper `wait_for`.
  - When called:
    - Check pgwf: if the child job is archived (complete), return its final output (read from its final Strata chapter) immediately.
    - If not archived: register with the engine to await child completion on the shared await channel. The engine may:
      - Keep the goroutine parked in-memory, translating notification jobs into `wake`.
      - Decide to recycle: persist resume info, reschedule the parent with `wait_for = [<childJobID>]`, and send `recycle` on the channel.
    - After any wake (from notification or reschedule), re-check pgwf; if still running (e.g., after a crash/restart before recycle), retarget/reschedule the notification job to the current worker’s notification capability and re-park with the await channel (engine can recycle again).
  - Completion signaling:
    - The async job, when finished, writes its final output chapter and completes its pgwf job.
    - The parent-created notification job (waiting on the child’s completion) fires to the parent engine’s notification capability; the engine converts that into a `wake` for any in-memory waiter, or the parent will wake on the rescheduled `wait_for`.

- **pgwf modeling**
  - Async child runs as its own pgwf job:
    - JobType: the provided `jobType` (no `async:` prefix), Capability: same `jobType` for step 1, then its inner tasks as usual.
    - Completion capability for await: `<childJobID>` (no extra prefix).
  - Parent reschedule (recycle case):
    - When recycling, reschedule the parent job with `wait_for = [<childJobID>]` and `next_need` pointing back to the parent job worker, and store the ordinal in payload to resume at the await point.
    - On wake-up, the future’s `Await` re-checks completion; if done, proceeds; otherwise can park again.

- **System payloads**
  - Existing payloads: `App`, `AppError`, `SystemError`.
  - Add a new system-owned payload (e.g., `AppChildJob`) to drive “run child job” operations; it carries the child job ID (and resume metadata) and is consumed by the runner/engine path that handles async awaits/notifications.

- **Notification channel (engine-local)**
  - Each engine creates and monitors a special pgwf capability, e.g., `NOTIFICATION-<workerId>`.
  - When spawning the async child, the parent also creates (in the same transaction, immediately after the child job) a lightweight notification pgwf job with `wait_for` on the child job’s completion and `NextNeed` pointed at the parent engine’s notification capability. The child job itself is unaware it is a child.
  - If the parent re-enters `Await` and finds the child still running (e.g., crash before recycle), it reschedules/retargets the existing notification job so its `NextNeed` matches the current worker’s notification capability.
  - Engines run a notification loop (similar to task runner) that consumes these jobs and routes them back to the awaiting runner via the single await channel shared with duration waits (in-memory map keyed by parent job ID/ordinal to a parked future). Messages carry a type to disambiguate duration vs. async-child wakes.
  - This enables prompt wakeups without polling Strata and avoids holding a pgwf lease while parked; the engine can still flip to `recycle` if conditions change.

- **Contexts & logging**
  - `JobContext` / `TaskContext` gain async APIs; logger stays injected so async operations log consistently.

- **Strata usage**
  - Parent story records:
    - Spawn chapter at ordinal N with async metadata.
    - (Optional) Await state chapter to document recycling/waiting.
  - Child story:
    - Chapter 0 = input data; final chapter = output data.
  - Await uses Strata to fetch the child’s final chapter, but uses pgwf archive status to determine completion.

- **Determinism & replay**
  - Because the async job ID is deterministic and journaled before submission, reruns of the same ordinal will reuse the same async job ID and not spawn duplicates.
  - On replay, if the async job already exists and is complete, the future’s `Await` returns immediately; if in-progress, parent can park/recycle.

- **Error handling**
  - If async job fails:
    - Await should surface the error (e.g., encode error in final chapter or via job status), and the parent can fail or branch accordingly.
  - If spawn fails after journaling:
    - Use the deterministic ID to retry submission on next run.

- **Testing hooks**
  - Provide an in-process implementation of async spawn/await for tests (possibly with an in-memory pgwf stub), and integration tests covering:
    1) Spawn + immediate await (completes inline).
    2) Spawn + recycle (parent awaits, recycles, wakes when child completes).
    3) Retry/replay: rerunning the parent doesn’t duplicate async child.

- **API sketches**
  - `type Future struct { JobID JobId; await func(context.Context) (TaskData, error) }` (runner owns creation; `await` may be method)
  - `SpawnAsync(ctx context.Context, taskType string, data TaskData) (*Future, error)`
  - `func (f *Future) Await(ctx context.Context) (TaskData, error)`; wired to the runner’s await channel path shared with duration waits.
  - Parent’s reschedule payload includes `awaitOrdinal`, `childJobID`, and resume info.

- **Library choice for Future**
  - Use `github.com/samber/lo`’s Promise as the underlying future/promise primitive. Wrap it in our `Future` type (thin adapter) so call sites stay simple and we can swap implementations later if needed. We’ll add context-aware helpers around `lo.Promise` for cancellation/timeouts.

- **Implementation steps**
  1) Add async APIs to contexts and engine.
  2) Implement deterministic child ID + journal write + pgwf submission.
  3) Add await logic with park/recycle and `wait_for` dependency.
  4) Define capabilities and status mapping for async completion; introduce the `AppChildJob` (system) payload.
  5) Add integration tests for spawn/await + recycling.
