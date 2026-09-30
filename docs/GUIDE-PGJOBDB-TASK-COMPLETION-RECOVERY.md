# pgjobdb: recoverable commit-if-waiting

## Ownership and rollout

The JobDB changes in this checkout implement SQLite recovery and change the
shared runtime-core completion sequence. The pgjobdb repository has **not** been
modified. Its owner must implement `runtimecore.Scheduler.ClaimTask` and update
its JobDB dependency together. The former `CompleteTaskWork` scheduler port and
`CompleteTaskWorkMutation` are removed.

Chapter storage and scheduling may reside in entirely separate systems. **No
transaction, database handle, or commit callback may span these stores.** The
previous guide's `TaskCompletionStore`, `Config.TaskCompletions`,
`PublishTaskOutput`, and `AppendWithMutation` design has been removed. The normal
`ChapterLog.Append` is sufficient.

`CompleteTaskIfWaitingRequest.ClientPayloadUpdate` and the corresponding HTTP
field are removed. `TaskHandle.FinishWithClientPayload` is also removed; use
`Finish(ctx, data)`. External completion preserves the existing client payload
and revision. Client-state updates remain supported by submission, explicit
leased reschedule/yield, and job completion. Do not forward a client update from
external completion into the final reschedule.

## c2j compatibility audit

Before removing the field, we inspected `colony-2/c2j` main at commit
[`e1334817a353d4868a97fa5452fe8fe3aee2fc13`](https://github.com/colony-2/c2j/commit/e1334817a353d4868a97fa5452fe8fe3aee2fc13)
on 2026-09-30. There are no code calls to `FinishWithClientPayload`, direct
`CompleteTaskIfWaiting` requests, or HTTP commit-if-waiting requests carrying a
client-payload update in that checkout.

External completion uses plain `Finish(ctx, data)` in:

- [`pkg/input/runtime.go`](https://github.com/colony-2/c2j/blob/e1334817a353d4868a97fa5452fe8fe3aee2fc13/pkg/input/runtime.go#L132)
- [`pkg/worker/workflow/control.go`](https://github.com/colony-2/c2j/blob/e1334817a353d4868a97fa5452fe8fe3aee2fc13/pkg/worker/workflow/control.go#L156)

Its external-completion integration tests also use `Finish`. Its actual
`ClientPayloadUpdate` uses are reschedule/yield and restart initialization, which
remain supported. Its migration documents mention the removed API but do not
constitute runtime usage. This audit covers the named main revision, not all
branches or deployed binaries.

## Recovery sequence

Previously core appended an output chapter and then advanced the waiting task
in the scheduler. A crash between these independent commits left the task
waiting even though its output ordinal already existed; retrying then conflicted.

Core now performs these independent operations:

1. Validate the request, prepare output, and check that its ordinal is appendable.
2. Call `Scheduler.ClaimTask` to atomically compare the full waiting slot, assign
   a finite lease, and install the job-only resume route. Commit the claim.
3. Validate the claim with the existing `Scheduler.ValidateLease`, then append
   through the ordinary `ChapterLog.Append`. The chapter store atomically
   enforces ordinal uniqueness and sequential insertion and commits independently.
4. Call the existing lease-authorized `Scheduler.RescheduleLease` with the
   job-only resume route to release the lease for immediate job execution.

| Last durable operation before failure | Recovery after lease expiry |
| --- | --- |
| No claim | Original external-task wait remains |
| Claim, no chapter | Job handler replays history and reconstructs the task wait, or executes the task if locally supported |
| Claim and chapter | Job handler consumes the recorded outcome and continues |
| Reschedule | Normal job execution |

The external caller must retry a result lost before chapter insertion. A retry
after the output is recorded may conflict; recovery does not depend on that
retry succeeding. A scheduler error after the append cannot roll back the
chapter and must not attempt to delete it.

This does not repair inconsistent rows created by older versions. Those need a
separate guarded repair or job restart; do not accept an arbitrary retry merely
because an output chapter exists.

## ClaimTask implementation

The public request is in
[`task_completion.go`](../pkg/jobdb/runtime/core/task_completion.go), and the
method is part of `runtimecore.Scheduler`:

```go
ClaimTask(context.Context, runtimecore.ClaimTaskRequest) (runtimecore.LeaseIdentity, error)
```

Add a native scheduler operation and expose it through
`pkg/pgjobdb/runtime/internal/runtimeadapter`. Within one **scheduler-only**
transaction, lock the active job row and:

- Verify no cancellation, terminal state, or unexpired lease. Compare every
  field of `req.Task`: route job type, task type, resume job type, input ordinal,
  output ordinal, and input hash. This is the complete observed slot, even if
  the public caller omitted optional guards. Map mismatches and competing claims
  to `jobdb.ErrConflict`.
- Require normal availability and satisfied dependencies. They need no reset for
  already eligible work. SQLite rejects unavailable work with `ErrConflict`.
- Generate a unique lease ID, record the supplied worker ID, and set a finite
  expiration using the database clock and requested duration (currently 30
  seconds). Return the identity only after the claim commits.
- Set `work_kind = 'JOB'` and `route_job_type` to the stored `resume_job_type`.
  Null `task_type`, `resume_job_type`, `task_input_ordinal`,
  `task_output_ordinal`, and `task_input_hash` as required by the existing
  `jobs_native_route_valid` constraint.
- Ensure an old alternate route cannot redirect recovery to a task or a different
  job. SQLite clears alternate routing; retaining the same job-only route is
  harmless.
- Preserve client payload and its revision. Do not write a chapter or release
  the lease during this transaction.

For ordinary workflow-generated waits, current and resume job types are equal:
this amounts to clearing the task type. The runtime API still supports distinct
resume job types, covered by `runtimetest/routes.go`; preserve that behavior.

If a claim's commit result is uncertain, do not append without a confirmed lease
identity. Leave recovery to the committed route, if any, and lease expiry.

## Lease and append semantics

The lease check and chapter append are intentionally separate. Check ownership
before attempting insertion, but do not claim that the check transaction fences
an append in another system. A lease may expire or be cancelled while a chapter
request is in flight. Its insert may complete later; competing inserts are
arbitrated by the chapter store's atomic ordinal constraint.

The final scheduler mutation must still reject a stale lease. Regardless of that
mutation's outcome, the recovery worker uses the durable chapter history. Keep
normal lease renewal/cancellation behavior where applicable; neither renewal nor
an in-memory lock creates a transaction across stores. Do not add a chapter
callback that acquires scheduler locks or writes scheduler/client state.

Use JobDB's public chapter store unchanged or supply any adapter satisfying
`ChapterLog`. Artifacts must be durable before their chapter is made visible,
according to the chapter adapter's existing contract. There is no new chapter
format or cross-store transaction requirement.

## Required validation in pgjobdb

Run public runtime conformance and add integration cases for:

1. Happy path with artifacts and distinct current/resume job types. Assert client
   payload and revision remain unchanged.
2. Stop after a committed claim, before insertion; reopen, expire the lease, and
   verify job workers can acquire it while task workers cannot. Replay must
   recreate the missing wait.
3. Stop after insertion, before reschedule; verify job replay consumes the output.
4. Concurrent claims, wrong guards, live leases, cancellation, changed slots, and
   unavailable tasks: failed claims must not append a chapter.
5. Append failure: the durable claim still routes expired work to a job worker.
6. Lease loss or scheduler failure after a successful append: the chapter remains
   committed, and no stale lease can reschedule the job.
7. Duplicate ordinal/competing appends: exactly one chapter persists at that ordinal.
8. A chapter adapter independent of the scheduler database, with no transaction
   hooks. Do not implement a custom publication method just for this API.
9. HTTP requests containing the removed `clientPayloadUpdate` field are rejected.

SQLite restart tests and core tests using independent scheduler/chapter ports
are in `pkg/jobdb/runtime/sqlite/task_completion_test.go` and
`pkg/jobdb/runtime/core/runtime_task_test.go`. They do not replace pgjobdb's own
scheduler integration tests. Update production and test adapters and any schema
installation checks for new native functions together with the dependency bump.
