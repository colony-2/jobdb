# pgjobdb: recoverable commit-if-waiting

## Ownership and rollout

The JobDB changes in this checkout implement SQLite recovery, change the shared
runtime-core completion sequence, and expose a Postgres chapter-publication
transaction hook. The pgjobdb repository has **not** been modified. Its owner
must implement and wire the backend described here before upgrading its JobDB
dependency.

`runtimecore.Config.TaskCompletions` is the new backend port. If it is absent,
`CompleteTaskIfWaiting` fails before making any changes; there is no fallback to
the unsafe append-then-complete behavior. Existing keyed configuration literals
still compile, so update production constructors and test constructors together.
Other runtime operations continue to use their existing ports.

The old `Scheduler.CompleteTaskWork` API remains for compatibility, but runtime
core no longer calls it for external completion. Do not use that method after
appending a chapter to implement the new port.

## Problem

Previously, core appended the output chapter and then called the scheduler's
`CompleteTaskWork`. The chapter store committed its own transaction before the
scheduler advanced the task. A crash, database error, or conflicting scheduler
mutation between them left a visible output chapter and a scheduler still
waiting for that output. Retrying failed because the ordinal already existed.

The fix is a durable claim followed by fenced publication and ordinary
rescheduling:

1. Validate the request and prepare its output in core.
2. Atomically claim the exact waiting slot, install an active lease, and change
   its route to job work. Commit this scheduler transaction.
3. Publish the output chapter and its client-payload update together, under the
   live lease. Commit this publication transaction.
4. Use the normal lease-authorized reschedule operation to release the claim and
   make the job immediately runnable. Do not apply the client-payload update again.

If the process stops after step 2, lease expiry routes work to a general job
handler. Without an output chapter, replay reconstructs the pending task (or runs
it locally if supported); the external caller must retry its result. With an
output chapter, replay consumes the outcome and continues. This is recoverable
execution, not a promise that a result lost before publication can be recreated.

| Durable state at failure | Recovery after lease expiry |
| --- | --- |
| No claim | Original task wait is unchanged |
| Claim, no output | Job handler rebuilds the task wait from history |
| Claim, output and client payload | Job handler consumes the output and continues |
| Rescheduled job | Normal job execution |

This does not repair inconsistent rows already created by older versions. Such
rows require a separate, guarded repair or job restart; do not blindly accept an
existing chapter as proof that an arbitrary retry is the same completion.

## Backend interface

The exact definitions are in
[`task_completion.go`](../pkg/jobdb/runtime/core/task_completion.go):

```go
type TaskCompletionStore interface {
    ClaimTask(context.Context, ClaimTaskRequest) (LeaseIdentity, error)
    PublishTaskOutput(context.Context, TaskOutputRequest) error
}
```

Implement this as an adapter in `pkg/pgjobdb/runtime/internal/runtimeadapter`.
It needs access to the scheduler database and JobDB's public Postgres chapter
store. Keep chapter encoding in JobDB core; the adapter receives an opaque
`EncodedChapter` and must not interpret or re-encode it.

Wire it into `pkg/pgjobdb/runtime/runtime.go` and integration-test constructors:

```go
runtimecore.Config{
    Scheduler: runtimeadapter.Scheduler{DB: db},
    Chapters: chapters,
    Schemas: schemas,
    TaskCompletions: runtimeadapter.TaskCompletions{
        DB: db,
        Chapters: chapters,
    },
}
```

The adapter type name above is illustrative; the Config field and interface are
implemented in JobDB. No imports from JobDB's private packages are needed.

## ClaimTask

Add a native scheduler operation for claiming a waiting task. Inside one database
transaction, lock the active job row and:

- Verify the tenant/job, no cancellation or terminal state, and no unexpired
  lease. Compare **all** fields of `ClaimTaskRequest.Task`: current route job type,
  task type, resume job type, input ordinal, output ordinal, and input hash. These
  are the complete observed slot, even when the public caller omitted optional
  guards. A competing claim must lose with `jobdb.ErrConflict` in the adapter.
- Require normal availability and satisfied dependencies. Claiming already
  eligible work needs no changes to `available_at` or `wait_for`. SQLite now
  rejects an unavailable task with `ErrConflict`; preserve that behavior in this
  adapter. Do not add a non-atomic eligibility check before acquiring the row lock.
- Generate a unique lease ID, record the supplied worker ID, and set a finite
  expiration using the requested duration and the database clock. Core currently
  requests 30 seconds. Return the committed lease identity, including expiration.
- Set `work_kind = 'JOB'`, set `route_job_type` to the stored `resume_job_type`,
  and null the task-specific columns. The `jobs_native_route_valid` constraint
  requires `task_type`, `resume_job_type`, `task_input_ordinal`,
  `task_output_ordinal`, and `task_input_hash` to be null for job work.
- Ensure an old alternate route cannot override recovery with task work or a
  different job route. SQLite clears alternate routing in this claim. It is
  harmless to retain an alternate route that resolves to the same job-only route,
  but do not leave a conflicting alternate route active.
- Do **not** apply `ClientPayloadUpdate`, release the lease, or write the chapter
  in the claim transaction.

The standard workflow runner sets the current and resume job types equal, so
this normally just clears the task type. However, the current runtime API and
`runtimetest/routes.go` explicitly support different resume job types. Preserve
that behavior rather than removing or ignoring `ResumeJobType` in this fix.

Return a lease only after the claim transaction commits. If the connection fails
with an unknown commit result, leave recovery to the durable route and lease
expiry; never publish without a confirmed lease identity.

## PublishTaskOutput

JobDB's public Postgres chapter store now provides:

```go
func (s *Store) AppendWithMutation(
    ctx context.Context,
    key runtimecore.ChapterLogKey,
    chapter runtimecore.EncodedChapter,
    mutation func(*sql.Tx) error,
) error
```

It prepares/stores artifacts first, then starts the chapter transaction and calls
`mutation(tx)` **before** inserting the chapter. If either the callback or chapter
write fails, both database mutations roll back. The callback must use the supplied
transaction and must not commit it, open another transaction, or mutate the
scheduler through the connection pool.

Use it in `PublishTaskOutput` as follows:

```go
return chapters.AppendWithMutation(ctx,
    runtimecore.ChapterLogKey{JobKey: req.Identity.JobKey},
    req.Chapter,
    func(tx *sql.Tx) error {
        // SELECT the active scheduler row FOR UPDATE using tx.
        // Check exact lease ID AND worker ID, unexpired lease, and no
        // cancellation/terminal state. Map failure to ErrExecutionLeaseLost.
        // Apply req.ClientPayloadUpdate, including its expected revision,
        // using tx and the existing native client-payload validation rules.
        // Keep the lease and route unchanged. Return without committing.
        return nil // Replace with the real guarded native mutation.
    })
```

Implement the guarded scheduler mutation as a native function or equivalent
transaction-bound code. Do not call a validation helper through `*sql.DB` and
then separately append: the row lock must remain held until the chapter
transaction commits, preventing lease takeover/cancellation from interleaving
between authorization and publication. Use the database clock after acquiring
the lock. Even when the client-payload update is nil, ownership must be locked
and checked.

Client-payload CAS failure must abort chapter publication. A duplicate ordinal
must roll back the payload update. The recovery worker must see either neither
change or both. External blob preparation may leave unreferenced objects on
failure; those are not published chapters.

Core performs the final `Scheduler.RescheduleLease` itself, using the claim
identity and job-only resume route, without a second payload update. Errors or
process death after publication leave the lease intact for replay recovery.
Long uploads may outlive a claim; reject an expired writer and recover through
lease expiry. If adding renewal, it must use the same identity and stop when
ownership is lost; never bypass the publication fence.

## Validation required in pgjobdb

Run the public runtime conformance suite and add integration tests covering:

1. Happy path, including artifacts, distinct current/resume job types, and a
   client-payload update applied exactly once.
2. Process loss after a committed claim but before output publication: expire the
   lease, reopen the runtime, and acquire it with a job selector, not a task
   selector. Replay must recreate the wait when the output is absent.
3. Process loss after output publication but before reschedule: after expiry, the
   job worker sees both the output and payload update and continues by replay.
4. Concurrent completion claims: only one gets ownership. Guard mismatch, active
   lease, cancellation, changed waiting slot, and unavailable work produce no
   output or payload update.
5. Pause an artifact upload until the lease expires and another worker takes
   ownership. The original writer must fail publication without a chapter or
   payload change.
6. Inject a failure during chapter insertion after the payload mutation. Verify
   rollback of the payload revision and value. Test client-payload CAS failure
   and duplicate output ordinal similarly.
7. Final reschedule failure: the output remains committed, and the recovery lease
   remains on the job-only route.
8. Missing `Config.TaskCompletions`: completion fails without any write.

The local SQLite tests in
[`task_completion_test.go`](../pkg/jobdb/runtime/sqlite/task_completion_test.go)
exercise the two failure boundaries across database reopen and validate fencing.
The Postgres chapter-store tests exercise the transaction hook against an actual
Postgres instance. These do not replace pgjobdb scheduler integration tests.

Ship the pgjobdb implementation together with its JobDB dependency update.
Existing schema-installation checks should also verify any new native functions.
A chapter-table format change is not required by the JobDB side of this fix.
