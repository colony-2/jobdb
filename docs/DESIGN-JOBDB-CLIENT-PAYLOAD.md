# Client payload design

Implemented contract: four operations, submit-only immutable settings, explicit
scheduling fields, and mutable client state.
Existing databases and APIs are replaced; see the [user migration guide](MIGRATION-CLIENT-PAYLOAD.md).

## Data model

| Data | Ownership and lifetime |
| --- | --- |
| Immutable job settings | Existing immutable fields, including job run policy, remain set only on submission. No update or patch API. |
| Scheduling state | `NextNeed`, waits, alternate routing, and pending-task coordinates are explicit typed fields. Each operation sets its applicable fields using the current operation semantics. |
| Client payload | One client-owned JSON value per job, automatically retained across tasks, retries, waits, and archival. Operations may explicitly patch or reset it. |

Remove the combined `Payload`/`LeasePayload` API and its interpretation of
`run_policy` and `task_wait`. Read framework settings and scheduling state from
typed fields. Those property names have no special meaning inside client JSON.
Job input, task input/results, and metadata keep their existing separate roles.

## The four operations

All four accept the same optional `ClientPayloadUpdate`.

| Operation | Immutable settings | Scheduling and completion | Client payload |
| --- | --- | --- | --- |
| Submit job | Set once. | Set initial route, availability, and prerequisites. | Apply update to an absent value; omission leaves it absent. |
| Reschedule job | Unchanged. | Set next route, waits, alternate route, and task coordinates; release lease even if `NextNeed` is unchanged. | Preserve, patch, or reset atomically with rescheduling. |
| Complete task if waiting | Unchanged. | Match the waiting task, record its result, clear pending-task state, and set the resume route. | Preserve, patch, or reset as part of the accepted task completion. |
| Complete job | Unchanged. | Record the final outcome and archive the job. | Preserve, patch, or reset before the final state becomes visible. |

Chapter completion is not a scheduler operation. Ordinary job/task chapter
writes, including in-process task results, must not touch scheduler rows and
accept no client-payload update. During execution, updates belong to explicit
reschedule/yield. `CompleteTaskIfWaiting` includes a resume/yield transition;
its payload update belongs to that transition, not its result-chapter write.
Submission initializes state and complete-job finalization archives it separately
from writing the corresponding chapters.

Expose `TaskWait` as a typed structure containing input/output ordinals, input
hash, and resume route. Keep fields such as `NextNeed`, `WaitUntil`,
`WaitForJobIDs`, `AlternateNeed`, and `AlternateAfter` directly on the requests
where applicable. `CompleteTaskIfWaiting.ResumeNeed` is its explicit resume-route
field. These fields use ordinary operation-specific assignment/default rules,
not a general patch/reset wrapper. There is no mutable run-policy setter.

## Client payload updates

```go
type ClientPayloadUpdate struct {
    Mode             string          // "patch" or "reset"
    Value            json.RawMessage
    ExpectedRevision *int64          // required for updates to an existing job
}
```

An omitted update preserves the authoritative stored value without reading or
copying it. A present update must name a valid mode:

| Update | Meaning |
| --- | --- |
| `patch` with a value | Apply JSON Merge Patch to current client state. |
| `reset` with a value | Replace the complete value exactly, including nested nulls. |
| `reset` without a value | Remove the stored client payload. |

Use [JSON Merge Patch (RFC 7396)](https://www.rfc-editor.org/rfc/rfc7396) for patching:
objects merge recursively, null object members remove properties, and arrays or
other non-object values replace their target. An absent target behaves like JSON
null. To store a null-valued object property, use reset with the full value.
Root JSON null remains a stored value; only reset without a value removes it.

For example, patching `{"cursor":"a","counts":{"ok":3,"bad":1}}` with
`{"cursor":"b","counts":{"bad":null}}` preserves `counts.ok` and removes
`counts.bad`. Resetting with `{"cursor":null}` replaces everything with that
exact JSON value.

Return `ClientPayload` and `ClientPayloadRevision` on leases, job retrieval, and
listings, including archived jobs. Use raw JSON with explicit presence: omitted
means absent, while `null` means a stored JSON null. Revisions start at 0 for
absence or 1 for an initialized value and increment on each accepted update;
preserve does not increment them. Submission omits `ExpectedRevision`.
REST revisions are decimal strings to avoid numeric rounding.

Keep JSON numbers lossless through patching, transport, and storage. Use raw
messages/number-preserving decoding, never float64 or metadata codecs. The
limits are 64 KiB per input/result and 128 nesting levels; reject invalid JSON,
duplicate object names, invalid Unicode (including U+0000, unsupported by
PostgreSQL text), and limit violations explicitly.

## Atomicity and workflow behavior

Apply the payload change to the stored value inside the operation's scheduler
transaction, together with route changes, pending-task cleanup, and lease release
or archival. Validate the expected revision there; conflicts leave the operation
unchanged. No helper should publish a payload change and then perform a separate
handoff.

Reschedule and complete-job writes require the current live lease and reject
cancellation. Complete-task-if-waiting uses its own existing authorization:
match the exact waiting route, ordinals, and input hash; require no live lease
and no cancellation. That authorized operation may update client payload too.
Recheck these conditions under the job lock, not only before entering storage.
Existing chapter/artifact preparation remains separate; this change guarantees
atomic scheduler acceptance, not a new cross-store transaction protocol.

Workflow helpers pass explicit updates into scheduler transitions. Chapter
helpers only persist chapters/artifacts; they do not write client state or flush
buffered updates. A task needing to persist a payload change during execution
must explicitly reschedule/yield. Transitions without an update retain the
payload. Clearing pending-task state never clears client state. Replay must not
apply live patches.

After a lost response, read the current state before reconciling. Retry only with
the original revision and valid ownership/waiting-task identity; do not attach a
new revision to a stale update. Submission retries compare the original initial
value, not the mutable current value, and never reset an existing job's payload.

## Backend changes

**JobDB:** update public requests/read types, core scheduler mutations, workflow
helpers, remote codecs, OpenAPI, and test doubles. Remove combined-payload
projection/parsing and mutable-policy update machinery. SQLite and toy have
independent runtime implementations and need the same changes explicitly.

**pgjobdb:** retain immutable settings in `job_facts`. Add a job-owned
`job_client_state` table keyed by tenant/job ID, holding nullable `client_payload
JSON`, its revision, and an immutable initial-value digest for submission retries.
Create its row with the job and retain it across active/archive transitions;
remove it on permanent job deletion.

Extend the native submit, reschedule, complete-task-work, and complete-job
functions and Go bindings with the client update. Implement one shared native
patch/reset helper, called under the job lock after authorization and revision
checks. Apply the same patch rules as JobDB without converting numbers to floats
or JSONB. The runtime adapter forwards the update; it must not do a separate
read/patch/write cycle outside the operation's transaction.

Keep client JSON separate from existing `to_jsonb(...)` get/list projections:
return it as a separate JSON column alongside job details and revision, in the
same query. Do the same for lease snapshots. Remove old `lease_payload` fields
and visibility flags. Rewrite the fresh schema and SQL signatures directly.

**SQLite/toy:** store client value, revision, and initial-value digest separately
from framework state. SQLite can use columns on its job row, which archives in
place; toy uses fields on its record. Mutate under the existing transaction or
mutex. Keep only immutable settings and typed scheduling data in internal codecs.

`SubmitJob` includes the initial-value digest in its first chapter for submission
reconciliation. Restart retries compare the digest stored with the new job; the
cloned chapter prefix continues to describe the source history. Child/restart jobs
start absent unless explicitly initialized. Schedule targets specify initial
client state for each new occurrence, not inheritance from the previous run.

## Delivery and checks

Ship matching JobDB and pgjobdb releases. Initialize fresh databases, reject old
formats, and remove old APIs; no conversion code, aliases, or compatibility
handlers. Update the [migration guide](MIGRATION-CLIENT-PAYLOAD.md) with final API names.

Test the four operations across core/pgjobdb, SQLite, toy, remote, and workflow
helpers: automatic persistence, nested patches, reset/null/absence, exact numbers,
immutable-field rejection, explicit routing, task-completion guards, cancellation,
revision conflicts, unchanged-route handoffs, submission retries, and archive
reads. Assert ordinary chapter completion performs no scheduler writes. Verify
fresh installation and rejection of old storage/API shapes.
