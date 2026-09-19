# Client payload migration guide

Routing changes: [typed route migration](MIGRATION-TYPED-ROUTES.md).

API contract: [client payload design](DESIGN-JOBDB-CLIENT-PAYLOAD.md).

This release requires fresh databases and updated servers, workers, clients,
and native pgjobdb bindings. There is no data conversion or API compatibility.

## Deployment

1. Update applications and generated REST clients to the new API.
2. Stop old servers, workers, submitters, and native database writers.
3. Delete/recreate the dedicated PostgreSQL database, or use a fresh SQLite file
   (remove old WAL/SHM files only after closing connections). Reset dedicated
   artifact storage or use a new namespace. Toy starts empty on restart.
4. Start matching JobDB/pgjobdb versions, re-register schemas, recreate schedules,
   submit new jobs, and start updated workers.

Old jobs, archives, task handles, lease tokens, and restart history do not carry
over. Installers reject old database formats; they do not delete or upgrade them.

## Application changes

| Old pattern | New pattern |
| --- | --- |
| Combined `Payload()` / `LeasePayload` | Read `ClientPayload` plus revision; read framework state from typed fields. |
| Run policy inside a reschedule payload | Set immutable job policy on submission only. |
| `task_wait` embedded in payload | Supply typed task coordinates and explicit next/resume route fields. |
| Copying payload into each wait/task transition | Omit the client update; persistence is automatic. |
| Replacing or clearing combined payload | Send a client-only patch or reset. |
| Task result completion cannot update client state | `CompleteTaskIfWaiting` accepts the same client update as the other operations. |
| Ordinary chapter/task-result write | Writes chapters/artifacts only; use explicit reschedule/yield to change client state during execution. |
| Old native SQL signatures and REST payload schema | Update callers; old signatures/fields are removed. |

Only client payload has patch/reset semantics. Next need, waits, and other
mutable scheduling fields retain their operation-specific assignment rules.
Each reschedule supplies its full route/waits; omitted alternate routing clears
it. Native unheld administrative operations preserve client state; client changes
require a lease or an authorized complete-task-if-waiting transition.
Job/task inputs, results, and metadata remain separate; do not rename unrelated
payload fields mechanically.

## Client updates

Submit job, reschedule job, complete task if waiting, and complete job all accept
an optional `ClientPayloadUpdate`:

- Omit it to preserve existing state (or start absent on submission).
- `mode: "patch"` with `value` applies JSON Merge Patch.
- `mode: "reset"` with `value` replaces the entire value.
- `mode: "reset"` without `value` removes the stored payload.

Example on a reschedule:

```json
{
  "nextRoute": {"jobType": "collect"},
  "clientPayloadUpdate": {
    "mode": "patch",
    "expectedRevision": "7",
    "value": {"cursor": "page-2"}
  }
}
```

Patch merges object members recursively; null members delete properties and
arrays replace as a whole. Reset with a complete object to store null-valued
properties. Reset with `value: null` stores JSON null; omitting the value removes
the payload. See the patch definition in the design for examples.

Include the observed revision for an existing job; omit it on submission.
On conflicts or uncertain responses, reconcile against current state rather
than retrying an old update with a new revision. Job lease operations retain
lease checks; task-if-waiting completion retains its exact waiting-task guards
and may update the payload without acquiring a lease.

Payload changes and the operation's scheduling/terminal transition commit
together. Updated workflow helpers preserve client state across tasks and pass
explicit patches/resets into that same operation. `CompleteTaskIfWaiting` applies
its update at the resume/yield transition. Its chapter write, like any ordinary
chapter completion, never updates scheduler rows or flushes client state.
Read-only replay never writes.
Child/restart jobs start absent unless initialized explicitly; schedule targets
provide the initial value for each occurrence.

## Go callers

- Read `lease.ClientPayload()`, `lease.ClientPayloadRevision()`, and
  `lease.ExecutionState()`. `JobInfo` and `JobSummary` expose equivalent fields.
- Set `SubmitJob.ClientPayloadUpdate` (also supported by restart jobs and schedule
  targets). Existing-job request types expose the same field.
- Use `JobContext.Yield(ctx, RescheduleExecutionRequest{...})` or
  `TaskContext.Yield` to publish a change during execution. Supply `NextRoute` and,
  for a task route, `TaskWait`. A successful yield stops the invocation without
  writing a chapter; gate it on persisted state so resumption does not repeat it.
- External task handles offer `FinishWithClientPayload(ctx, data, update)`;
  `Finish` preserves client state. Ordinary in-process task results never publish
  updates. Read-only replay returns no live client snapshot and rejects yield.
- Native pgjobdb uses `SubmitJobRequest.ClientPayloadUpdate`,
  `RescheduleRequest.ClientPayloadUpdate`, `CompleteTaskWorkRequest.ClientPayloadUpdate`,
  and `Completion.ClientPayloadUpdate`. Native leases and job details expose
  `ClientPayload` and `ClientPayloadRevision`. The stored JSON is in
  `pgjobdb.job_client_state`, separate from immutable `job_facts`.

Payloads have a 64 KiB input/result limit and a maximum of 128 container levels.
Duplicate object names, malformed Unicode, and U+0000 are rejected. Use raw JSON
when handling numbers that cannot be represented exactly by your language's
ordinary numeric type. Storage format is now 3 for SQLite and PostgreSQL.

Release pgjobdb against the matching JobDB revision. To test adjacent checkouts
before release, create a Go workspace including both modules (`go work init` /
`go work use`); no absolute-path module replacement is needed in either release.
