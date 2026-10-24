# Integrating task-scoped delegation in c2j

Use `workflow.TaskContext.CreateDelegation` when an operation needs a restricted
JobDB lease. JobDB automatically sets the credential's absolute deadline from
the current task and enclosing execution. c2j supplies permissions and,
optionally, a stricter deadline.

There is no need to read execution history, expose an effective-deadline getter,
or pass the owner lease into operation code. Creation with the remote runtime is
local attenuation: no HTTP request, history append, or server-side child record.
The existing token format, import path, and lease operations are unchanged.

## Where to integrate

The reviewed c2j revision is `165adbd2be0dbf6f8a7cc404849bd77001d2a051`.

1. Update c2j's JobDB dependency to include this API. Its production compiler,
   runjob, supplied-lease, story/live, and recipe-test paths already use the
   versioned execution engine. See the
   [runner migration review](REVIEW-Legacy-Runner-Removal.md).
2. In `pkg/worker/ops/taskworker.go`, retain the live `TaskContext` passed to
   `taskWorker.Run`. The existing `ops.TaskBasedJobTool{TaskContext: ctx}`
   preserves it. Forward `CreateDelegation` through c2j's `JobTool` abstraction
   if the operation launcher accesses JobDB through that interface.
3. Create the restricted lease at the operation handoff, after preparation and
   before starting the isolated process. Copying the whole `TaskContext` is
   supported. Reconstructing it with `NewTaskContext` or a struct literal loses
   the engine-owned delegation capability.
4. Export only the resulting child capability through a private pipe or inherited
   file descriptor. Configure the child service URL independently. The isolated
   operation process never receives the owner token.
5. Import the child in that process and use the ordinary lease methods for
   permitted operations. Return operation output to the parent task worker;
   the owner workflow runner persists execution results and completes the job.

This repository implements the JobDB API, not new c2j command-line flags.
At the reviewed c2j revision, compilation against current JobDB also requires
updating its validation context for the existing `ResolveSecretsFresh` method;
that dependency migration issue predates this API.

## Issuer example

The following helper runs inside the live task worker or its trusted launcher.
Imports are `context`, `io`, `github.com/colony-2/jobdb/pkg/jobdb`,
`github.com/colony-2/jobdb/pkg/jobdb/runtime/remote`, and
`github.com/colony-2/jobdb/pkg/workflow`.

```go
func exportForOperation(ctx context.Context, task workflow.TaskContext,
    handoff io.Writer) error {
    child, err := task.CreateDelegation(ctx, jobdb.CreateDelegationRequest{
        Permissions: jobdb.DelegationPermissions{
            Secrets: []string{"model_api_key"},
            Read: &jobdb.DelegationChapters{Ordinals: []int64{0}},
            Submit: &jobdb.DelegationSubmitPermission{
                JobTypes: jobdb.DelegationNames{Names: []string{"summarize"}},
                JobIDs: jobdb.DelegationNames{Any: true},
            },
        },
    })
    if err != nil { return err }
    capability, err := remote.ExportLease(child)
    if err != nil { return err }
    encoded, err := capability.Encode()
    if err != nil { return err }
    n, err := handoff.Write(encoded)
    if err == nil && n != len(encoded) { return io.ErrShortWrite }
    return err
}
```

Grant only the actions the particular operation requires; omit the others.
`JobIDs.Any` permits generated IDs. A finite ID allowlist requires explicit IDs.
Permissions remain signed restrictions and are subject to the parent's
allowlist. Calling the helper cannot widen them.

Do not put the encoded capability in logs, story records, or process arguments.
The general [delegated-lease guide](GUIDE-Lease-Delegation-Tokens.md) covers
receiver import, secret resolution, submission, chapter access, and the separate
delegation-only HTTP ingress for isolated workers.

## Deadline rules

The returned child's `Delegation().NotAfter` is the earliest applicable bound:

| Bound | Source |
|---|---|
| Task attempt | Engine-selected attempt deadline, including preparation time |
| Task total | Existing total budget across retries |
| Enclosing scopes and job | Earliest active execution budget |
| Execution caller | Deadline on the context running the workflow |
| Parent capability | Existing signed `NotAfter`, if restricted |
| Requested limit | Optional `CreateDelegationRequest.NotAfter` |

Absent bounds are ignored. If every bound is absent, `NotAfter` is absent and
the child lives only while the exact parent lease remains live. A requested
later deadline is capped by this task API; a requested earlier deadline is
preserved. Never compute `time.Now().Add(configuredTaskTimeout)`: that would
restart the budget.

The `ctx` passed to `CreateDelegation` controls cancellation of the issuance
call. Its deadline does not become the credential deadline. For example, a
one-second RPC timeout must not replace the task's five-minute execution budget.
Set `NotAfter` explicitly for an additional operation-specific limit.

Repeated creation during the same attempt does not move its deadline. A retry
or recovered execution receives the deadline selected by the existing engine
policy: permitted attempt resets still apply, and existing total/enclosing
budgets remain limits. External-task dispatch uses the preserved wait deadline.
Completed results replay without rerunning the operation or issuing credentials.

The renewable owner lease's current expiry snapshot is not a child hard
deadline. The owner keeps refreshing its own lease independently. The child
uses its token directly with JobDB, without contacting the owner process or
running a child refresh scheduler. `Renew` and `KeepAlive` remain compatible
validation operations; they extend neither `NotAfter` nor the parent lease.

At or after `NotAfter`, child operations and renewal fail. Parent expiry,
release, completion, cancellation, or replacement also invalidates the child.
Early task completion or cancellation alone does **not** revoke an already
issued token while its parent remains live; there is no individual-child
revocation. The closed task context cannot issue additional tokens. Continue
using the task context to cancel/terminate the operation process as appropriate.

## Runtime support and errors

The remote runtime implements optional `jobdb.LeaseDelegator`. A wrapper around
`jobdb.WorkflowRuntime` must explicitly forward `CreateDelegation`; embedding
only that interface hides optional capabilities. Built-in direct toy/sqlite
runtimes do not issue portable remote credentials. Use their remote server
adapter when testing the process boundary.

Manually constructed task contexts and unsupported runtimes return
`jobdb.ErrLeaseDelegationUnsupported`. Offline replay cannot issue credentials.
Canceled execution returns its existing cancellation cause. An elapsed hard
deadline or forbidden permission returns `jobdb.ErrLeasePermissionDenied`;
parent-lease loss at use returns `jobdb.ErrExecutionLeaseLost`. Local creation
cannot prove backend liveness, so import or use can still reject a locally
created token. Do not retry using the owner credential.

Outside workflow tasks, `remote.Runtime.CreateDelegation(ctx, parent, request)`
remains available. It knows only the parent capability, so it cannot apply task
budgets; it also rejects an explicitly later parent deadline instead of capping
it. Use the task method for task handoffs.

## c2j validation checklist

- Preparation delays consume the original task budget; repeated creation does
  not reset it. Shorter task, scope, job, caller, and explicit bounds all win.
- Local and externally dispatched tasks preserve the proper bounds through
  retries/recovery. Offline replay does not launch or mint.
- The child process receives only its restricted capability. Allowed secret,
  submission, and chapter operations succeed; disallowed ones fail.
- A live owner can renew independently while the child continues working.
  Child expiry and parent loss stop access, without an owner-token fallback.
- A manually constructed test task context returns unsupported; tests needing
  real tokens run a registered worker against a remote test server.
- Child operation output returns to the owning task. Do not use a restricted
  child as a full workflow dispatcher or grant chapter writes using a guessed
  execution ordinal. Existing delegated writes support task-outcome chapters,
  not arbitrary versioned execution-history records.
