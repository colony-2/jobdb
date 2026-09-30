# Executing a supplied lease

A dispatcher may acquire work and pass the same lease to another execution
process. `workflow.GetJobForRunWithLease` runs that lease through the usual replay,
task, artifact, retry, and rescheduling machinery. It never calls acquisition APIs,
including when work waits, changes route, or loses its lease.

## Remote handoff

The dispatcher deliberately exports credentials:

```go
capability, err := remote.ExportLease(lease)
if err != nil { return err }
encoded, err := capability.Encode()
if err != nil { return err }
// Send encoded using a protected file or input channel.
```

The receiver creates its remote runtime using a separately configured server URL
and decodes/imports the capability:

```go
capability, err := remote.DecodeLeaseCapability(encoded)
if err != nil { return err }
lease, err := runtime.ImportLease(ctx, capability)
if err != nil { return err }

runnable, err := workflow.GetJobForRunWithLease(ctx, runtime, lease,
    workflow.GetJobForRunRequest{
        JobKey: lease.Job().JobKey,
        JobWorker: jobWorker,
        TaskWorkers: taskWorkers,
        BeforeRun: func(ctx context.Context, lease jobdb.ExecutionLease) error {
            // Check execution-environment admission here. This context is
            // cancelled if renewal fails. Return lease.Reschedule(ctx, ...)
            // to reject admission and finish without invoking the job worker.
            return nil
        },
    })
if err != nil { return err }
outcome, err := runnable.Run(listener)
```

The import call validates the signed capability and renews the exact backend lease.
`Run` validates and renews again before admission or application code, so delaying
execution after import does not bypass validation. It checks the requested job key
and the workers' supported routes. Leave `WorkerID` unset to preserve the original
owner; supplying a different worker ID is rejected. `LeaseDuration` in the request
is an acquisition option and is not used to replace the supplied lease's renewal
policy. The existing `LeaseAcquired` outcome field means the invocation held a
lease; it does not imply the receiver called an acquisition API.

The version-1 export format is JSON with `version`, `tenantId`, `jobId`, `leaseId`,
and `leaseToken`. It deliberately contains no editable execution snapshot or
connection URL. Unknown fields and unsupported versions are rejected. Execution
metadata is loaded from the authoritative backend during renewal.

`LeaseCapability` and remote lease diagnostic formatting redact credentials.
Only `Encode` deliberately emits the transport credential. Do not log its output
or put it in command-line arguments. Tokens remain valid only against the same
logical service/signing keys; configure `NewServerWithOptions` with a shared signing
key for replicas or server restarts that must continue accepting existing tokens.

The dispatcher must stop its execution and renewal before handing responsibility
to the receiver. Exporting does not revoke the dispatcher's copy or transfer
exclusive ownership. External side effects remain the application's responsibility.

## Embedded leases and renewal

Already-held leases from SQLite, toy, and the shared core runtime can be passed
directly to `GetJobForRunWithLease`; no serialization is required. Remote export is
supported only for token-bearing leases. Unsupported supplied leases return
`jobdb.ErrLeaseRenewalUnsupported`, without acquisition.

The optional `jobdb.RenewableExecutionLease` interface leaves `WorkflowRuntime` and
`ExecutionLease` unchanged. `Renew(ctx)` is a synchronous authoritative operation
that returns a new lease snapshot, preserves identity/owner, and starts no goroutine.
Use the returned value for subsequent operations. `LeaseExpiry()` reports a safe
validity deadline; remote leases include the earlier signed token deadline.

The shared runner uses this interface for both ordinary and supplied execution.
It renews initially, then schedules renewal after one third of the remaining safe
lifetime. Renewal calls are bounded by the previous safe deadline and a ten-second
maximum. Any renewal failure stops execution immediately; there is no retry or
replacement claim. Completion/rescheduling and renewal are serialized, and all
runner exit paths stop and join the heartbeat. The admission hook may perform
lease-scoped operations but must not manage its own heartbeat.

Wrappers must forward renewal, owner, expiry, schema metadata, and the *current*
`LeaseToken()` capability where available. Embedding only `ExecutionLease` does not
forward those optional methods. The runner's internal wrapper preserves metadata
and uses the renewed capability for writes and child submissions.

Older custom leases remain supported by ordinary claim-and-run through their legacy
`KeepAlive` lifecycle. They must implement `RenewableExecutionLease` to opt into
supplied execution and the new heartbeat guarantees. Old remote servers lacking an
authoritative snapshot in the renewal response return unsupported; they must be
upgraded before using the new remote runner lifecycle.

## Errors, cancellation, and persistence

Use `errors.As` with a `*jobdb.LeaseRenewalError` target to recognize runner renewal failures
and `errors.Is(err, jobdb.ErrExecutionLeaseLost)` for invalid/lost authority. Remote
transport failures are available through `*remote.LeaseTransportError`; they are
not reported as proof of lost authority. Import may directly return these underlying
errors. A terminal job read cannot hide this invocation's renewal or lease-loss error.

Caller cancellation and heartbeat failure cancel the execution context and stop
further runner dispatch. The runner returns without completing the job as an
application failure. Application goroutines must cooperate with cancellation; Go
cannot forcibly terminate arbitrary user code. The admission hook receives the
execution context, and runner-directed waits observe it.

Backend scheduler mutations validate current ownership. Chapter insertion remains
an independent atomic append with ordinal arbitration. A live-authority check before
an append is not a transaction coupling scheduler revocation to a separate chapter
store. This feature introduces no such transaction or exactly-once guarantee.

For PostgreSQL adapter requirements, see
[the pgjobdb owner request](GUIDE-PGJOBDB-SUPPLIED-LEASE.md).
