# Clearing a job's crash concern

A job enters `CRASH_CONCERN` after repeated expired-lease reclaims. Workers stop
acquiring it until an operator clears the consecutive expiration count. After
fixing the worker problem, clear the job through the CLI, REST API, or Go runtime.
The operation is supported by the toy, SQLite, and PostgreSQL runtimes, including
access through the remote runtime.

## CLI

```sh
jobdb jobs clear-crash-concern -t c2 JOB_ID \
  -u http://127.0.0.1:9047 \
  --reason 'worker issue fixed' \
  --worker-id operator
```

`reset-crash-concern` is an alias for `clear-crash-concern`. Tenant and job ID
are required. The URL defaults to `http://127.0.0.1:9047`; the worker ID defaults
to `jobdb-cli`. Set `JOBDB_BEARER_TOKEN` when the deployment requires bearer
authentication. `--json` (`-j`) prints:

```json
{"tenantId":"c2","jobId":"JOB_ID","cleared":true}
```

## REST API

```http
POST /v1/tenants/c2/jobs/JOB_ID/clear-crash-concern
Content-Type: application/json
Authorization: Bearer OPERATOR_TOKEN

{"reason":"worker issue fixed","workerId":"operator"}
```

The JSON body is required; both fields are optional, so `{}` is valid. `reason`
and `workerId` provide backend tracing context. When tracing is enabled, PostgreSQL
records a `crash_concern_cleared` event when the counter changes; toy and SQLite do not
persist these audit fields. Authorization uses the deployment's operator access
controls, as cancellation does. Delegated lease credentials cannot call this
endpoint.

| HTTP status | Meaning |
| --- | --- |
| `204` | Cleared, or already clear and inactive. |
| `400` | Invalid request. |
| `403` | Caller denied, including delegated lease credentials. |
| `404` | Job does not exist in the specified tenant. |
| `409` | Job has a live lease, or is completed, cancelled, or hidden. |

The wire contract is in [jobdb-runtime.yaml](../openapi/jobdb-runtime.yaml).

## Go API

Call the method on any `jobdb.WorkflowRuntime`, including a remote client:

```go
err := runtime.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{
    JobKey:   jobdb.JobKey{TenantId: "c2", JobId: "JOB_ID"},
    WorkerID: "operator",
    Reason:   "worker issue fixed",
})
```

Errors support `errors.Is`: `jobdb.ErrInvalidCrashConcernRequest`,
`jobdb.ErrJobNotFound`, and `jobdb.ErrConflict`. PostgreSQL also exposes
`pgjobdb.ClearCrashConcern(ctx, db, req)`, which accepts a database or borrowed
transaction and uses the same request and errors.

## Recovery behavior

Clearing atomically sets only the consecutive expired-lease count to zero. It
preserves the job ID, chapters, payload, route, dependencies, availability,
execution deadlines, and lifetime expiration statistics. Workers acquire the
existing job normally and resume using its recorded history. Task work and
alternate routes remain subject to their normal eligibility rules.

The operation does not acquire a lease, execute the job immediately, revive an
old lease, or revive a completed, cancelled, or hidden job. Other scheduling
constraints still apply. Retrying the clear is harmless while the job remains
inactive; if a worker has already acquired it, the retry returns a conflict.

The expired lease record is retained. When a worker next reclaims that lease,
the consecutive count advances from zero to one. Continued crashes can therefore
block the job again. A successful reschedule resets the streak normally.
