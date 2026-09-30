# pgjobdb changes required for supplied-lease execution

## Request

Update pgjobdb to support JobDB's supplied-lease execution contract. The dispatcher
claims a lease, hands its bearer capability to another process, and that process
executes the same lease without acquisition or ownership reassignment.

JobDB now implements the workflow entry point, remote export/import, authoritative
renewal transport, and the shared heartbeat lifecycle. This document requests the
remaining pgjobdb work; no pgjobdb source changes are included in the JobDB change.

## Existing support and dependency update

pgjobdb already exposes `ValidateLease` and `KeepAliveLease`. Its scheduler adapter
implements JobDB core's `ValidateLease` and `KeepAliveLease` ports, and its runtime
embeds `*runtimecore.Runtime`.

After updating the JobDB dependency, that embedded runtime supplies:

```go
RenewExecutionLeaseByID(
    ctx context.Context,
    key jobdb.JobKey,
    leaseID, workerID string,
    duration time.Duration,
) (jobdb.RenewableExecutionLease, error)
```

This calls the existing scheduler `KeepAliveLease` operation and returns a fresh
execution lease from its authoritative snapshot. No new scheduler interface method
or separate pgjobdb remote protocol implementation is required. The backend method
is a trusted transport entry point; it does not itself validate signed tokens.
JobDB's remote server validates those before calling it.

JobDB's optional `RenewableExecutionLease` contract provides `Renew(ctx)`,
`LeaseWorkerID()`, and `LeaseExpiry()`. The runner synchronously renews before
application execution and then manages periodic renewal. It does not use the legacy
core background `KeepAlive` implementation for renewable leases.

## Required correctness fix: preserve the acquired effective route

In the reviewed pgjobdb SQL, `get_native_work`:

1. Computes `effective_job_type`, `effective_work_kind`, and `effective_task_type`,
   including an alternate route when its delay is due.
2. Matches the worker against those effective fields.
3. Updates the lease ID, owner, expiry, and expiration counters.
4. Returns the effective route, without persisting it as the lease's execution route.

`validate_native_lease` instead returns the job row's original `route_job_type`,
`work_kind`, and `task_type`. `KeepAliveLease` renews and then calls `ValidateLease`,
so it returns that original route too.

For example, a task wait can have an eligible job-only alternate route. Acquisition
returns job work, but validation or renewal of that same lease returns task work.
A receiver importing the capability must not execute a different route from the
one the dispatcher acquired.

Please make acquisition, validation, and renewal agree on the effective execution
route and task coordinates for each lease. Persisting the acquired effective route
or recording a separate lease snapshot are possible solutions; choose the approach
that preserves pgjobdb's intended scheduling semantics. If promoting an alternate
route into the active row, maintain the work-kind/task-field constraints and define
whether the alternate route is consumed. Do not attempt to recompute the acquired
route from the current time after acquisition: renewal changes lease expiry and
can change the alternate-route eligibility calculation.

Relevant source locations in the reviewed checkout:

- `pkg/pgjobdb/pgjobdb.sql`: `get_native_work`, `validate_native_lease`,
  `renew_native_lease`.
- `pkg/pgjobdb/leases.go`: `ValidateLease`, `KeepAliveLease`, and lease decoding.
- `pkg/pgjobdb/runtime/internal/runtimeadapter/leases.go`: snapshot conversion and
  lease-error translation.

## Renewal and error requirements

Verify that renewal:

- Preserves tenant/job/lease/worker identity and never acquires or revives a lease.
- Rejects cancellation, completion, expiry, revocation, and superseding ownership.
- Returns the actual backend expiry, effective route, task coordinates, run policy,
  schema identity, client payload, and payload revision.
- Honors context cancellation and deadlines.
- Maps invalid authority to `jobdb.ErrExecutionLeaseLost` while preserving database
  and transport failures as distinct errors.

The current separate renewal and validation calls may remain separate. If renewal
commits but validation fails, JobDB stops execution; it does not assume the renewal
was rolled back. There is no requirement for a chapter transaction here.

The adapter currently defaults a validation-only snapshot's duration to one minute.
Do not treat that default as proof of remaining validity. The new core renewal path
passes an explicit duration; the returned expiry must reflect the database's actual
result and supported duration rounding.

## Acceptance tests for the pgjobdb owner

Run native PostgreSQL tests plus JobDB remote/workflow tests against the pgjobdb
runtime. Include:

1. Acquire a normal job route, normal task route, job-only alternate route, and task
   alternate route. For each, compare acquisition, validation, and repeated renewal:
   same lease identity, effective route, task coordinates, and owner.
2. Exercise alternate eligibility across the time boundary and after renewal;
   execution metadata must not revert to the primary route.
3. Export a claimed lease and import it through a separate remote client. Deny and
   instrument receiver acquisition calls; import and execution must make zero.
4. Execute beyond the initial token lifetime, including chapter/artifact writes and
   child submission after token refresh. Complete under the original lease ID.
5. Cancel, expire, supersede, or complete a lease before import/run. No application
   code should run. Lose it during execution and verify cancellation, typed loss,
   and no application-failure completion by that invocation.
6. Inject a database failure or deadline during renewal, including after the update
   commits but before validation returns. Execution must stop with an observable
   renewal error, without acquiring replacement work.
7. Race renewal with successful completion/rescheduling. Intentional release must
   not become a spurious lease-loss result; heartbeat goroutines must exit.
8. Run the existing scheduler/runtime conformance suite and the relevant Go race
   tests after the dependency update.

The JobDB tests `supplied_lease_integration_test.go`, `lease_session_test.go`, and
`runtime/core/runtime_lease_renewal_test.go` demonstrate the client and runner
contracts. The SQLite-backed remote tests do not replace PostgreSQL coverage of
its SQL route and lease behavior.

## Boundaries

Chapter insertion remains independent of scheduler operations, including when the
stores live on different systems. Preserve atomic chapter append and ordinal
arbitration. Validate authority before protected writes, but do not claim atomic
revocation-versus-append fencing across independent stores or add a shared
chapter/scheduler transaction.

Bearer handoff does not make the receiver the exclusive holder. The dispatcher
must stop executing and renewing before handing responsibility to the receiver.
No atomic ownership-transfer or exactly-once external-side-effect mechanism is
requested. No client-payload update is added to commit-if-waiting.
