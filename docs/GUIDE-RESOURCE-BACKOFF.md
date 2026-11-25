# Durable resource backoff

A task that cannot obtain a temporary resource can return a typed signal after
stopping and joining its own activity:

```go
return nil, workflow.EAGAINWait(ctx,
    workflow.WithInitialWait(2*time.Second),
    workflow.WithBackoffGrowth(2),
    workflow.WithMaxWait(30*time.Second),
    workflow.WithBackoffJitter(.2),
    workflow.WithCrashBudget(100))
```

All arguments are optional. Defaults are 2s, growth 2, maximum 30s, jitter .2,
and a symbolic backend-default crash allowance. SQLite/toy resolve that default
to five when a policy is first accepted. Timing values are whole milliseconds;
initial must be at least 1ms and no greater than maximum; maximum is at most 30s.
Growth must be finite and at least one, jitter finite in [0,.2], and an explicit
allowance a positive int64. Duplicate or invalid options reject through execution
infrastructure handling. Explicit zero jitter is valid; zero allowance is not.

The engine derives the invocation path, ancestor attempts and recipe attempt.
Its bounded reason is `resource_temporarily_unavailable`. It intercepts the
signal before task output serialization, artifacts, retries, catches and failure
events. Cancellation, timeouts and infrastructure faults retain precedence.
New task admission freezes, active engine contexts are cancelled and callbacks
must quiesce before release. A task that ignores cancellation prevents successful
resource acknowledgment. This does not undo user side effects.

Completed history stays committed. A versioned `resource_checkpoint` preserves
unfinished input, selected task implementation and original attempt/total timers.
Only the scheduler-selected checkpoint authorizes resource recovery; a prepared
but unselected checkpoint does not acknowledge a wait. Incompatible execution
readers reject the distinct kind before entering the body. Upgrade workers on
feature-enabled routes before activation.

## Backend adoption

Required `ExecutionContext`, `ExecutionLease`, `ConsumerRuntime` and ordinary
`RescheduleExecutionRequest` method sets are unchanged. Native/decorated leases
must deliberately forward `ResourceBackoffLease`. Trusted transports optionally
implement `ResourceBackoffTransport` and `ResourceBackoffReceiptReader`.
Absent ports/capabilities fail with `ErrResourceBackoffUnsupported`; ordinary
reschedule is never a fallback. Manual/offline contexts reject the helper.

Core backend owners implement the separate `runtimecore.ResourceBackoffScheduler`
with context-aware installed capability checking, atomic mutation and receipt-only
lookup. `ResourceBackoffMutation` carries the authenticated lease identity and
complete typed request. The backend owns continuation/history validation and all
transactional checks; `ValidateResourceCheckpoint` and
`ValidateResourceTaskCheckpoint` support append-only chapter validation.

A fresh mutation must validate the current live lease, project authority,
cancellation/finality, route/task generation, checkpoint/hash, payload revision
and incompatible waits. It then binds the policy, increments the independent
invocation/attempt/reason sequence, samples one delay, applies payload CAS,
assigns crash allowance, selects continuation, writes the absolute gate and
immutable authenticated receipt, and releases the lease atomically. Rollback
must preserve every field. Preserve exact int64s and clone caller-owned data.

For count n, nominal delay is `min(initial*growth^(n-1), maximum)`. One uniform
sample chooses a factor in `[1-jitter,1+jitter]`, truncating to nanoseconds and
capping the final result. `ResourceBackoffDelay` and `ResourceBackoffWake` provide
shared saturating arithmetic. There is no resource retry limit. Policy and
resolved allowance bind per invocation/attempt/reason; older sequences remain
retained. A default selector remains symbolic in the request, distinct from an
explicit allowance, even after first resolution.

Authenticate the complete mutation binding and check exact retained receipts
before resolving current lease/default state. An exact duplicate returns the
original receipt without another sample, increment, CAS, refill or mutation of
a newer lease. Conflicting and stale bodies reject. Retain the original request
ID/body through transport acknowledgment retries. Rejected or unresolved
acknowledgment is infrastructure failure, never a task failure or successful
suspension. Retain sequence and receipt state until job cleanup.

`ExecutionState` optionally carries `CrashBudgetRemaining`, `ResourceBackoff`
and `ResourceResume`; missing credit means unknown, zero is known. Forward
acquired, renewed and supplied snapshots intentionally. Detail/list projections
must use existing bulk reads rather than per-row detail lookups. Capability
protocol 1 certifies the complete options/countdown contract. Durability 1
additionally requires real installed persistence/schema and all eligibility gates;
interface presence alone cannot certify it. Toy advertises durability 0.

Remaining crash credit is independent of the resource count. Fresh generations,
ordinary successful reschedules and guarded clearing assign the current default.
Resource waits assign their bound allowance. Successful expired execution
reclaims alone debit once; the final 1→0 lease stays ACTIVE until expiry. Reads,
renewals, expiry, child submission and existing-job append never refill a parent.
Internal append expiry preserves its existing exemption. A separate resource
not-before gate applies to every poll/target/task/completion/append claim and
availability projection, with earlier cancellation/deadline handling retained.
The persisted resource gate must remain equal to the receipt's WakeAt even when
an original attempt or total deadline is earlier. Independently retain a job-only
deadline wake, including after a crashed deadline handler. It may admit timeout
processing with zero remaining credit, without refilling or making the balance
negative; live zero-credit leases retain ACTIVE precedence. Task-body claims,
external successful completion, and append retain the full resource gate and
ordinary credit checks. Timeout recovery replays original timers and input and
must not reenter the expired task body. Discovery/detail/list status and effective
routes must agree with this job-only admission using bulk stored projections.
Ordinary join alternative wakes and completion races retain their semantics.

## HTTP authority

Both `/v1/tenants/{tenantId}` and `/v1/projects/{projectId}` lease families expose
POST `jobs/{jobId}/leases/{leaseId}/resource-backoff` with a receipt response.
The new action rejects duplicate keys, unknown fields, missing normalized policy
fields, null options and unsupported versions. Requests/receipts/snapshots retain
exact signed 64-bit values.

Only this action supports receipt replay after credential expiry. Signature,
trust audience, tenant/job/worker/lease and original owner/delegation authority
remain authenticated; only expiry is bypassed, and only an exact retained receipt
may be returned. Fresh, forged, conflicting, cross-scope or uncertain requests
cannot mutate or disclose a receipt. Verification-key retirement fails closed.
Durability 1 over HTTP requires a stable `LeaseTokenSigningKey` and audience across
restart; ephemeral signing downgrades advertised durability to zero.

Delegates need explicit `DelegationPermissions.ResourceBackoff` (`resource.backoff`
on the wire), which grants neither ordinary reschedule nor chapter reads.
Resource-authorized snapshots forward credit/capability and payload revision.
Resume/task coordinates are exposed only with unrestricted chapter-read permission;
narrow delegates retain hidden coordinates. Attenuation cannot expand authority.

## SQLite upgrade

Fresh stores use format 15. Constructors refuse formats 11–14 without mutation.
Stop every incompatible writer, back up the database, then use the supported
exclusive offline path:

```sh
jobdb sqlite check-migrate --db /absolute/path/jobdb.db
jobdb sqlite migrate --db /absolute/path/jobdb.db
```

Format-13 conversion atomically assigns `max(5-oldCount,0)` to the explicitly
named countdown column before advancing the marker to 15. Negative/noninteger counts
and unsupported/malformed schemas reject. It never debits expiry, renews a lease,
or refills balances. Live zero-credit leases and all existing scheduler/history
state remain. Repeating migration/reopen cannot reconvert custom balances.
The legacy column remains migration provenance and is no longer a policy input.
Formats 11/12 first use the existing retained-goal conversion in the same
transaction. Mixed old scheduler writers and downgrade are unsupported; old
remote clients retain ordinary operations against an upgraded server.

## Public conformance

External owners call `runtimetest.RunResourceBackoffConformance(t, harness)` with
the dedicated additive `ResourceBackoffHarness`, leaving the old harness intact.
Every fixture supplies runtime, injected backend clock/RNG and an actual append
claim/release hook. Durable mode additionally requires actual close/reopen,
transaction faults at validated/sequence/job/receipt, and an in-transaction
barrier. Missing fixtures and zero selected checks fail. Memory mode must report
and validate durability zero; it is not restart acceptance.

`PrepareResourceBackoffCheckpoint` prepares a genuine checkpoint through public
chapter APIs for backend checks. The shared suite covers policy/count isolation,
atomic validation/faults, immutable duplicates, competing transitions and reclaim,
independent eligibility gates, countdown boundaries, snapshots and retention.
Workflow, offline migration, HTTP authority and old-reader compatibility have
additional JobDB tests. `scripts/testdata/resource-backoff-consumer` contains a
public consumer and frozen required-interface/old-reader controls.

PostgreSQL owns its physical schema, database time, configurable defaults,
threshold-preserving migration and P01–P04 acceptance. Adopt the exact published
JobDB module normally, wire the optional scheduler port and fixtures, and run
those checks against real storage. JobDB source acceptance makes no PostgreSQL,
worker-upgrade or deployment claim.
