# Implementing job goals in pgjobdb

Implement goals in the Postgres scheduler transaction domain alongside jobs,
leases, and dependency waits. JobDB core forwards the public goal APIs and
prepares workflow history; it does not provide an independent fallback goal
store. Updating the Go dependency alone is insufficient.

The authoritative behavioral contract is [SPEC-Job-Goals.md](SPEC-Job-Goals.md).
The [c2j guide](GUIDE-C2J-JOB-GOALS.md) covers callers. This document supplements
the [runtime adapter guide](GUIDE-PGJOBDB-RUNTIME-IMPLEMENTATION.md) with the new
storage and transaction obligations. It describes work to perform in pgjobdb;
no pgjobdb checkout was changed by the JobDB implementation.

## Public port changes

Update to a JobDB revision containing
[`GoalRuntime`](../pkg/jobdb/goals.go) and
[`runtimecore.GoalScheduler`](../pkg/jobdb/runtime/core/goals.go).
`runtimecore.Scheduler` now embeds `GoalScheduler`, and `WorkflowRuntime` embeds
`GoalRuntime`. Implementations and wrappers must satisfy the expanded interfaces.

```go
type GoalScheduler interface {
    jobdb.GoalRuntime
    PrepareJob(context.Context, runtimecore.JobPreparation) (runtimecore.PreparedJob, error)
}
```

`GoalRuntime` adds `GetGoal`, `AddGoalCandidate`, `FailGoal`, and
`GetDependencyWait`. The runtime core delegates these calls to the scheduler.
Use the exported validation functions in `pkg/jobdb` where appropriate:
`ValidateDependencies`, `ValidateDependencyWait`, `ValidateDependencyReschedule`,
and request `Validate` methods. Persistent-state validation remains the adapter's
responsibility.

Carry these new fields through the existing adapter mappings:

| Core/public surface | Required mapping |
| --- | --- |
| `CreateJobRequest` | `GoalID`, `ShareGoal`, `Dependencies` |
| `StoredJob` | Primary `GoalID`, sorted derived `OtherGoalIDs` |
| Scheduler `ListJobsRequest` | `GoalIDs` filter over both membership paths |
| `LeaseSnapshot` | `GoalID`, including renewal, import, and inspection paths |
| `RescheduleMutation` | `DependencyWait` |
| Public handles and summaries | Preserve the fields produced by core, including schedule-run responses |

`JobPreparation` exposes `Key`, `RequestID`, `PriorJobID`, `ShareGoal`,
`Descriptor`, and `Dependencies`. `PreparedJob` returns `Key`, `GoalID`, and
`Published`. These exported aliases can be used in adapter signatures without
importing private packages. Do not import `pkg/jobdb/internal/goalstate`; its
implementation is not an external adapter API.

## REST changes

Paths are relative to `/v1/tenants/{tenantId}`:

| Method | Path | Successful result |
| --- | --- | --- |
| `GET` | `/goals/{goalId}` | `200 GoalInfo` |
| `PUT` | `/goals/{goalId}/candidates/{jobId}` | `200 GoalCandidateInfo`; no request body |
| `POST` | `/goals/{goalId}/fail` | `200 GoalInfo`; body `{"message":"human reason"}` |
| `GET` | `/jobs/{jobId}/dependency-waits/{waitId}` | `200 DependencyWaitInfo` |

Existing submit/restart, job inspection/query, polling/lease, schedule-summary,
and lease-reschedule endpoints gain fields defined by the
[OpenAPI contract](../openapi/jobdb-runtime.yaml). In particular:

- Submit accepts `job.dependencies` and returns `goalId`.
- Restart accepts `job.shareGoal`, `job.dependencies`, and top-level `requestId`.
  The destination remains the path ID for `PUT /jobs/{jobId}/restart`.
- Inspection returns `goalId` and `otherGoalIds`; query accepts `goalIds`.
- Reschedule accepts `dependencyWait` and still returns `204`.

Use the shared JobDB remote server/codec with the upgraded runtime where the
deployment already uses it. Do not maintain a second goal protocol. If pgjobdb
has its own transport, match the same schemas, validation, and error mapping.
Tenant lifecycle authority is required for candidate attachment and failure;
a worker lease or delegated execution capability alone is insufficient.

Return the exported typed errors so the shared server can distinguish invalid
requests (`400`), missing goals/jobs/waits (`404`), and goal, idempotency, wait,
or cycle conflicts (`409`). A failed goal or failed wait is an existing resource:
its read endpoint returns `200` with the business outcome.

## Storage layout and indexes

The following is a logical layout, not a migration to copy verbatim. Use the
existing pgjobdb ID, timestamp, and completion encodings.

| Relation | Essential contents and indexes |
| --- | --- |
| `goals` | PK `(tenant_id, goal_id)`; state, revision, successful job ID, failure snapshot, creation/update/close times |
| `jobs` | Non-null immutable `goal_id`; index `(tenant_id, goal_id, job_id)` |
| `goal_candidates` | Extra associations only; PK `(tenant_id, goal_id, job_id)`; `added_at`; reverse index `(tenant_id, job_id, goal_id)` |
| Dependency waits | PK `(tenant_id, consumer_job_id, wait_id)`; immutable descriptor/deadline, group state, ordered dependency outcomes |
| Pending dependency targets | Target lookup index `(tenant_id, target_kind, target_id)` pointing to pending wait entries; consumer lookup for cancellation |
| Job preparations | Reserved destination/goal IDs, semantic descriptor, source/sharing data, publication state; unique destination and tenant-scoped nonempty request ID |

Index pending deadlines for timeout processing. Store full immutable outcomes
as JSONB or normalized records with equivalent fidelity. Preserve ordered
bindings, `failedDependencyIndex`, and structured failure fields. Subscription
rows can be removed after settlement; result rows must remain available for
recovery and replay.

Use tenant-scoped foreign keys where compatible with the existing schema.
Every published job references a real goal in the same tenant. A missing goal
row is an integrity problem or not-found result, never implicit `OPEN` state.

Primary membership lives only in `jobs.goal_id`. Do not insert a candidate row
for it. Extra rows cannot duplicate that job's primary membership. A job's
`otherGoalIds` is a sorted, duplicate-free derived view of extra rows, including
closed goals for provenance.

All membership-dependent operations use both representations:

```text
Candidates(G) = jobs whose goal_id = G
               UNION jobs linked to G through goal_candidates

Goals(J) = J.goal_id UNION goal_candidates.goal_id for J
```

Apply `goalIds` query filtering before pagination, deduplicate jobs matching
multiple goals, and intersect other job filters normally. Use indexed `EXISTS`
or a union of IDs instead of a join that multiplies result rows. Batch-load extra
associations for the returned page. Failure snapshots and cycle checks include
hidden candidates even though normal listings omit hidden jobs.

## Coordination strategy

All mutations must have a consistent serialization order across publication,
attachment, completion, explicit failure, wait registration/settlement, and
cancellation. A conditional `OPEN` update alone does not protect a concurrent
candidate insertion, failure snapshot, or cycle check.

A straightforward first implementation can take a tenant-scoped transaction
advisory lock before reading or locking scheduler state for these mutations.
Every participating path must take the same lock, including deletion,
reconciliation, and timeout paths when they perform these transitions. Take
that lock before job/goal row locks to avoid inverted lock ordering. Keep chapter
and artifact preparation outside these short scheduler transactions.

This is an implementation option, not a required global lock: it serializes
related writes within a tenant and trades throughput for a simple correctness
argument. Ordinary reads and claims that do not change dependency topology or
settle outcomes can use the existing coordination. A claim that also settles a
deadline or finalizes a job must participate in the transition protocol.

If measured contention warrants finer locking, use consistent multi-row lock
ordering plus protection against graph write skew, such as serializable
transactions with full-transaction retries. Concurrent changes to different
rows can jointly introduce a cycle; locking only the individual target goals
does not prevent that. Include completion/attachment and closure/publication
races in the proof and tests before relaxing coordination.

Use `SKIP LOCKED` for suitable work claiming, not to skip a goal resolution
that must be processed. Notify workers only after commit. Notifications improve
latency; durable wait state determines eligibility if a notification is lost.

## Prepare and publish jobs

`PrepareJob` reserves stable identities before core prepares or clones history.
Persist the semantic `Descriptor` supplied by core; it excludes incidental
request timing and includes the content identity of relevant inputs/artifacts.
Do not reconstruct it from transport JSON or compare only the source job ID.

1. Resolve an existing explicit destination or tenant-scoped restart request
   ID. An equivalent request returns the original `PreparedJob`; a changed
   descriptor returns an existing-job mismatch.
2. For a new operation, reserve destination and goal IDs durably. Fresh jobs
   and independent restarts get a new independent goal ID. Shared restarts use
   the source job's immutable primary goal.
3. Return `Published: true` for an already published equivalent operation.
   Core can then return the original handle without re-reading restart source
   history. The destination may have progressed or the goal may have closed.
4. Otherwise core prepares history/artifacts and calls `CreateJob`. In its
   scheduler transaction, create the fresh goal if needed, publish the job,
   register/evaluate submission dependencies, and mark preparation published.
   For sharing, recheck that the existing goal is open at publication.

The first goal row and first job become visible atomically. An unpublished
reservation is not a public job or goal. Recovery reuses reserved identities
after interrupted preparation. Coordinate concurrent equivalent callers so
they cannot publish conflicting histories or mappings. Any cleanup must
preserve idempotency for retries of operations that may have published.

Generated-ID shared restarts require a request ID; an explicit destination ID
is the alternative. Distinct operations may publish concurrent candidates.
Sharing inherits only primary membership, never extra associations. Run normal
restart-boundary validation and conservative dependency-cycle validation.

## Commit completion and resolve goals

Core persists successful result/history/artifacts before scheduler completion.
Keep that ordering: a visible winning job must have a readable result. Then,
in `CompleteLease` or the equivalent terminalization transaction:

1. Validate the live lease and publish the job's actual terminal outcome.
2. Close any pending waits owned by this terminal consumer and settle affected
   exact-job dependencies according to `complete` versus `success`.
3. For a successful job, obtain its primary and extra goal IDs. For each open
   goal, atomically set `SUCCEEDED`, `successful_job_id`, revision `2`, and
   terminal timestamps. Skip goals already closed.
4. Settle the affected pending goal waits in this transaction, preserving
   bindings already assigned. Check each pending group's deadline first.
5. Commit, then notify newly eligible workers.

Only one committed resolution can win each goal. Both racing jobs retain their
real successful job outcomes. One job can resolve multiple goals atomically;
a closed goal must not reject completion or prevent resolution of other goals.
Failed, cancelled, or exhausted jobs never fail their goals. Existing retry
mechanisms do not automatically become new jobs or candidates.

Every path that finalizes a job needs equivalent handling, including recovery,
administrative cancellation, and deletion. A transient attempt failure awaiting
retry is not a terminal job failure. Preserve existing lease validation and
completion recovery rules.

## Add candidates and explicitly fail goals

For `AddGoalCandidate`, validate tenant-scoped goal/job existence and check both
membership representations. An existing pair returns success even if the goal
is closed. Return primary membership without inserting an extra row.

For a new pair, require an open goal, validate the prospective graph, and insert
the extra row. If the job is already successful, resolve the goal and its waits
in that transaction. A running or unsuccessful job leaves the goal open.
Coordinate with job completion so an attachment racing completion cannot miss
success. Evaluate a successful attachment's prospective settled graph; it need
not introduce a pending goal-to-job edge.

For `FailGoal`, validate the nonblank UTF-8 message of at most 16 KiB, then:

1. Under the same coordination, return the existing snapshot for an already
   failed goal with the identical message. A different message or succeeded
   goal conflicts.
2. Read all primary and extra candidates, including hidden jobs. Partition by
   authoritative terminal status, sort IDs, and freeze `terminalJobIds` and
   `nonTerminalJobIds` with the message, `goalId`, `code: goal_failed`, and time.
3. Set `FAILED`, revision `2`, and terminal timestamps; settle pending waiters
   with the same failure snapshot, subject to deadlines; commit and notify.

Never cancel candidate jobs here. Later completion cannot rewrite the snapshot,
and a candidate may still satisfy another goal. There is no candidate removal,
goal reopening, goal cancellation, or cancel-candidates option in v1.

## Durable dependency waits and scheduling

A wait descriptor contains a stable wait ID, ordered dependencies, and optional
absolute deadline. Each target is exactly one job or goal. Jobs accept
`complete` or `success`; goals accept only `success`. Reject duplicate targets.
Submission dependencies use reserved ID `submission` and cannot coexist with
legacy prerequisites. Ordinary callers cannot register that reserved ID.

`RescheduleLease` must validate/register/evaluate the wait and release the lease
in one transaction. Structured waits cannot coexist in that request with legacy
job waits, `waitUntil`, or alternate-route timing. Reusing a wait ID with the
same descriptor preserves its result and deadline; changing dependencies, order,
or deadline conflicts. Idempotency does not waive live lease validation.

All entries are required. Bind each success independently to its concrete job.
Fail immediately when a dependency fails; if several failures are present in
one evaluation, choose the lowest declaration index. On group closure, end
remaining pending subscriptions while preserving earlier results. Closing
subscriptions never cancels referenced jobs or goals.

Pending groups block their resume route. `SATISFIED`, `FAILED`, and `TIMED_OUT`
groups allow the consumer to resume and observe the recorded outcome, subject
to normal scheduling/lease rules. Cancelled or terminal consumers must not
become runnable just because their waits closed. Replacement scheduling must
close abandoned pending subscriptions without deleting retained outcomes.

Check absolute deadlines before assigning new pending outcomes on every
settlement path. A deadline already passed yields `TIMED_OUT`; an outcome
settled earlier stays unchanged. Timeout processing must make the resume route
eligible without requiring a candidate event. Readiness, targeted acquisition,
and ordinary work polling must agree on this behavior.

`GetDependencyWait` is a read-only coherent snapshot. If a pending deadline has
passed, project its timeout consistently without using the read to mutate
scheduler state. Pending goal diagnostics distinguish active candidates
(`waiting_for_execution`) from no non-terminal candidates (`awaiting_candidate`).
Neither a nil error from `DependencyWaitInfo.Err()` nor a successful HTTP read
alone proves the group is satisfied; inspect its state.

For cycle validation, use edges from jobs to unresolved dependency targets and
from open goals to non-terminal candidates. Reject any prospective directed
cycle on registration, attachment, or shared publication. This is deliberately
conservative even when an alternative candidate could make progress. Serialize
concurrent topology mutations so separate checks cannot both admit a cycle.

## History, failure fidelity, and retention

The shared workflow SDK records full dependency outcomes: execution joins use
`join_result`, task joins use `dependency_binding`, and submission admission
records its result. Persist these through the existing chapter APIs without
dropping unfamiliar fields. Replay uses recorded outcomes, not a fresh goal
lookup. A crash after scheduler settlement but before history recording must
recover the exact same winner/failure from the durable wait.

Retained history on a consumer restart keeps its old bindings; new dependencies
are checked at the continuation boundary. Do not assign primary goal identity
from a cloned chapter: the authoritative scheduler job owns that identity.

Preserve `GoalFailedError` and its full failure snapshot through the shared error
codecs. Failure must not become an unstructured message or a retryable transport
error. An exact-job success dependency has its own `job_dependency_failed`
failure; it is not explicit goal failure.

Soft deletion retains goals, candidate links, wait outcomes, and referenced
history/artifacts. Deleting an unfinished candidate can terminate that job but
does not fail its goals. Hard purge must retain data needed by waits/replay or
reject deletion while it is referenced.

## Migration and rollout

Backfill one independently generated goal per existing job. Persist the mapping
so migration retries do not allocate another goal. Do not infer shared goals
from historical restart ancestry and do not create primary candidate rows.

- A known terminal successful job gets a `SUCCEEDED` goal with itself as winner.
- An unfinished or terminal unsuccessful job gets an `OPEN` goal.
- An unknown historical outcome requires reconciliation rather than a guessed
  success or silent normalization.

Use a transactional migration or a resumable migration with durable mappings.
Coordinate old writers so none can publish a job without its required goal.
Add and validate tenant-scoped constraints and indexes before enabling the new
surface. Preserve old prerequisites, persisted waits, and retry semantics.

Deploy the compatible backend before enabling c2j goal features. New clients
using goals must reject missing goal fields or unsupported structured waits;
`shareGoal` cannot silently degrade to an independent restart. Coordinate old
strict response decoders and workflow-history readers with the SDK upgrade.

## Performance expectations

The common single-job lifecycle adds one goal insert and one terminal update,
with no `goal_candidates` row. A shared restart adds no goal or extra candidate
row. Ordinary progress, unsuccessful completion, and attachment of a running
or unsuccessful job do not update goal revision or timestamps. Locks do not
require rewriting the goal row.

Use indexed membership lookups and pending-target subscriptions; do not scan
every goal or job on completion. Avoid candidate counts, current-attempt
pointers, and membership generations on the goal row. Revision versions goal
state only, not candidate lists. Explicit failure necessarily reads all its
candidates once to freeze the required snapshot.

Measure fan-out when one job satisfies many goals or one goal has many waiters.
V1 requires affected waits to settle in the outcome transaction; moving that
work to an asynchronous queue would change the ordering contract and needs a
separate design. Benchmark tenant-lock contention before replacing the simpler
coordination strategy.

## Acceptance tests

Run the existing public runtime conformance harness, then add goal-specific
Postgres integration tests. The existing harness alone is not a substitute for
the following cases. Use separate connections/runtime instances and controlled
races, not just a single process mutex:

| Area | Required cases |
| --- | --- |
| Publication | Eager atomic goal/job creation; no primary extra row; crash before publication; stable identities on retry |
| Restart | Fresh versus shared goals; running source; no extra inheritance; lost response; retry after destination progress or goal closure; mismatched request ID |
| Membership | Running/successful/unsuccessful attachment; primary-pair no-op; duplicate after closure; new pair on closed goal rejected |
| Resolution | Two competing successes; one job resolves multiple goals; one closed goal does not block the others |
| Races | Attach versus complete; attach versus fail; shared publication versus closure; wait registration versus resolution; concurrent graph changes |
| Failure | Exact message and sorted immutable candidate lists, including hidden jobs; same-message retry; candidates keep running |
| Waits | Mixed targets; declaration-order failures; immutable bindings; timeout without producer event; deadline wins on late settlement; consumer cancellation |
| Recovery | Restart database/runtime after settlement; crash before history append; replay preserves the same winner and typed failure |
| Queries | Primary and extra filtering before pagination, deduplication, sorted association views, tenant isolation |
| Migration | Successful/failed/running legacy jobs; unknown outcomes; interrupted migration; no inferred ancestry sharing |
| Compatibility | Legacy waits/retries, lease loss, deletion, schedule occurrences, remote API schemas and error codes |
| Write cost | No goal rewrite on failed candidate/progress; no primary candidate row; indexed lookup and batched enrichment |

Reference scenarios in this repository:

- [Runtime and HTTP goal tests](../pkg/jobdb/runtime/remote/goals_test.go)
- [SQLite migration, durability, and competing-connection tests](../pkg/jobdb/runtime/sqlite/goals_test.go)
- [Core scheduler-port tests](../pkg/jobdb/runtime/core/goals_test.go)
- [Workflow recovery and replay tests](../pkg/workflow/execution_goals_test.go)

Port the scenarios through public interfaces; do not import private SQLite or
goalstate implementation packages into pgjobdb.
