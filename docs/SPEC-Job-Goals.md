# Job goals: immutable membership and sequential recovery

Every published job has exactly one immutable primary goal selected at creation.
An ordinary submission or independent restart creates a new goal. A selected
submission or shared-goal restart joins an existing open goal at publication.
There is no standalone goal creation or post-creation attachment operation.
`OtherGoalIDs` remains deprecated and empty; it supplies no secondary membership
or filtering behavior. Goal IDs and job IDs are distinct opaque identities.

## Admission and coordination

Fresh publication atomically checks all retained primary members. Any nonterminal
member blocks a new attempt, including hidden, delayed, blocked, task-routed,
unleased, expired-lease and cancellation-requested jobs. A request to cancel is
not terminal cancellation. An ordinary failed or cancelled attempt leaves its
goal open for recovery. Success and explicit permanent failure close admission.
Inherited historical overlaps may remain stored; all must terminate before a
new attempt can publish. Compatible source alone does not establish global drain.

Preparation reserves identities and the semantic descriptor without taking
admission capacity or publishing a goal. Publication rechecks current authority,
ownership, openness, cycles and all active members in the scheduler transaction.
Direct submission, supported lease routes, shared restart and HTTP must enforce
the same rule. An unsupported path must reject rather than submit independently.

Equivalent accepted requests return their original complete captured handles
before fresh admission or closure checks, after current authority checks. Changed
descriptors, revoked authority or unavailable receipts reject without a replacement.
Retain receipts through hiding and cleanup; exact accepted replay neither unhides
jobs nor changes identity. See [selection and replay](GUIDE-PRIMARY-GOAL-SELECTION.md).

A top-level coordinating job can delegate independent child jobs under their own
goals, evaluate their outputs, and publish its own result. Sequential replacements
can then preserve the coordinator's goal. This permits parallel exploration
without competing attempts for the same goal or later attachment. Selection and
permanent failure retain their existing authority; no lifecycle permission is
implied by task submission or a worker lease.

## Closure and protected failure

The first committed success closes its primary goal with the actual producer ID.
Ordinary unsuccessful outcomes never close the goal. Later completion preserves
the real job outcome but cannot replace an existing goal closure.

`FailGoal` requires an existing goal and current lifecycle authority. Its message
must be valid UTF-8, nonblank and at most 16 KiB; exact bytes are retained. One
transaction publishes an archived protected failed job, immutable primary
membership, first closure and durable waiter settlement. Its frozen snapshot
contains sorted pre-publication terminal and nonterminal member IDs, including
hidden members and excluding the new failure record. It adds no execution
chapters, routes, tasks, lease events or artifacts.

Equal-message authorized replay returns the original failure job, message, time
and snapshot, including after hiding or reopen. A different message or a succeeded
goal conflicts. Failure leaves existing work uncancelled. A later success cannot
replace that failure. Failure publication, closure and waits roll back together
on error; a lost committed response remains replayable.

`GoalInfo.FailureJobID` identifies the protected job. The older snapshot's `JobID`
keeps its historical meaning. `GOAL_FAILURE` and `MIGRATED_GOAL_FAILURE` are protected
record kinds; ordinary metadata, job type and errors cannot forge them. Detail,
list and run readers expose exact ownership and failure without execution history.
Output access returns nil data and typed `ErrJobFailed` / `ErrGoalFailed` with the
saved reason. Restart and execution replay reject protected records. Unknown
kinds and malformed ordinary archives remain errors. See the full
[protected record contract](GUIDE-TERMINAL-JOB-RECORDS.md).

## Durable dependencies

Dependencies target exactly one job or goal. Exact jobs allow `complete` or
`success`; goals allow only `success`. Duplicate targets are invalid. Submission
waits use reserved ID `submission`; structured dependencies cannot mix with
legacy prerequisites. All entries are required, in declaration order.

`RescheduleLease` registers/evaluates a wait and releases the lease atomically.
Reusing a wait ID with the same ordered descriptor/deadline preserves its result;
a changed descriptor conflicts. Live lease validation still applies. Reject
incompatible legacy waits, alternate-route timing and structured waits together.

A successful goal wait binds the actual winning job permanently. An ordinary
failed attempt leaves it pending; explicit goal failure returns its typed saved
snapshot. Exact-job waits continue to follow that recorded job. The first failing
declaration index decides a simultaneously failing group. Group closure ends
remaining subscriptions without cancelling producers or rewriting earlier bindings.

Check deadlines before new settlement. Earlier frozen outcomes remain unchanged;
expired pending groups project timeout consistently and become eligible under
normal scheduling rules. Terminal/cancelled consumers stay nonrunnable. Replaced
waits end abandoned subscriptions while retaining outcomes. Readiness, polling
and targeted leasing must agree. A successful read or nil `Err()` alone does not
prove satisfaction: inspect the state.

Cycle checking includes pending job-to-target edges and open-goal-to-nonterminal
member edges. Serialize concurrent topology changes to prevent graph write skew.
Completion, failure, admission and registration share a transaction ordering;
notify only after commit. Workflow history records exact bindings/failures and
replay reads those records rather than electing a current winner.

## Public surfaces

| Surface | Supported behavior |
| --- | --- |
| `SubmitJob.GoalID` | Creation-time selection under existing trusted selection authority. |
| `SubmitRestartJob.ShareGoal` | Creation-time inheritance of the source primary goal. |
| `GetGoal`, `ListGoals` | Coherent goal projection and filtered/paginated discovery. |
| `ListJobs` with `GoalIDs` | Primary member discovery, with ordinary visibility filters. |
| `FailGoal` | Atomic permanent closure with one protected failed record. |
| `GetDependencyWait`, `ListDependencyWaits` | Durable outcomes and pending diagnostics. |
| `GET /v1/capabilities/job-goals` | Separate immutable/admission/failure capability flags. |
| `GET /v1/tenants/{tenantId}/goals/{goalId}` | Read `GoalInfo`. |
| `POST /v1/tenants/{tenantId}/goals/{goalId}/fail` | Fail with `{"message":"reason"}`. |
| `jobdb goals get`, `candidates`, `list`, `waits`, `fail` | HTTP-backed read/failure commands. |

The former `AddGoalCandidate`, request/result types, HTTP mutation and
`jobdb goals add-candidate` command are removed. Removed HTTP paths receive
ordinary routing errors without mutation. See [OpenAPI](../openapi/jobdb-runtime.yaml)
for complete current request schemas and routes. Keep caller tenant/visibility
restrictions and typed 400/403/404/409/501 errors. A failed goal read is still a
successful resource read with a failed business outcome.

Selection-only support cannot imply stronger guarantees. Core, client and server
check the required flags independently. Unknown support fails with typed
`ErrJobGoalSemanticsUnsupported`, HTTP 501 `job_goal_semantics_unsupported`, without
an ordinary-submit fallback. See [capabilities](GUIDE-JOB-GOAL-CAPABILITIES.md).

## Persistence, conversion and release

Origin ownership, primary membership, first closure and failure identity/snapshot
belong to retained jobs. Toy uses job records and a separate wait-only store.
Fresh SQLite format 16 and successful conversion omit both legacy lifecycle
tables; neither keeps an equivalent renamed goals store. Indexed member/origin/
closure lookups support admission and discovery without reading execution chapters.
Apply tenant, ownership, visibility, state, type and time filters before pagination.
Hidden facts still govern admission, identity and first closure.

SQLite constructors refuse legacy formats 11–15 with typed manual-upgrade guidance before
ancillary initialization. Operators stop all serving writers, take a consistent
backup including committed WAL and external stores, inspect `jobdb sqlite
check-migrate --db PATH`, then run `jobdb sqlite migrate --db PATH`. The converter
exclusively rechecks input, commits all preservation/table removal/format changes
atomically, and rolls back on failure. Reruns on valid format 16 are read-only
`CURRENT`. See [manual conversion](MIGRATION-IMMUTABLE-GOALS.md).

Primary membership wins: secondary links and their future effects disappear.
Report each loss and affected pending wait/preparation without retargeting it.
Preserve actual jobs, rowids, histories, results, artifacts, receipts, declarations,
configuration, lease events and frozen waits. Historical cross-primary success
keeps the exact original producer and closure time on the target origin without
moving membership. Historical explicit failure becomes a marked migrated record
with its original time, message and snapshot, including dropped members.

The staged sequence is add constrained APIs → migrate consumers → remove old APIs.
Consumer retirement order is Cortex → c2j → PostgreSQL, followed by JobDB
contract removal. Those consumer retirements are published and PostgreSQL
v0.0.27 passed independent published-module acceptance. This revision removes
the JobDB attachment surface. PostgreSQL backend conversion/pin update and later
consumer terminal-record integration remain separately owned. Strict deployment activation also requires writer exclusion,
actual migration reports and inherited overlap drain. Runtime capability flags
certify compatible local writers, not those operational conditions.

## Q&A

**Can an independent goals table disappear?** Yes. Toy and SQLite demonstrate
this using retained job origin/closure facts. No independent lifecycle table is
needed for this contract.

**Is a goal ID field alone enough?** No. Open recovery gaps need stable original
ownership after all attempts terminate or become hidden. First closure, explicit
failure identity/message/snapshot and historical cross-primary producer references
must remain durable. Those sparse facts are retained with jobs.

**Why restrict one active attempt?** It gives the coordinating job one clear
recovery owner and prevents sibling attempts from racing goal closure. Parallel
work remains possible under independent child goals. Retained accepted receipts
are essential so retries do not accidentally compete with later recovery.

**Does failure cancel the coordinator or children?** No. It closes goal admission
and settles goal waiters. Existing executions and exact-job dependencies keep
their own lifecycle; cancellation still requires its existing authority.

## Validation

Shared public conformance runs against native toy, SQLite and both HTTP adapters.
Local tests cover hidden-aware admission, accepted replay, protected output,
dependent settlement, tenant nondisclosure, independent SQLite writers and both
controlled closure/admission/wait commit orders. Frozen real legacy inputs cover
exclusive conversion, preservation, rollback, rerun, discovery and retirement.
Source checks include removed route/CLI behavior, generated binding reproducibility,
public snapshot, the untagged Go suite and affected race suites. These checks do
not certify PostgreSQL deployment, production conversion or overlap drain.
