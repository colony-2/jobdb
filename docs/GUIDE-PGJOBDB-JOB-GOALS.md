# Implementing immutable job goals in pgjobdb

Pgjobdb owns its PostgreSQL schema, installer, scheduler transactions, capability
advertisement and JobDB pin update. This JobDB change does not implement or certify
them. Use the [shared specification](SPEC-Job-Goals.md),
[selection contract](GUIDE-PRIMARY-GOAL-SELECTION.md),
[protected record contract](GUIDE-TERMINAL-JOB-RECORDS.md) and
[runtime adapter guide](GUIDE-PGJOBDB-RUNTIME-IMPLEMENTATION.md).

## Shared interface and storage

`GoalScheduler` embeds `jobdb.GoalRuntime` and exposes `PrepareJob`.
Implement goal/wait reads and `FailGoal`; the attachment method and its types
are removed. Preserve preparation descriptors, selected goal IDs, complete
accepted receipts and stable cell ownership across the public/core ports.
`OtherGoalIDs` stays deprecated and empty. Job filtering uses primary membership.
The shared remote server/codec carries the reduced OpenAPI protocol.

Store immutable `goal_id` and retained origin/first-closure facts with jobs.
Do not retain a separate goals/candidates lifecycle store. Origins preserve
creation and ownership across open gaps and hiding. Sparse closure preserves
state, original closure time and exact producer references; protected failure
jobs preserve the message, failure identity and frozen member snapshot.
Historical cross-primary producers must remain exact without moving membership.
Keep preparations, receipts, waits and pending-target indexes separately for
their existing purposes. Index tenant/goal primary membership, origins, closures
and pending target/deadline lookups. Discovery filters precede pagination.

## Atomic mutations

Use one consistent transaction coordination protocol for publication, completion,
failure, wait registration/settlement, cancellation and relevant deletion/timeout
paths. A tenant advisory lock before row locks is one option; serializable
transactions with full retries are another. Protect both admission and graph
write skew. `SKIP LOCKED` for work claims must not skip required goal settlement.
Do not add an unconditional unique active-member index that prevents preserving
historical overlaps during conversion.

Preparation reserves identities without admission capacity. Resolve equivalent
accepted descriptors first, reauthorize and return the full captured handle even
after closure or hiding. Fresh publication rechecks ownership, authority, cycles,
openness and every retained nonterminal member, including hidden/blocked/delayed/
expired/task-routed/cancellation-requested members. Publish the job and receipt
atomically. An unpublished preparation creates no discoverable goal.

Successful completion stores its real result before exposing success, then
atomically establishes first closure and settles waits. Ordinary failure and
terminal cancellation leave recovery open. Explicit `FailGoal` validates current
lifecycle authority and the exact nonblank UTF-8 message, up to 16 KiB. Freeze
sorted pre-publication member IDs; atomically create one protected archived failed
job, closure and waiter outcomes. Equal-message replay returns that record;
incompatible closure conflicts. No implicit cancellation, chapters, routes,
lease events or fabricated execution output is allowed.

Settled waits preserve exact producer IDs, error snapshots and declaration order.
Deadline checks, cycle protection, cancellation and wait replacement retain the
shared dependency semantics. Notify workers after commit. Hiding must retain
origins, closure and receipts and cannot reopen a goal.

## Capability, migration and rollout

Advertise immutable membership, atomic admission and terminal failure records
only after enforcing them. Selection-only support is insufficient. Unsupported
operations return the shared typed error; never fall back to ordinary submission.
Existing lifecycle/selection authority remains unchanged.

Cortex, c2j and PostgreSQL consumer retirement precede JobDB contract removal.
Those retirements are published; PostgreSQL v0.0.27 passed independent
published-module acceptance against the retained-runtime preparation. The next
PostgreSQL step is backend conversion against the published reduced contract. Conversion must
preserve actual identities/results/history/artifacts and frozen bindings. Primary
membership wins; report dropped secondary links and affected pending references
without retargeting them. Preserve historical successful producers and translate
saved explicit failures into marked protected records. Keep live overlaps visible
to admission and drain them before claiming strict global uniqueness.

Pgjobdb's installer and independent-session checks remain its owner's work.
Exclude obsolete writers and review actual database reports before activation;
source capability flags are not deployment evidence. SQLite's
[manual upgrade](MIGRATION-IMMUTABLE-GOALS.md) demonstrates preservation expectations
but its format/commands are not PostgreSQL migration commands.

Run reusable JobDB selection, admission and terminal-failure conformance through
native pgjobdb and actual HTTP. Add independent PostgreSQL-session contention,
both closure/admission/wait commit orders, rollback, reopen, hidden facts,
discovery plans, conversion and installer checks. Keep ordinary lease/history/
artifact/dependency regressions. A dependency update alone proves none of these.
