# pgjobdb: implement the named-cell runtime contract

Status: implementation plan for colony-2/pgjobdb. Shared semantics, HTTP contract,
and cutover: [SPEC-Tenant-Cells.md](SPEC-Tenant-Cells.md). Architecture context:
[GUIDE-PGJOBDB-RUNTIME-IMPLEMENTATION.md](GUIDE-PGJOBDB-RUNTIME-IMPLEMENTATION.md).
This plan defines obligations against JobDB's public ports; exact pgjobdb schema
and function edits must be located in that repository during implementation.

The Postgres backend is a required adopter wherever deployed. Updating its Go
dependency alone is insufficient. Initialize a fresh execution schema, discard
old jobs/history/artifacts/schedules/leases, and remove pre-cell record codecs,
schema definitions, and compatibility routes. There is no ALTER/backfill rollout,
metadata-derived identity migration, or mixed-version scheduler operation.

## Public adapter and storage

Implement the new mandatory cell runtime/scheduler ports published by JobDB.
Use `pkg/jobdb`, `pkg/jobdb/runtime/core`, `pkg/jobdb/runtime/cellstate`, and
`pkg/jobdb/runtimetest`; do not copy
private JobDB implementation packages. Update all mappings for SubmitJob,
preparation/publication, StoredJob, handles, schedules, goals, work selection,
lease snapshots, bounded name resolution, and delegation authority.

Use the same public name/repository normalizer and typed errors. Postgres must
not normalize differently, defer ownership to JSON metadata, or make network
calls to verify Git identities. Administrative root bootstrap is explicit; root
job submission alone cannot create the reserved root cell. Support the shared
`jobdb cell create --name root` CLI through the standard administrative HTTP API; no
separate Postgres-specific bootstrap command is needed. Preserve the root name
and designation permanently, including during multi-property PATCH.

Fresh schema obligations:

| Relation | Required facts/constraints |
| --- | --- |
| Cells | Tenant/ID primary key, unique canonical repo within tenant, state, revision, timestamps, description, metadata. |
| Cell names | Current names and rename aliases in one tenant-unique namespace, FK to cells; retired names remain reserved. |
| Root binding | One root ID per tenant, consistent with permanent `root` name. |
| Jobs/goals/schedules/preparations | Required same-tenant cell ID, with indexes supporting authorized query and acquisition; immutable job/creator/preparation repo snapshots and schedule expected repo. |
| Runtime children | History/waits/artifacts derive authority from required owner records. |

Choose Postgres equality/collation and constraints to implement exact
case-sensitive names and normalized repo identities. SQL NULL/default values
must not turn required cell ownership into an optional compatibility field.
Add a new storage-format/version marker and reject old schemas before serving
traffic. Delete replaced old-format conversion and inference branches.

## Transaction and publication contract

Cell registration belongs in the same transaction/visibility domain as job
publication. Do not call a separate registry service/create transaction before
submitting the first job. The backend must coordinate:

1. Existing tenant-wide job-ID lookup and semantic comparison for retries.
2. Name/alias resolution, repo assertion, and name/repo uniqueness.
3. Current target state and caller/root-parent submission authority.
4. Parent lease liveness and attenuation for leased submissions at commit.
5. New cell/job/goal/initial chapter/idempotency publication as one visible result.

The current prepare/history/publish boundary may need replacement. Work with
JobDB's public ports to reserve names/IDs privately and publish atomically; do
not expose a cell while its job/history can still fail validation. Recovery of
new-format reservations must revalidate authority and ownership. Clean failed
private reservations and staged artifacts. Durable cell-only leftovers after a
rejected first job violate the contract.

Use row/advisory locks or an equivalent proven serialization strategy for
submission, rename, repo PATCH, retirement, scheduling, and acquisition. Document lock
ordering to avoid deadlocks when root and target cells differ. Concurrent
matching name/repo submissions converge; conflicting names/repos fail without
partial publication. Retirement cannot commit while a competing admission
creates unfinished work, and acquisition cannot succeed after retirement.

PATCH validates revision, all supplied properties, root protection, and current
repo/name uniqueness atomically. Permit root repo/description/metadata changes,
but reject changing its name from `root`. Repointing releases the former repo
locator; it never rewrites stored job snapshots or stable ownership. Capture
the current repo on fresh job publication, and reject stale repo assertions.
Existing explicit-ID retries compare against the job snapshot before current
repo resolution, so confirmation still works after an edit. Equivalent
normalized repo updates preserve revision when no other field changes.

Retirement eligibility includes nonterminal hidden/waiting/retrying jobs, live
leases, active schedules, open goals, and pending submission reservations.
Check revision and eligibility in the mutation transaction. Reads of retired
records remain supported for jobs created after the release; no restore/delete
operation is introduced.

## Authorization, queries, and scheduling

Root identity comes from stored ownership plus the tenant's root binding, not
metadata, a request's name, or a worker's current directory. Root owner leases
can submit to any tenant cell name and register missing targets with a repo.
Non-root scope and delegated jobTypes/jobIds/cellNames restrictions intersect;
missing jobs.submit denies submission. New jobs in ordinary cells acquire
ordinary ceilings even when their parent was root.

Resolve cellNames selectors and intersect allowed cells before SQL pagination,
counts, route discovery, polling, and bulk mutation. Never acquire a batch and
filter out unauthorized leases afterward. Bind cursor state to the principal
and resolved cell set. Schedules store cell ID and expected repo, and recheck
ACTIVE state and repo under the publication transaction before runs. A changed
repo pauses emission with visible `cell_repo_mismatch` until an explicit target
update/rebuild. Restarts resolve to the source's existing cell and retain its
repo snapshot; they cannot register or transfer ownership.

Preserve authorized cross-cell child/goal/dependency behavior for fresh jobs.
Goal/candidate mutation and durable wait observations need their own checks;
root submission authority alone grants no history/artifact/secret access.
Return shared typed errors so the remote server uses one HTTP error contract.

## Delivery and acceptance

1. Coordinate port changes with JobDB before implementation freezes; avoid a
   Postgres-only wire protocol or weaker interpretation of publication semantics.
2. Replace SQL schema/functions, runtime mappings, codecs, queries, scheduler
   transactions, and bootstrap. Remove old signatures and decoder branches.
3. Run JobDB's shared conformance suite and Postgres-specific transaction/race
   tests, including two connections competing on registration and retirement.
4. Deploy through the [fresh-state release barrier](SPEC-Tenant-Cells.md#coordinated-hard-break-cutover),
   recreate tenant/root administration, and start only compatible workers.

Cover rollback on invalid job and lease loss, crash/retry of new reservations,
concurrent name/repo claims, idempotency after rename/retirement, signed target
ceilings, authorized pagination/acquisition, scheduling races, root protection,
and rejection of pre-cell storage/tokens/requests. Audit removal of old code;
passing tests with an old fallback still enabled is not completion.

## Published backend port details

`core.Scheduler` now embeds `jobdb.CellRuntime`, requires
`ResolveCellWithLeaseByID` and `ResolveScheduleCell`, and embeds the mandatory
`GoalScheduler.DiscardJobPreparation` cleanup method. `JobPreparation.Target`,
`PreparedJob.Target`, `CreateJobRequest.Cell`, `StoredJob.Cell`, and
`LeaseSnapshot.Cell` carry the immutable binding. Return the committed binding
from `CreateJob`: another concurrent first submission may win the cell ID.
`StoredSchedule.PauseReason` exposes repo-change pauses. Resolve and pin a
schedule before hashing, then recheck its binding in `UpsertSchedule` and any
mutation that activates it.

The public `cellstate` package supplies registry validation, PATCH/retirement,
request replay, HMAC cell-list cursors, and selector resolution. Job-list cursor
helpers bind queries to principal, resolved selection and authority; they are
not credentials, so always reapply access filters. Use `cellstate.Lease` and
`cellstate.Schedule` only as trusted internal inputs, and validate stored lease
or schedule state in the transaction. Run `runtimetest.RunCellConformance` over
both the direct runtime and HTTP adapter, plus Postgres transaction race tests.
