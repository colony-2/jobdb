# JobDB implementation specification: named cells

Status: implemented in JobDB; downstream adoption is tracked separately.
Normative HTTP contract: [jobdb-runtime.yaml](../openapi/jobdb-runtime.yaml).
The [proposal slice](../openapi/jobdb-cells.proposed.yaml) is retained as design context.

This document owns the shared cell contract, JobDB implementation, and release
barrier. Project-specific work is split into:

| Owner | Plan |
| --- | --- |
| JobDB | This specification: public types, semantics, core, Toy/SQLite, HTTP, SDK, and conformance. |
| c2j | [GUIDE-C2J-CELLS.md](GUIDE-C2J-CELLS.md): names, configuration, submission, contexts, workers, and permissions. |
| Cortex | [GUIDE-CORTEX-CELLS.md](GUIDE-CORTEX-CELLS.md): server catalog, UI, submission, and filtering. |
| pgjobdb | [GUIDE-PGJOBDB-CELLS.md](GUIDE-PGJOBDB-CELLS.md): Postgres schema, scheduler, adapter, and publication transactions. |
| Recipes | [GUIDE-RECIPES-CELLS.md](GUIDE-RECIPES-CELLS.md): root provisioning workflows and name-based recipe targets. |

The existing Git host needs no new API for this release. Other consumers of
JobDB/c2j must rebuild against the new contract if they submit jobs, poll work,
or deserialize the changed cell-bearing models.

## Hard break: release policy

Start with fresh JobDB execution storage. Discard all pre-change jobs, chapters,
artifacts, goals, waits, schedules/runs, submission reservations, lease/delegation
state, idempotency records, and pagination tokens. Do not backfill or import
cell ownership from old metadata, and do not replay or resume old jobs.

Replace the affected implementation paths in the same release. Delete old cell
identity inference, metadata-based ownership/filtering, repository-as-cell-ID
code, compatibility projections, and old request/token decoders. Do not add
fallbacks, dual reads/writes, compatibility flags, transitional aliases, or a
parallel old API. Required cell fields apply immediately. The current versioned
routes are replaced in place as part of a coordinated server/client deployment.

New cell rename aliases and retained history of jobs created after the break
are ordinary features of the new model. They are never populated from old jobs,
old metadata, or an imported cell catalog. Configuration and Git source repos
are inputs to fresh bootstrap, not execution-state migration sources.

Startup rejects pre-cell stores with a reset-required error; it does not delete
application data. SQLite uses fresh storage format 10.

## Core decisions

JobDB is the durable registry for `cell name -> generated cell ID + repository`.
Every new job submission requires `job.cell`. For an existing cell, `cellRepo`
is optional and, when supplied, must match. A missing ordinary cell requires
`cellRepo` plus registration authority. Registration and its first job publish
atomically. Job IDs remain tenant-wide.

`root` is a special, administratively bootstrapped cell with a permanent name.
A live owner lease on a job actually belonging to root can submit to any cell
name in the same tenant, creating a missing target when its repo is provided.
Submission into root itself does not implicitly create root. Delegation
attenuation still applies. Non-root leases default to their own cell unless
explicit policy grants additional existing targets. Cells retire rather than
delete. No combined Git/JobDB service is required.

## Identity, names, and repository registration

Keep three concepts explicit:

| Field | Meaning | Owner |
| --- | --- | --- |
| `Cell.id` / response `cellId` | Opaque generated identity, immutable across rename and retirement. | JobDB |
| `Cell.name` / submission `job.cell` | Unique human-facing cell name used by c2j. | JobDB after registration |
| `Cell.repo` | Current canonical Git repository locator; administratively editable. | JobDB registry |
| Submission/response `job.cellRepo` | Optional assertion on submission; resolved repo snapshot on the stored job. | Fixed at submission |

Use one `name` field for lookup. Names use the short-name alphabet: 1–128 ASCII letters, digits,
dots, underscores, or hyphens, excluding `.` and `..`. Comparison is exact and
case-sensitive. The exact lowercase name `root` is reserved. Optional richer
presentation properties can live in cell metadata.

A cell also has `description` (up to 2048 characters, default empty), `metadata`
(an application JSON object, default `{}`, at most 64 top-level keys of 1–128
characters and 16 KiB compact UTF-8 JSON), `state` (`ACTIVE`/`RETIRED`), `isRoot`,
server timestamps, and an opaque revision. Metadata is not authorization policy.

`id`, tenant, root designation, state, aliases, revision, and timestamps cannot
be set through metadata/PATCH. Name, repo, description, and metadata are editable
properties of active cells, except that root's name is permanently `root`.
One current canonical repository maps to one cell within a tenant, including
retired cells. All branches and commits share that cell. Git refs are
job execution inputs rather than cell identity. Store the resolved commit when
execution needs a reproducible source version.

### Repository locators without a combined server

JobDB registers a credential-free repository locator; it does not call Git,
verify that the repository exists, clone it, or create it during submission.
Registration authority comes from the JobDB caller/lease. Git credentials and
source trust remain separate responsibilities of c2j and the worker.

Implement one versioned normalization contract shared by JobDB, c2j, and Cortex.
For ordinary hosted repositories, accept canonical `host/owner/repo`, HTTPS
clone URLs, and standard-port SSH/SCP forms. Normalize those recognized forms to
`host/owner/repo`: lowercase the host, remove one trailing `.git` and trailing
slash, and preserve repository-path case. Reject query/fragment components,
HTTP credentials, empty paths, and traversal segments. SSH transport usernames
such as `git@` are not part of repository identity. Examples:

```text
github.com/colony-2/payments
https://github.com/colony-2/payments.git
git@github.com:colony-2/payments.git
  -> github.com/colony-2/payments
```

Do not guess equivalence for custom ports, HTTP-only endpoints, custom transport
paths, or unrelated hosts: use an explicit deployment-managed alias mapping or
reject them until supported. A `file://` repository needs an explicit deployment
mapping to a shared identity; a machine-local path is not portable registration.
Preserve clone transport/profile in c2j configuration independently of identity.
Publish one shared normalizer and conformance vectors for clients. All accepted
repository forms must follow the same contract at registration and comparison.

Change `Cell.repo` through administrative PATCH with revision and uniqueness
checks. This updates the registry locator; it does not move, clone, or rename a
Git repository or change credentials/policy. Ordinary job submission cannot
change `Cell.repo`. A future combined server can attach its immutable Git ID to
the existing cell ID without moving jobs or changing c2j's name-based API.

Repository edits do not require an idle cell. Existing jobs, including queued
jobs, retain the repo snapshot captured at submission; leases, execution,
retry/replay, and restarts use that snapshot. Submit a fresh job to use the new
repo. Job ownership never changes. A former repo locator is released for reuse
by another cell; historical jobs are never re-resolved by repo. Only the current
registry mapping is unique, including mappings held by retired cells.

Schedules bind both cell ID and expected repo. Before emitting a run, compare
that repo to the current mapping under the publication transaction. A mismatch
pauses the schedule with visible `cell_repo_mismatch` until its target is
explicitly updated/rebuilt; it must not silently execute code from a new repo.
Already emitted jobs retain their snapshots.

### Name changes and retries

PATCH can rename an ordinary active cell. Preserve names from post-release
renames as read-only `aliases` bound permanently to the same ID, including after retirement. Aliases
resolve on submission and exact-name lookup. Current names and aliases share a
tenant-wide uniqueness constraint. This prevents a stale config or delayed job
retry from creating a replacement cell under an old name. Rename to an alias of
the same cell is allowed atomically; the alias list excludes the current name.
No alias removal/reassignment is included initially. Root cannot be renamed,
retired, or replaced, and ordinary cells cannot claim `root` as a name or alias.

Job responses expose `cell` (current canonical name), `cellId` (stable ownership),
and `cellRepo` (the job's repo snapshot, not the cell's current locator). Cell GET
and name-resolution responses return the current registry locator. Persist `submittedCell` separately for
history and idempotency; it is not a second authority. A job may have been
submitted with an alias. c2j can adopt the canonical name on later new requests
while retaining the original request on retries.

## Submission contract

The existing SubmitJob body gains required `cell` and optional `cellRepo`:

```json
{
  "job": {
    "cell": "payments",
    "cellRepo": "github.com/colony-2/payments",
    "jobType": "recipe",
    "data": {"data": {}, "artifacts": []}
  }
}
```

Later submissions can omit `cellRepo`. The same body applies to ordinary POST,
explicit-ID PUT, and their lease-scoped child variants. c2j must serialize the
cell even when targeting self; the server does not infer a missing required
name from metadata or a parent. Submitted metadata cannot override these fields.

| Target state | Repository input | Authorized result |
| --- | --- | --- |
| Active existing name/alias | Omitted | Submit using its stored repo and ID. |
| Active existing name/alias | Matches after normalization | Submit using its stored repo and ID. |
| Active existing name/alias | Different repo | `409 cell_repo_mismatch`; no mutation. |
| Missing ordinary name | Omitted | `400 cell_repo_required`; no mutation. |
| Missing ordinary name | Supplied, root lease or administrative creation authority | Atomically create cell and submit job. |
| Missing ordinary name | Supplied, insufficient creation/target authority | `403 forbidden`; no mutation. |
| Missing name with repo already registered to another cell | Supplied | `409 cell_conflict`; do not infer a rename or create an alias. |
| Retired name/alias | Any | `409 cell_retired`; no new work or recreation. |
| Missing `root` | Any | `409 root_cell_required`; trusted bootstrap is required. |

Check authority before revealing registry state. Normal non-root callers without
access to an existing target receive `404 not_found`, even if their repo input
would conflict. Root's tenant-wide submission authority permits the bounded
resolution required to submit; it does not grant general history/secret access.

Validate the entire job, including schema, references, target authority and
payload limits, before publishing anything. Commit cell registration, job,
goal, initial chapter, and idempotency binding atomically, or expose equivalent
all-or-nothing durable publication. Validation or parent-lease loss before commit
must not leave an empty cell behind. Artifact staging may be internal but cannot
publish a cell early. Serialize against retirement, repository PATCH, and
repository/name claims. Capture the current repo on the job at commit; a fresh
submission with an obsolete repo assertion fails with `cell_repo_mismatch`.
Concurrent submissions of the same name and repo converge on one cell; different
repos claiming the same name conflict without publishing the losing job.

Cell creation initializes description/metadata to empty values. SubmitJob's
metadata remains job metadata. Administrative creation can initialize richer
cell properties separately.

### Job idempotency

Retain existing explicit-job-ID PUT semantics and `existing_job_mismatch`.
Lookup an existing `(tenantId, jobId)` before resolving a possibly new cell name.
Persist the submitted name, normalized optional repo assertion, resolved cell
ID/repo, parent binding, and ordinary durable job comparison data. An identical
retry rechecks current caller/lease authority against stored ownership and
returns the existing job; it never creates another cell or re-routes the job.
Allow a current name or reserved alias for that same stored ID and equivalent
ordinary job payload to confirm the job. Compare any repo assertion against the
stored job snapshot before consulting the current registry repo. An omitted
repo and an explicit matching snapshot repo are equivalent for an existing job,
including after a repo PATCH; a different assertion is `existing_job_mismatch`.
An existing job in a retired cell can be confirmed without creating new work;
reads/confirmations are different from attempting a new submission there.

A changed target ID, mismatched repo, parent binding, or job payload returns
`409 existing_job_mismatch`. Existing-job success after retirement requires
current read/confirmation authority; lease callers must still hold a valid
parent lease and sufficient submission authority for that target. Server-ID POST
has its existing non-idempotent job semantics; c2j should use explicit job IDs
when retry safety is required. Cell registration itself always remains unique.

## Root and submission authority

Trusted administration bootstraps the one root cell per tenant with its actual
configured repository and records the server-owned root ID. Missing root never
appears implicitly through job submission. Fresh tenant bootstrap registers the
configured root repository; a label `root` in arbitrary metadata grants nothing.
Explicit creation of `root` requires `cells.create` and `cells.bootstrapRoot`;
the latter is assigned only to trusted tenant provisioning/administration.
Restrict who can submit executable jobs into root, acquire their leases, and
select the code those workers run: a root owner lease is a tenant-wide job
submission capability by design.

### Explicit root bootstrap command

Add `jobdb cell create-root --tenant TENANT --repo REPO --url URL` to
`pkg/jobdb/cli`, using the CLI's administrative authentication configuration.
It calls the normal cell-create API with the fixed name `root`; callers cannot
choose a name, ID, or root designation. Require `cells.create` and
`cells.bootstrapRoot`. This command works against any conforming backend,
including pgjobdb, and is the documented fresh-tenant bootstrap entrypoint.

On successful creation, print the generated cell ID, name, and canonical repo.
For an already-created root, an authorized lookup may confirm and report the
existing record when its repo matches after normalization. That lookup requires
`cells.list` and `cells.get`; denial is an error. A different repo is an explicit conflict; use
PATCH to change it. The command never changes an existing root's properties.
Concurrent bootstrap calls must converge on the one root binding or return the
specific conflict; a repo owned by another cell is not adopted as root.

Root always keeps the exact name `root`. PATCH with `name: root` may succeed as
a no-op, but any different name returns `409 root_cell_protected`, atomically
rejecting other changes in that request. Authorized administrators may still
PATCH root's repo, description, and metadata. Its ID and root role stay fixed;
it cannot retire. Submitting a job into a missing root still fails with
`root_cell_required`; it never substitutes for this command.

### Lease authority

A root owner lease's `jobs.submit` ceiling includes every cell name in that
tenant and implicit registration of missing names with a supplied repo. No
separate `cells.create` lease grant is needed. Existing job type/ID restrictions,
lease liveness, signed path binding, deadlines, tenant isolation, and attenuation
still apply. A delegation lacking `jobs.submit` cannot submit, even from root.
A new optional `cellNames` selector on delegated submission narrows the ceiling;
omission inherits the parent's target-cell scope, not universal access. Existing
names/aliases resolve to stable IDs; reserving aliases prevents retargeting.

Non-root owner leases default to their own cell. Explicit policy can grant
additional existing targets, but only root lease authority implicitly creates
cells through the lease path. Administrative callers can submit where authorized
and need tenant `cells.create` when the target is missing. The root exception
allows submission, not arbitrary secret reads, cell edits, retirement, or root
configuration changes. A job submitted into a non-root cell receives that cell's
normal lease authority when acquired, not the parent's root authority.

For name-only c2j calls that need to load the repository's recipe before job
submission, expose read-only `resolveCellWithLease` at
`POST /jobs/{parentJobId}/leases/{leaseId}/cells/resolve` with `{ "cell": "payments" }`.
Return only `{cell, cellId, cellRepo}` for an active name/alias within the lease's
effective submission target scope. Root can resolve any name; ordinary leases
resolve only their authorized targets. This permission reveals no cell metadata,
job history, or secrets. Missing or unauthorized targets return `404`, an
authorized retired target `409 cell_retired`; resolution never creates a cell.
Job type/ID restrictions are still checked on subsequent submission. c2j must
retain the resolved repo assertion until submission; the server resolves again
under the mutation transaction. Administrative clients use the normal cell lookup.

Expose submission scope through the public runtime and signed lease contracts.
Implement `cellNames` parsing, intersection, and signing in the delegation code.
A client-provided job context cannot establish root ownership or widen a ceiling.

## Management and retirement

Management uses stable IDs; c2j can discover them through name lookup:

| Method | Path under `/v1/tenants/{tenantId}` | Behavior |
| --- | --- | --- |
| `POST` | `/cells` | Administrative explicit creation with name, repo, optional properties. |
| `GET` | `/cells?name=payments` | Exact current-name/alias lookup in the visible cell list. |
| `GET` | `/cells/{cellId}` | Read an active or retired cell by stable ID. |
| `PATCH` | `/cells/{cellId}` | Change name, repo, description, or metadata, with `If-Match`. |
| `POST` | `/cells/{cellId}/retire` | Retire an idle ordinary cell, with `If-Match`. |
| `POST` | `/jobs/{parentJobId}/leases/{leaseId}/cells/resolve` | Read name/ID/repo within lease submission scope, without registration. |

Listing defaults to active cells and supports `state=RETIRED|ALL`, `isRoot`,
`pageSize` (100 default, 1–1000), and opaque `pageToken`. Visibility and filters
apply before pagination, sorting by ID. Bind tokens to tenant, principal, and
repeated filters; recheck authorization every page. No unfiltered total count.
Use `cells: []` for empty results and omit the final nextPageToken. Traversal is
live; creations/renames/retirement may change matches between pages.

Use ETag revisions on read/create/update/retire. PATCH and retire require one
strong If-Match (missing `428`, stale `412`, malformed/weak/wildcard/list `400`).
PATCH requires `cells.get` and `cells.update`, preserves omitted fields, and
replaces an entire supplied metadata map. Metadata values may be arbitrary JSON,
including nested objects, arrays, and null; `{}` clears the map. To edit one
key, GET the map, modify it, and PATCH the resulting map with that ETag. An empty
description clears that field. This is a typed `application/json` PATCH, not
JSON Merge Patch: null metadata values are data, not deletion instructions.
No-op preserves revision/time, including an equivalent normalized repo. Null
top-level fields and empty patches are invalid. Check revision, root protection,
name/alias uniqueness, and normalized repo uniqueness in one transaction. If
any field fails validation, none of the changes apply. Registry-only administration
requires its own permissions; root submission authority does not authorize it.

Explicit administrative create retains an optional principal/tenant-bound
requestId for matching retries. Compare normalized properties, preserve its
binding for the cell lifetime, and return `409 request_conflict` on different
properties. Matching retries return the original record snapshot only while
active, possibly stale after updates; retired targets return `409 cell_retired`.
The HTTP response replays `201` with the original record, ETag, and Location.

Retirement is an `ACTIVE -> RETIRED` transition requiring no
nonterminal jobs, live leases, active schedules, open goals, or pending
submission reservations. Otherwise return `409 cell_busy`; it does not cancel
or drain work automatically. Root retirement returns `409 root_cell_protected`.
Preserve ID, names/aliases, repo, metadata, jobs, and artifacts. Authorized reads
continue; new jobs, restarts, acquisitions, schedule activation, and cell
updates fail. Pausing/archiving an existing schedule remains possible. Existing-job idempotent confirmation follows the rule above.
There is no automatic restore/recreation and no Git deletion/archive side effect.
A drain state remains an alternative requiring a separate lifecycle decision.

## JobDB implementation work

Implement this feature directly in the public contract and mandatory backend
ports. Do not use an optional fallback that substitutes metadata inference when
a backend lacks cell support. Each item below includes its removal work.

### Public types and transport

- Add public `Cell`, `CellState`, name/repository value validation, typed errors,
  management requests/results, and `CellRuntime` operations. Make cell management
  available through `WorkflowRuntime`; add bounded name resolution on leases.
- Add required name and optional repo assertion to `SubmitJob`; expose resolved
  `cell`, `cellId`, and `cellRepo` on handles, job details/summaries, goals,
  schedules, and lease snapshots. Jobs and their leases carry the submission
  repo snapshot; goals retain their creator's snapshot and schedules their
  expected repo. Registry reads return the current repo. Require the cell name for restart and schedule
  inputs as well. Keep `JobKey` tenant-wide.
- Add first-class `cellNames` query/poll selectors; resolve once per request,
  bind cursors to resolved IDs and principal, and intersect authorized cells
  before pagination/claims. Omitted list selectors mean all cells the caller may
  inspect; omitted poll selectors mean all cells the worker may acquire, never
  all tenant cells by default authority. Explicit empty selectors are invalid.
  An explicit inaccessible/missing target fails without broadening the query.
- Extend `DelegationSubmitPermission` with optional `cellNames`, and supply
  server-owned root/normal ceilings. Change the token profile/version to bind
  cell ownership and reject pre-change lease/delegation formats. Update supplied
  lease import/export, renewal, local attenuation, and all wrappers together.
- Merge the approved proposal into `openapi/jobdb-runtime.yaml`, replace affected
  models, and regenerate `pkg/jobdb/internal/runtimeapi/zz_generated.go` through
  its existing generate directive. Update the remote codec/client/server and
  public API snapshots; remove superseded shapes and code paths.
- Validate required ownership fields in Go and embedded calls as well as HTTP.
  Reject a missing cell even when application metadata includes `cell_name`,
  `cell_id`, or `repo`. Metadata predicates may remain for application data but
  can never establish ownership, grant access, or widen acquisition scope.

The draft OpenAPI contains cell management and ordinary/leased submission. It
is a proposal slice referencing unchanged runtime payloads, not a second API to
ship. Restart, schedule, discovery, lease, and response-model changes in this
section must be included in the main OpenAPI before implementation is complete.

### Core and backend boundary

Update `pkg/jobdb/runtime/core/scheduler.go` and `core/goals.go` ports for cell
ownership, resolved-target preparation, and atomic publication. `CreateJobRequest`,
`StoredJob`, `PreparedJob`, `JobPreparation`, `LeaseSnapshot`, work selectors,
schedule records, and mutations must carry the required authoritative fields.
Publish the ports through public packages for pgjobdb; do not ask pgjobdb to
import JobDB private code.

The current prepare/history/publish sequence needs an explicit design for cell
registration. A name/repo reservation may be durable for crash recovery but must
remain invisible to cell listing, name resolution, polling, and job reads until
publication. An invalid job or lost parent lease must not publish an empty cell.
Retry/recovery rechecks state and authority and preserves the reserved identity.
If the existing ports cannot guarantee this, replace them in this release;
do not introduce a standalone create-cell call before SubmitJob.

Keep scheduler authority in the backend transaction domain. Cell resolution,
root-parent verification, live lease validation, uniqueness, repo edits, retirement checks,
job/goal publication, and idempotency must be coordinated there. HTTP preflight
alone cannot prevent lease-loss/retirement races. Where chapter/blob storage is
separate, the visibility boundary must still be all-or-nothing; discard failed
unpublished reservations. Staged chapter/blob data can remain unreachable after
a failed publication; physical reclamation is a storage maintenance concern.

`jobdb.WithCellAuthority` supplies trusted policy for embedded calls and HTTP
middleware. It scopes cell management, submission, job listing, and acquisition;
names, IDs, and application metadata are never proof. `LeaseSubmitCells` is an
explicit allowlist of existing IDs for additional non-root submission targets;
omitting it preserves the own-cell ceiling. Root leases derive their wider
submission authority from live stored ownership, not middleware labels.

This release establishes ownership and these enforcement hooks. It does not
implement complete resource-wide cell RBAC: deployments must continue enforcing
their existing access rules for history, artifacts, goals, waits, cancellation,
and other legacy runtime operations. A future security rollout must extend
those checks across relationships before exposing the runtime to mutually
untrusted cell users. Chapter/artifact reads require a published owning job.
Tenant configuration/schema registry remain tenant-level; secret providers now
receive the immutable cell/repo snapshot in `SecretLeaseContext.Cell`.

The bundled CLI server supplies explicit local-administrator authority. A
production embedding installs its own authenticated middleware before
`remote.NewServer`; authority is never accepted from JSON or a client header.

### Fresh Toy and SQLite storage

Use the new schema from initial creation:

| Relation | Required constraints |
| --- | --- |
| Cells | Tenant/ID primary key, canonical repo unique per tenant, state/revision/timestamps, metadata, root designation. |
| Cell names | `(tenant, name)` unique across current names and permanent rename aliases, pointing to exactly one cell; all states reserve names. |
| Tenant root binding | Exactly one bootstrapped root ID, consistent with reserved name `root`. |
| Jobs, goals, schedules, preparations | Required cell ID; jobs use a same-tenant FK and cell index; goals/schedules/preparations retain checked cell bindings in their persisted snapshots; job/creator/preparation repo snapshots and schedule expected repo. |
| Child/history records | Owner identity required; no metadata fallback. |

Toy must enforce the same contract with its locking/publication boundary. SQLite
must enforce it with constraints and transactions, including concurrent create,
rename, repo updates, retirement, schedule emission, and lease acquisition. Index authorized
cell selection with existing route/status/time filters rather than filtering
results after pagination or acquiring unauthorized work first.

Introduce a new storage-format marker. Startup rejects pre-cell stores with an
explicit reset-required error; it never upgrades them by inferring cells. Remove
the replaced pre-cell schema and record-decoding branches. Update fixtures and
embedded-runtime setup to initialize the fresh format and required cells.

### Implementation order and acceptance

1. Finalize types, full OpenAPI changes, normalizer, and authorization decisions;
   publish backend port changes for pgjobdb in the same release series.
2. Implement core semantics and fresh Toy/SQLite storage/publication. Remove
   replaced pre-cell paths as each surface is converted.
3. Implement remote transport, lease profiles, delegation attenuation, public
   runtime/SDK mappings, and `jobdb cell create-root` in `pkg/jobdb/cli`.
4. Add shared conformance cases in `pkg/jobdb/runtimetest`; run against Toy,
   SQLite, remote, and pgjobdb before release.
5. Regenerate clients/types, update fixtures/examples, and verify every required
   consumer guide is complete. Ship behind the coordinated cutover below.

Acceptance covers required cell name, missing/matching/conflicting repo,
concurrent registration and complete rollback, exact-job retries after rename
or retirement/repo updates, atomic multi-property PATCH and metadata replacement,
repo snapshot preservation on retries/restarts, schedule pause on repo mismatch,
root bootstrap retries/conflicts/denial/concurrency, root rename rejection,
root owner versus attenuated grants, non-root restrictions,
root spoofing through metadata/workspace, alias/name uniqueness, retired cells,
filter/poll authorization before pagination, cross-cell workflows, stale
configuration conflicts, and crash recovery of newly written format records.
Old request bodies/tokens/stores must be rejected, not adapted. Audit deleted
branches as well as passing positive tests. Do not retain old-job fixtures to
prove compatibility; retain only rejection fixtures where needed.

## Coordinated hard-break cutover

1. Build and validate JobDB, pgjobdb if deployed, c2j, Cortex, and the affected
   recipes against one contract. Stop submitting work before deployment.
2. Stop old servers, workers, schedulers, and recipe controllers. Cancel/abandon
   in-flight work; do not drain it into the new release or export/import jobs.
3. Reset/decommission old JobDB execution databases and job-artifact namespaces.
   Initialize fresh new-format stores and signing keys/audience as appropriate;
   reject all old tokens and discard client cursors, lease files, and job caches.
   The reset includes archived/hidden jobs and scheduled work, not just queues.
4. Reapply tenant settings, source-trust policy, and credential-provider setup
   from their administrative source of truth. Bootstrap each tenant's root cell
   with `jobdb cell create-root --tenant TENANT --repo REPO --url URL`.
   Explicitly create any ordinary cells needed before
   their first root-submitted job. No old-job scan or identity mapping is run.
5. Start only the new servers/clients/workers. Recreate desired schedules and
   submit fresh work by cell name. Old browser/CLI job links may return not found;
   no redirect or old-job inspection service is provided.
6. Verify root submission creates one cell/job, a later name-only submission
   uses its repo, stale repo assertions conflict, non-root creation is denied,
   and Cortex/worker discovery uses only the new registry. Keep old binaries out
   of the deployment; requests missing required fields, old lease formats, and
   pre-cell stores must fail validation rather than operate through fallback paths.

An application rollback cannot reuse new-format execution state with old code.
If the release must be replaced, stop it and provision a fresh compatible
runtime; do not restore old jobs as part of this plan. Git repositories and
recipe source files remain ordinary source inputs, independently of JobDB's
execution-state reset.
