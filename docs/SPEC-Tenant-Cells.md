# Tenant cells

This is the JobDB implementation contract. Integrations are described separately
in [C2J](GUIDE-C2J-CELLS.md), [pgjobdb](GUIDE-PGJOBDB-CELLS.md), and
[recipes](GUIDE-RECIPES-CELLS.md). The authoritative HTTP schema is
[OpenAPI](../openapi/jobdb-runtime.yaml); the cell-focused proposal is
[jobdb-cells.proposed.yaml](../openapi/jobdb-cells.proposed.yaml).

## Identity and explicit creation

A tenant owns a registry of cells. Each cell has a server-generated immutable ID,
a tenant-unique mutable name, a canonical repository locator, description,
arbitrary metadata, state, revision, and creation/update/retirement timestamps.
Jobs have tenant-wide IDs, not IDs scoped to a cell. A cell is intended to map to
one Git repository; JobDB stores the locator but performs no Git operations or
remote identity verification.

Cells must be explicitly created before submission. Submitting a job never
registers a cell, even when the caller has `cells.create` and supplies `cellRepo`.
The name `root` is ordinary: it conveys no authority and can be renamed or retired
under the same rules as every other cell. There is no root flag, binding,
bootstrap permission, root-only command, or root list filter.

Names are case-sensitive ASCII `[A-Za-z0-9._-]{1,128}`, excluding `.` and `..`.
Renaming permanently reserves the previous name as an alias for the same ID.
Current names and aliases are unique across active and retired cells. Renaming
to the cell's own alias is allowed. A name cannot be reused for another identity.

Repository locators normalize hosted HTTPS, SSH, and scp-style forms to
`host/path`: lowercase host, case-sensitive path, no trailing slash or `.git`.
The current canonical locator must be unique within the tenant, including
retired cells. Repointing a cell releases its former locator. Names, repo
locators, and immutable IDs are distinct; a changed locator never implies a
rename or a new identity.

## API and command

- `POST /v1/tenants/{tenantId}/cells`: requires `cells.create`; accepts `name`,
  `repo`, optional `description`, `metadata`, and `requestId`. Generates an ID
  and ACTIVE record atomically. No Git API call occurs.
- `GET /v1/tenants/{tenantId}/cells`: requires `cells.list`; filters by `name`
  (current or alias) and `state` (ACTIVE by default, RETIRED, or ALL), and pages
  with `pageSize`/`pageToken`. Only authorized cells are included.
- `GET /v1/tenants/{tenantId}/cells/{cellId}`: requires `cells.get`.
- `PATCH /v1/tenants/{tenantId}/cells/{cellId}`: requires `cells.get`,
  `cells.update`, and a current strong `If-Match`. Changes name, repo,
  description, or metadata atomically.
- `POST /v1/tenants/{tenantId}/cells/{cellId}:retire`: requires `cells.get`,
  `cells.retire`, and a current strong `If-Match`.

```sh
jobdb cell create --name payments --tenant acme \
  --repo github.com/acme/payments --url http://localhost:9047
```

`--url` is the JobDB API URL. Deployment authentication may use
`JOBDB_BEARER_TOKEN`. The command works against any conforming backend. Retrying
the command may confirm an existing ACTIVE cell with matching name/repository;
it does not update properties or change the POST idempotency contract.

Creation `requestId` is scoped to tenant and authenticated principal. Store a
hash of normalized creation properties and the original creation result for
the cell lifetime. Matching replay rechecks authority and retirement, then
returns the original snapshot and ETag; changed properties return
`request_conflict`. No request ID means POST is not idempotent.

Description defaults to empty string and metadata to `{}`. Description is limited
to 2048 characters; metadata to 64 keys and 16 KiB. Metadata values may be nested
objects, arrays, scalars, or null. PATCH is typed `application/json`, not JSON
Merge Patch: omitted properties are unchanged, metadata replaces the map,
`{}` clears it, and an empty description clears it. Empty patches, unknown
fields, and null top-level properties are invalid. Server-owned properties
cannot be supplied. A semantic no-op preserves the revision and update time.

Missing `If-Match` returns 428, malformed or weak validators 400, and stale
validators 412. Name/repo conflicts return `cell_conflict`; no partial updates
commit. Reads return strong ETags and `Cache-Control: no-store`.

List pages sort by immutable ID. Tokens bind tenant, principal, and repeated
filters and expire after 24 hours. Authorization is rechecked on every page;
traversal is live rather than a frozen snapshot. Name lookup returns zero or one
visible cell. Read operations never create cells.

## Submission and leases

Every submission requires `cell`, resolved through current names and aliases.
`cellRepo` is an optional assertion against the selected repository snapshot.

| Target | Result |
| --- | --- |
| Existing active cell, no repo assertion | Submit using registered repo. |
| Existing active cell, matching normalized repo | Submit using registered repo. |
| Existing cell, conflicting repo assertion | `cell_repo_mismatch`. |
| Missing cell, with or without repo | `not_found`; no cell, job, goal, or history is published. |
| Retired cell, new work | `cell_retired`. |

Administrative submission requires `jobs.submit` on the target. Normal execution
leases may submit to any registered active cell in their tenant. Delegated leases
require `jobs.submit`: omitted `cellNames` adds no restriction, an explicit list
allows only those names, and an empty selector allows none. Descendants inherit
ancestor restrictions and cannot widen them. There is no separate permission
for submitting to a cell other than the owning cell.

Lease-scoped `ResolveCell` uses the same action and cell-name scope. Trusted
principal policy, when supplied, may additionally restrict tenant, permissions,
and allowed cell IDs through `CellAuthority.Cells`. Those restrictions apply
equally to the owning cell and other cells; a cell outside the principal's list
returns `not_found`. No additional cross-cell grant is required.

Normal execution leases may explicitly register any cell name within their
tenant. Delegated `cells.create` grants permit all names with `{ "any": true }`
or just the names in `{ "names": ["example"] }`. Omitting the action denies it.
Creation and submission are independent actions: creation does not add a
submission permission or widen a submission name list. An unrestricted submitter
can use a newly created cell. `root` follows the same rules as every other name.

`POST /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/cells` accepts the
same properties as administrative creation and returns 201 with the cell, ETag,
Location, and `Cache-Control: no-store`. The lease method infers the tenant when
omitted and rejects a different tenant. It verifies the exact live parent in the
creation transaction, including on request-id replay. Replay identity is scoped
to the tenant, parent job, lease ID, and worker. A delegated `cells.create` selector
uses `{ "any": true }` or `{ "names": ["example"] }`; omission denies creation,
and descendants may only narrow it. Submission remains a separate permission.

The backend derives parent identity from a live stored lease, validating tenant,
worker, lease ID, expiry, cancellation, and stored ownership in the admission
transaction. Metadata, workspace labels, client headers, and serialized context
cannot establish authority. Lease validation runs again at publication, after
preparation and chapter writes. Expired/replaced leases cannot publish.

Job handles and leases carry resolved cell ID/name/repo. Jobs, goals, schedules,
and preparations have stable cell ownership. Job reads report the current name;
existing leases retain their issued snapshot. Existing-job confirmation and
restart preserve the original repository snapshot, even after a repo edit.
Confirmation of an already published job may succeed after retirement following
current authorization checks; restart into a retired cell is rejected.

Job IDs remain unique across the tenant. Explicit-ID retries must match the
original preparation and cell identity. Preparation reserves job/goal identities
but never cell identities; its cell foreign key must already exist. Failed
publication leaves no visible job/goal/history and discards an unpublished
preparation when the runtime can do so safely.

## Updates, schedules, and retirement

Repo PATCH affects fresh submissions. Existing jobs retain their recorded repo
for execution, retries, and restarts. Schedules pin a cell ID and repo snapshot;
repo changes pause active schedules with `cell_repo_mismatch`. Rebuild the target
explicitly before resuming. Schedule creation never registers a cell.

Retirement requires no nonterminal jobs, live leases, active schedules, open
goals, or unpublished preparations. Otherwise return `cell_busy`. Serialize
retirement with admission, acquisition, scheduling, and repo edits. Pending
preparations block retirement by cell ID, including after rename.

Retirement preserves ID, name/aliases, repo, metadata, and historical data.
Authorized reads remain available; new work and updates are denied. There is
no delete, restore, or implicit resurrection. Repeating retirement with the
current ETag returns the retired record.

## Persistence and authorization boundary

Use ordinary columns for every cell field except arbitrary metadata. Store
aliases in a related name table and idempotency replays in rows with ordinary
result fields and a descriptor hash. Store the cursor signing key as bytes.
Jobs, preparations, goals, and schedules have ordinary cell ID/name/repo columns.
Preparations additionally have requested name/repo and parent/prior job columns.
Use tenant/cell foreign keys, unique tenant/name and tenant/repo constraints,
and indexes for ownership checks. Do not serialize whole cells or preparations,
extract cell identities from JSON, or derive relational columns from JSON.

Existing non-cell structured JSON in goals, waits, schedules, and run policies
is outside this change and is to be normalized separately. Application metadata,
schemas, and other unstructured values supplied from above may remain JSON.

SQLite serializes mutations in its transaction domain. PostgreSQL takes the
tenant admission lock before row locks. Cell existence, current authority,
state, repository assertions, and idempotency are rechecked at publication.
No independent implicit registration transaction exists.

Embedding servers must install verified `CellAuthority` context. The standalone
CLI server installs its explicit administrative ingress. This implementation
covers registry administration, submission, discovery, and acquisition. It is
not a complete authorization retrofit for every history, artifact, goal, secret,
or mutation endpoint. Those surfaces retain their existing access contracts.
Lease/delegation v2 binds ownership; older token profiles are rejected.

## Hard break and validation

SQLite uses fresh format 11. No old jobs, migration path, compatibility commands,
or old lease decoders are retained. Stop old services, reset storage, explicitly
register configured cells, and start compatible clients/workers. Do not scan old
job metadata to recover cells. c2j `.c2/config` supplies deployment inputs, not
an alternative identity authority.

Conformance covers direct and HTTP runtimes: explicit creation, missing-cell
rejection, ordinary root rename/retirement, lease ceilings, aliases, property
updates, idempotency, repo snapshots, tenant-wide IDs, schedule mismatch,
authorized pagination/acquisition, retirement versus pending admission, and
concurrent mutations. Storage tests check that cell ownership is relational and
is not duplicated inside legacy JSON snapshots.
