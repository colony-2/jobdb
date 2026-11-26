# Project identity, creation receipts and first-use admission

A project is an explicit identity for the scope already stored as `tenant`.
Existing keys and historical bytes retain that identity. The accepted Direction
keeps `c2` as both ID and name, keeps tenant Go and storage contracts, excludes
rename and new authorization policy, and retains the tenant HTTP family during
manual deployment. New project names never address resources.

`pkg/jobdb/projects.go` adds a separate catalog capability without expanding
`WorkflowRuntime` or `ConsumerRuntime`. `projectcatalog.Service` validates
creation requests and persists immutable project records, creation receipts,
tombstones and first-use seals. New IDs are KSUIDs. Explicit installations may
seed the inventoried legacy IDs `c2` and embedded `0`.

The embedding connects its existing protected administration policy through
`WithProjectAdministration`. An absent callback denies catalog administration;
a cell permission or execution lease does not supply the callback. This adds no
principal, grant, role or login system. Resource admission grants no resource
permission and never replaces live lease checks.

## Creation and deletion

Creation requires a nonblank RequestID and a unique lowercase creation name.
Description and object metadata have bounded sizes and nesting. The decoder
rejects unknown fields, supplied IDs, duplicate keys and invalid JSON. Creation
canonicalization sorts object keys and preserves number tokens and array order;
`1` and `1.0` are different requests. Historical resource JSON is never passed
through that canonicalizer.

One transaction saves the project and its full creation receipt. An equal retry
returns the original ID, body, status, ETag and Location, including after project
deletion. A changed request with the same RequestID conflicts. Deletion retains
the immutable record and receipt, releases the name and never reuses the ID.
Matching strong If-Match is required. Lists use ID order and authenticated keyset
cursors bound to their catalog, name filter and page size, without a snapshot.

Deletion requires a trusted `CanDelete` check while the catalog transaction is
held. That check must establish complete participant and external-use coverage.
No check, any resource, any failed probe or uncertainty refuses deletion. The
current tests use synthetic complete coverage; they do not establish coverage
for a deployed runtime or an external provider.

## Admission before independent resource effects

`AdmitWrite` requires an installed catalog and an active ID. Before an independent
store can begin an effect, admission commits a permanent first-use seal. The
seal does not change public revision. Deletion and first admission serialize:
if deletion commits first, admission rejects; if admission commits first,
deletion rejects even if the resource write is delayed or its process dies.
A failed or uncertain downstream write may therefore make deletion permanently
unavailable. Expiry and resource removal never clear the seal.

Already sealed active records require no further mutation. They cannot be deleted,
so that read is sufficient for repeated admission. Missing installation markers
and unsupported versions fail closed. Constructors do not import existing scopes.

## Storage and installation

The memory store serializes through an authoritative mutex. SQLite and PostgreSQL
use one versioned catalog document containing project records and creation receipts.
Resource records remain in their existing stores. This keeps installation,
name uniqueness, receipts, tombstones and seals in one atomic boundary, at the
cost of reading the whole administrative catalog. It is intended for a small
project catalog; receipt growth and workloads should inform a later indexed
storage implementation without changing the public contract.

SQLite configures every borrowed connection's busy timeout, takes the writer lock
before reading, and uses FULL synchronous commits for catalog mutations.
PostgreSQL locks the installation row and requests synchronous commit locally.
Metadata and receipt bodies remain raw JSON inside BLOB/BYTEA storage; large
number tokens never pass through floating-point decoding. Database ownership
and PostgreSQL search paths stay with the caller.

Schema creation and installation are explicit. Installation requires a SHA-256
inventory manifest and an approved seed list; repeated installation compares the
same manifest and verifies imported identity/seal records. Imported projects start
sealed. The service trusts the upgrader to verify every participant before
installation. It does not discover stores, fence writers, back up data or certify
that an inventory is complete. A partial installation remains unavailable.

## Runtime and HTTP integration

Core, SQLite and toy compose `ProjectControl` and gate resource mutation,
preparation, chapter/artifact publication, configuration/schema writes and secret
issuance with durable admission. Shared Postgres chapter/schema stores and
configuration stores accept admission explicitly; external runtime owners must
wire the same catalog into every independently reachable writer. Missing admission
fails closed. Constructors do not install or seed a catalog. Test-only permissive
fixtures must never be selected by production composition.

The retained tenant routes/DTOs remain unchanged. The project resource family uses
`/v1/projects/{projectId}/...` and project identity envelopes. `remote.New` selects
the retained tenant contract; `remote.NewProjectRuntime` selects the new contract
explicitly. Neither falls back after a mutation failure. Both enter the same
protected business handlers. Existence is checked after the retained ingress
permission checks. Catalog CRUD additionally requires trusted administration;
execution and delegation credentials cannot administer the catalog.

OpenAPI generates both client models and explicit identity-envelope plans. The
adapter splices only declared identity keys using JSON token offsets. Metadata,
application JSON, schemas, signatures, artifacts and unknown fields are opaque;
the adapter does not recursively rename arbitrary tenant keys. Scoped poll and
query requests cannot override their path identity. Go and storage tenant fields
continue to mean immutable project ID.

The CLI advertises `--project`, retains `--tenant/-t` as input aliases and rejects
conflicting selectors before HTTP. Project creation prints a generated RequestID
before making an effect. Save and reuse it after uncertain results. Catalog get,
list and delete use IDs; names are discovery filters only.

## Explicit SQLite maintenance

`jobdb project installation initialize --database NEW_PATH --participants-sha256 SHA256`
creates fresh main storage and an empty installed catalog, with no automatic
project. `inspect --database PATH --participants-sha256 SHA256` hashes format-16
main records/schema without modifying them. Save the JSON, then use
`install --database PATH --inventory FILE --seed c2` (and separately approved
`--seed 0` where inventoried). Installation rechecks the exact inventory under the
writer lock and commits the catalog completion atomically. Unknown scopes,
stale inventories and unsupported/incomplete schemas refuse installation.

Fence all writers/providers and inventory independent stores, backups and keys
before maintenance. The supplied participant SHA-256 binds that separately
verified inventory; these commands do not inspect it or establish a runtime
fence. They preserve existing tenant IDs and resource bytes. Imported identities
start sealed. A failed initialization leaves fenced, uninstalled storage for
explicit recovery; opening a server does not complete installation.

## Publication and migration limits

This candidate is the JobDB prerequisite. Native pgjobdb scheduler/installer and
c2j/Cortex adoption require separate source changes against the published module.
The recipe verifies, merges and publishes; source tests establish no deployment
or production migration. Retain tenant URLs through manual fleet deployment and
until a later reviewed retirement.

Cortex operation durability remains a separate human choice. Actual runtime,
fleet, Cortex, viewer/ingress and required historical helper custody, independent
store coverage, retained program availability and old-link verification must be
established before dependent migration acceptance. They are not proven by the
JobDB tests. Default deletion remains disabled without complete trusted usage
coverage.

SQLite formats 11–15 require the separate, explicit offline migration before project inspection or installation. Project installation never performs conversion. Review the migration report and fence all participants before taking the format-16 project inventory, which includes 17 resource tables and job-resident goal/terminal records.
