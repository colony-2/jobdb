# C2J integration with explicit JobDB cells

JobDB owns cell identity and the name-to-repository registry. c2j can continue
using names, while retaining returned immutable IDs and repository snapshots in
job/lease context. See [JobDB contract](SPEC-Tenant-Cells.md).

Before submitting work, an authorized administrator or provisioning service
explicitly registers the cell with `POST /v1/tenants/{tenantId}/cells` or:

```sh
jobdb cell create --name payments --tenant acme \
  --repo github.com/acme/payments --url http://localhost:9047
```

The URL points at JobDB. Git repository provisioning remains a separate step
until the services are combined. A failed Git operation must not be reported as
successful cell provisioning; reconcile retries using the explicit registry.

`.c2/config` should supply names and optional repository assertions, not generate
or claim authoritative `cell_id` values. Existing repo-to-cell mappings can be
inputs to an explicit registration operation. They cannot create cells by
submission. The name `root` is ordinary and may be renamed or retired. Remove
reserved-name handling, automatic tenant-wide root scope, and implicit-create
fallbacks from c2j.

Submission requires `cell`; `cellRepo` optionally checks the registered locator.
After explicit creation, name-only submission is supported. Unknown cells fail
with `not_found` even if a repo is supplied. Surface that as a missing
registration, not a reason to retry submission with more metadata. Repository
mismatch requires an explicit configuration or registry correction.

Use the registry or a live lease's `ResolveCell` method for name resolution.
A normal lease covers all registered active cells in its tenant. A submission
permission with an explicit cell-name list covers only those names. Preserve
delegated job-type/job-ID/cell-name restrictions; no extra cross-cell grant is needed.
Cell creation is an explicit `CreateCell` call on a normal execution lease, or
an administrative registration. Delegated leases require a `cells.create` name
selector; creation does not expand their submission scope. Working in another repository does not change job ownership or
expand authority. Bind workspace Git operations to the returned repo snapshot;
keep Git refs separate from cell names and repository locators.

On PATCH, stable IDs survive name changes and previous names remain aliases.
New jobs use the current repo, while existing jobs and restarts keep their repo
snapshot. Schedules bind existing active cells and pause after repo mismatch;
update their targets explicitly before resuming. Arbitrary cell metadata is
application-owned and can be replaced through conditional PATCH.

This is a hard break: reset old execution state, explicitly register configured
cells, and use compatible JobDB clients and workers. Test missing-cell rejection,
explicit-create retries, name-only submission, aliases, repo mismatch, ordinary
root names, lease/delegation ceilings, and snapshot-preserving restart behavior.
