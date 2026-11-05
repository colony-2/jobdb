# Investigation: registered cells hidden from lease-scoped resolution

Investigated on 2026-11-05 against JobDB commit `24f616c` and pgjobdb commit
`2d980ab`, with read-only queries to the reported server. Related request:
[INVESTIGATION-REQUEST-JobDB-Lease-Cell-Resolution.md](../INVESTIGATION-REQUEST-JobDB-Lease-Cell-Resolution.md).

## Finding

The reported behavior was reproducible at JobDB `24f616c`: the backend imposed
an unintended same-cell default and required an additional `LeaseSubmitCells`
grant for other cells. That implementation did not match the intended contract:
`jobs.submit` permits all cells in the tenant unless a cell-name list narrows it.
The same all-or-listed-names model applies independently to `cells.create`.

The correction removes that extra grant and same-cell default. Explicit
principal restrictions and delegated selectors remain enforced. A registered
cell no longer needs a second permission merely because it differs from the
parent job's owning cell. `root` remains an ordinary name.

The exact historical production branch is still not proven: the deployed
server version, middleware, and failed requests' verified claims were not
available in the retrieved evidence. The old backend restriction reproduces
both observed failures without a registry or tenant-context defect.
Production checks were read-only; no lease was acquired, imported, renewed, or
reused, and no production configuration or job state was changed.

## c2j mapping

Checked c2j commit `472f2b4669e438cf260e7ee32c3df7a068042397`, matching the
recorded execution version (the recorded dirty changes remain unknown).

- `pkg/permissions/access.go` locally checks the action and optional name list
  before submitting a job or creating a cell.
- `pkg/permissions/issuer.go` maps `jobs.submit: true` to a JobDB submission
  grant. With no list, `Submit.CellNames` is nil; with a list, it contains the
  exact names. Job types and job IDs are unrestricted at this mapping layer.
- The same issuer maps unrestricted creation to `CreateCells.Any = true` and
  restricted creation to `CreateCells.Names`.
- `pkg/executionruntime/git_profile.go` checks the recipe's submission action and
  name list before resolving a workspace using the current execution lease.
- Ordinary remote submission and creation use the restricted delegation;
  embedded/controller-mediated paths apply the local guard. Hook-managed child
  submission uses the controller path to preserve its child-policy ceiling.

These mappings are correct. c2j was not dropping or mistranslating the lists.
Its permission and relevant execution-runtime tests pass against the corrected
JobDB source without c2j modifications.

## Failing condition in the previous implementation

At `24f616c`, `pkg/jobdb/runtime/cellstate/state.go` contained:

```go
c, ok := s.ByName(name)
if parent != nil && (!ok || c.ID != parent.ID) {
    a, trusted := jobdb.CellAuthorityFromContext(ctx)
    if !ok || !trusted || !a.LeaseSubmitCells[c.ID] ||
        jobdb.AuthorizeCell(ctx, tenant, c.ID, "jobs.submit") != nil {
        return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
    }
}
```

For the locally reproduced registered targets, `ok` is true and the target ID
differs from the parent cell ID. With the default administrative ingress,
`trusted` and principal `jobs.submit` are present, but
`a.LeaseSubmitCells[c.ID]` is false. That is the failing condition.

`AuthorizeCell` additionally checks a nonempty principal, matching tenant or
wildcard tenant, the permission, and membership in `Cells` when that map is
non-nil. A target grant does not override these restrictions.

In pgjobdb, `ResolveCellWithLeaseByID` first validates the parent lease and then
checks whether the name exists before calling `State.Resolve`. Both the missing
name check and the access-denial branch return the same error. Invalid stored
parent identity, expired/cancelled work, or an inactive parent cell instead
return `ErrExecutionLeaseLost` in this implementation.

## Production evidence retrieved

Read-only GETs returned HTTP 200 for the current job, its chapters, the registry,
the original job identified in chapter 0, its chapters, and the shared goal.

| Fact | Observation |
| --- | --- |
| Current job | `3LfpdJcrPi0541AQ2z938222370`, tenant `c2`, `COMPLETED` / `failed_app` |
| Owning cell | `gitvend`, ID `3LfLqRLdIsqPOCzYldh1Mqb6ceN` |
| `c2j` | ACTIVE, ID `3LfLqRETDDdYF406EfoXvuNIDPr`, created and last updated `2026-11-05T00:59:25.504236Z` |
| `root` | ACTIVE, ID `3LfpbCzTtSzAZZ4yCupo18QpNlq`, created and last updated `2026-11-05T05:04:05.900436Z` |
| Chapter 42 | `c2j` resolution failed with `not_found` at `05:04:23.594478546Z` |
| Chapter 43 | `root` resolution failed with `not_found` at `05:04:23.62898488Z` |
| Failing activation | `3LfpdOfyKgHDJTGMLWsmt9o5d6b`, started `05:04:23.585004005Z` |
| Recorded worker dependency | JobDB commit `24f616ca4ffe`, as reported in activation history; this does not identify the server binary |

Both task inputs contain the permission state described in the request, without
an explicit recipe grant or ceiling. Those inputs cannot establish backend
authority. A locally permissive recipe and a permissive delegation selector do
not add entries to `LeaseSubmitCells`.

The current job has 46 retained chapters. Original job
`6f4aa8aa-61ae-4a15-bc3e-d786de17d3ff` is also present and has 44. Both share goal
`3LflaIyhX3AxXryNx1ti91kvUAX` and the same owning cell. Entire chapter records
0–39 match exactly; the histories diverge at chapter 40. This is strong evidence
of retained/copied history and explains the original `job_id` in chapter 0.
It does not establish which job/lease IDs c2j used for the failed HTTP requests.

Chapter 40 records worker `Mac.lan:45317-3LfJSnENdbJ0cQlXk6r7pd7gI4v`. The failing
activation/result chapters do not include a worker ID or lease ID. That earlier
worker field should not be treated as proof of the failed requests' verified
worker identity. Principal, owner-versus-delegation token, signed restrictions,
and effective server authority remain unverified for those requests.

## Tenant, storage, and lookup behavior

The remote handler builds the parent key from the route tenant and job ID,
validates the lease token against that key and lease ID, and passes the verified
worker ID to the backend. It does not use chapter 0's application `job_id` to
select the parent.

The pgjobdb implementation loads the cell registry for that tenant inside its
cell transaction and validates the stored parent job/lease there. Administrative
registry reads and lease resolution use the same cell tables for the same
runtime/database. Name lookup uses current names and aliases; scope grants use
immutable IDs. There is no long-lived registry cache in this path that would
explain a newly registered target being invisible.

The completed production job cannot now prove that the failing request reached
the same deployed runtime/database as the later administrative reads. Proxy
routing and the actual historical request paths still require server logs or
deployment inspection. The current implementation and the controlled test do
not require a tenant or storage mismatch to explain the observed errors.

## Controlled PostgreSQL regression

`TestRemoteLeaseCellResolutionScope` in
`/pgjobdb/pkg/pgjobdb/runtime/integration/cell_resolution_scope_test.go` uses the
real remote API against a local PostgreSQL runtime with registered `gitvend`,
`c2j`, and `root` cells. It exercises owner, imported-owner, and restarted leases,
including retained chapter-0 input with an older job ID.

The original regression reproduced the old denial and demonstrated that the
extra target-ID grant was responsible. The corrected regression requires
unrestricted resolution and submission to succeed, while explicit principal
lists and delegated name lists continue to narrow access. There is no owning-cell
exemption from an explicit list. A missing submission action still denies it.

Command:

```sh
GOTOOLCHAIN=go1.26.1 GOWORK=off go test -race -tags=integration \
  ./pkg/pgjobdb/runtime/integration \
  -run '^TestRemoteLeaseCellResolutionScope$' -count=1
```

This tests the local PostgreSQL implementation. The deployed backend type and
binary were not independently identified.

## Corrective change

Remove the same-cell restriction and `CellAuthority.LeaseSubmitCells`. Normal
owner leases can submit to all registered active cells in their tenant.
Delegated `jobs.submit` with no new name restriction retains its ancestor scope;
an explicit name list narrows it. Delegated `cells.create` independently permits
all names or its listed names. Omitted actions remain denied. Trusted principal
`CellAuthority.Cells` restrictions, when supplied, still apply.

No c2j mapping change, registry recreation, special root permission, or new
cross-cell grant configuration is needed. The prior suggestion to configure
`LeaseSubmitCells` was a workaround for the unintended policy and is superseded
by this correction. Existing clients already send the supported selectors.

## Remaining production confirmation

For each failed request, correlate the route tenant/job/lease IDs, verified
worker and principal, owner/delegation classification, effective delegation
selectors, target-name lookup result, and effective cell-scope checks (including the old `LeaseSubmitCells` gate
when investigating an older server). Record authorization decisions without logging bearer tokens. Also
record the deployed server/backend version and database routing.

That evidence would distinguish the reproduced backend policy mismatch from a
different historical deployment or routing problem. The current evidence does
not support recreating either cell.
