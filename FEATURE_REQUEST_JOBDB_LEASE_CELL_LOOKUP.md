# Feature request: cell lookup independent of job submission

Owner: JobDB. Consumer: c2j.
Status: implemented in JobDB and pgjobdb; coordinated consumer rollout pending.

## Problem

A c2j job owned by cell A needs to run an operation or hook in cell B's Git
workspace. It needs B's registered repository location, then uses its available
Git credentials to access that repository. It may have job submission disabled
and no authority to submit jobs to B.

JobDB's current lease-based cell resolver treats this lookup as submission
validation. Consequently, a job can have valid Git access to B's repository and
still fail to select its workspace because it cannot submit jobs to B.

Provide a supported, lease-authenticated metadata lookup that resolves a cell
name within the lease's tenant without requiring submission permission. Git
credentials and the Git server continue to authorize repository access.

## Current behavior

Inspected dependency:
`github.com/colony-2/jobdb v0.0.71-0.20261105015051-24f616ca4ffe`.
Paths below are relative to the JobDB repository unless marked c2j.

This dependency describes the original report. The current implementation has
already removed `LeaseSubmitCells`; unrestricted owner submission now covers
the tenant, while explicit principal and delegated restrictions still apply.

| Location | Current behavior |
| --- | --- |
| `pkg/jobdb/runtime.go` | `ExecutionLease.ResolveCell(context.Context, string)` is the public lease resolver. |
| `pkg/jobdb/runtime/toy/internal/toyimpl/cells.go` and `pkg/jobdb/runtime/sqlite/cells.go` | `ResolveCellWithLeaseByID` validates the live owning lease, then calls `cellstate.State.Resolve`. |
| `pkg/jobdb/runtime/cellstate/state.go` | `State.Resolve` validates submission. Another cell requires trusted `LeaseSubmitCells` authority and `jobs.submit`; rejection can appear as `ErrCellNotFound`. |
| `pkg/jobdb/runtime/remote/delegation_server.go` | Delegated `cells/resolve` requests require `policy.Submit` and are filtered by `policy.Submit.CellNames`. |
| c2j `pkg/executionruntime/git_profile.go` | The workspace resolver also checks `jobs.submit` and `jobs.cellNames` before forwarding lookup through the live owning lease. |

c2j can remove its local prerequisite, but JobDB's backend checks would still
prevent lookup. Calls using delegated credentials also encounter the HTTP gate.

## Requested API

Add a lookup-only operation available through a live execution lease. Suggested
Go shape, with final naming left to JobDB:

```go
LookupCell(ctx context.Context, name string) (ResolvedCell, error)
```

Use the existing `ResolvedCell` representation where practical: stable cell ID,
registered cell name, and canonical repository locator. Expose a corresponding
remote endpoint and client method. A possible route is the existing lease route
with a `cells/lookup` suffix.

Prefer a separate API so the existing `ResolveCell` contract and callers using
it for submission validation retain their behavior. The new operation should
reuse registry lookup and lease validation without invoking submission checks.

## Required semantics

### Lookup authority

- A valid owning or delegated execution lease can look up any active registered
  cell in that lease's tenant, including another cell outside its submission scope.
- Lookup works with no submit grant, with an empty delegated permission grant,
  and when an existing submit grant's cell-name filter excludes the target.
- Add no workspace permission, lookup grant, or per-hook cell allowlist. This
  operation supplies registry metadata; repository access uses Git credentials.
- Derive the tenant from the authenticated lease. Remote tenant/job/lease route
  parameters must match that authority. Validate the current lease and delegated
  credential on every request using existing lifecycle rules, including expiry,
  revocation, and lease replacement.
- Require no administrative credentials, configuration-read grant, or access to
  the tenant's full cell listing. Accept an exact cell name using existing name
  and alias rules, and return only the resolved identity and repository mapping.

### Results and errors

- Validate names with the existing cell-name rules. Return the registered
  canonical repository location; do not synthesize a location from a name.
- Preserve typed errors for malformed names, missing cells, retired cells, and
  invalid/lost lease authority, with consistent remote mappings. A target outside
  submission scope must not be treated as missing by this lookup API.
- Each successful call returns one coherent view of the cell's current registry
  mapping. Consumers can record the result for durable execution; lookup need not
  freeze future calls or change registry update semantics.
- Perform no cell creation, registry mutation, job submission, Git access, or
  expansion of lease/delegation permissions.

### Submission remains independently authorized

Actual job submission and restart submission must continue enforcing their
existing target-cell authority, delegated submit grants, and
cell-name filters. A successful lookup, or passing its returned ID/repository
into a later submission request, must not satisfy or bypass those checks.

Creating, editing, retiring, and listing cells retain their existing contracts.
The request adds a lookup operation rather than changing those operations.

## Implementation scope

Provide matching behavior in the toy, SQLite, and pgjobdb backends, core/runtime adapters,
and remote client/server paths. Include public API/OpenAPI definitions, generated
clients, execution-lease wrappers, and relevant test implementations.

Remote delegation routing must admit the new lookup for a valid delegated lease
independently of `policy.Submit`. Keep the existing submission routes and their
filters intact. JobDB, pgjobdb, and c2j will roll out together. Compatibility with
older servers or clients, version negotiation, and compatibility fallbacks are
not required. Lookup must use the existing lease without administrative access
or broader grants.

## c2j integration boundary

As part of the coordinated rollout, c2j will update its dependency and use this
API for workspace cell lookup, removing the local `jobs.submit` / `jobs.cellNames`
gate from that path. Repository assertions remain checked against the returned
registry mapping. Child-job submission retains its separate authorization.

c2j will also separate hook source identity from execution workspace. Hook
permissions remain in the authoritative JobDB configuration block, interpreted
by c2j. Existing secret grants still control obtaining Git credentials stored
as JobDB secrets. No new hook configuration field or JobDB capability is needed
for this lookup feature.

## Acceptance tests

Use the public lease API and real remote delegation middleware, with equivalent
coverage across embedded and remote runtimes:

1. Register active cells A and B. Acquire a lease for a job in A with no authority
   to submit to B. Lookup of B succeeds with its registered ID, name, and repo.
2. Repeat through a delegated lease with no submit grant, including an empty
   permission grant. Lookup succeeds; submission remains denied.
3. Give a delegated lease submission permission filtered to A. Lookup of B
   succeeds; submission to B still fails. Authorized submission to A still works.
4. After successful lookup, attempt ordinary and restart submissions using the
   returned target. Verify that all existing submission restrictions still apply.
5. Reject forged, expired, revoked, or superseded credentials and tenant/job/lease
   mismatches. Identically named cells in two tenants resolve only in the lease's
   own tenant; a name present only in another tenant remains unavailable.
6. Cover invalid names, missing and retired cells, canonical repo normalization,
   and existing alias behavior. Preserve typed errors across remote transport.
7. Update a cell's registry mapping between calls. Later lookup reflects the
   updated mapping without mutating a previously returned result.
8. Verify lookup creates no jobs or cells and grants no additional capabilities.
   Existing resolver, submission, cell administration, and delegation tests pass.

Deliver the implementation, regression coverage, public API documentation, and
a module version for the coordinated c2j rollout. Document any interface changes
required by execution-lease implementers.
