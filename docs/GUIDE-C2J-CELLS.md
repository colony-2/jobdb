# c2j: adopt named JobDB cells

Status: implementation plan for colony-2/c2j. Authoritative behavior and release
barrier: [SPEC-Tenant-Cells.md](SPEC-Tenant-Cells.md). HTTP draft:
[jobdb-cells.proposed.yaml](../openapi/jobdb-cells.proposed.yaml).

This is a hard break. Rebuild against the new JobDB types and discard old local
jobs, captured contexts, supplied leases, caches, and compiled recipe histories.
Remove superseded cell code in the same release; there is no old-job decoder,
metadata adapter, parallel identity implementation, or fallback to an old server.
Git repositories and edited configuration are fresh-work inputs.

## Current touchpoints to replace

Reviewed c2j `f1a153b898c57e93e9b01477d08cc02c1509ee4b`:

| Area | Current behavior / replacement |
| --- | --- |
| [recipejob/target.go](https://github.com/colony-2/c2j/blob/f1a153b898c57e93e9b01477d08cc02c1509ee4b/pkg/recipejob/target.go) | Resolves short names, repos, and paths into repository targets. Replace with explicit name plus optional repo assertion and registry resolution. |
| [recipejob/start.go](https://github.com/colony-2/c2j/blob/f1a153b898c57e93e9b01477d08cc02c1509ee4b/pkg/recipejob/start.go) | Builds name/repo context without populating CellID. Submit required cell name and populate identity from JobDB's result/lease. |
| [starter/start_recipe.go](https://github.com/colony-2/c2j/blob/f1a153b898c57e93e9b01477d08cc02c1509ee4b/pkg/starter/start_recipe.go) | Emits cell_id/cell_name/repo identity metadata. Remove these identity projections and use first-class runtime fields. |
| [cellref/context.go](https://github.com/colony-2/c2j/blob/f1a153b898c57e93e9b01477d08cc02c1509ee4b/pkg/cellref/context.go) | Captured config resolves root and short names to repos. Replace authoritative identity resolution with the JobDB registry; config only supplies first-use repo assertions. |
| [permissions/issuer.go](https://github.com/colony-2/c2j/blob/f1a153b898c57e93e9b01477d08cc02c1509ee4b/pkg/permissions/issuer.go) | Converts jobs.submit into lease restrictions. Add cellNames attenuation and preserve JobDB's root/normal ceiling. |
| [story/internal/service/service.go](https://github.com/colony-2/c2j/blob/f1a153b898c57e93e9b01477d08cc02c1509ee4b/pkg/story/internal/service/service.go) | Filters by metadata names/IDs. Replace with first-class cell selectors. |

Also update `pkg/joblist`, `pkg/jobcontext`, `pkg/contextual`, `pkg/childbroker`,
`pkg/workspacesubmit`, `pkg/worker`, `pkg/hookpolicy`, recipe schema/compiler,
CLI commands/config inspection, and generated guides/tests.

## New name and configuration contract

Use `--cell NAME` for a cell name only. Add `--cell-repo REPO` for first-use
registration or an assertion about an existing mapping. Remove repository/local
path overloading of `--cell`. A caller with only a repo must explicitly choose
its cell name; never invent identity by stripping a basename.

Add explicit `self.cell` to the new `.c2/config` schema. For example:

```yaml
self:
  cell: payments
  repo: github.com/colony-2/payments
root:
  repo: github.com/colony-2/root
```

`root` is always the reserved JobDB cell name. `root.repo` supplies bootstrap
configuration, not authority. Administrators bootstrap it using
`jobdb cell create --name root --tenant TENANT --repo REPO --url URL`; submitting into
root cannot create it. Configured naming patterns may produce a repo
assertion for a specified new name; they cannot overwrite a registered mapping.
Update source configurations to this schema directly and delete the reader
branches that infer self identity from repo/basename alone. Dependents can remain
routing/discovery configuration, but they do not define the authoritative catalog
or grant submission rights.

For a registered cell, use JobDB's returned repo and generated ID. If config or a
flag also supplies a repo, normalize with JobDB's shared public normalizer and
reject disagreement. Clone transport and Git credentials remain c2j settings.
Delete duplicate repository-identity normalizers and metadata-based lookup
fallbacks; use the new shared contract everywhere.

An administrative repo PATCH may make local config assertions stale. Report
that conflict on fresh submissions; update config explicitly or use name-only
resolution. Submission never repoints a registered cell.

Administrative clients resolve through the cell management API. Lease-bound
helpers use `resolveCellWithLease` before loading a target recipe when only its
name is known. Resolution does not register a cell. For a missing target, a root
caller must have a repo assertion and a usable recipe source; missing target
content cannot be loaded from a repository that has not been initialized yet.

## Submission, execution contexts, and recipes

Every SubmitJob call, including self/embedded/child submission, serializes
`job.cell`. Send `cellRepo` on first use and optionally on later calls as an
assertion. Use an explicit job ID for retries and retain the original request.
Do not perform a separate ensure/create call before root child submission.
JobDB commits new cell registration with the first job; failure publishes neither.

The returned handle/lease supplies canonical cell name, `cellId`, and the job's
repo snapshot. Use that snapshot for execution, retry/replay, and restarts;
never replace a running or queued job's source with a fresh registry lookup.
Fresh submissions use the current registry mapping and retain the resolved repo
assertion through commit, so a concurrent repo edit cannot change their source.
Populate typed `Workflow.CellID` from this server value only. Keep
`context.workflow.cell` as the owning cell name and Git execution context as
source/ref/commit data. Use typed runtime fields for current/parent ownership and
inspection. Remove old JobMetadata cell_id/cell_name/repo identity projections
and their fallback readers; legitimate recipe input/provenance Git fields remain
execution data and must not be consulted as ownership authority.

When the cell ID is unavailable until submission commits, inject it from the
acquired handle/lease at execution startup. Do not guess it, derive it from repo,
or mutate the persisted initial chapter to fill it later. New recorded contexts
have one required format; remove old-context replay branches and obsolete
workspace-history policies that only support discarded jobs. Preserve current
explicit snapshot/empty-workspace semantics in the new format.

Distinguish job ownership from a node's working directory. A root-owned job can
prepare a workspace for another cell; `context.workflow.cell` stays root while
`context.workspace.cell` names that target. Working in a root workspace does not
make a non-root job root-owned. Child submission chooses its actual target name
explicitly and records the server-assigned ownership on the new job.

Change `workspace.cell` to accept a name only and add optional `workspace.repo`
for a bootstrap/assertion locator. `workspace.initial: empty` may prepare a new
repository workspace from those inputs without resolving a nonexistent ref or
registering a cell. Carry the repo assertion into child submission. Existing
workspace snapshot transfer and recipe-source trust checks still apply. Update
`recipes.run`/child-group inputs to carry names and repo assertions separately;
remove forms where a repository string is used as a cell name.

Restarts must supply a cell name resolving to the source job's ID and retain its
repo snapshot. To execute against a changed repo, submit a fresh job. Schedules
must bind to an existing active cell; they do not implicitly register missing
cells. Store expected repo with that ID; a changed mapping pauses emission with
`cell_repo_mismatch` until the target is explicitly updated/rebuilt. Already
emitted jobs retain their snapshots. Adopt new query/poll selectors; explicit
cell names resolve before pagination/acquisition, while an omitted selector is
bounded by current caller/worker authority.

## Permissions, clients, and removal work

A root owner lease automatically has tenant-wide job submission scope. Preserve
job-type/job-ID restrictions and implement optional `cellNames` attenuation in
permission parsing, intersections, issuer, broker callbacks, and SDK wrappers.
A delegation without jobs.submit has no submission or bounded-resolution access.
Non-root leases cannot inherit a root parent's ceiling by copying job context.
There is no separate cells.create lease permission or ensure API to introduce.

The broker must use its live owning lease for child calls and never substitute
an administrative credential to recover from denial. Root ownership comes from
JobDB; config, workspaces, recipe metadata, and environment variables cannot
establish it. Audit hook policies against the new registry and initialize fresh
policy from administrative configuration, without importing job-derived grants.

Replace `c2j cells` authoritative listing with JobDB's visible registry. If
configured dependents are still displayed, label them as routing configuration.
Update list/delete/goal/story filters to first-class cells; remove metadata-only
cell equality predicates and repo-as-ID fallbacks. Retired records remain
inspectable via the new state filter but cannot receive fresh work.

Embedded development/test runtimes use fresh-format stores and explicit fixture
bootstrap. They must exercise the same name-required/root semantics, not bypass
them with a metadata adapter. Delete old fixtures and helpers for compatibility;
add rejection fixtures only where the new contract requires rejection.

## Delivery and acceptance

1. Update dependency, configuration/CLI/recipe schemas, normalizer usage, and
   resolved-target models together.
2. Replace submitters, brokers, contexts, worker startup, policies, filters, and
   inspection paths; delete superseded implementations in the same change set.
3. Regenerate c2j schemas/docs and update recipes through the
   [recipes guide](GUIDE-RECIPES-CELLS.md). Rebuild Cortex against this c2j API.
4. Participate in the [fresh-state cutover](SPEC-Tenant-Cells.md#coordinated-hard-break-cutover);
   do not restart old workers or import their leases/history.

Test first root submission with name/repo, later name-only submission, mismatch
errors, implicit registration rollback, ordinary/root/delegated ceilings,
workspace-versus-owner identity, empty initial workspace, child retries,
renames/aliases, retirement, first-class filtering, and missing-cell rejection.
Assert that old metadata cannot supply a missing first-class field. Tests should
exercise fresh jobs, including normal replay of those new jobs.
