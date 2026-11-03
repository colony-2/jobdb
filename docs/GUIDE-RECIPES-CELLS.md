# Recipes: adopt named cells and root-owned provisioning

Status: implementation plan for colony-2/recipes. Shared runtime contract:
[SPEC-Tenant-Cells.md](SPEC-Tenant-Cells.md). Required compiler/broker changes:
[GUIDE-C2J-CELLS.md](GUIDE-C2J-CELLS.md).

This is a hard break for recipe inputs, compiled contexts, and execution history.
Update source YAML and fixtures to one new contract. Old recipe jobs will not
resume or replay; remove alternate input forms and branches kept only to support
those histories. Keep ordinary retry/replay of newly created jobs correct.

## Ownership and source touchpoints

Recipes owns workflow routing, review gates, initialization, and Git publication.
JobDB owns cell registration/identity and lease authority; c2j owns the name/repo
inputs, workspace/compiler behavior, and submission transport.

The local recipes checkout reviewed at
`0e04887230fb2c879fda1a50fcddedfe718dd0ff` contains these adoption points:

- `create.yaml`: root review/orchestration and catalog handling.
- `recipes/create/publish.yaml`: switches target workspace and launches the
  initialization child; currently passes repository strings as cell targets.
- `recipes/create/initialize.yaml`: target-owned initialization and publication.
- `recipes/adaptive/create-request.yaml`, `recipes/kernel/create-request.yaml`,
  coordination/consultation and resolve-target helpers: target/root routing.
- Build/workspace recipes using `context.git.repo` as `workspace.cell`, plus
  generated guides and native recipe tests for those forms.

The existing root-reviewed creation proposal is useful for product approval and
Git publication, but its registration/catalog steps must follow this new JobDB
contract. Do not preserve a second config-only registration workflow.

## Root authority is job ownership

Run orchestration that can create cells as a job actually submitted into the
special `root` cell. Changing a node's workspace to root does not grant a root
lease. Likewise, switching a root job's workspace to a target repository does
not change the owning root job's lease authority.

Administrators first bootstrap root with `jobdb cell create --name root`; workflows
cannot implicitly create or rename this reserved cell.

A non-root workflow requests provisioning through an approved root-workflow
entrypoint with the required target authority. If it lacks authority to submit
that root job, route the request through the trusted controller/admin path;
do not copy a root label into context or reuse an administrative credential.
The root workflow must execute trusted reviewed code because its owner lease
can submit to every cell name in the tenant.

Root delegates still require jobs.submit. Restrict job types/IDs/cell names for
untrusted helpers as appropriate through c2j's attenuation model. Do not introduce
a separate cells.create lease permission or call an ensure-cell API.

## Name/repo contract and initialization flow

Use names in cell/root/consultant routing inputs. The root name is exactly
`root`; repository URL and Git ref are separate fields. Update repository-as-cell
expressions directly, without a compatibility union that accepts both forms.
With the new c2j workspace schema, an initial workspace can use:

```yaml
workspace:
  cell: payments
  repo: github.com/colony-2/payments
  ref: main
  initial: empty
```

`workspace.repo` is a new c2j field specified in the c2j guide, not functionality
claimed to exist today. It supplies bootstrap/validation data; entering the
workspace alone creates no JobDB cell and grants no new authority.

The root workflow should:

1. Review the proposed responsibility, cell name, repository, and initial source.
2. Prepare a target workspace without requiring a nonexistent ref to resolve.
   Load or embed the initialization recipe from an available trusted source;
   do not require it to be fetched from the uninitialized target repo.
3. Submit the initialization child under its live root lease with the explicit
   target cell name and repo assertion. c2j fills required JobDB fields. JobDB
   atomically publishes the new cell and child job; invalid submission publishes
   neither. Record returned cell ID, canonical name, repo, and job ID.
4. Let the target-owned initialization job create/publish Git contents using its
   separate Git credentials. The child runs with ordinary target-cell authority,
   not the parent's root ceiling.
5. Verify Git publication and initialization outcomes separately from JobDB
   registration. Registration does not prove the remote repository exists or
   was published. A failed initialization may leave a valid registered cell and
   failed job; resolve the same cell and retry/restart under the new rules.
6. Use name-only targeting for later jobs, optionally retaining repo assertions
   to catch stale configuration. Do not edit `.c2/config` as a second registry.
   Routing/dependent configuration may be updated as application policy after
   registration, but JobDB remains the catalog and identity authority.

Cell creation needs no combined Git server or Git-provider ID. Repository
publication can occur after registration; the registered locator reserves the
intended pairing. Git-side failures are workflow failures, not permission to
silently recreate or rebind a JobDB cell under another name/repository.

Administrators can change a cell's current repo with PATCH. Fresh work resolves
that mapping; existing jobs and their retry/replay/restarts keep their submitted
repo snapshot. A recipe's old repo assertion must produce a visible conflict on
fresh submission rather than rebinding the cell. Submit a new job deliberately
to use a changed repo, and explicitly update/rebuild any paused schedule target.

## Required removals and validation

Remove repo-valued cell selectors, legacy name/ID shape adapters, job-metadata
ownership probes, config-only registration success checks, and fallback root
selection from the current workspace/repository. Use authoritative owning-cell
context and registry results. Keep Git source/ref/commit fields as execution
data, separately from cell identity.

Update approval/publication artifacts to carry explicit name/repo/returned ID
fields and one fresh-format contract. Replace old fixtures and documented YAML
forms; no old-job output adapters or two-stage deprecation deployment are needed
for this hard-break release.

Coordinate with c2j before publishing YAML that uses the new workspace/child
input fields. Ship with the [shared release barrier](SPEC-Tenant-Cells.md#coordinated-hard-break-cutover),
then submit fresh root workflows. Do not resume previous creation jobs or import
their pending publication records.

Tests should cover actual root versus root workspace, missing repo rejection,
first-child atomic registration, name-only reuse, config mismatch, restricted
root delegates, non-root failure to create, target-owned empty initialization,
Git publication failure after successful registration, retry with stable job/cell
identity, and retired-cell rejection. Retain workflow review/publication gates
while removing the old identity mechanisms.
