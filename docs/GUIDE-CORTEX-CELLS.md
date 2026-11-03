# Cortex: adopt the JobDB cell registry

Status: implementation plan for colony-2/cortex. Shared contract and release
barrier: [SPEC-Tenant-Cells.md](SPEC-Tenant-Cells.md). Client dependency work:
[GUIDE-C2J-CELLS.md](GUIDE-C2J-CELLS.md).

Cortex ships only the new cell model in this release. Reset browser/server caches,
saved pagination cursors, and execution selections. Old jobs are discarded; old
job URLs return not found. Remove old cell catalog/filter code and response
fallbacks rather than retaining a second UI mode or legacy decoding path.

## Current touchpoints to replace

Reviewed Cortex `86f850a6d0791374641161348ab99c0457268153`:

- [server/internal/cortex/cells.go](https://github.com/colony-2/cortex/blob/86f850a6d0791374641161348ab99c0457268153/server/internal/cortex/cells.go)
  builds cells from c2j's self/dependent config and uses repository source as ID.
  Replace this catalog with JobDB List/GetCell results and generated IDs.
- [server/internal/cortex/handlers.go](https://github.com/colony-2/cortex/blob/86f850a6d0791374641161348ab99c0457268153/server/internal/cortex/handlers.go)
  resolves cell filters into repo metadata predicates. Replace them with
  first-class cell selectors, preserving authorization before pagination.
- `web/app/src/components/CellsList.tsx`, `utils/cellPresentation.ts`, job/goal
  views, submission pickers, types, URL state, and fixtures must consume required
  cell fields. Delete the metadata-driven Unknown cell identity/filter path;
  a missing required field is a protocol failure, not another cell option.

## Registry and UI identity

Use JobDB `Cell.id` as the row/cache identity, `name` as the visible label, and
`repo` as the repository locator. Preserve root/state/description/metadata and
revision where the UI needs them. Names may change; IDs do not. Do not use repo
URLs, display labels, loaded-job metadata, or configured dependents as identity.

The Cells view lists the durable registry even when cells have no jobs. Support
active/retired/all views and root designation using the backend filters. Paginate
through JobDB; expose only authorized results. Cell-name lookup resolves current
names and post-release rename aliases. Name/repo disagreement must be visible,
not silently reconciled in the browser.

Job rows get current canonical cell name and stable ownership from first-class
job response fields. A job's `cellRepo` is its submitted source snapshot; the
cell's `repo` is its current registry locator. Keep both meanings distinct after
repo edits and do not rewrite historical job source links. Cell filters send `cellNames` through c2j/JobDB before
pagination. Do not derive filter choices solely from the current jobs page or
run a second metadata-based filter that changes which cells appear selectable.
A renamed cell keeps its selection through stable ID; refresh its displayed name
from the registry before new submissions. Keep retired cells available for
historical inspection of jobs created in this release.

## Submission and management

Existing-cell submission sends the selected cell name. Name-only submission
uses the registry repo. Optional explicit repo input is an assertion; conflicts
are shown to the user. A root workflow creating a target supplies both name and
repo through its live lease; there is no separate UI ensure-cell transaction.

Browser requests are ordinary user/admin requests. Being on a root-cell screen
or selecting root in a picker does not give the browser a root lease. New-cell
submission outside an executing root job requires the user's administrative
creation authority. A request to a privileged root workflow must use an approved
entrypoint and code policy. Never synthesize a root principal or use a service
admin token to bypass the user's denied action.

Keep cell name and repo as separate input concepts. Remove the old behavior
where a repository URL doubles as a cell ID/name. A first-use repository needs an
explicit cell name. Report missing repo, mismatched repo, reserved name/repo,
retired cell, and insufficient authority distinctly using the new API errors.

If management screens are exposed, PATCH name/repo/description/metadata with
If-Match and reload on revision conflict. Metadata can contain arbitrary nested
JSON; PATCH replaces the supplied map, so preserve unedited keys. Repo edits
change the registry only; existing jobs keep their source snapshot. Surface
paused schedules requiring an explicit target update after a repo change.
Retire only when authorized and idle; show
cell_busy without implying cancellation/draining. Root cannot be renamed or
retired; disable its name editor but allow authorized repo/description/metadata
edits. Root bootstrap uses the administrative `jobdb cell create-root` command.
Git cloning/publication remains a separate concern; cell registration
success is not evidence that a repository has been published.

## Removal and delivery checklist

1. Update JobDB/c2j dependencies and replace server catalog, submission mapping,
   job filters, and response types.
2. Replace UI types and identity/filter code. Delete compatibility branches that
   accept missing IDs, repo-as-ID, or metadata-derived cell identity. Remove old
   saved-state hydration and version/cache keys so stale browser state is ignored.
3. Update fixtures, API/UI tests, examples, and README around the new registry.
   Old deep links/cursors are unsupported; do not add redirects to old job stores.
4. Deploy with the [hard-break release](SPEC-Tenant-Cells.md#coordinated-hard-break-cutover).
   No mixed old/new server or frontend bundle should remain in service.

Test registry cells with zero jobs, pagination/visibility, current names after
rename, aliases, active/retired filtering, root badges and restrictions, required
cell fields on rows, backend filtering across pages, valid name-only submission,
first-use name/repo submission, repo conflicts, and failure to gain privilege
through a UI selection or edited request. Verify no view reads legacy job
metadata to reconstruct cells.
