# Recipes and explicit cell provisioning

Recipes use tenant cell names for targets and separate repository locators and
Git refs. JobDB supplies stable cell identity and repo snapshots; see the
[contract](SPEC-Tenant-Cells.md) and [c2j integration](GUIDE-C2J-CELLS.md).

Provisioning requires two explicit operations: provision or identify the Git
repository, then register the JobDB cell through administration or an explicit
lease `CreateCell` call. Normal execution leases may create cells within their
tenant; delegated leases need a `cells.create` name selector. Submit initialization
work only after registration succeeds and submission access is granted. A cell
named `root` has no special authority.
Product review workflows may retain that name by convention, but permission
must come from trusted policy rather than naming or workspace context.

Keep review/approval steps separate from the registration credential. Route
approved registration to a service or delegated lease with `cells.create`; do not copy privileged
metadata into child context. Cross-cell submission requires an explicit trusted
target grant, still constrained by delegation restrictions. A child job gets
the ordinary authority of its own cell when acquired.

Use a creation request ID for retried registration, then submit by cell name
with an optional repo assertion. Unknown targets fail without side effects.
Do not treat a successful Git push, config edit, or workspace switch as evidence
that the JobDB cell exists. PATCH repo or metadata explicitly when needed and
honor ETags; existing job snapshots do not change.

Reset old execution state for the hard break. Register configured cells before
starting new workflows. Cover explicit registration success/failure/replay,
missing-cell rejection, root as an ordinary name, cross-cell permission denial,
alias resolution, repo mismatch, and restart snapshot preservation in tests.
