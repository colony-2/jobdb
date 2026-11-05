# pgjobdb cell implementation

Implement the [JobDB cell contract](SPEC-Tenant-Cells.md) through the native API,
runtime adapter, HTTP API, and generic `jobdb cell create` command. There is no
special root endpoint or permission. All cells are explicitly registered;
submission to a missing name fails regardless of repository input or caller
creation authority.

Expose `CreateCell` on execution leases through `CreateCellWithLeaseByID` in the
runtime scheduler adapter. Validate the live parent and create the cell in one
transaction using `cellstate.State.CreateWithLease`. Normal leases can create in
their tenant; delegated leases additionally need `cells.create`, enforced by the
remote delegation boundary. Registration does not expand submission scope.

Store cells, aliases, creation replays, and tenant cursor keys in ordinary rows.
Only arbitrary cell metadata remains JSON. Replays retain a normalized request
hash and the original result fields. Remove whole-cell/catalog/preparation JSON
columns and all root flags, constraints, and generated identity columns. Jobs,
goals, schedules, and preparations reference existing cell IDs with tenant/cell
foreign keys; keep repo snapshots in ordinary columns. Cell fields must not also
be embedded in legacy non-cell JSON snapshots.

Take the tenant admission advisory lock before row locks for registration,
PATCH, retirement, submission, scheduling, and acquisition. Revalidate live
lease authority and the existing cell at publication, including when a caller
supplies an existing transaction. Use savepoints so failed borrowed-transaction
operations cannot leave partial writes even if the caller subsequently commits.
No pending preparation reserves a nonexistent cell or a provisional cell ID.

Require unique tenant/name (including permanent aliases) and canonical current
repo across active and retired cells. Conditional PATCH is atomic. Renaming
`root` follows the normal alias rules; retiring it follows normal busy checks.
Repo edits affect fresh work and pause schedules with mismatched pinned repos.
Existing jobs, confirmations, and restarts retain their submission snapshots.

Retirement checks active jobs/leases, open goals, active schedules, and
unpublished preparations by immutable ID. Select authorized cells before
pagination or acquisition; intersect trusted lease targets with delegation
attenuation. Metadata never establishes cell ownership or authority.

Use a new storage format and reject previous databases without modifying them.
No old jobs, legacy SQL overloads, or compatibility decoders are preserved.
Pin the tested JobDB revision after committing its implementation. Run shared
runtime conformance directly and over HTTP, native integration tests, transaction
rollback/race tests, installer format checks, and static checks. Keep broader
normalization of old non-cell structured JSON in a separate change.
