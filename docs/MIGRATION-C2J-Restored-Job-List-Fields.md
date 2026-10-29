# c2j migration: restored stored fields in JobDB lists

Ordinary JobDB lists again return `CompletionDetail`, the full opaque
`ClientPayload`, `ExecutionState`, and `WaitFor`. c2j can render completion
messages, inspect published execution demand, and retain task coordinates and
prerequisite IDs from its existing list response. No detail flag, extra read,
or database migration is needed for this correction.

This supersedes the instructions to treat these four fields as detail-only in
the initial compact-list migration. The
[updated list contract](MIGRATION-Compact-Job-Lists.md) still excludes additional
goal memberships and evaluated dependency explanations.

## Required versions and deployment order

The feature request identified JobDB `0bd5f2c1c456` as c2j's affected dependency.
That revision omits the restored fields. No release tag is assigned by this
guide: use a published JobDB revision containing the restoration, and a pgjobdb
revision containing the matching `compact_job_lists.sql` and adapter changes.

1. Release JobDB with the restored SQLite, core, and toy projections and updated
   HTTP contract. Field names and types are unchanged.
2. Update pgjobdb's JobDB dependency to that release as well as its own list query
   and adapter. Build and deploy the updated server to every endpoint used by
   c2j. Updating only pgjobdb's SQL projection is insufficient when an older
   JobDB core still discards the fields.
3. Update c2j's JobDB dependency and rebuild embedded runtimes, CLI binaries, and
   services. Retain c2j's existing mapping if it already preserves supplied
   fields. Ensure other adapters between c2j and the server do likewise.
4. Once all supported backends return the restored contract, restore c2j's
   normal absent-payload execution semantics. Until then, retain the
   `not_loaded` interpretation for missing payloads on waiting list rows.

For the dependency updates in both the pgjobdb and c2j checkouts, set
`JOBDB_RESTORE_VERSION` to the chosen published tag or resolvable commit:

```sh
go get "github.com/colony-2/jobdb@${JOBDB_RESTORE_VERSION:?set the restored JobDB version}"
go mod tidy
go test ./...
```

Commit the resulting `go.mod` and `go.sum` changes in each downstream repository.
For local pre-release testing, use a temporary Go workspace containing JobDB,
pgjobdb, and c2j. Do not commit an absolute-path module replacement. The pgjobdb
checkout accompanying this change was tested against the local restored JobDB
using such a workspace; its published dependency pin still needs the release
step above.

Existing SQLite rows and PostgreSQL active/archive snapshots already contain the
required data. Do not reset databases, rewrite jobs, or backfill history for this
correction. Earlier storage-format migrations have their own prerequisites;
this guide does not replace those migration procedures.

## Preserve the supplied projection

Apply the same mapping to ordinary pages, active/archive selection, child-job
lists, keyed lists, workflow listings, and CLI JSON output.

| JobDB field | c2j handling |
| --- | --- |
| `CompletionStatus` and `CompletionDetail` | Preserve unfamiliar status strings and exact detail text, including whitespace and Unicode. Empty detail is valid. A retrying job may have no terminal completion. Do not synthesize completion from scheduler status. |
| `ClientPayload` and `ClientPayloadRevision` | Retain the complete payload and its paired revision. Interpret c2j's namespace locally while preserving every other application namespace. |
| `ExecutionState.RunPolicy` | Preserve the supplied run policy. |
| `ExecutionState.TaskWait` | Preserve input/output ordinals, input hash, and resume job type, including stored archived coordinates. These are coordinates, not task data. |
| `WaitFor` | Copy the stored prerequisite IDs in order. Do not resolve or evaluate their targets to populate a row. Terminal state reflects what the backend stored; a backend may clear prerequisites at completion. |
| `BlockingWaitSummary` | Retain the persisted wait ID and optional deadline for display and selected-wait inspection. |

Keep `ClientPayload` as `json.RawMessage` through Go projections and JSON output.
Do not decode and re-encode the entire value through `map[string]any`, whose
ordinary JSON decoding loses precision for large integers. To interpret a
namespace, decode a separate copy into typed fields, raw submessages, or a
number-preserving decoder. Preserve arrays, explicit JSON null, large integers,
and payloads up to the existing 65,536-byte limit without pruning or truncation.

The raw payload and revision come from the same backend list query. Do not
replace the payload with a digest, revision, or just `c2j.execution`. A later
lease can legitimately contain newer state than the discovery page.

## Missing payloads during rollout

There is no new capability flag on the list request or response. Enforce the
supported server version through deployment/version coordination; do not infer
it from an individual row, payload revision, or a nonempty field on another job.

| Backend contract | Missing payload on a waiting list row |
| --- | --- |
| Old compact backend, or unknown/mixed deployment | Keep `not_loaded`; omission cannot establish absence. |
| All supported backends upgraded to the restored contract | Treat it as stored absence and use c2j's established absent-payload interpretation. |

A present payload containing JSON `null` is distinct from an absent payload.
Keep that distinction through serialization and pass present values through
c2j's existing execution-demand validation. Invalid demand should retain its
existing error behavior.

Do not fall back to immutable submission hints while a server might be omitting
a newer published demand. Do not add `GetJob` calls to resolve this ambiguity.
If an old backend must be supported again after rollout, restore the compatible
`not_loaded` behavior before accepting its missing payloads as authoritative.

## Keep one backend page per list page

An ordinary c2j page must make exactly one JobDB `ListJobs` call and pass through
its continuation unchanged. Do not hydrate rows with `GetJob`, `GetWaitingTask`,
history, artifact, goal-membership, or dependency-explanation calls.

`OtherGoalIDs` and full `BlockingWait` remain unloaded on ordinary lists. Their
empty values do not mean there are no memberships or dependencies. Fetch those
projections only for explicit selected-job/wait inspection. Server-side goal
filtering continues to work without returning additional memberships.

Keep the currently rejected compatibility list filters rejected unless a
separate pagination contract is agreed. Inspecting a supplied payload is local
work, but scanning more backend pages to fill a filtered c2j page would violate
the one-page contract. This restriction is independent of returning full
payloads.

Cortex should retain these supplied fields in its list rows without introducing
per-row reads. Pulse should continue using the acquired lease as authoritative
before execution. Claiming before determining allocation requirements remains
compatible with this change; restoring list payloads does not require changing
allocation or execution policy. Until backend rollout completes, Pulse versions
that reject `not_loaded` still need the claim-first ordering described in the
feature request.

## Downstream acceptance checks

Run these checks in c2j before releasing the dependency/behavior update:

- Verify ordinary, archived, child, and keyed lists through embedded SQLite and
  the deployed remote pgjobdb server. Exercise workflow APIs and CLI JSON.
- Assert exact completion text, Unicode/whitespace, unknown completion status,
  empty detail, and a retrying job without terminal completion.
- Round-trip arbitrary application namespaces, arrays, JSON null, absent values,
  `9007199254740993`, and a 65,536-byte payload. Compare raw JSON or use a
  number-preserving comparison; a floating-point comparison can hide corruption.
- Check run policy, every task-wait coordinate, archived final state, and stored
  prerequisites. Publish a payload update and verify both payload and revision
  appear together on the next list.
- Instrument the c2j backend boundary for pages of 1 and 200 rows: one `ListJobs`
  call, no hydration calls, no additional page scans, and unchanged continuation.
  Exercise secondary-goal filters and pagination ties as well.
- Test old/unknown-backend omission separately from upgraded-backend absence.
  Confirm no immutable-hint fallback or implicit dependency reads were added.
- Verify workers use lease state when it differs from an earlier discovery page.

JobDB's shared compact-list suite covers direct and HTTP projections, including
raw payload values and archived task state. SQLite and pgjobdb tests also count
queries below the public API boundary and guard against history/membership/wait
explanation enrichment. These upstream checks complement the c2j mapping and
CLI checks above.
