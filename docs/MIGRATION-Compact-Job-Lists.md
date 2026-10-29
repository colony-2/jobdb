# Migrate to compact job lists

`ListJobs` and `POST /v1/tenants/{tenantId}/jobs/query` return stored job summaries
without per-row enrichment. There is no request flag or legacy-detail mode.

The initial compact migration overpruned stored columns. Ordinary lists now
restore `CompletionDetail`, full `ClientPayload`, `ExecutionState`, and `WaitFor`.
Their existing meanings, types, and wire names are unchanged. Only additional
goal memberships and evaluated dependency explanations remain detail-only.

## What lists retain

- Job key/type, primary goal ID, parent ID, schema hash.
- Status, persisted completion status (including unfamiliar values), and exact
  completion detail. Empty detail is valid; retrying jobs need not have a
  terminal completion.
- Stored next route, cancellation flag, creation/archive/availability/expiry/lease
  expiry timestamps where supported by the backend.
- Full opaque client payload and its revision from the same list query. All
  application namespaces, JSON null, arrays, and numeric precision are retained;
  payloads are not truncated or restricted to c2j fields.
- Execution state, including run policy and stored task-wait input/output
  ordinals, input hash, and resume job type. Archived jobs use stored final state.
  These coordinates are not task data and require no task or artifact read.
- Stored `WaitFor` prerequisite IDs, without resolving their targets.
- **All application metadata**, preserving existing metadata filtering and UI
  labels. Runtime-private metadata remains excluded as before.
- `BlockingWaitSummary` / `blockingWaitSummary`, containing `waitId` and an
  optional `deadline`. This reads persisted pending wait state, projecting an
  elapsed deadline as no longer blocking. It does not evaluate target goals,
  enumerate candidates, or explain why a dependency is pending.

Status still uses scheduler readiness semantics. Filtering by status, available
routes, additional goal membership, metadata, tenant, job key, parent, creation
time, store, and visibility still happens before selecting the returned page.
The returned `nextRoute` remains the stored route even when an alternate is due.

## Move enriched reads to selection/inspection

| Deprecated list field | Replacement |
| --- | --- |
| `BlockingWait` / `blockingWait` | `GetJob.BlockingWait`, or `GetDependencyWait` using the job key and `BlockingWaitSummary.WaitID` |
| `OtherGoalIDs` / `otherGoalIds` | `GetJob.OtherGoalIDs` |

These two fields retain their source/wire shapes for compatibility. Their empty
list values mean “not loaded,” not “no dependencies or memberships.” On HTTP,
`otherGoalIds` is an empty array and `blockingWait` is omitted when unloaded.

Do not call `GetJob` for every row to recover completion messages, payloads,
prerequisites, or task coordinates. Use explicit inspection for selected jobs
needing additional memberships, evaluated waits, task data, or results.
Task completion remains guarded by recorded input hash/ordinals and route.
Workers must use their lease's current state as authoritative before execution.

This change is scoped to ordinary job listing. `GetJob`, `GetJobRun`, dependency
inspection, and schedule-run inspection keep their existing inspection details.
`JobSummary` is also embedded in schedule responses, so its remaining Go
annotations must not be mistaken for removal from those APIs.

## Backend rollout

For c2j dependency updates, mixed-version handling, and downstream validation,
follow the [c2j migration guide](MIGRATION-C2J-Restored-Job-List-Fields.md).

SQLite filters, sorts, and paginates in SQL with one query per page. Scheduler
state and prerequisite IDs are decoded locally with the existing codecs. Core
maps stored fields without calling detail getters, and toy and HTTP runtimes
preserve the same projection.

pgjobdb must return stored completion/task/prerequisite fields from active and
archive rows, run policy from the existing facts join, and full raw JSON payload
from the existing client-state join. Keep page selection and existing joins;
no enriched detail query or new storage columns are needed. See the
[backend request](FEATURE-REQUEST-PGJOBDB-Compact-Job-Lists.md).

Selective filters can still examine many candidates, and full metadata/payloads
add read and serialization bytes. Neither cost justifies discarding stored
fields or adding client hydration loops.

Coordinate the JobDB dependency release, deployed pgjobdb upgrade, and downstream
c2j pin. An older compact server still omits the restored fields, so missing
payloads cannot yet be treated as authoritative absence by mixed-version
clients. A c2j-only release cannot restore server-omitted values. Payload-based
client filters need a separate pagination decision; scanning extra backend
pages to fill a filtered page would break the one-page contract.

See the [removal plan](REMOVAL-Deprecated-Job-List-Details.md) for eventual cleanup
of only the two enriched projections.
