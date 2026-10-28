# Migrate to compact job lists

`ListJobs` and `POST /v1/tenants/{tenantId}/jobs/query` now return compact
summaries by default. There is no request flag and no legacy-detail mode.

Go fields and HTTP property types remain available for source/wire compatibility,
and the removed projections are marked deprecated. **Field values change:**
existing consumers must stop relying on details in list responses. Empty values
mean “not loaded,” not “this job has no dependencies, memberships, or payload.”
Older servers can still populate these deprecated fields; migrated clients must
not depend on their presence. Coordinate rollout with consumers requiring those
values. This is not behaviorally transparent compatibility.

## What lists retain

- Job key/type, primary goal ID, parent ID, schema hash.
- Status and persisted completion status, including unfamiliar completion values.
- Stored next route, cancellation flag, creation/archive/availability/expiry/lease
  expiry timestamps where supported by the backend.
- Client payload revision, allowing consumers to detect changes without loading
  the payload on every refresh.
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

## Move detail reads to selection/inspection

| Deprecated list field | Replacement |
| --- | --- |
| `BlockingWait` / `blockingWait` | `GetJob.BlockingWait`, or `GetDependencyWait` using the job key and `BlockingWaitSummary.WaitID` |
| `OtherGoalIDs` / `otherGoalIds` | `GetJob.OtherGoalIDs` |
| `WaitFor` / `waitFor` | `GetJob.WaitFor` |
| `CompletionDetail` / `completionDetail` | `GetJob.CompletionDetail` |
| `ClientPayload` / `clientPayload` | `GetJob.ClientPayload` |
| `ExecutionState` / `executionState` | `GetJob.ExecutionState` |

`GetJob` now also exposes `CompletionStatus`, `CompletionDetail`, `WaitFor`, and
`NextRoute`. HTTP names are `completionStatus`, `completionDetail`, `waitFor`,
and `nextRoute`. The task route and task coordinates can therefore be read from
one detail response. Completed-job detail reads can also load stored results;
use the dependency-wait endpoint when only wait details are needed.

```go
page, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{
    TenantIds: []string{tenant}, PageSize: 100,
})
// Render page.Jobs using status, route, timestamps, and Metadata.
// On selection, fetch the details for that job:
info, err := runtime.GetJob(ctx, selectedJobKey)
// Use info.ClientPayload, info.ExecutionState, info.OtherGoalIDs, etc.
```

Do not automatically call `GetJob` for every row on every list refresh. Fetch
only selected/expanded jobs, or for an explicit operation needing their details.
Task discovery is such an operation: JobDB's workflow SDK now fetches detail
coordinates for the tasks it returns. Task completion remains guarded by the
recorded input hash/ordinals and route, so a stale handle cannot complete newer
work. Job-run inspection and worker run outcomes likewise obtain their required
details explicitly.

For the retained HTTP shape, `otherGoalIds` and `waitFor` are empty arrays,
`executionState` contains an empty compatibility object, and optional detail
properties are omitted/null/empty according to their existing encodings. In Go,
those fields have zero values. Do not interpret them as authoritative detail.

This change is scoped to ordinary job listing. `GetJob`, `GetJobRun`, dependency
inspection, and schedule-run inspection continue exposing their existing
inspection details. `JobSummary` is also embedded in schedule responses, so its
Go deprecation annotations must not be mistaken for removal from those APIs.

## Backend rollout

SQLite filters, sorts, and paginates in SQL with one query per page. Pure SQL
callbacks preserve the existing metadata equality, encoded prerequisite IDs,
and nanosecond deadline semantics. There are no per-result enrichment queries.
Selective metadata/status filters can still require examining many candidates;
this is not a promise of constant total query work. Large application metadata
also remains a response-size cost by request.

The core runtime exposes the compact projection, but an external scheduler can
still do unnecessary work before returning it. Update pgjobdb using the
[backend request](FEATURE-REQUEST-PGJOBDB-Compact-Job-Lists.md). Merely updating
JobDB does not remove pgjobdb's per-job blocking-wait queries.

See the [removal plan](REMOVAL-Deprecated-Job-List-Details.md) for eventual schema
cleanup; this release retains the deprecated properties and struct fields.
