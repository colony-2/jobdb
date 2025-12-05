# SWF Job Listing APIs (Draft)

## Goals
- Provide read-only APIs to enumerate jobs from the pgwf backing tables with no Strata/story access.
- Support listing active jobs, archived jobs, and a combined view with filtering by status, job type, singleton key, and time windows.
- Return lightweight summaries suitable for UIs/CLI tooling; avoid fetching chapter payloads or artifacts.
- Keep pagination deterministic and cheap by leaning on Postgres ordering and keyset tokens.

## Non-Goals
- No Strata reads (stories/chapters/artifacts) and no cross-table enrichment beyond pgwf schema.
- No mutation (restart/cancel) and no task-level listing in this slice.
- No attempt-level history; summaries represent the latest known job row.

## Data Sources (pgwf-only)
- `pgwf.jobs_with_status` view for active/leaseable jobs. Fields used: `job_id`, `status`, `next_need` (parsed to job type), `singleton_key`, `available_at`, `expires_at`, `wait_for`, `created_at`, `lease_expires_at`, `cancel_requested`, `payload`.
- `pgwf.jobs_archive` for completed jobs. Treat presence as `COMPLETED`; if `archived_at` exists in the schema, surface it, otherwise leave `archived_at` nil.

## API Shape (Go)
```go
// Summary row built purely from pgwf Postgres data.
type JobSummary struct {
    JobID           swf.JobId
    Status          swf.JobStatus // Active statuses from jobs_with_status; COMPLETED when read from archive.
    JobType         string        // Parsed from next_need for active rows; empty when unknown.
    SingletonKey    *string
    WaitFor         []swf.JobId   // From wait_for
    AvailableAt     time.Time
    ExpiresAt       *time.Time
    LeaseExpiresAt  *time.Time
    CancelRequested bool
    CreatedAt       time.Time     // jobs_with_status.created_at
    ArchivedAt      *time.Time    // From jobs_archive when present
    Payload         json.RawMessage // Always included; sourced from payload for active rows, nil for archive.
}

type JobStore string

const (
    JobStoreActive   JobStore = "ACTIVE"
    JobStoreArchived JobStore = "ARCHIVED"
)

type ListJobsRequest struct {
    Statuses      []swf.JobStatus // e.g., ACTIVE, READY, CANCELLED, COMPLETED
    Stores        []JobStore      // default: both; modeled like statuses
    JobTypes      []string        // match parsed job type (from next_need)
    SingletonKeys []string
    CreatedAfter  *time.Time
    CreatedBefore *time.Time
    PageSize      int             // clamp to sane max (e.g., 200)
    PageToken     string          // opaque keyset token
}

type ListJobsResponse struct {
    Jobs          []JobSummary
    NextPageToken string // empty when no further pages
}

// New interface exposed by SWFEngine (non-exported to mirror existing patterns).
type jobsListApi interface {
    ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)
}
```
- `SWFEngine` embeds `jobsListApi`, and the toy engine implements the same API over its in-memory map.
- Pagination uses keyset: `(created_at, job_id)` as the cursor for stable ordering (newest-first only).

## Query Behavior
- Status/store routing:
  - If `Statuses` is provided, short-circuit table selection: `COMPLETED` -> archive; any other status -> active. Mixed sets query both.
  - If `Statuses` is empty, use `Stores` (default both). `JobStoreArchived` maps to archive; `JobStoreActive` maps to active.
- Active listings: select from `pgwf.jobs_with_status` with filters:
  - `status IN (...)` when provided.
  - `next_need` parsed to job type; filter with `job_type IN (...)` using `next_need IN (...) OR next_need LIKE 'jobType:%'` as needed.
  - `singleton_key IN (...)` when provided.
  - `created_at` bounded by `CreatedAfter/Before` when provided.
  - Always project `payload`.
  - Avoid `= ANY` patterns; use `IN` lists or OR chains for efficiency.
- Archived listings: select from `pgwf.jobs_archive`; map every row to `Status=COMPLETED`.
  - Apply filters only when the data is available; if a filter cannot be evaluated (e.g., job type not present), treat the row as non-matching rather than silently including it.
  - If `Statuses` excludes `COMPLETED`, skip archive entirely.
- Combined listings: single Postgres `UNION ALL` between active and archive projections, ordered by `(created_at, job_id)` descending, then keyset paginated. Apply filters in each branch; rows that cannot satisfy filters (e.g., job type on archive rows) are excluded in their respective branch.
- Job type extraction: parse `next_need` to derive job type (e.g., strip any `task` suffix like `<job>:<task>`); store both the raw `next_need` and parsed `JobType` to support filtering.

## Errors and Limits
- Validate `Statuses` and `Stores` against known enum values; reject unknown filters.
- Clamp `PageSize` to a small max (e.g., 200) to keep the calls lightweight.
- Filters that cannot be satisfied for a row (e.g., job type on archive rows without type data) cause that row to be excluded.

## Testing Plan
- Integration tests using embedded Postgres/pgwf schema:
  - Status-only filters route to the correct table(s) (COMPLETED -> archive; others -> active).
  - Pagination over more rows than `PageSize`, using the returned `NextPageToken`.
  - Combined listing yields both active and archived jobs, ordered by `created_at` then `job_id`.
  - Payload is always populated for active rows.
  - Filters on `CreatedAfter/Before`, `JobTypes`, and `SingletonKeys` behave as expected; archive rows are excluded when filters cannot match.
- Toy engine tests mirror the same contract over its in-memory store.
