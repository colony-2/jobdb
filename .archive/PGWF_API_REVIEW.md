# pgwf-go API Review: Answers to Open Questions

## Overview

This document reviews the actual pgwf-go query APIs as implemented in `/pgwf/pgwf-go/pkg/pgwf/query.go` to answer all open questions from the swf-go migration spec.

---

## Question 1: Multi-Tenant Filtering in ListJobs

**Question**: Does pgwf.ListJobs support multiple tenants in one query?

**Answer**: ❌ **NO** - Single tenant only

**Evidence**:
```go
// Line 91 in query.go
type ListJobsOptions struct {
    TenantID        string      // Single tenant (required)
    // ...
}

// Line 1083-1085 - Validation
if opts.TenantID == "" {
    return nil, wrap(ErrInvalidOptions, fmt.Errorf("tenant_id is required"))
}

// Line 1110-1112 - Query always filters by single tenant
conditions = append(conditions, fmt.Sprintf("tenant_id = $%d", argIdx))
args = append(args, opts.TenantID)
```

**Implications for swf-go**:
- swf-go's `ListJobs` accepts `TenantIds []string` (multiple tenants)
- Must make **multiple pgwf.ListJobs calls** (one per tenant) and merge results
- Or implement client-side filtering after fetching with empty tenant filter (NOT SUPPORTED)
- Pagination becomes complex when merging results from multiple tenants

**Recommendation**:
- **Option 1**: Make multiple pgwf.ListJobs calls in parallel and merge results in swf-go
- **Option 2**: Extend pgwf API to support `TenantIDs []string` (requires pgwf-go change)
- **Option 3**: Keep custom SQL in swf-go for multi-tenant queries

---

## Question 2: FindJobs Multi-Tenant Support

**Question**: Does FindJobs support multiple tenants?

**Answer**: ✅ **YES** - Multiple tenants supported

**Evidence**:
```go
// Line 134 in query.go
type FindJobsOptions struct {
    TenantIDs []string  // Multiple tenants supported (empty = all tenants)
    Status    JobStatus
    NextNeed  string
    Limit     int
}

// Line 744-748 - Implementation supports tenant array
if len(opts.TenantIDs) > 0 {
    tenantFilter = fmt.Sprintf("tenant_id = ANY($%d) AND ", argIdx)
    args = append(args, pq.Array(opts.TenantIDs))
    argIdx++
}
```

**Implications for swf-go**:
- ✅ `FindTasksWaitingForCapability` can directly use pgwf.FindJobs with multiple tenants
- No client-side filtering needed
- Straightforward migration

---

## Question 3: Job Type Pattern Matching

**Question**: Does pgwf support OR patterns for multiple job types?

**Answer**: ⚠️ **PARTIAL** - Single SQL LIKE pattern only

**Evidence**:
```go
// Line 93 in query.go
type ListJobsOptions struct {
    JobTypePattern  string      // Single SQL LIKE pattern
    // ...
}

// Line 1127-1130 - Implementation
if opts.JobTypePattern != "" {
    conditions = append(conditions, fmt.Sprintf("next_need LIKE $%d", argIdx))
    args = append(args, opts.JobTypePattern)
    argIdx++
}
```

**What's Supported**:
- ✅ Single LIKE pattern: `"workflow%"` (all job types starting with "workflow")
- ✅ Single LIKE pattern: `"%:emailTask"` (all job types with emailTask)
- ✅ Single exact match: `"workflow:emailTask"`

**What's NOT Supported**:
- ❌ Multiple job types: `["workflow1", "workflow2"]`
- ❌ OR patterns: `"workflow1" OR "workflow2"`
- ❌ Complex patterns: `["workflow%", "batch%"]`

**swf-go Requirements**:
```go
// swf-go ListJobsRequest supports:
type ListJobsRequest struct {
    JobTypes []string           // Multiple job types
    JobTasks []JobTaskFilter    // Multiple job:task combinations
}
```

**Implications for swf-go**:
- swf-go needs to filter by **multiple** job types and job:task combinations
- Must either:
  1. Make multiple pgwf.ListJobs calls (one per pattern) and merge
  2. Fetch all jobs and filter client-side (inefficient)
  3. Build single LIKE pattern that matches all (limited capability)
  4. Keep custom SQL in swf-go for complex filtering

**Recommendation**:
- **Option 1**: For simple single job type filters, use pgwf.ListJobs
- **Option 2**: For complex multi-pattern filters, keep custom SQL
- **Option 3**: Extend pgwf API to support `JobTypePatterns []string` with OR semantics

---

## Question 4: Payload Inclusion

**Question**: Which APIs include payload?

**Answer**: Varies by API

| API | Payload Included? | Control |
|-----|-------------------|---------|
| `GetJobStatus` | ✅ Always | No option |
| `GetJob` | ⚠️ Optional | `GetJobOptions.IncludePayload` flag |
| `ListJobs` | ❌ Never | No option (excluded for efficiency) |
| `FindJobs` | ❌ Never | No option (excluded for efficiency) |
| `GetJobStatusBatch` | ✅ Always | No option |

**Evidence**:
```go
// GetJobStatus - Line 227-239 - Always includes payload
SELECT ... payload, ... FROM pgwf.jobs_with_status

// GetJob - Line 498-501 - Optional
payloadCol := "NULL as payload"
if opts.IncludePayload {
    payloadCol = "payload"
}

// ListJobs - Line 1197-1211 - Never includes payload
// JobListItem type (line 68-86) doesn't have Payload field

// FindJobs - Line 751-765 - Never includes payload
// JobInfo type (line 141-158) doesn't have Payload field
```

**Implications for swf-go**:
- ✅ Current swf.ListJobs doesn't return payload - matches pgwf behavior
- ✅ If swf-go needs payload, must call `GetJob(IncludePayload: true)` separately
- ⚠️ `GetJobStatus` always fetches payload (potential performance impact)

---

## Question 5: Cursor Pagination Implementation

**Question**: Is cursor-based pagination fully implemented?

**Answer**: ❌ **NO** - Placeholder only

**Evidence**:
```go
// Line 1246-1250 in query.go
// For now, leave cursor implementation as empty string
// Full cursor-based pagination can be added later if needed
if hasMore {
    result.NextCursor = "next-page" // Placeholder
}
```

**Current Behavior**:
- ✅ Limit-based pagination works (fetch N items)
- ✅ `HasMore` flag works (fetches limit+1 to detect more results)
- ❌ Cursor is hardcoded to `"next-page"` string
- ❌ Cannot actually paginate through pages using cursor

**Implications for swf-go**:
- **BLOCKING ISSUE** for ListJobs migration
- swf-go currently has working cursor pagination (encodes last seen timestamp + job ID)
- Cannot migrate to pgwf.ListJobs until cursor pagination is implemented
- Would lose existing pagination functionality

**Options**:
1. **Keep swf-go's custom SQL** until pgwf implements proper cursors
2. **Implement cursor in swf-go** by calling pgwf multiple times with offset (inefficient)
3. **Contribute cursor implementation** to pgwf-go
4. **Break pagination** temporarily (not acceptable for production)

**Recommendation**: **Keep custom SQL in swf-go** for ListJobs until pgwf cursor pagination is implemented

---

## Question 6: IncludeArchived Default Behavior

**Question**: Does ListJobs include archived jobs by default?

**Answer**: ❌ **NO** - Archived jobs excluded by default

**Evidence**:
```go
// Line 97 in query.go
IncludeArchived bool // Whether to include archived jobs (default: false, only active)

// Line 1161-1194 - Implementation
if opts.IncludeArchived {
    // Query both active and archived with UNION
} else {
    // Query only active jobs (default)
}
```

**Implications for swf-go**:
- swf-go's ListJobs currently includes both active and archived by default (unless `Stores` is specified)
- Must explicitly set `IncludeArchived: true` to match current behavior
- **Behavior difference** if swf-go doesn't set the flag

---

## Question 7: Archived Job Status

**Question**: What status do archived jobs return?

**Answer**: Special "COMPLETED" status (not in JobStatus enum)

**Evidence**:
```go
// Line 368-375 in query.go - GetJobStatus for archived jobs
// Archived jobs: status is CANCELLED if cancel_requested, otherwise assume completed
if info.CancelRequested {
    info.Status = JobStatusCancelled
} else {
    // We don't have a COMPLETED status in the view, so we infer it for archived jobs
    // that weren't cancelled
    info.Status = JobStatus("COMPLETED")
}

// JobStatus constants (line 16-24) - COMPLETED not defined
const (
    JobStatusActive         JobStatus = "ACTIVE"
    JobStatusCancelled      JobStatus = "CANCELLED"
    JobStatusAwaitingFuture JobStatus = "AWAITING_FUTURE"
    JobStatusPendingJobs    JobStatus = "PENDING_JOBS"
    JobStatusCrashConcern   JobStatus = "CRASH_CONCERN"
    JobStatusExpired        JobStatus = "EXPIRED"
    JobStatusReady          JobStatus = "READY"
)
// Note: No JobStatusCompleted constant!
```

**Implications for swf-go**:
- pgwf returns `JobStatus("COMPLETED")` for archived jobs
- This is **not a defined constant** in pgwf package
- swf-go must handle this special status value
- Status mapping must account for "COMPLETED" string

**Recommended Status Mapping**:
```go
func convertPgwfStatusToSwf(status pgwf.JobStatus) swf.JobStatus {
    switch status {
    case pgwf.JobStatusReady:
        return swf.JobStatusReady
    case pgwf.JobStatusActive:
        return swf.JobStatusActive
    case pgwf.JobStatusCancelled:
        return swf.JobStatusCancelled
    case pgwf.JobStatusAwaitingFuture:
        return swf.JobStatusAwaitingFuture
    case pgwf.JobStatusPendingJobs:
        return swf.JobStatusPendingJobs
    case pgwf.JobStatusCrashConcern:
        return swf.JobStatusCrashConcern
    case pgwf.JobStatusExpired:
        return swf.JobStatusExpired
    case JobStatus("COMPLETED"):  // Special undocumented status
        return swf.JobStatusCompleted
    default:
        // Unknown status - log warning
        return swf.JobStatusReady
    }
}
```

---

## Question 8: API Function Signatures

**Question**: Are these module-level functions or client methods?

**Answer**: **Module-level functions** - Not methods on a client object

**Evidence**:
```go
// All query functions are module-level functions that take DB as first param
func GetJobStatus(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (*JobStatusInfo, error)
func CheckJobExists(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (*JobExistence, error)
func GetJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, opts GetJobOptions) (*JobDetail, error)
func FindJobs(ctx context.Context, db DB, opts FindJobsOptions) ([]JobInfo, error)
func ListJobs(ctx context.Context, db DB, opts ListJobsOptions) (*ListJobsResult, error)
// etc.

// DB interface (line 28-31 in types.go)
type DB interface {
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
```

**Implications for swf-go**:
- **No client object needed** - just call pgwf functions directly
- Pass `*sql.DB` or `*sql.Tx` as the DB parameter
- Can use transactions by passing `*sql.Tx`

**Migration Spec Correction**:
The migration spec mentions adding `pgwfClient *pgwf.Client`, but pgwf doesn't have a Client type for query operations. Instead:

```go
// Don't need this:
type swfEngineImpl struct {
    pgwfClient *pgwf.Client  // ❌ No such type for queries
}

// Just use module functions directly:
status, err := pgwf.GetJobStatus(ctx, s.udb, tenantID, jobID)
```

---

## Summary of Findings

### ✅ Can Migrate (Straightforward)

1. **CheckJobStatus** → `pgwf.GetJobStatus`
2. **GetJobResult** → `pgwf.IsJobArchived`
3. **jobResultIfComplete** → `pgwf.GetJobStatus` or `pgwf.IsJobArchived`
4. **ensureChildAndNotificationJobs** → `pgwf.CheckJobExistsWithTenant`
5. **FindTasksWaitingForCapability** → `pgwf.FindJobs` (supports multi-tenant!)
6. **GetWaitingTask** → `pgwf.GetJob`

### ⚠️ Requires Workarounds

7. **ListJobs** → `pgwf.ListJobs` with limitations:
   - ❌ **Cursor pagination not implemented** (placeholder only)
   - ❌ **Single tenant only** (must make multiple calls or merge)
   - ❌ **Single LIKE pattern** (complex job type filtering unsupported)

### Recommended Approach

**Phase 1: Migrate Low-Hanging Fruit** ✅
- Migrate functions 1-6 (straightforward, no blockers)
- Significant code reduction (~100 lines)
- Better abstraction

**Phase 2: Handle ListJobs** ⚠️
- **Option A**: Keep custom SQL until pgwf adds cursor pagination (RECOMMENDED)
- **Option B**: Contribute cursor pagination to pgwf-go first
- **Option C**: Accept degraded functionality (no cursors, single tenant)

### Updated Timeline

| Phase | Duration | Confidence |
|-------|----------|------------|
| 1. Simple migrations (functions 1-6) | 1-2 weeks | High |
| 2. ListJobs decision | 1 day | High |
| 3. ListJobs migration (if proceeding) | 2-3 weeks | Low (blockers exist) |

---

## Recommendations for Migration Spec Updates

1. **Remove pgwfClient field** - Use module functions directly
2. **Mark ListJobs as DEFERRED** until cursor pagination implemented
3. **Focus on migrating 6 of 7 functions** - Gets most benefits
4. **Update timeline** to reflect partial migration
5. **Add section on contributing cursor pagination to pgwf**

---

## Bonus Finding: GetJobStatusBatch

**Not mentioned in original spec**, but pgwf has a batch API:

```go
func GetJobStatusBatch(ctx context.Context, db DB, tenantID TenantID, jobIDs []JobID) (map[string]*JobStatusInfo, error)
```

**Use case**: If swf-go ever needs to check multiple job statuses efficiently (e.g., polling multiple child jobs), can use this instead of multiple GetJobStatus calls.

---

## API Completeness Matrix

| swf-go Need | pgwf API | Status | Notes |
|-------------|----------|--------|-------|
| Single job status | GetJobStatus | ✅ Complete | Always includes payload |
| Job existence check | CheckJobExists | ✅ Complete | |
| Job existence + tenant validation | CheckJobExistsWithTenant | ✅ Complete | |
| Archive check | IsJobArchived | ✅ Complete | |
| Get job details | GetJob | ✅ Complete | Optional payload |
| Find by capability (multi-tenant) | FindJobs | ✅ Complete | TenantIDs array supported |
| List with pagination | ListJobs | ⚠️ Partial | No cursor, single tenant only |
| Batch status check | GetJobStatusBatch | ✅ Bonus | Not in original requirements |
| List archived only | ListArchivedJobs | ✅ Bonus | More efficient than ListJobs |
