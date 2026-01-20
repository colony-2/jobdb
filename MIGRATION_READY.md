# ✅ swf-go → pgwf-go Migration: Ready for Full Implementation

## Status: ALL BLOCKERS RESOLVED

The pgwf-go library has been updated with all requested features. **Full migration of all 7 database access functions is now possible.**

---

## What Changed in pgwf-go

### 1. ✅ Cursor-Based Pagination (IMPLEMENTED)

**Location**: `/pgwf/pgwf-go/pkg/pgwf/query.go:1074-1230`

**Features**:
- `paginationCursor` struct with query hash validation
- Base64-encoded JSON cursors (opaque to consumers)
- Row comparison: `(sort_field, job_id) > ($1, $2)` for stable pagination
- Automatic detection of "has more" results via `limit + 1` pattern
- Query parameter validation to prevent cursor reuse across different queries

**API**:
```go
type ListJobsOptions struct {
    Cursor string // Opaque cursor for pagination
    // ...
}

type ListJobsResult struct {
    Jobs       []JobListItem
    NextCursor string // Empty if no more results
    HasMore    bool
}
```

---

### 2. ✅ Multi-Tenant Filtering (IMPLEMENTED)

**Location**: `/pgwf/pgwf-go/pkg/pgwf/query.go:89-92, 1254-1262`

**Features**:
- `TenantIDs []string` field for filtering multiple tenants
- Backwards compatible with deprecated `TenantID string`
- Uses PostgreSQL `= ANY($1)` with `pq.Array()` for efficiency
- Single query returns jobs from all specified tenants

**API**:
```go
type ListJobsOptions struct {
    TenantID  string   // DEPRECATED: Use TenantIDs
    TenantIDs []string // Filter by multiple tenant IDs
    // ...
}
```

**Generated SQL**:
```sql
WHERE tenant_id = ANY($1)
-- With args: pq.Array(["tenant-1", "tenant-2", "tenant-3"])
```

---

### 3. ✅ Multi-Pattern Job Type Filtering (IMPLEMENTED)

**Location**: `/pgwf/pgwf-go/pkg/pgwf/query.go:96-97, 1138-1147`

**Features**:
- `JobTypePatterns []string` for multiple LIKE patterns with OR semantics
- Backwards compatible with deprecated `JobTypePattern string`
- Each pattern generates separate `LIKE` condition
- All patterns combined with `OR`

**API**:
```go
type ListJobsOptions struct {
    JobTypePattern  string   // DEPRECATED: Use JobTypePatterns
    JobTypePatterns []string // Multiple SQL LIKE patterns (OR semantics)
    // ...
}
```

**Generated SQL**:
```sql
WHERE (next_need LIKE $1 OR next_need LIKE $2 OR next_need LIKE $3)
-- With args: ["workflow%", "batch:process", "cron:daily"]
```

---

## Migration Impact

### Before (Current State)

| Function | Database Access | Lines of Code | Status |
|----------|-----------------|---------------|--------|
| CheckJobStatus | Direct GORM query | ~20 | ❌ Direct DB |
| GetJobResult | Direct GORM query | ~25 | ❌ Direct DB |
| jobResultIfComplete | Direct GORM query | ~15 | ❌ Direct DB |
| ensureChildAndNotificationJobs | Direct GORM query | ~30 | ❌ Direct DB |
| FindTasksWaitingForCapability | Direct GORM query | ~30 | ❌ Direct DB |
| GetWaitingTask | Direct GORM query | ~25 | ❌ Direct DB |
| ListJobs | Raw SQL with UNION ALL | ~240 | ❌ Direct DB |
| **TOTAL** | | **~385 lines** | **7/7 Direct** |

### After (Migration Complete)

| Function | Database Access | Lines of Code | Status |
|----------|-----------------|---------------|--------|
| CheckJobStatus | pgwf.GetJobStatus() | ~15 | ✅ Via API |
| GetJobResult | pgwf.IsJobArchived() | ~10 | ✅ Via API |
| jobResultIfComplete | pgwf.GetJobStatus() | ~12 | ✅ Via API |
| ensureChildAndNotificationJobs | pgwf.CheckJobExistsWithTenant() | ~15 | ✅ Via API |
| FindTasksWaitingForCapability | pgwf.FindJobs() | ~20 | ✅ Via API |
| GetWaitingTask | pgwf.GetJob() | ~15 | ✅ Via API |
| ListJobs | pgwf.ListJobs() | ~50 | ✅ Via API |
| **TOTAL** | | **~137 lines** | **0/7 Direct** |

**Net Result**:
- ✅ **100% elimination** of direct database access
- ✅ **~248 lines removed** (64% code reduction)
- ✅ **Zero external API changes** to swf-go

---

## Implementation Quick Reference

### Example 1: Simple Migration (CheckJobStatus)

**Before**:
```go
func (s *swfEngineImpl) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
    db := s.dbFromCtx(ctx)
    var job pgwfJobWithStatus
    db.First(&job, "job_id = ?", jobKey.JobId)

    if job.JobID == "" {
        var archived pgwfJobArchive
        db.First(&archived, "job_id = ?", jobKey.JobId)
        if archived.JobID == "" {
            return "", fmt.Errorf("job not found: %v", jobKey)
        }
        return swf.JobStatusCompleted, nil
    }

    return convertPgwfStatus(job.Status), nil
}
```

**After**:
```go
func (s *swfEngineImpl) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
    status, err := pgwf.GetJobStatus(ctx, s.udb,
        pgwf.TenantID(jobKey.TenantId),
        pgwf.JobID(jobKey.JobId))
    if err == pgwf.ErrJobNotFound {
        return "", fmt.Errorf("job not found: %v", jobKey)
    }
    if err != nil {
        return "", fmt.Errorf("failed to get job status: %w", err)
    }

    // Check if archived
    if status.ArchivedAt != nil {
        if status.CancelRequested {
            return swf.JobStatusCancelled, nil
        }
        return swf.JobStatusCompleted, nil
    }

    return convertPgwfStatusToSwf(status.Status), nil
}
```

---

### Example 2: Complex Migration (ListJobs)

**Before**: ~240 lines of SQL building

**After**:
```go
func (s *swfEngineImpl) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
    // Build job type patterns from swf's JobTypes + JobTasks
    patterns := make([]string, 0, len(req.JobTypes)*2+len(req.JobTasks))
    for _, jt := range req.JobTypes {
        patterns = append(patterns, jt, jt+":%")  // Exact + prefix
    }
    for _, task := range req.JobTasks {
        if task.JobType != "" && task.TaskType != "" {
            patterns = append(patterns, task.JobType+":"+task.TaskType)
        }
    }

    // Map to pgwf options
    opts := pgwf.ListJobsOptions{
        TenantIDs:        req.TenantIds,
        Statuses:         convertSwfStatusesToPgwf(req.Statuses),
        JobTypePatterns:  patterns,
        IncludeArchived:  shouldIncludeArchived(req.Stores, req.Statuses),
        CreatedAfter:     req.CreatedAfter,
        CreatedBefore:    req.CreatedBefore,
        Limit:            normalizePageSize(req.PageSize),
        Cursor:           req.PageToken,
        SortBy:           pgwf.SortByCreatedAt,
        SortOrder:        pgwf.SortDesc,
    }

    // Call pgwf API
    result, err := pgwf.ListJobs(ctx, s.udb, opts)
    if err != nil {
        return swf.ListJobsResponse{}, fmt.Errorf("failed to list jobs: %w", err)
    }

    // Convert response
    jobs := make([]swf.JobSummary, len(result.Jobs))
    for i, job := range result.Jobs {
        jobs[i] = convertPgwfJobToSwfSummary(job)
    }

    return swf.ListJobsResponse{
        Jobs:          jobs,
        NextPageToken: result.NextCursor,
    }, nil
}
```

---

## Required Helper Functions

### Status Conversion

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
    case pgwf.JobStatus("COMPLETED"):  // Undocumented from archived jobs
        return swf.JobStatusCompleted
    default:
        return swf.JobStatusReady
    }
}

func convertSwfStatusesToPgwf(statuses []swf.JobStatus) []pgwf.JobStatus {
    result := make([]pgwf.JobStatus, 0, len(statuses))
    for _, st := range statuses {
        switch st {
        case swf.JobStatusCompleted:
            // Skip - handled by IncludeArchived flag
            continue
        default:
            result = append(result, pgwf.JobStatus(st))
        }
    }
    return result
}
```

### Archive Inclusion Logic

```go
func shouldIncludeArchived(stores []swf.JobStore, statuses []swf.JobStatus) bool {
    // Check if explicitly requested via stores
    if len(stores) > 0 {
        for _, store := range stores {
            if store == swf.JobStoreArchived {
                return true
            }
        }
        return false
    }

    // Check if COMPLETED status requested
    for _, st := range statuses {
        if st == swf.JobStatusCompleted {
            return true
        }
    }

    // Default: include both active and archived
    return len(statuses) == 0
}
```

---

## Migration Checklist

### Phase 1: Infrastructure (2-3 days)
- [ ] Add status conversion helpers
- [ ] Add archive inclusion logic
- [ ] Add job type pattern builder
- [ ] Unit tests for all helpers

### Phase 2: Simple Operations (3-5 days)
- [ ] Migrate CheckJobStatus
- [ ] Migrate GetJobResult
- [ ] Migrate jobResultIfComplete
- [ ] Integration tests for each

### Phase 3: Idempotency (2-3 days)
- [ ] Migrate ensureChildAndNotificationJobs
- [ ] Test async child job creation
- [ ] Test tenant validation

### Phase 4: Discovery (2-3 days)
- [ ] Migrate FindTasksWaitingForCapability
- [ ] Migrate GetWaitingTask
- [ ] Test external task pattern

### Phase 5: ListJobs (5-7 days)
- [ ] Implement request mapping
- [ ] Implement response mapping
- [ ] Test cursor pagination
- [ ] Test multi-tenant filtering
- [ ] Test multi-pattern filtering
- [ ] Test all status combinations
- [ ] Performance comparison

### Phase 6: Cleanup (2-3 days)
- [ ] Remove pgwfJobWithStatus model
- [ ] Remove pgwfJobArchive model
- [ ] Remove jobListRow model
- [ ] Remove makePlaceholders helper
- [ ] Remove buildJobTypeClause helper
- [ ] Update documentation

### Phase 7: Validation (3-5 days)
- [ ] Run full test suite
- [ ] Performance benchmarks
- [ ] Integration test pass
- [ ] Code review
- [ ] Documentation review

---

## Success Metrics

After migration:

1. ✅ **Zero direct queries** to `pgwf.jobs_with_status` or `pgwf.jobs_archive`
2. ✅ **Zero GORM models** for pgwf tables
3. ✅ **All tests passing** without modification
4. ✅ **Performance maintained** or improved
5. ✅ **Code reduced** by ~248 lines (64%)
6. ✅ **swf-go external API** completely unchanged

---

## Timeline

**Total Estimated Duration: 3-4 weeks (20-28 working days)**

| Week | Focus | Deliverable |
|------|-------|-------------|
| 1 | Infrastructure + Simple Ops | 3 functions migrated + helpers |
| 2 | Idempotency + Discovery | 3 more functions migrated |
| 3 | ListJobs | Most complex migration complete |
| 4 | Cleanup + Validation | All code removed, tests passing |

---

## Next Steps

1. **Review updated migration spec**: `/src/SWF_PGWF_MIGRATION_SPEC.md`
2. **Start with Phase 1**: Infrastructure and helpers (low risk)
3. **Migrate incrementally**: One function at a time with tests
4. **Validate continuously**: Run test suite after each migration
5. **Complete in 3-4 weeks**: Full decoupling achieved

---

## Documentation References

- **Detailed Migration Plan**: `/src/SWF_PGWF_MIGRATION_SPEC.md`
- **API Review**: `/src/PGWF_API_REVIEW.md`
- **Feature Request** (fulfilled): `/src/PGWF_FEATURE_REQUEST.md`
- **pgwf-go Implementation**: `/pgwf/pgwf-go/pkg/pgwf/query.go`

---

## Questions or Issues?

All previously identified blockers have been resolved. The migration can proceed without any feature gaps or workarounds.
