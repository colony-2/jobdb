# swf-go Migration Spec: Replacing Direct Database Access with pgwf-go APIs

## Overview

This specification provides a detailed migration plan for eliminating all direct database queries in swf-go by using the new read/query APIs provided by pgwf-go. After this migration, swf-go will have zero direct SQL queries to `pgwf.*` tables and will interact exclusively through pgwf-go's client APIs.

**Important**: This is a **pure internal refactoring**. The external swf-go API (swf.Engine interface) remains completely unchanged. No consumers of swf-go need to change their code.

## Migration Goals

1. ✅ Eliminate all direct queries to `pgwf.jobs_with_status` and `pgwf.jobs_archive`
2. ✅ Remove all GORM models that reference pgwf tables
3. ✅ Replace raw SQL queries with pgwf-go client API calls
4. ✅ Maintain or improve performance
5. ✅ Pass all existing tests without modification
6. ✅ **Zero changes** to swf-go's external API - consumers unaffected

## Current State: Direct Database Access Points

Based on the catalog in the initial analysis, swf-go has **7 instances** of direct database access:

1. **CheckJobStatus** (`engine.go:614-634`) - Status checks
2. **GetJobResult** (`engine.go:636-664`) - Archive verification
3. **jobResultIfComplete** (`engine.go:666-680`) - Completion polling
4. **ensureChildAndNotificationJobs** (`engine.go:979-1033`) - Idempotency checks
5. **ListJobs** (`engine.go:738-977`) - Complex listing with pagination
6. **FindTasksWaitingForCapability** (`engine.go:1062-1094`) - Capability-based search
7. **GetWaitingTask** (`engine.go:1119-1143`) - Job lookup by ID

---

## Migration Details by Function

### 1. CheckJobStatus - Use `pgwf.GetJobStatus`

**Current Implementation** (`engine.go:614-634`):
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

**New Implementation**:
```go
func (s *swfEngineImpl) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
    status, err := s.pgwfClient.GetJobStatus(ctx, jobKey.JobId)
    if err == pgwf.ErrJobNotFound {
        return "", fmt.Errorf("job not found: %v", jobKey)
    }
    if err != nil {
        return "", fmt.Errorf("failed to get job status: %w", err)
    }

    // Job is archived/completed
    if status.ArchivedAt != nil {
        if status.CancelRequested {
            return swf.JobStatusCancelled, nil
        }
        return swf.JobStatusCompleted, nil
    }

    // Job is active - convert pgwf status to swf status
    return convertPgwfStatusToSwf(status.Status), nil
}
```

**Required Changes**:
- Add `pgwfClient *pgwf.Client` field to `swfEngineImpl` struct
- Add helper function `convertPgwfStatusToSwf(pgwf.JobStatus) swf.JobStatus` for status mapping
- Remove `pgwfJobWithStatus` and `pgwfJobArchive` GORM models (no longer needed)

**Status Mapping**:
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
    default:
        // Unknown status - treat as Ready
        return swf.JobStatusReady
    }
}
```

---

### 2. GetJobResult - Use `pgwf.IsJobArchived`

**Current Implementation** (`engine.go:636-664`):
```go
func (s *swfEngineImpl) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
    db := s.dbFromCtx(ctx)
    var archived int64
    db.Table("pgwf.jobs_archive").
       Where("job_id = ?", jobKey.JobId).
       Count(&archived)

    if archived == 0 {
        return swf.TaskData{}, fmt.Errorf("job not complete: %v", jobKey)
    }

    // Fetch result from Strata
    return s.fetchResultFromStrata(ctx, jobKey)
}
```

**New Implementation**:
```go
func (s *swfEngineImpl) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
    isArchived, err := s.pgwfClient.IsJobArchived(ctx, jobKey.JobId)
    if err != nil {
        return swf.TaskData{}, fmt.Errorf("failed to check job archive status: %w", err)
    }

    if !isArchived {
        return swf.TaskData{}, fmt.Errorf("job not complete: %v", jobKey)
    }

    // Fetch result from Strata
    return s.fetchResultFromStrata(ctx, jobKey)
}
```

**Alternative Implementation** (if we need to check cancellation):
```go
func (s *swfEngineImpl) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
    status, err := s.pgwfClient.GetJobStatus(ctx, jobKey.JobId)
    if err == pgwf.ErrJobNotFound {
        return swf.TaskData{}, fmt.Errorf("job not found: %v", jobKey)
    }
    if err != nil {
        return swf.TaskData{}, fmt.Errorf("failed to get job status: %w", err)
    }

    if status.ArchivedAt == nil {
        return swf.TaskData{}, fmt.Errorf("job not complete: %v", jobKey)
    }

    if status.CancelRequested {
        return swf.TaskData{}, fmt.Errorf("job was cancelled: %v", jobKey)
    }

    // Fetch result from Strata
    return s.fetchResultFromStrata(ctx, jobKey)
}
```

**Required Changes**:
- Use `IsJobArchived` for simple archive check
- Or use `GetJobStatus` if we need to distinguish between cancelled and completed

---

### 3. jobResultIfComplete - Use `pgwf.GetJobStatus`

**Current Implementation** (`engine.go:666-680`):
```go
func (s *swfEngineImpl) jobResultIfComplete(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, bool, error) {
    db := s.dbFromCtx(ctx)
    var archived int64
    db.Table("pgwf.jobs_archive").
       Where("job_id = ?", jobKey.JobId).
       Count(&archived)

    if archived == 0 {
        return swf.TaskData{}, false, nil // Not complete
    }

    result, err := s.fetchResultFromStrata(ctx, jobKey)
    return result, true, err
}
```

**New Implementation**:
```go
func (s *swfEngineImpl) jobResultIfComplete(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, bool, error) {
    status, err := s.pgwfClient.GetJobStatus(ctx, jobKey.JobId)
    if err == pgwf.ErrJobNotFound {
        // Job doesn't exist - treat as not complete
        return swf.TaskData{}, false, nil
    }
    if err != nil {
        return swf.TaskData{}, false, fmt.Errorf("failed to get job status: %w", err)
    }

    // Check if job is archived (completed or cancelled)
    if status.ArchivedAt == nil {
        return swf.TaskData{}, false, nil // Not complete
    }

    // Job is complete - fetch result
    result, err := s.fetchResultFromStrata(ctx, jobKey)
    return result, true, err
}
```

**Alternative - Performance Optimized** (if status checks are expensive):
```go
func (s *swfEngineImpl) jobResultIfComplete(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, bool, error) {
    // Quick archive check without full status fetch
    isArchived, err := s.pgwfClient.IsJobArchived(ctx, jobKey.JobId)
    if err != nil {
        return swf.TaskData{}, false, fmt.Errorf("failed to check archive status: %w", err)
    }

    if !isArchived {
        return swf.TaskData{}, false, nil // Not complete
    }

    // Job is complete - fetch result
    result, err := s.fetchResultFromStrata(ctx, jobKey)
    return result, true, err
}
```

**Required Changes**:
- Choose between `GetJobStatus` (full info) or `IsJobArchived` (faster)
- Handle `ErrJobNotFound` gracefully (return false, not error)

---

### 4. ensureChildAndNotificationJobs - Use `pgwf.CheckJobExistsWithTenant`

**Current Implementation** (`engine.go:979-1033`):
```go
func (s *swfEngineImpl) ensureChildAndNotificationJobs(
    ctx context.Context,
    tenantID pgwf.TenantID,
    parentJobID pgwf.JobID,
    childJobID pgwf.JobID,
    // ... other params
) error {
    db := s.pgwfDB(ctx)

    // Check if child already archived
    var archived int64
    db.Table("pgwf.jobs_archive").
       Where("job_id = ?", string(childJobID)).
       Count(&archived)

    if archived > 0 {
        return nil // Already complete
    }

    // Check if child exists in active jobs
    var existing pgwfJobWithStatus
    db.Where("job_id = ?", string(childJobID)).First(&existing)

    if existing.JobID != "" {
        // Validate tenant
        if existing.TenantID != string(tenantID) {
            return fmt.Errorf("child job %s belongs to different tenant", childJobID)
        }
        return nil // Already exists
    }

    // Create child and notification jobs
    // ... submission logic
}
```

**New Implementation**:
```go
func (s *swfEngineImpl) ensureChildAndNotificationJobs(
    ctx context.Context,
    tenantID pgwf.TenantID,
    parentJobID pgwf.JobID,
    childJobID pgwf.JobID,
    // ... other params
) error {
    // Check if child job exists and validate tenant
    exists, err := s.pgwfClient.CheckJobExistsWithTenant(ctx, string(childJobID), string(tenantID))

    if err == pgwf.ErrTenantMismatch {
        return fmt.Errorf("child job %s belongs to different tenant", childJobID)
    }

    if err != nil && err != pgwf.ErrJobNotFound {
        return fmt.Errorf("failed to check child job existence: %w", err)
    }

    if exists != nil && exists.Exists {
        // Job already exists (either active or archived)
        return nil
    }

    // Job doesn't exist - create child and notification jobs
    // ... submission logic using pgwf.SubmitJob
}
```

**Required Changes**:
- Replace dual table checks with single `CheckJobExistsWithTenant` call
- Handle `ErrTenantMismatch` and `ErrJobNotFound` appropriately
- Remove `pgwfJobWithStatus` model dependency

---

### 5. ListJobs - Use `pgwf.ListJobs`

**Current Implementation** (`engine.go:738-977`):
```go
func (s *swfEngineImpl) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
    // ~200+ lines of complex SQL building
    // - Builds UNION ALL query for active and archived jobs
    // - Handles status filtering, job type patterns, pagination
    // - Cursor encoding/decoding
    // - Raw SQL execution

    var rows []jobListRow
    db.Raw(sql, allArgs...).Scan(&rows)

    // Convert to swf.JobSummary
    // ...
}
```

**New Implementation**:
```go
func (s *swfEngineImpl) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
    // Convert swf.ListJobsRequest to pgwf.ListJobsOptions
    opts := pgwf.ListJobsOptions{
        Limit:  req.PageSize,
        Cursor: req.PageToken,
    }

    // Set default/max page size
    if opts.Limit <= 0 {
        opts.Limit = swf.DefaultListJobsPageSize
    } else if opts.Limit > swf.MaxListJobsPageSize {
        opts.Limit = swf.MaxListJobsPageSize
    }

    // Convert tenant IDs
    if len(req.TenantIds) > 0 {
        opts.TenantID = req.TenantIds[0] // pgwf API takes single tenant
        // TODO: Handle multiple tenants if needed
    }

    // Convert statuses
    if len(req.Statuses) > 0 {
        opts.Statuses = make([]pgwf.JobStatus, 0, len(req.Statuses))
        for _, st := range req.Statuses {
            opts.Statuses = append(opts.Statuses, convertSwfStatusToPgwf(st))
        }
    }

    // Handle job type filtering
    if len(req.JobTypes) > 0 || len(req.JobTasks) > 0 {
        // pgwf uses JobTypePattern (SQL LIKE pattern)
        // Build pattern from JobTypes and JobTasks
        opts.JobTypePattern = buildJobTypePattern(req.JobTypes, req.JobTasks)
    }

    // Handle singleton key
    if req.SingletonKey != "" {
        opts.SingletonKey = req.SingletonKey
    }

    // Time range filtering
    if !req.CreatedAfter.IsZero() {
        opts.CreatedAfter = &req.CreatedAfter
    }
    if !req.CreatedBefore.IsZero() {
        opts.CreatedBefore = &req.CreatedBefore
    }

    // Archive inclusion
    if len(req.Stores) > 0 {
        includeArchive := false
        for _, store := range req.Stores {
            if store == swf.JobStoreArchived {
                includeArchive = true
                break
            }
        }
        opts.IncludeArchived = includeArchive
    } else {
        opts.IncludeArchived = true // Default: include both
    }

    // Execute query
    result, err := s.pgwfClient.ListJobs(ctx, opts)
    if err != nil {
        return swf.ListJobsResponse{}, fmt.Errorf("failed to list jobs: %w", err)
    }

    // Convert pgwf.JobListItem to swf.JobSummary
    jobs := make([]swf.JobSummary, len(result.Jobs))
    for i, job := range result.Jobs {
        jobs[i] = swf.JobSummary{
            JobKey: swf.JobKey{
                TenantId: job.TenantID,
                JobId:    job.JobID,
            },
            Status:          convertPgwfStatusToSwf(job.Status),
            NextNeed:        job.NextNeed,
            SingletonKey:    job.SingletonKey,
            WaitFor:         convertWaitForJobKeys(job.WaitFor),
            AvailableAt:     job.AvailableAt,
            ExpiresAt:       nullTimeToPointer(job.ExpiresAt),
            LeaseExpiresAt:  nullTimeToPointer(job.LeaseExpiresAt),
            CancelRequested: job.CancelRequested,
            CreatedAt:       job.CreatedAt,
            ArchivedAt:      job.ArchivedAt,
        }
    }

    return swf.ListJobsResponse{
        Jobs:          jobs,
        NextPageToken: result.NextCursor,
    }, nil
}
```

**Helper Functions Needed**:
```go
func convertSwfStatusToPgwf(status swf.JobStatus) pgwf.JobStatus {
    switch status {
    case swf.JobStatusReady:
        return pgwf.JobStatusReady
    case swf.JobStatusActive:
        return pgwf.JobStatusActive
    case swf.JobStatusCancelled:
        return pgwf.JobStatusCancelled
    case swf.JobStatusCompleted:
        // Special case: pgwf uses ArchivedAt to indicate completion
        // This status is used for filtering archived jobs
        return pgwf.JobStatusReady // Completed jobs in archive don't have active status
    case swf.JobStatusAwaitingFuture:
        return pgwf.JobStatusAwaitingFuture
    case swf.JobStatusPendingJobs:
        return pgwf.JobStatusPendingJobs
    case swf.JobStatusCrashConcern:
        return pgwf.JobStatusCrashConcern
    case swf.JobStatusExpired:
        return pgwf.JobStatusExpired
    default:
        return pgwf.JobStatusReady
    }
}

func buildJobTypePattern(jobTypes []string, jobTasks []swf.JobTaskFilter) string {
    // Build SQL LIKE pattern for job type filtering
    // This may require multiple patterns or special handling
    // Depends on pgwf.ListJobs API capabilities
    // TODO: Verify if pgwf supports multiple patterns or need to filter client-side
    if len(jobTypes) > 0 {
        return jobTypes[0] + "%" // Simple case
    }
    if len(jobTasks) > 0 && jobTasks[0].JobType != "" {
        return jobTasks[0].JobType + ":" + jobTasks[0].TaskType
    }
    return ""
}

func convertWaitForJobKeys(waitFor []string) []swf.JobKey {
    keys := make([]swf.JobKey, len(waitFor))
    for i, jobID := range waitFor {
        keys[i] = swf.JobKey{JobId: jobID}
        // Note: TenantId may not be available in wait_for array
    }
    return keys
}

func nullTimeToPointer(t time.Time) *time.Time {
    if t.IsZero() || t.Unix() == -62135596800 { // Postgres '-infinity'
        return nil
    }
    return &t
}
```

**Required Changes**:
- **Complete rewrite** - replace ~200 lines of SQL building with API call
- **Multi-tenant handling**: pgwf API may take single tenant; need to handle multiple tenants
- **Job type pattern**: May need client-side filtering if pgwf doesn't support complex patterns
- **Status mapping**: Handle swf.JobStatusCompleted specially (archive-only)
- **Cursor translation**: Translate between swf's PageToken format and pgwf's cursor format internally

**Migration Risk**: LOW ✅ (All blockers resolved!)

**Required Changes**:
- Replace ~200 lines of SQL building with pgwf.ListJobs API call
- Map swf.ListJobsRequest to pgwf.ListJobsOptions
- Handle job type pattern conversion (swf has JobTypes + JobTasks, pgwf uses JobTypePatterns)
- Status mapping as with other functions
- Cursor format changes internally (opaque to consumers, so no breaking change)

**Implementation**:
```go
func (s *swfEngineImpl) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
    // Build job type patterns from JobTypes and JobTasks
    patterns := make([]string, 0, len(req.JobTypes)*2+len(req.JobTasks))
    for _, jt := range req.JobTypes {
        patterns = append(patterns, jt, jt+":%")  // Exact + prefix match
    }
    for _, task := range req.JobTasks {
        if task.JobType != "" && task.TaskType != "" {
            patterns = append(patterns, task.JobType+":"+task.TaskType)
        }
    }

    // Convert to pgwf options
    opts := pgwf.ListJobsOptions{
        TenantIDs:        req.TenantIds,
        Statuses:         convertSwfStatusesToPgwf(req.Statuses),
        JobTypePatterns:  patterns,
        SingletonKey:     req.SingletonKey,  // First one, or iterate if multiple
        CreatedAfter:     req.CreatedAfter,
        CreatedBefore:    req.CreatedBefore,
        IncludeArchived:  shouldIncludeArchived(req.Stores, req.Statuses),
        Limit:            req.PageSize,
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

### 6. FindTasksWaitingForCapability - Use `pgwf.FindJobs`

**Current Implementation** (`engine.go:1062-1094`):
```go
func (s *swfEngineImpl) FindTasksWaitingForCapability(
    ctx context.Context,
    jobType string,
    taskType string,
    tenantIds []string,
) ([]swf.TaskHandle, error) {
    db := s.dbFromCtx(ctx)
    query := db.Where("next_need = ? AND status = ?", jobType+":"+taskType, "READY")
    if len(tenantIds) > 0 {
        query = query.Where("tenant_id IN ?", tenantIds)
    }

    var jobs []pgwfJobWithStatus
    err := query.Find(&jobs).Error
    if err != nil {
        return nil, err
    }

    // Convert to TaskHandles
    handles := make([]swf.TaskHandle, len(jobs))
    for i, job := range jobs {
        handles[i] = swf.TaskHandle{
            JobKey: swf.JobKey{
                TenantId: job.TenantID,
                JobId:    job.JobID,
            },
            TaskType: taskType,
        }
    }
    return handles, nil
}
```

**New Implementation**:
```go
func (s *swfEngineImpl) FindTasksWaitingForCapability(
    ctx context.Context,
    jobType string,
    taskType string,
    tenantIds []string,
) ([]swf.TaskHandle, error) {
    // Build capability string
    capability := jobType + ":" + taskType

    // Use pgwf.FindJobs API
    jobs, err := s.pgwfClient.FindJobs(ctx, pgwf.FindJobsOptions{
        TenantIDs: tenantIds,
        Status:    pgwf.JobStatusReady,
        NextNeed:  capability,
        Limit:     1000, // Default limit
    })

    if err != nil {
        return nil, fmt.Errorf("failed to find tasks: %w", err)
    }

    // Convert to TaskHandles
    handles := make([]swf.TaskHandle, len(jobs))
    for i, job := range jobs {
        handles[i] = swf.TaskHandle{
            JobKey: swf.JobKey{
                TenantId: job.TenantID,
                JobId:    job.JobID,
            },
            TaskType: taskType,
        }
    }
    return handles, nil
}
```

**Required Changes**:
- Replace GORM query with `pgwf.FindJobs`
- Handle limit appropriately (may need pagination if > 1000 results)
- Remove `pgwfJobWithStatus` model dependency

**Migration Risk**: LOW
- Straightforward 1:1 replacement
- API matches perfectly

---

### 7. GetWaitingTask - Use `pgwf.GetJob`

**Current Implementation** (`engine.go:1119-1143`):
```go
func (s *swfEngineImpl) GetWaitingTask(ctx context.Context, key swf.JobKey) (swf.TaskHandle, error) {
    db := s.dbFromCtx(ctx)
    var job pgwfJobWithStatus
    db.Where("job_id = ? AND status = ?", key.JobId, "READY").First(&job)

    if job.JobID == "" {
        return swf.TaskHandle{}, fmt.Errorf("job not found or not ready: %v", key)
    }

    // Parse task type from next_need
    parts := strings.Split(job.NextNeed, ":")
    taskType := ""
    if len(parts) == 2 {
        taskType = parts[1]
    }

    return swf.TaskHandle{
        JobKey: swf.JobKey{
            TenantId: job.TenantID,
            JobId:    job.JobID,
        },
        TaskType: taskType,
    }, nil
}
```

**New Implementation**:
```go
func (s *swfEngineImpl) GetWaitingTask(ctx context.Context, key swf.JobKey) (swf.TaskHandle, error) {
    // Get job without leasing
    job, err := s.pgwfClient.GetJob(ctx, key.JobId, pgwf.GetJobOptions{
        IncludePayload: false,
    })

    if err == pgwf.ErrJobNotFound {
        return swf.TaskHandle{}, fmt.Errorf("job not found: %v", key)
    }
    if err != nil {
        return swf.TaskHandle{}, fmt.Errorf("failed to get job: %w", err)
    }

    // Verify job is in READY status
    if job.Status != pgwf.JobStatusReady {
        return swf.TaskHandle{}, fmt.Errorf("job not ready: %v (status: %s)", key, job.Status)
    }

    // Parse task type from next_need
    parts := strings.Split(job.NextNeed, ":")
    taskType := ""
    if len(parts) == 2 {
        taskType = parts[1]
    }

    return swf.TaskHandle{
        JobKey: swf.JobKey{
            TenantId: job.TenantID,
            JobId:    job.JobID,
        },
        TaskType: taskType,
    }, nil
}
```

**Required Changes**:
- Replace GORM query with `pgwf.GetJob`
- Add status validation (READY check)
- Remove `pgwfJobWithStatus` model dependency

**Migration Risk**: LOW
- Straightforward replacement
- Maintains same semantics

---

## API Usage Pattern

### pgwf Query APIs are Module-Level Functions

**Important**: pgwf query APIs are **module-level functions**, not methods on a client object.

**Current Struct** (`engine.go`):
```go
type swfEngineImpl struct {
    strata          *strataclient.Client
    db              *gorm.DB
    udb             *sql.DB
    // ... other fields
}
```

**No changes needed to struct** - pgwf functions take DB as parameter:
```go
// pgwf query functions signature:
func pgwf.GetJobStatus(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID) (*pgwf.JobStatusInfo, error)
func pgwf.CheckJobExists(ctx context.Context, db pgwf.DB, tenantID pgwf.TenantID, jobID pgwf.JobID) (*pgwf.JobExistence, error)
// etc.

// pgwf.DB interface (satisfied by *sql.DB, *sql.Tx, etc):
type DB interface {
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
```

### Usage in swf-go

**Simply call pgwf functions directly**:
```go
func (s *swfEngineImpl) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
    // Call pgwf module function directly, passing s.udb
    status, err := pgwf.GetJobStatus(ctx, s.udb, pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId))
    if err == pgwf.ErrJobNotFound {
        return "", fmt.Errorf("job not found: %v", jobKey)
    }
    // ...
}
```

**Can also use with transactions**:
```go
func (s *swfEngineImpl) someTransactionalOperation(ctx context.Context) error {
    tx, err := s.udb.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // Pass transaction to pgwf functions
    exists, err := pgwf.CheckJobExists(ctx, tx, tenantID, jobID)
    // ...

    return tx.Commit()
}
```

---

## GORM Models to Remove

After migration, these GORM models are **no longer needed**:

**File: `pkg/swf/impl/pggorm.go`** (or wherever pgwf models are defined):
```go
// DELETE these models
type pgwfJobWithStatus struct {
    TenantID        string         `gorm:"column:tenant_id"`
    JobID           string         `gorm:"column:job_id;primaryKey"`
    Status          string         `gorm:"column:status"`
    NextNeed        string         `gorm:"column:next_need"`
    SingletonKey    *string        `gorm:"column:singleton_key"`
    WaitFor         pq.StringArray `gorm:"column:wait_for;type:text[]"`
    AvailableAt     time.Time      `gorm:"column:available_at"`
    ExpiresAt       *time.Time     `gorm:"column:expires_at"`
    LeaseExpiresAt  *time.Time     `gorm:"column:lease_expires_at"`
    CancelRequested bool           `gorm:"column:cancel_requested"`
    CreatedAt       time.Time      `gorm:"column:created_at"`
    Payload         datatypes.JSON `gorm:"column:payload"`
}

func (pgwfJobWithStatus) TableName() string {
    return "pgwf.jobs_with_status"
}

type pgwfJobArchive struct {
    TenantID        string         `gorm:"column:tenant_id"`
    JobID           string         `gorm:"column:job_id;primaryKey"`
    NextNeed        string         `gorm:"column:next_need"`
    SingletonKey    *string        `gorm:"column:singleton_key"`
    WaitFor         pq.StringArray `gorm:"column:wait_for;type:text[]"`
    CreatedAt       time.Time      `gorm:"column:created_at"`
    ExpiresAt       *time.Time     `gorm:"column:expires_at"`
    CancelRequested bool           `gorm:"column:cancel_requested"`
    ArchivedAt      time.Time      `gorm:"column:archived_at"`
}

func (pgwfJobArchive) TableName() string {
    return "pgwf.jobs_archive"
}
```

These models can be completely removed after migration.

---

## Migration Strategy

### Phase 1: Add Status Conversion Helpers (2-3 days)
1. Add helper functions for status conversion (pgwf.JobStatus ↔ swf.JobStatus)
2. Handle special "COMPLETED" status from archived jobs
3. Unit tests for conversion functions
4. **No functional changes yet** - just infrastructure

### Phase 2: Migrate Simple Read Operations (3-5 days)
1. Migrate `CheckJobStatus` → `pgwf.GetJobStatus`
2. Migrate `GetJobResult` → `pgwf.IsJobArchived`
3. Migrate `jobResultIfComplete` → `pgwf.GetJobStatus`
4. Run full test suite after each migration
5. **Goal**: Low-risk migrations first

### Phase 3: Migrate Idempotency Checks (Week 2)
1. Migrate `ensureChildAndNotificationJobs` → `pgwf.CheckJobExistsWithTenant`
2. Run async child job tests extensively
3. Verify tenant validation works correctly

### Phase 4: Migrate Discovery APIs (Week 2-3)
1. Migrate `FindTasksWaitingForCapability` → `pgwf.FindJobs`
2. Migrate `GetWaitingTask` → `pgwf.GetJob`
3. Test external task worker pattern

### Phase 5: Migrate ListJobs (5-7 days) ✅
1. **All blockers resolved!** - Full migration now possible
2. Implement request/response mapping:
   - Map `JobTypes` + `JobTasks` to `JobTypePatterns`
   - Map `Stores` + `Statuses` to `IncludeArchived` flag
   - Pass through `TenantIds`, time ranges, pagination
3. Add helper functions:
   - `convertSwfStatusesToPgwf()` - Status array conversion
   - `shouldIncludeArchived()` - Determine archive inclusion
   - `convertPgwfJobToSwfSummary()` - Result conversion
4. Replace ~200 lines of SQL with ~50 lines of API mapping
5. Extensive testing:
   - Cursor pagination correctness
   - Multi-tenant filtering
   - Multi-pattern job type filtering
   - Status filtering across active/archived
   - Time range filtering

### Phase 6: Cleanup (2-3 days)
1. Remove GORM models for pgwf tables
2. Remove `db.Table("pgwf.*")` queries
3. Update documentation
4. Final test pass

### Phase 7: Performance Testing (2-3 days)
1. Benchmark before/after performance for migrated functions
2. Identify any regressions
3. Optimize if needed
4. Document performance characteristics

**Note**: ListJobs performance not affected (keeping existing implementation)

---

## Testing Requirements

### Unit Tests
- Mock `pgwf.DB` interface for all new code paths (or use test transactions)
- Test error handling (ErrJobNotFound, ErrTenantMismatch, etc.)
- Test status conversion functions (including "COMPLETED" special case)
- Test helper functions
- Test type conversions (swf types ↔ pgwf types)

### Integration Tests
- All existing swf-go integration tests must pass
- Specific tests for each migrated function
- Multi-tenant scenarios
- Archive query scenarios
- Pagination edge cases

### Performance Tests
- Benchmark ListJobs before/after
- Benchmark status check operations
- Ensure no performance regression > 10%

### External API Compatibility Tests
- swf.Engine interface unchanged
- swf.ListJobsRequest/Response types unchanged
- All public swf-go API behavior identical

---

## Risks and Mitigation

### Risk 1: Feature Gaps in pgwf.ListJobs
**Impact**: Can't fully replicate swf.ListJobs functionality internally
**Mitigation**:
- Identify gaps early (multi-tenant, complex patterns)
- Extend pgwf API before migrating
- Implement client-side filtering in swf-go for unsupported features
- Or keep custom SQL for complex cases if necessary

### Risk 2: Performance Regression
**Impact**: Slower queries after migration
**Mitigation**:
- Benchmark extensively before and after
- Ensure pgwf uses proper database indexes
- Add caching layer if needed
- Profile query execution plans

### Risk 3: Status Mapping Mismatches
**Impact**: Wrong status returned by swf-go APIs
**Mitigation**:
- Comprehensive status conversion tests
- Document all status mappings
- Special handling for archived jobs (completed vs cancelled)
- Integration tests covering all status transitions

---

## Success Criteria

After migration is complete:

1. ✅ **Zero direct SQL queries** to `pgwf.*` tables in swf-go codebase
2. ✅ **Zero GORM models** referencing pgwf tables
3. ✅ **All tests passing** - no test modifications needed (except mocks)
4. ✅ **Performance maintained** - no regression > 10%
5. ✅ **External API unchanged** - swf.Engine interface identical, zero impact to consumers
6. ✅ **Code reduction** - ~300+ lines of SQL building code removed
7. ✅ **Better abstraction** - swf-go isolated from pgwf schema changes

---

## API Review Findings

**See `/src/PGWF_API_REVIEW.md` for detailed analysis**

### ✅ UPDATE: All Features Now Implemented in pgwf-go!

**As of latest pgwf-go update**, all previously identified blockers have been resolved:

1. ✅ **Cursor-based pagination** - IMPLEMENTED
   - Full cursor encoding/decoding with query hash validation
   - Row comparison: `(sort_field, job_id) > ($1, $2)` for stable pagination
   - See `query.go:1074-1230`

2. ✅ **Multi-tenant filtering** - IMPLEMENTED
   - `TenantIDs []string` field (backwards compatible with `TenantID`)
   - SQL: `tenant_id = ANY($1)` with `pq.Array()`
   - See `query.go:89-92, 1254-1262`

3. ✅ **Multi-pattern job type filtering** - IMPLEMENTED
   - `JobTypePatterns []string` field (backwards compatible with `JobTypePattern`)
   - OR semantics: `(next_need LIKE $1 OR next_need LIKE $2 ...)`
   - See `query.go:96-97, 1138-1147`

### Key Findings from pgwf-go API Review:

1. ✅ **GetJobStatus, CheckJobExists, GetJob, FindJobs** - Fully supported, straightforward migration
2. ✅ **FindJobs multi-tenant** - Supports `TenantIDs []string` (multiple tenants)
3. ✅ **ListJobs cursor pagination** - ✨ NOW IMPLEMENTED with query hash validation
4. ✅ **ListJobs multi-tenant** - ✨ NOW SUPPORTS `TenantIDs []string`
5. ✅ **ListJobs job type filtering** - ✨ NOW SUPPORTS `JobTypePatterns []string` with OR
6. ⚠️ **Archived job status** - Returns undocumented `JobStatus("COMPLETED")` string (handle in swf-go)
7. ⚠️ **API structure** - Module-level functions (not Client methods), takes DB as parameter

### Migration Status: ALL FUNCTIONS READY

**All 7 functions can now be migrated!** No blockers remain.

---

## Appendix: Status Mapping Reference

### pgwf.JobStatus → swf.JobStatus

| pgwf Status | swf Status | Description |
|-------------|------------|-------------|
| `READY` | `JobStatusReady` | Ready to be leased |
| `ACTIVE` | `JobStatusActive` | Currently leased |
| `CANCELLED` | `JobStatusCancelled` | Cancellation requested |
| `AWAITING_FUTURE` | `JobStatusAwaitingFuture` | Waiting for available_at |
| `PENDING_JOBS` | `JobStatusPendingJobs` | Blocked by dependencies |
| `CRASH_CONCERN` | `JobStatusCrashConcern` | Too many lease expirations |
| `EXPIRED` | `JobStatusExpired` | Hit expires_at timestamp |
| `"COMPLETED"` ⚠️ | `JobStatusCompleted` | **Undocumented** - returned for archived jobs |

**Special Cases**:
- Archived job with `cancel_requested=true` → `JobStatusCancelled`
- Archived job with `cancel_requested=false` → `JobStatus("COMPLETED")` (literal string, not a defined constant)
- **Important**: "COMPLETED" is not a defined constant in pgwf.JobStatus enum - it's constructed dynamically for archived jobs

---

## Timeline Summary

### ✅ Full Migration Now Possible!

**All pgwf-go features implemented** - Complete migration of all 7 functions is achievable.

| Phase | Duration | Tasks | Status |
|-------|----------|-------|--------|
| 1. Infrastructure | 2-3 days | Add status conversion helpers, tests | ✅ Ready |
| 2. Simple Reads | 3-5 days | Status checks, archive checks (3 functions) | ✅ Ready |
| 3. Idempotency | 2-3 days | Child job existence checks (1 function) | ✅ Ready |
| 4. Discovery | 2-3 days | Find/Get waiting tasks (2 functions) | ✅ Ready |
| 5. ListJobs Migration | 5-7 days | Complex mapping, extensive testing | ✅ Ready |
| 6. Cleanup | 2-3 days | Remove old GORM models, docs | ✅ Ready |
| 7. Performance | 3-5 days | Benchmark all migrated functions | ✅ Ready |

**Full Migration Total: 3-4 weeks** (20-28 working days)

---

## Conclusion

This migration plan enables a **complete migration** of all database access to pgwf-go APIs:

### ✅ Will Achieve (ALL 7 of 7 functions):
- **Eliminate** 100% of direct database access from swf-go ✨
- **Remove** ~300 lines of code (GORM models, raw SQL queries, custom pagination)
- **Improve** abstraction for all read operations
- **Enable** pgwf schema evolution without breaking swf-go
- **Preserve** swf-go's external API completely - zero impact to consumers
- **Maintain** full functionality - no feature loss
- **Benefit from pgwf improvements** - cursor pagination, validation, error handling

### Key Benefits:

1. **Complete Decoupling**: swf-go no longer directly queries `pgwf.*` tables
2. **Reduced Complexity**: ~300 lines of SQL building replaced with ~100 lines of API calls
3. **Better Maintainability**: All database schema changes handled by pgwf-go
4. **Consistent Error Handling**: Unified error types across all operations
5. **Production-Tested Pagination**: Cursor implementation validated by pgwf-go
6. **No Breaking Changes**: swf-go's external API unchanged

### Migration Path:

The migration can be executed in **3-4 weeks** through a phased approach:
1. Weeks 1-2: Migrate simple operations (functions 1-4)
2. Week 2-3: Migrate ListJobs with extensive testing
3. Week 3-4: Cleanup, performance testing, documentation

### Recommendation:

**Proceed with full migration** now that all pgwf-go features are implemented. This delivers complete abstraction and eliminates all coupling to pgwf database schema. The migration is low-risk as swf-go's external API remains unchanged and all functionality is preserved.
