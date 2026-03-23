# User Guide: Completion Status/Detail + Job Prerequisites

This guide summarizes the new completion status/detail fields and the new prerequisite feature for `StartJob` and `RestartJob`.

## Completion Status + Completion Detail

### What changed
When a job finishes, SWF now records **completion status** and **completion detail** in PGWF (archived jobs). This enables downstream systems to distinguish success, failure types, and reason strings without inspecting Strata.

### Status values
SWF currently writes these completion status values:
- `success`
- `failed_app`
- `failed_system`
- `failed_timeout`
- `cancelled`

`completion_detail` is a short, human‑readable message derived from the error payload (or empty on success).

### Where to read it
Use PGWF APIs (archived jobs only), e.g. `pgwf.GetJob(...)` or `pgwf.ListJobs(...)` with `IncludeArchived` to read:
- `CompletionStatus`
- `CompletionDetail`

## Job Prerequisites

### What changed
`StartJob` and `RestartJob` now accept **prerequisites**. A job can wait for other jobs to **complete** or to **complete successfully** before running.

### API
```go
type JobPrereqCondition string

const (
    JobPrereqComplete JobPrereqCondition = "complete" // job must be archived (any outcome)
    JobPrereqSuccess  JobPrereqCondition = "success"  // job must be archived + succeeded
)

type JobPrerequisite struct {
    JobID     string
    Condition JobPrereqCondition
}

type StartJob struct {
    ...
    Prerequisites []JobPrerequisite
}

type RestartJob struct {
    ...
    Prerequisites []JobPrerequisite
}
```

### Behavior
- All prereq job IDs are submitted to PGWF `wait_for`.
- `complete` prereqs require only that the prereq job is archived.
- `success` prereqs require `completion_status == "success"`.
- If a `success` prereq fails, the dependent job is **failed immediately** with a non‑retryable error.

### Where prereqs are stored
Prereqs are stored in **Strata job metadata** (chapter metadata), not in PGWF payloads.

### Restart-specific rule
If you provide prereqs for a restart, you **must** also provide `ExtraTaskOutput` (and optionally `ExtraTaskInput`).
This ensures prereqs can be stored on the `RestartExtra` chapter.

### Evaluation points
- **Job start**: chapter 0 prereqs (if present) are enforced before the job runs.
- **Restart extra**: if prereqs are attached to the `RestartExtra` chapter, they are evaluated when that chapter is encountered.

## Minimal Example
```go
// Start a job that waits for another job to succeed.
_, _ = engine.StartJob(ctx, swf.StartJob{
    TenantId: "t1",
    JobType:  "report",
    Data:     swf.NewTaskDataOrPanic(map[string]any{"n": 1}),
    Prerequisites: []swf.JobPrerequisite{
        {JobID: "job-abc", Condition: swf.JobPrereqSuccess},
    },
})

// Restart with prereqs (requires ExtraTaskOutput)
_, _ = engine.RestartJob(ctx, swf.RestartJob{
    PriorJobKey: baseKey,
    LastStepToKeep: 0,
    ExtraTaskInput:  swf.NewTaskDataOrPanic(map[string]any{}),
    ExtraTaskOutput: swf.NewTaskDataOrPanic(map[string]any{"n": 2}),
    Prerequisites: []swf.JobPrerequisite{
        {JobID: "job-xyz", Condition: swf.JobPrereqSuccess},
    },
})
```
