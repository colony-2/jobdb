# Proposal: pgjobdb Package

## Summary

Create `github.com/colony-2/pgjobdb` as a new JobDB-native Postgres repo. It
should copy the current pgwf SQL layer and pgwf-go client code, keep that
copied scheduler code working, and then extend it into one coherent
JobDB-native package.

This is a hard break. There is no data migration, backfill tooling, dual-read
period, rollback mirror, or compatibility layer for old pgwf databases.
The first deployment uses a brand-new empty Postgres database. Subsequent
starts use the initialized `pgjobdb` schema; startup rejects a database with
old pgwf or JobDB chapter state instead of adopting it.

This document is intended to be executed from an empty initial `pgjobdb` repo
directory. Every implementation step ends with a commit in that repo.

## Goals

- Provide one repo that owns Postgres DDL, stored procedures, Go bindings, and
  a JobDB-facing runtime adapter.
- Copy existing working scheduler mechanics instead of rewriting the scheduler
  from scratch.
- Make JobDB job type, task routing, job/time blockers, run policy, parent job,
  schema hash, schedule definitions, schedule occurrence fields, app metadata,
  and completion first-class database fields.
- Move Postgres schedule definition/control state from `jobdb_schedules` into
  `pgjobdb.schedules`.
- Keep job waits inline as scheduler state; do not normalize job waits.
- Keep prerequisite proceed/fail policy in JobDB-owned runtime metadata, not in
  the scheduler wait set.
- Preserve the current JobDB lease payload JSON view and archived job summary
  fields while moving their scheduler storage to first-class columns.

## Non-Goals

- Do not modify pgwf or pgwf-go in place.
- Do not support old pgwf data or old `jobdb_schedules` data.
- Do not provide compatibility views or public generic queue wrappers.
- Do not make `pgjobdb` run workflow user code.
- Do not expose low-level SQL procedures as JobDB's public remote API.
- Do not copy JobDB core private or internal runtime implementation into
  `pgjobdb`.

## JobDB Dependency Boundary

Yes, `pgjobdb` should import `github.com/colony-2/jobdb/pkg/jobdb`.

Use that dependency for two things:

- tests and conformance checks against core JobDB public types;
- a public `pgjobdb.Runtime` that implements `jobdb.WorkflowRuntime` by
  composing JobDB-owned runtime core APIs with `pgjobdb` scheduler database
  APIs.

This is safe as long as `github.com/colony-2/jobdb/pkg/jobdb` and its public
runtime core do not import `pgjobdb`. Other concrete runtimes can compose that
same core without importing `pgjobdb`. The JobDB CLI and an optional
`pkg/jobdb/runtime/direct` compatibility wrapper may import `pgjobdb`.

`pgjobdb` must not import `pkg/jobdb/internal/...` and should not copy those
private implementations. If exposing `jobdb.WorkflowRuntime` needs JobDB core
behavior that is currently internal, that behavior should move behind a clean
public core API first.

The initial JobDB core feature requests are written separately for submission
to the JobDB project:
[FEATURE-REQUESTS-JOBDB-CORE-FOR-PGJOBDB.md](FEATURE-REQUESTS-JOBDB-CORE-FOR-PGJOBDB.md).
Implementing those requests is separate work in the JobDB repo; the `pgjobdb`
repo should wait for public APIs instead of vendoring private JobDB code.

## Repository Shape

Start from an empty git repo:

```text
github.com/colony-2/pgjobdb
  go.mod
  ddl/
  sql/
  internal/
    db/
    leasequeue/
    sqlscan/
  pgjobdb.go
  types.go
  jobs.go
  schedules.go
```

The exact internal package names can change during implementation. The
important split is public API at the repo root, scheduler mechanics under
`internal/`, and SQL/DDL versioned in the repo.

## Storage Model

Normalize around ownership boundaries:

- `pgjobdb.job_facts` stores immutable facts about every job, active or
  archived.
- `pgjobdb.jobs` stores only mutable active scheduler state and references
  `job_facts`.
- `pgjobdb.jobs_archive` stores terminal completion and a final snapshot of
  mutable scheduler state needed by `jobdb.JobSummary`, and references
  `job_facts`.
- `pgjobdb.schedules` stores schedule definitions and schedule control state.
- `pgjobdb.job_facts` also stores nullable schedule occurrence fields for jobs
  created by a schedule.

This keeps immutable facts in one table while retaining final mutable state in
the archive for JobDB reads. It also moves schedule control state into the same
Postgres scheduling package as the jobs it controls.

### Job Facts

Recommended immutable job facts:

```sql
tenant_id TEXT NOT NULL,
job_id TEXT NOT NULL,
job_type TEXT NOT NULL,

run_policy JSONB NOT NULL DEFAULT '{}'::JSONB,
app_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,

schema_hash TEXT,
parent_job_id TEXT,

schedule_id TEXT,
schedule_generation BIGINT,
schedule_spec_hash TEXT,
scheduled_at TIMESTAMPTZ,
schedule_run_id TEXT,
schedule_reason TEXT,
schedule_manual BOOLEAN NOT NULL DEFAULT FALSE,
schedule_backfill_id TEXT,
schedule_previous_job_id TEXT,
schedule_failure_history JSONB,

created_by_worker_id TEXT,
created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
expires_at TIMESTAMPTZ NOT NULL DEFAULT 'infinity',

PRIMARY KEY (tenant_id, job_id),
FOREIGN KEY (tenant_id, schedule_id)
    REFERENCES pgjobdb.schedules (tenant_id, schedule_id)
```

The schedule occurrence fields are nullable because most jobs are not scheduled
jobs. They belong in `job_facts` because a scheduled occurrence produces exactly
one JobDB job in the current model, and the occurrence metadata is immutable
provenance for that job.

Use check constraints to keep the schedule fields coherent: when `schedule_id`
is set, `schedule_generation`, `scheduled_at`, and `schedule_run_id` should be
set too. A partial unique index on `(tenant_id, schedule_id, schedule_run_id)`
prevents duplicate stored occurrences for the same schedule run.

### Schedules

Recommended schedule definition and control state:

```sql
tenant_id TEXT NOT NULL,
schedule_id TEXT NOT NULL,

state TEXT NOT NULL,
generation BIGINT NOT NULL,
spec_hash TEXT NOT NULL,

trigger JSONB NOT NULL,
target_job_type TEXT NOT NULL,
target_snapshot JSONB NOT NULL,
overlap_policy TEXT NOT NULL,
failure_policy JSONB NOT NULL,

next_fire_at TIMESTAMPTZ,
next_job_id TEXT,

created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

PRIMARY KEY (tenant_id, schedule_id)
```

`state` stores desired control state:

```text
ACTIVE
PAUSED
ARCHIVED
```

Derived states such as `FAILURE_PAUSED` remain projections calculated from
schedule occurrence history and failure policy. They are not written as the
schedule row's desired state.

`target_snapshot` is opaque to `pgjobdb`. JobDB core serializes its schedule
target there, including input/artifact descriptors, run policy, and application
metadata.

### Active State

Recommended active scheduler state:

```sql
tenant_id TEXT NOT NULL,
job_id TEXT NOT NULL,

route_job_type TEXT NOT NULL,
work_kind TEXT NOT NULL,
task_type TEXT,
resume_job_type TEXT,
task_input_ordinal BIGINT,
task_output_ordinal BIGINT,
task_input_hash TEXT,

wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],

available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
alternate_job_type TEXT,
alternate_task_type TEXT,
alternate_after_seconds INTEGER,

lease_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
lease_id TEXT,
lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
lease_expiration_count BIGINT NOT NULL DEFAULT 0,
consecutive_expirations BIGINT NOT NULL DEFAULT 0,
cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
cancel_requested_by TEXT,
cancel_requested_at TIMESTAMPTZ,

PRIMARY KEY (tenant_id, job_id),
FOREIGN KEY (tenant_id, job_id)
    REFERENCES pgjobdb.job_facts (tenant_id, job_id)
```

`work_kind` is routing, not a wait condition:

```text
JOB
TASK
```

Time blocking is represented by `available_at`. Job blocking is represented by
the inline `wait_for` array. `route_job_type` is mutable routing state; it
starts as the immutable `job_facts.job_type` and preserves public
`NextNeed`/alternate-route behavior when work is routed to another job type.
Task coordinates are required for `work_kind = 'TASK'` and null otherwise.

### Completion State

Recommended terminal completion state:

```sql
tenant_id TEXT NOT NULL,
job_id TEXT NOT NULL,
archived_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
completion_status TEXT NOT NULL,
completion_detail TEXT,
completion_error_kind TEXT,
completion_retryable BOOLEAN,

-- Final scheduler-state snapshot for archived JobDB reads:
final_route_job_type TEXT NOT NULL,
final_work_kind TEXT NOT NULL,
final_task_type TEXT,
final_resume_job_type TEXT,
final_task_input_ordinal BIGINT,
final_task_output_ordinal BIGINT,
final_task_input_hash TEXT,
final_wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
final_available_at TIMESTAMPTZ NOT NULL,
final_lease_expires_at TIMESTAMPTZ,
final_cancel_requested BOOLEAN NOT NULL,
final_lease_payload JSONB NOT NULL DEFAULT '{}'::JSONB,

PRIMARY KEY (tenant_id, job_id),
FOREIGN KEY (tenant_id, job_id)
    REFERENCES pgjobdb.job_facts (tenant_id, job_id)
```

Use JobDB completion values:

```text
success
failed_app
failed_system
failed_timeout
cancelled
```

Archiving inserts the terminal row with a snapshot of the active row, deletes
the active scheduler row, removes the completed job ID from other rows' inline
`wait_for` arrays, and keeps the immutable `job_facts` row. The snapshot
preserves the existing archived `JobSummary` contract without copying immutable
job facts.

## Stored Procedures

Use JobDB-native procedures instead of generic queue signatures:

```sql
pgjobdb.upsert_schedule(...)
pgjobdb.pause_schedule(...)
pgjobdb.resume_schedule(...)
pgjobdb.archive_schedule(...)
pgjobdb.get_schedule(...)
pgjobdb.list_schedules(...)

pgjobdb.submit_job(...)
pgjobdb.get_work(...)
pgjobdb.get_job_lease(...)
pgjobdb.cancel_job(...)
pgjobdb.validate_lease(...)
pgjobdb.extend_lease(...)
pgjobdb.reschedule_job(...)
pgjobdb.complete_job(...)
pgjobdb.complete_task_work(...)
pgjobdb.list_jobs(...)
pgjobdb.get_job(...)
pgjobdb.get_job_status(...)
pgjobdb.list_schedule_runs(...)
```

Job procedures write `job_facts` plus active state directly and never require
callers to build `next_need`, generic `payload`, or metadata envelopes.
`complete_task_work` atomically compares the expected task route and
coordinates and rejects a competing lease or completion before rescheduling.

## Go API

The root package can expose both typed scheduler operations and a
`jobdb.WorkflowRuntime` implementation.

Core identifiers:

```go
type TenantID string
type JobID string
type WorkerID string
type JobType string
type TaskType string

type WorkKind string

const (
    WorkKindJob  WorkKind = "JOB"
    WorkKindTask WorkKind = "TASK"
)
```

Run policy:

```go
type RetryPolicy struct {
    InitialIntervalMillis  int64
    BackoffCoefficient     float64
    MaximumIntervalMillis  int64
    MaximumAttempts        int32
    NonRetryableErrorTypes []string
}

type RunPolicy struct {
    Retry                   RetryPolicy
    InvocationTimeoutMillis *int64
    TotalTimeoutMillis      *int64
}
```

Task routing and runtime metadata:

```go
type TaskWork struct {
    JobType       JobType
    TaskType      TaskType
    ResumeJobType JobType
    InputOrdinal  int64
    OutputOrdinal int64
    InputHash     string
}

type ScheduleOccurrence struct {
    ScheduleID     string
    Generation     int64
    SpecHash       string
    ScheduledAt    time.Time
    RunID          string
    Reason         string
    Manual         bool
    BackfillID     string
    PreviousJobID  string
    FailureHistory json.RawMessage
}

type RuntimeMetadata struct {
    SchemaHash  string
    ParentJobID JobID
    Schedule    *ScheduleOccurrence
}
```

Schedule API:

```go
type ScheduleState string

const (
    ScheduleStateActive   ScheduleState = "ACTIVE"
    ScheduleStatePaused   ScheduleState = "PAUSED"
    ScheduleStateArchived ScheduleState = "ARCHIVED"
)

type Schedule struct {
    TenantID       TenantID
    ScheduleID     string
    State          ScheduleState
    Generation     int64
    SpecHash       string
    Trigger        json.RawMessage
    TargetJobType  JobType
    TargetSnapshot json.RawMessage
    OverlapPolicy  string
    FailurePolicy  json.RawMessage
    NextFireAt     *time.Time
    NextJobID      *JobID
    CreatedAt      time.Time
    UpdatedAt      time.Time
}

func UpsertSchedule(ctx context.Context, db DB, req UpsertScheduleRequest) (*Schedule, error)
func GetSchedule(ctx context.Context, db DB, tenantID TenantID, scheduleID string) (*Schedule, error)
func ListSchedules(ctx context.Context, db DB, opts ListSchedulesOptions) (*ListSchedulesResult, error)
func PauseSchedule(ctx context.Context, db DB, req ScheduleMutationRequest) (*Schedule, error)
func ResumeSchedule(ctx context.Context, db DB, req ScheduleMutationRequest) (*Schedule, error)
func ArchiveSchedule(ctx context.Context, db DB, req ScheduleMutationRequest) (*Schedule, error)
```

Job API:

```go
func SubmitJob(ctx context.Context, db DB, req SubmitJobRequest) (*SubmitJobResult, error)
func GetWork(ctx context.Context, db DB, worker WorkerID, selector WorkSelector, opts GetWorkOptions) (*JobLease, error)
func GetJobLease(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, selector WorkSelector, opts GetJobLeaseOptions) (*JobLease, error)
func CancelJob(ctx context.Context, db DB, req CancelJobRequest) error
func ValidateLease(ctx context.Context, db DB, req LeaseIdentity) (*JobLease, error)
func KeepAliveLease(ctx context.Context, db DB, req KeepAliveLeaseRequest) (*JobLease, error)
func RescheduleUnheldJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, req RescheduleRequest) error
func CompleteUnheldJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, worker WorkerID, completion Completion) error
func CompleteTaskWork(ctx context.Context, db DB, req CompleteTaskWorkRequest) error
func ListJobs(ctx context.Context, db DB, opts ListJobsOptions) (*ListJobsResult, error)
func GetJob(ctx context.Context, db DB, tenantID TenantID, jobID JobID, opts GetJobOptions) (*JobDetail, error)
func GetJobStatus(ctx context.Context, db DB, tenantID TenantID, jobID JobID) (*JobStatusInfo, error)
func ListScheduleRuns(ctx context.Context, db DB, opts ListScheduleRunsOptions) (*ListScheduleRunsResult, error)
```

`ListJobsOptions` must support JobDB's job-key, task selector, parent/root,
metadata, status, and active/archive filters. `GetWorkOptions` must support
metadata filtering. Both list and schedule-run pagination must apply all
filters before selecting a page.

`jobdb.ExecutionLease.Payload()` retains its current JSON view. The adapter
constructs it from immutable `run_policy`, typed task work, and opaque
`lease_payload`. On reschedule it decodes known fields, checks that run policy
matches the immutable job fact, and saves only other application fields in
`lease_payload`.

Runtime adapter:

```go
type Config struct {
    PostgresDSN            string
    BlobStoreURI           string
    MaxInlineArtifactBytes int64
    Logger                 *slog.Logger
}

type Runtime struct {
    // implements jobdb.WorkflowRuntime
}

func New(db *gorm.DB, cfg Config) (*Runtime, error)
func NewFromConfig(cfg Config) (*Runtime, error)
```

The runtime adapter imports `github.com/colony-2/jobdb/pkg/jobdb`, wires
`pgjobdb` scheduler database operations into JobDB-owned runtime core APIs, and
does not reimplement JobDB workflow semantics.

## Implementation Plan

### Step 1: Bootstrap The Repo

From an empty repo directory:

- initialize `go.mod` as `github.com/colony-2/pgjobdb`;
- add baseline `.gitignore`, README, license, CI skeleton, and test command;
- add `github.com/colony-2/jobdb` as a dependency, using a local `replace` only
  for development if needed.

Commit:

```text
bootstrap pgjobdb module
```

### Step 2: Copy Existing Scheduler Code

- copy the current pgwf SQL and pgwf-go Go code into the new repo;
- keep the copied public surface temporarily so tests can prove behavior before
  extension;
- mechanically rename module paths, package names, SQL schema names, and
  procedure namespace to `pgjobdb`;
- run the copied tests and fix only mechanical breakage.

Commit:

```text
copy pgwf scheduler mechanics into pgjobdb
```

### Step 3: Add Runtime Adapter Boundary

- add a `Runtime` adapter skeleton that imports only public
  `github.com/colony-2/jobdb/pkg/jobdb` APIs;
- build on the existing public `pkg/jobdb/runtime/core` ports and
  `pkg/jobdb/runtimetest` harness; finish the composing runtime facade in the
  JobDB repo before wiring workflow behavior;
- use the public `pkg/jobdb/chapterstore/postgres` chapter-log adapter;
- use the public `pkg/jobdb/schemastore/postgres` schema-store adapter;
- identify any other JobDB-owned runtime behavior needed by `pgjobdb.Runtime`
  that is not public today;
- cross-check any gaps against the JobDB core feature-request document and
  submit updates to the JobDB project if new blockers are found;
- do not copy `pkg/jobdb/internal/...` packages or direct-runtime
  implementation into `pgjobdb`.

Commit:

```text
add JobDB runtime adapter boundary
```

### Step 4: Add JobDB-Native DDL

- add `pgjobdb.schedules`;
- add `pgjobdb.job_facts`;
- reshape active state in `pgjobdb.jobs`;
- reshape terminal state in `pgjobdb.jobs_archive`, including the final
  mutable-state snapshot needed for archived JobDB summaries;
- add constraints and indexes for schedule fields, task routing, app metadata,
  and inline `wait_for`.

Commit:

```text
add JobDB-native Postgres schema
```

### Step 5: Add Job Procedures

- implement `submit_job`;
- implement `get_work` and `get_job_lease`;
- implement cancellation, lease validation, and lease renewal;
- implement `reschedule_job`;
- implement `complete_job`;
- implement `complete_task_work`;
- implement `list_jobs`, `get_job`, and `get_job_status`;
- implement schedule-run listing and apply every filter before pagination;
- reuse copied lease, archive, notify, and keepalive mechanics.

Commit:

```text
add JobDB-native job procedures
```

### Step 6: Add Schedule Procedures

- implement `upsert_schedule`;
- implement `pause_schedule`, `resume_schedule`, and `archive_schedule`;
- implement `get_schedule` and `list_schedules`;
- wire schedule occurrence fields into job submission and listing;
- keep `target_snapshot` opaque.

Commit:

```text
add JobDB-native schedule procedures
```

### Step 7: Add Typed Go Bindings

- add typed scheduler structs for jobs, leases, task routing, schedule
  definitions, runtime metadata, and completion;
- bind each new stored procedure;
- keep copied scheduler DB, scanning, lease, and keepalive helpers private;
- delete or hide generic queue APIs before release.

Commit:

```text
add typed pgjobdb Go API
```

### Step 8: Expose The JobDB Runtime API

- make `Runtime` implement `jobdb.WorkflowRuntime` using public JobDB runtime
  core APIs and any public APIs added through accepted JobDB core feature
  requests;
- wire JobDB submit, restart, lease, reschedule, completion, list, and schedule
  paths to the new scheduler operations through the runtime core boundary;
- satisfy chapter and artifact APIs through the public JobDB-owned chapter store
  package or port;
- add a compile-time assertion that `*pgjobdb.Runtime` implements
  `jobdb.WorkflowRuntime`;
- remove generic `next_need`, payload, and metadata-envelope encoding from the
  Postgres/direct path;
- do not import or copy JobDB core internal packages.

Commit:

```text
expose JobDB WorkflowRuntime from pgjobdb
```

### Step 9: Add Tests Against JobDB Core

- import `github.com/colony-2/jobdb/pkg/jobdb` in tests;
- add direct runtime conformance tests using core JobDB public requests and
  responses;
- test schedule definition round trips;
- test scheduled run listing through `job_facts` schedule fields;
- test prerequisite `complete` and `success` behavior after unblocking;
- test task work coordinate conflicts;
- test archived summaries retain their final route, wait set, task work,
  payload view, and cancellation fields;
- test the public lease payload view matches existing workflow and remote
  worker expectations;
- test the first deployment initializes an empty database and later startup
  accepts its initialized `pgjobdb` schema;
- test hard-break startup failure when the configured schema is not `pgjobdb`.

Commit:

```text
add JobDB conformance tests
```

### Step 10: Remove Temporary Generic Surface

- delete copied public generic queue APIs that are not part of `pgjobdb`;
- keep only private helpers that still support JobDB-native behavior;
- remove temporary tests for public generic behavior;
- update README and package docs around the hard-break contract.

Commit:

```text
remove generic queue compatibility surface
```

## JobDB Core Cutover

After the `pgjobdb` repo is ready, update the JobDB repo separately:

- use a released JobDB core version as the `pgjobdb` dependency, then depend
  on a released `pgjobdb` version for the JobDB direct wrapper;
- replace pgwf imports with `github.com/colony-2/pgjobdb`;
- make `pkg/jobdb/runtime/direct` a thin compatibility wrapper around
  `pgjobdb.Runtime`;
- remove Postgres/direct `jobdb_schedules` DDL and SQL;
- remove Postgres/direct generic payload/metadata encoding and `next_need`
  parsing;
- keep SQLite, toy, and remote behavior aligned through existing conformance
  tests.

## Open Questions

- Should the root package expose both low-level scheduler functions and the
  `jobdb.WorkflowRuntime`, or should low-level functions live under an internal
  package with only the runtime public?
- Should `run_policy` remain JSONB, or should retry and timeout fields be
  decomposed immediately?
- Should `target_snapshot` stay opaque JSONB indefinitely, or should schedule
  run policy and app metadata become schedule-level first-class fields too?
