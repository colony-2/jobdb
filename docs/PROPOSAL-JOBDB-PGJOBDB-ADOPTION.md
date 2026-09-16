# Proposal: Move JobDB Direct Runtime to pgjobdb

## Summary

JobDB's direct Postgres runtime currently adapts its JobDB model to pgwf's
generic queue model. The adaptation works, but conventions leak through the
code:

- `startJob` writes job type into `pgwf.JobDependencies.NextNeed`;
- task routing uses capability strings such as `jobType:taskType`;
- run policy and task work coordinates are stored in pgwf `payload`;
- schema hash, parent job, schedule occurrence, and app metadata are stored in
  a pgwf `metadata` envelope;
- schedule definition/control state is stored in `jobdb_schedules`, separate
  from the scheduler package;
- `ListJobs` uses generic queue filters and decodes payloads for task details;
- external task completion validates task routing by decoding current pgwf
  payload.

The new direction is to create `github.com/colony-2/pgjobdb`, a JobDB-native
Postgres scheduler package that includes the SQL layer and Go APIs together.
JobDB core should move the direct runtime to that package instead of changing
pgwf and pgwf-go in place.

`pgjobdb` is an optional concrete runtime. Applications import it when they
choose Postgres; other runtime implementations can compose the same public
JobDB runtime core without importing `pgjobdb`. `pkg/jobdb` and the public
runtime core must not import concrete backends. The JobDB CLI and an optional
`pkg/jobdb/runtime/direct` compatibility wrapper may import `pgjobdb`.

This is a hard break. There is no data migration, dual-read period, rollback
mirror, or compatibility layer for existing pgwf databases. Implement the new
package by copying the current pgwf and pgwf-go code, preserving the useful
scheduler mechanics, and extending that copy for JobDB-native behavior.
Do not copy JobDB core private runtime internals into `pgjobdb`; if the new
package needs a core capability that is not public, open a feature request
against the JobDB project and land it there first.

The `pgjobdb` repo/package plan is in
[PROPOSAL-PGJOBDB.md](PROPOSAL-PGJOBDB.md).

## Goals

- Replace direct runtime dependence on pgwf and pgwf-go with `pgjobdb`.
- Remove JobDB's `next_need`, generic `payload`, and generic `metadata`
  encoding logic for runtime concepts.
- Move Postgres schedule definition/control state from `jobdb_schedules` into
  `pgjobdb.schedules`.
- Keep `pkg/jobdb` and `pkg/workflow` public APIs stable where possible.
- Use `pgjobdb` parent, schedule, schema, task routing, and app metadata filters
  for direct runtime list and lease paths.
- Keep SQLite, toy, and remote runtime behavior aligned with direct runtime.
- Make a clean cutover for the Postgres/direct runtime.

## Non-Goals

- Do not expose `pgjobdb` types through JobDB's public remote runtime API.
- Do not support old pgwf data, `jobdb_schedules` data, or dual-write rollback
  for the Postgres/direct runtime.
- Do not change JobDB's public chapter or artifact semantics.
- Do not copy JobDB core private or internal runtime implementation into
  `pgjobdb`.
- Do not change workflow user APIs unless the direct runtime cutover forces it.
- Do not require toy and SQLite runtimes to use `pgjobdb`.

## Current Direct Runtime Hotspots

The main code paths to change are in
`pkg/jobdb/runtime/direct/internal/directimpl`:

- `startJob`: calls `pgwf.SubmitJob` with `NextNeed`, generic payload, and
  metadata envelope.
- `PollWork` and `GetJobLease`: convert worker capabilities to
  `[]pgwf.Capability`.
- `RescheduleJobWithLeaseByID`: builds `pgwf.JobDependencies` and stores
  scheduler payload JSON.
- `CompleteJobWithLeaseByID`: maps JobDB completion status to pgwf generic
  completion strings.
- `CompleteTaskIfWaiting`: loads pgwf payload to validate task work
  coordinates and reschedules an unheld job.
- `ListJobs`: translates job/task filters to generic queue patterns, filters
  parent IDs in memory, strips runtime metadata, and fetches payload for task
  work details.
- schedule submission: writes schedule occurrence metadata into the pgwf
  metadata envelope before submitting a job.
- schedule APIs: read and write `jobdb_schedules`, which should become
  `pgjobdb.schedules` for the Postgres/direct runtime.

## Target Shape

Add a narrow scheduler adapter inside `pgjobdb.Runtime`, using the public
JobDB runtime core. Keep `pkg/jobdb/runtime/direct` as a thin compatibility
wrapper for existing imports; it must not own workflow semantics:

```go
func pgjobdbSubmitRequestFromJobDB(req jobdb.SubmitJobRequest, schemaHash string, parentJobID string) (pgjobdb.SubmitJobRequest, error)
func pgjobdbRestartRequestFromJobDB(req jobdb.SubmitRestartJobRequest, schemaHash string, parentJobID string) (pgjobdb.SubmitJobRequest, error)
func pgjobdbWorkSelector(capabilities []string) (pgjobdb.WorkSelector, error)
func pgjobdbRescheduleRequest(req jobdb.RescheduleExecutionRequest) (pgjobdb.RescheduleRequest, error)
func pgjobdbCompletion(req jobdb.CompleteExecutionRequest) pgjobdb.Completion
func jobSummaryFromPgjobdb(job pgjobdb.JobListItem) jobdb.JobSummary
func pgjobdbScheduleFromJobDB(req jobdb.UpsertScheduleRequest) (pgjobdb.UpsertScheduleRequest, error)
func jobdbScheduleFromPgjobdb(schedule pgjobdb.Schedule) (jobdb.ScheduleInfo, error)
```

The public runtime core continues to operate on JobDB concepts. Applications
that use another backend import only that backend and JobDB core.

`pgjobdb` imports `github.com/colony-2/jobdb/pkg/jobdb` and its public runtime
core for the adapter and tests. That does not create a package import cycle as long as
`pkg/jobdb` does not import `pgjobdb`; concrete runtime packages already sit
outside the core package boundary.

If that public boundary is not enough, treat the missing capability as a JobDB
core feature request, not as code to copy into `pgjobdb`. The preferred shape
is a JobDB-owned runtime core with explicit backend ports, so chapter, artifact,
schema, schedule target, preflight, prerequisite, retry, and lease
authorization semantics stay in JobDB core.

The initial feature-request document for the JobDB core project is
[FEATURE-REQUESTS-JOBDB-CORE-FOR-PGJOBDB.md](FEATURE-REQUESTS-JOBDB-CORE-FOR-PGJOBDB.md).

## Normalized Reads

JobDB should treat `pgjobdb.job_facts` as the canonical source for immutable
submit-time fields: job type, run policy, app metadata, schema hash, parent job,
schedule occurrence fields, creation time, and expiration time.

JobDB should treat `pgjobdb.schedules` as the canonical source for
Postgres/direct schedule definition and control state. The current
`jobdb_schedules` table is removed from the Postgres/direct runtime path.

The active `pgjobdb.jobs` row describes current scheduler state. The archive
`pgjobdb.jobs_archive` row stores terminal completion and a final snapshot of
the mutable fields exposed by `jobdb.JobSummary`: route, wait set, availability,
lease expiry, cancellation request, lease payload, and task work coordinates.
Archiving moves that state rather than discarding it. Immutable fields remain
only in `job_facts`; JobDB reads go through `pgjobdb` joined results.

## Submit Jobs

Change `startJob` to call `pgjobdb.SubmitJob`:

```go
pgjobdb.SubmitJob(ctx, r.schedulerDB(ctx), pgjobdb.SubmitJobRequest{
    TenantID:    pgjobdb.TenantID(jobKey.TenantId),
    JobID:       pgjobdb.JobID(jobKey.JobId),
    WorkerID:    pgjobdb.WorkerID(r.requestWorkerID(workerID)),
    JobType:     pgjobdb.JobType(jobType),
    RunPolicy:   toPgjobdbRunPolicy(payload.RunPolicy),
    AppMetadata: appMetadata,
    Runtime: pgjobdb.RuntimeMetadata{
        SchemaHash:  schemaHash,
        ParentJobID: pgjobdb.JobID(parentJobID),
        Schedule:    toPgjobdbScheduleOccurrence(schedule),
    },
    WaitFor:      toPgjobdbWaitForJobIDs(prereqs),
    LeasePayload: nil,
    AvailableAt:  optionalAvailableAt(availableAt),
})
```

`jobdb.BuildJobMetadataEnvelope` should stop being part of the Postgres
scheduler write path. It can remain for non-Postgres runtimes.

Idempotent explicit job IDs still need the same behavior:

- chapter story creation remains the first durable JobDB write;
- scheduler row creation remains recoverable when the chapter story exists;
- reconcile code compares first-class `pgjobdb` fields instead of comparing an
  encoded metadata envelope.

## Worker Capabilities

JobDB can keep public worker capability strings internally for worker registry
compatibility, but the `pgjobdb` boundary should convert them to typed
selectors.

Current public convention:

```text
jobType
jobType:taskType
```

New direct-runtime conversion:

```go
func pgjobdbWorkSelector(capabilities []string) (pgjobdb.WorkSelector, error) {
    for _, cap := range capabilities {
        jobType, taskType, isTask := splitWorkerCapability(cap)
        if isTask {
            selector.Tasks = append(selector.Tasks, pgjobdb.TaskSelector{
                JobType: pgjobdb.JobType(jobType),
                TaskType: pgjobdb.TaskType(taskType),
            })
            continue
        }
        selector.JobTypes = append(selector.JobTypes, pgjobdb.JobType(jobType))
    }
    return selector, nil
}
```

`PollWork` and `GetJobLease` should call `pgjobdb.GetWork` and
`pgjobdb.GetJobLease`.

The returned lease wrapper should expose existing JobDB runtime values:

- `Capability()` returns `jobType` for job leases;
- `Capability()` returns `jobType:taskType` for task leases;
- `Payload()` returns the existing JobDB lease payload JSON view, synthesized
  from immutable run policy, current task work, and opaque application lease
  payload. Existing workflow workers and remote clients keep their payload
  contract without storing run policy or task coordinates inside
  `lease_payload`.

## Reschedule

`RescheduleJobWithLeaseByID` should map `jobdb.RescheduleExecutionRequest` to
`pgjobdb.RescheduleRequest`.

Mapping rules:

- `NextNeed` with no task work maps to `WorkKindJob`.
- `WaitUntil` maps to the time blocker while preserving the selected route.
- `WaitForJobIDs` maps to inline `wait_for` job IDs.
- task work payload maps to `WorkKindTask` with `TaskWork` fields:
  task type, input ordinal, output ordinal, input hash, and resume job type.
- `AlternateNeed` maps to `AlternateRoute`.
- the adapter decodes known run policy and task work fields from the existing
  public payload request; it validates run policy against immutable job facts,
  stores task work in typed columns, and stores remaining opaque application
  fields in `LeasePayload`.

JobDB should translate prerequisites to `wait_for` job IDs for scheduling, but
keep prerequisite policy such as `complete` versus `success` in JobDB-owned
runtime metadata. When `pgjobdb` unblocks the job after dependencies archive,
JobDB decides whether the parent proceeds or fails.

## Completion

`CompleteJobWithLeaseByID` should call `pgjobdb.JobLease.Complete` or
`pgjobdb.CompleteUnheldJob` with JobDB's existing completion classes:

```text
success
failed_app
failed_system
failed_timeout
cancelled
```

The completion chapter write still belongs to JobDB. The scheduler completion
mutation should happen only after `ensureCompletionChapter` succeeds, as today.

## External Task Completion

`CompleteTaskIfWaiting` should use first-class task work fields:

1. Load or lock the current `pgjobdb` job row.
2. Validate job type, task type, input ordinal, output ordinal, and input hash.
3. Write the task outcome chapter.
4. Call `pgjobdb.CompleteTaskWork` to clear task fields and route the job back
   to job workers.

`CompleteTaskWork` must atomically compare the current route and task
coordinates before mutating the row, so a lease or competing completion cannot
race the earlier read. A retry after the chapter write must recover
idempotently.

Keep the existing chapter-before-scheduler ordering and conflict/idempotency
checks. Do not add new cross-store transaction coordination as part of this
cutover.

## List Jobs

`ListJobs` should call `pgjobdb.ListJobs` with typed filters:

| JobDB filter | `pgjobdb` filter |
| --- | --- |
| `TenantIds` | `TenantIDs` |
| `Statuses` | `Statuses` |
| `JobTypes` | `JobTypes` |
| `JobTasks` | `TaskSelectors` |
| `JobKeys` | `JobKeys` |
| `ParentJobIDs` | `ParentJobIDs` |
| `RootOnly` | `RootOnly` |
| `MetadataFilter` | `AppMetadataEquals` |
| `CreatedAfter` / `CreatedBefore` | same |
| active/archive stores | `IncludeArchived` plus status filters |

JobDB should no longer use generic queue patterns, prepend `app` to metadata
paths, filter parent IDs in memory, or fetch details solely to decode task
payload.

`jobdb.JobSummary` can stay unchanged initially. Fill existing fields from
`pgjobdb` fields:

- `JobType` from `job.JobType`;
- `NextNeed` synthesized from job/task fields;
- `WaitFor` from `job.WaitFor`;
- `Metadata` from `job.AppMetadata`;
- `SchemaHash` from `job.Runtime.SchemaHash`;
- `ParentJobID` from `job.Runtime.ParentJobID`;
- task work fields from `job.TaskWork`.

Archived summaries use the final scheduler-state snapshot described above,
including task fields and visible payload when the job ended on a task route.

## Schedule Integration

Direct runtime schedule code currently stores occurrence metadata in pgwf
metadata and definition/control state in `jobdb_schedules`. After the hard
break:

- change `UpsertSchedule`, `GetSchedule`, `ListSchedules`, `PauseSchedule`,
  `ResumeSchedule`, and `ArchiveSchedule` to call `pgjobdb` schedule APIs;
- keep JobDB's existing schedule target serialization, but pass it as
  `pgjobdb.UpsertScheduleRequest.TargetSnapshot`;
- keep JobDB responsible for turning a schedule target snapshot into start
  chapters, input artifacts, and an occurrence job;
- update schedule preflight to load the current schedule row through `pgjobdb`;
- scheduled job submissions pass a typed `pgjobdb.ScheduleOccurrence`;
- `pgjobdb.job_facts` stores occurrence fields;
- list schedule runs filters by schedule fields on `pgjobdb.job_facts`.

SQLite and toy runtimes can keep their local schedule tables/maps because they
do not use the Postgres scheduler package.

## Implementation Order

Phase 0: settle the public JobDB core boundary.

- Build on the existing public `pkg/jobdb/runtime/core` scheduler, chapter-log,
  and schema ports. They do not yet provide a complete `WorkflowRuntime`
  facade.
- Reuse the existing public `pkg/jobdb/runtimetest` conformance harness.
- Reuse the public Postgres `ChapterLog` and `SchemaStore` adapters in
  `pkg/jobdb/chapterstore/postgres` and `pkg/jobdb/schemastore/postgres`.
- Finish the JobDB-owned runtime facade before `pgjobdb.Runtime` implements
  workflow behavior.
- Audit the public backend ports against every `WorkflowRuntime` operation,
  including cancellation, lease validation and renewal, task completion,
  schedule runs, and list/lease filters.
- Open JobDB core feature requests for missing public boundaries instead of
  copying internal direct-runtime code into `pgjobdb`.
- Use
  [FEATURE-REQUESTS-JOBDB-CORE-FOR-PGJOBDB.md](FEATURE-REQUESTS-JOBDB-CORE-FOR-PGJOBDB.md)
  as the initial request list.
- Land required JobDB core changes before wiring `pgjobdb.Runtime` to JobDB
  workflow semantics.
- Keep the `pgjobdb` dependency limited to public JobDB packages.

Phase 1: create the new package from existing code.

- Create the `pgjobdb` repo.
- Copy pgwf SQL mechanics and pgwf-go database helpers into it.
- Mechanically rename module, package, SQL schema, and procedure namespace.
- Keep existing lease, keepalive, completion, notification, and test behavior
  working before adding new behavior.

Phase 2: extend storage and procedures.

- Add `job_facts`, `schedules`, normalized active state, normalized archive
  state, constraints, and indexes.
- Add JobDB-native procedures for submit, lease, reschedule, completion, task
  completion, list, get, and status.
- Add schedule procedures that own what `jobdb_schedules` owns today.

Phase 3: add Go API coverage.

- Add `pgjobdb` Go types and procedure bindings.
- Keep useful copied scheduler helper code private.
- Remove public generic queue APIs from `pgjobdb` before release.

Phase 4: cut JobDB direct runtime over.

- Start this phase only after any required JobDB core feature requests have
  landed.
- Release a JobDB core version first, then a `pgjobdb` version built against
  it, then a JobDB version whose direct wrapper imports that `pgjobdb`
  version. Keep the core package import graph free of concrete backends.
- Replace pgwf imports with `pgjobdb`.
- Replace `schedule_schema.go` / `jobdb_schedules` direct SQL usage with
  `pgjobdb` schedule APIs.
- Replace direct-runtime submit, lease, reschedule, complete, task completion,
  list, get, and status paths with `pgjobdb.Runtime` composed from the JobDB
  runtime core. Keep the direct package as a thin compatibility wrapper.

Phase 5: remove obsolete direct-runtime code.

- Remove helpers whose only purpose is generic scheduler encoding.
- Remove direct-runtime `jobdb_schedules` DDL and SQL code.
- Remove old pgwf payload/metadata decoding from Postgres/direct paths.

## Test Plan

Add direct runtime integration tests for:

- public Postgres `ChapterLog` and `SchemaStore` adapters compose with the
  JobDB runtime core without private imports;
- submit job writes first-class job type, run policy, schema hash, app
  metadata, parent job, and schedule occurrence fields;
- poll job worker by job type without `next_need`;
- poll task worker by typed task selector without `jobType:taskType` storage;
- task work list projection without decoding generic payload;
- parent and root filters handled by `pgjobdb`;
- metadata filters applied to `app_metadata`;
- upsert/list/pause/resume/archive schedule through `pgjobdb.schedules`;
- schedule operations backed by `pgjobdb.schedules`, with no
  `jobdb_schedules` table in the Postgres/direct path;
- schedule run listing through `pgjobdb.job_facts` schedule fields;
- prerequisite `complete` and `success` behavior in JobDB after unblocking;
- external task completion coordinate mismatch;
- lease completion status mapping;
- hard-break startup fails clearly if the old pgwf schema is configured instead
  of `pgjobdb`.
- first deployment initializes a brand-new empty Postgres database; later
  restarts accept only the initialized `pgjobdb` schema and its own data.

Also update runtime conformance tests so direct, SQLite, toy, and remote
runtimes continue to agree on public JobDB behavior.

## Risks

- A hard break means existing pgwf databases are not readable by the new direct
  runtime. Deployment requires a brand-new empty database, with no old pgwf
  scheduler or JobDB chapter rows. This must be called out in release notes and
  deployment docs.
- The public runtime-core boundary must stay tight enough that `pgjobdb` does
  not become a second implementation of JobDB workflow semantics.
