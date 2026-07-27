# Design: JobDB Runtime Extension Surface

## Status

Draft.

This document defines a generic implementation design for third-party and
out-of-tree JobDB runtime backends. The goal is to let a backend provide
scheduler, chapter, artifact, and schema persistence for a JobDB runtime
without importing private JobDB packages, copying runtime semantics, or forcing
JobDB core to expose broad internal helpers.

## Goal

Keep JobDB core tight:

- expose conformance tests so external runtimes can prove compatibility;
- expose one composable runtime implementation boundary, not many helper
  packages;
- keep workflow semantics in JobDB-owned code;
- keep scheduler persistence and atomic lease mutations in the backend;
- keep internal encodings, metadata layouts, and chapter machinery hidden
  unless they are deliberately part of a small public port.

The behavioral source of truth is
[SPEC-WorkflowRuntime-Guarantees.md](SPEC-WorkflowRuntime-Guarantees.md).
The public harness and runtime core should preserve that contract rather than
inventing a second runtime semantics document.

## What Schema Validation Means

Schema validation here means JobDB's workflow-schema subsystem, not database
DDL validation.

JobDB core should own:

- resolving `jobdb.JobSchemaSelector` during submit and restart;
- canonicalizing inline schemas and computing their `sha256:` hash;
- registering inline schemas through a schema registry;
- loading hash-selected schemas and rejecting missing or archived schemas;
- validating schema documents when they are registered;
- validating first, ordinary, and final chapters against the schema's chapter
  shapes before those chapters are committed.

The backend may store and retrieve schema rows, but it should not decide how
schemas are interpreted or how chapters are validated.

## Design Principles

1. Public tests before public production API.
2. One production extension point is better than many public helper packages.
3. Public ports should speak logical JobDB runtime concepts, not the current
   direct or SQLite implementation's private structs.
4. Backend code owns atomic database behavior. Runtime core owns policy and
   orchestration.
5. Chapter bytes and runtime metadata should be opaque to backend storage
   unless a field is explicitly part of the backend contract.
6. Built-in runtimes should migrate through the same public boundary, or a thin
   adapter around it, so external runtime implementations are not second-class
   implementations.

## Recommended Shape

Add two public packages:

```text
github.com/colony-2/jobdb/pkg/jobdb/runtimetest
github.com/colony-2/jobdb/pkg/jobdb/runtime/core
```

The first is test-only infrastructure. The second is the production extension
surface. It should expose a runtime constructor and small backend ports. It
should not expose the current internal `runtimecodec`, `jobschema`,
`leaseauth`, or `chapterstore/story/storage` packages directly.

The target composition should look like:

```go
rt, err := runtimecore.New(runtimecore.Config{
    Scheduler: schedulerDriver,
    Chapters:  chapterLog,
    Schemas:   schemaRegistry,
})
```

`rt` implements `jobdb.WorkflowRuntime`. If `Schemas` is present, `rt` should
also implement `jobdb.JobSchemaRegistry` by validating schema documents in core
and delegating persistence to the registry.

`Schemas` should be a separate port from `Scheduler` unless an implementation
chooses to satisfy both with the same object. Schema storage is not scheduler
queue behavior, and keeping it separate avoids turning the scheduler driver
into a grab bag.

## Decisions

- Put the public runtime conformance harness in
  `github.com/colony-2/jobdb/pkg/jobdb/runtimetest`.
- Use one public harness package. It may import `pkg/workflow` internally for
  engine-backed cases, but external runtime implementations should only need to
  provide a runtime fixture.
- Use `jobdb.JobSchemaRegistry`, or a runtime-core-local interface with the
  same method shape, as the schema storage port. The backend should persist
  schema rows behind that port; runtime core should own canonicalization,
  schema-document validation, lifecycle checks, and chapter validation.
- Use a small `runtimecore.ChapterLog` port. A backend may implement physical
  chapter and artifact storage behind that port, but the stored chapter payload
  is opaque runtime-core-owned data.
- Do not publish a reusable chapter-store implementation in the first pass.
  Consider that later only if multiple external runtimes need the same storage
  implementation.
- A complete external runtime should support all `jobdb.WorkflowRuntime` APIs.
  Schedules, schema registry, list/read APIs, chapter APIs, artifact APIs, and
  lease/task mutation APIs are in scope for a conforming runtime.

## Request 1: Public Runtime Conformance Harness

This request is reasonable and should be done first.

Expose a public conformance package that external runtimes can import. The
package should run behavior suites; it should not expose internal test helper
packages wholesale.

Recommended API shape:

```go
package runtimetest

type Capabilities struct {
    Leases         bool
    Schedules      bool
    SchemaRegistry bool
    RuntimeStorage bool
    LongPoll        bool
}

type Fixture struct {
    Runtime        jobdb.WorkflowRuntime
    SchemaRegistry jobdb.JobSchemaRegistry
    WorkerTenantID string
    Cleanup        func(context.Context) error
}

type Harness struct {
    Name         string
    Capabilities Capabilities
    New          func(testing.TB) Fixture
}

func RunWorkflowRuntimeConformance(t *testing.T, h Harness)
```

Some conformance cases need `pkg/workflow` workers. Keep those cases inside
`pkg/jobdb/runtimetest` rather than adding a second public test package. The
public contract should remain "provide a runtime fixture and capabilities";
external implementations should not need to copy JobDB's fixture jobs or
private harness types.

Acceptance criteria:

- Built-in runtimes run through this public harness or a wrapper around it.
- An external runtime can run the same lease, chapter, artifact, idempotency,
  schedule, metadata, schema, and conflict tests from its own module.
- The public harness imports no JobDB `internal` package from the consumer's
  perspective.

## Request 2: Runtime Core With Backend Driver

The original "backend ports" request is directionally correct, but the best
pattern is a single public runtime-core package with one scheduler driver and
one chapter-log port. Avoid exposing standalone helpers for every semantic
subsystem.

Runtime core should own these workflows end to end:

- `SubmitJob` and explicit-ID reconciliation;
- `SubmitRestartJob` and restart-prefix validation;
- input hashing and artifact key assignment;
- chapter encoding, decoding, metadata conversion, and schema validation;
- run-policy normalization, retry metadata, and backoff calculation;
- prerequisite normalization and `complete` versus `success` semantics;
- `PutChapter`, including lease-token authorization and append checks;
- `Complete`, including final chapter validation before scheduler completion;
- `CompleteTaskIfWaiting`, including waiting-slot guard checks and task output
  chapter construction;
- schedule target snapshotting, spec hashes, deterministic occurrence IDs,
  occurrence metadata, preflight decisions, and cancellation chapters;
- conversion from backend snapshots to public `jobdb.JobInfo`,
  `jobdb.JobSummary`, `jobdb.ScheduleInfo`, and schedule-run summaries.

The scheduler driver should own only backend-specific durable state and
concurrency:

- creating job facts and active job records;
- acquiring queue and targeted leases atomically;
- keeping leases alive;
- completing or rescheduling a live lease atomically;
- completing waiting task work atomically against its guards;
- cancel requests;
- storing and listing schedule records;
- storing and listing active/archive job state;
- returning typed snapshots needed by runtime core.

Schema storage should use the existing `jobdb.JobSchemaRegistry` shape or a
small superset of it. Runtime core calls that port, validates schema documents,
and validates chapters; the storage implementation only persists canonical
schema bytes and lifecycle state.

The driver must still validate live scheduler state during mutations. For
example, core can validate a remote lease token before it calls the driver, but
the driver must still reject stale, mismatched, expired, archived, or already
rescheduled leases atomically because only the scheduler row can prove current
ownership.

Recommended driver concepts:

```go
type Scheduler interface {
    CreateJob(context.Context, CreateJobRequest) (StoredJob, error)
    GetJob(context.Context, jobdb.JobKey) (StoredJob, error)
    ListJobs(context.Context, ListJobsRequest) (ListJobsResponse, error)

    AcquireWork(context.Context, WorkRequest) ([]LeaseSnapshot, error)
    AcquireJobLease(context.Context, JobLeaseRequest) (*LeaseSnapshot, error)
    KeepAliveLease(context.Context, LeaseMutation) (time.Time, error)
    CompleteLease(context.Context, CompletionMutation) error
    RescheduleLease(context.Context, RescheduleMutation) error
    ValidateLease(context.Context, LeaseIdentity) (LeaseSnapshot, error)

    GetWaitingTask(context.Context, jobdb.JobKey) (WaitingTaskSnapshot, error)
    CompleteTaskWork(context.Context, CompleteTaskWorkMutation) error

    UpsertSchedule(context.Context, StoredScheduleMutation) (StoredSchedule, error)
    GetSchedule(context.Context, jobdb.ScheduleKey) (StoredSchedule, error)
    ListSchedules(context.Context, ListSchedulesRequest) (ListSchedulesResponse, error)
    MutateSchedule(context.Context, StoredScheduleMutation) (StoredSchedule, error)
    ListScheduleRuns(context.Context, ListScheduleRunsRequest) (ListScheduleRunsResponse, error)
}
```

This is a sketch, not an API freeze. The important part is the separation:
runtime core handles semantic decisions; the driver handles atomic state
changes and typed persistence.

The scheduler DTOs should avoid private JobDB encodings. For example, a stored
job should expose first-class fields such as job type, parent job ID, app
metadata, schema hash, schedule occurrence fields, current route, task-wait
coordinates, lease identity, availability, completion status, and timestamps.
It should not require a backend to understand chapter envelope bytes or the old
encoded runtime metadata envelope.

## Request 3: Chapter Store Or Chapter Log

Publish a small chapter-log port used by runtime core. Do not publish the
current internal chapter store wholesale as the first move.

The chapter-log port should model only the externally required chapter
behavior:

- create a new job log with ordinal `0`;
- clone a prefix for restart and optionally append restart-extra output;
- append exactly the next ordinal;
- get one committed chapter record;
- list committed chapter records in ascending ordinal order;
- open a committed artifact;
- return a visible chapter count for schedule preflight and append checks.

Recommended concept:

```go
type ChapterLog interface {
    Create(context.Context, ChapterLogKey, EncodedChapter) error
    ClonePrefix(context.Context, ClonePrefixRequest) error
    Append(context.Context, ChapterLogKey, EncodedChapter) error
    Get(context.Context, ChapterLogKey, int64) (EncodedChapter, error)
    List(context.Context, ChapterLogKey, ChapterRange) ([]EncodedChapter, error)
    Count(context.Context, ChapterLogKey) (int64, error)
    OpenArtifact(context.Context, ArtifactLookup) (jobdb.ArtifactReader, error)
}
```

`EncodedChapter` should contain runtime-core-owned opaque bytes plus artifact
commit data. Backends may store those bytes, but they do not interpret the
chapter wire format. Runtime core remains responsible for encoding and
decoding between opaque records and public `jobdb.Chapter` values.

An external backend can implement this port using its own database and artifact
storage. That does not make the backend responsible for chapter semantics: it
stores opaque chapter bytes, enforces append/clone/read atomicity, and returns
artifact readers. Runtime core remains the only code that constructs,
validates, and interprets chapters.

If multiple external runtimes later need the same storage implementation,
publish a small `pkg/jobdb/chapterlog` implementation package then. Do not
expose the current internal `story`, `storage`, `pagination`, `artifact`, and
`core` packages as public API unless they are redesigned as a stable product
surface.

## Submission Plan

1. Publish `pkg/jobdb/runtimetest` and move built-in runtime conformance to it.
2. Define `pkg/jobdb/runtime/core` with `Scheduler`, `ChapterLog`, and schema
   registry ports.
3. Adapt SQLite to the runtime core first. SQLite is the best proving ground
   because it currently duplicates much of the direct runtime semantic code but
   has a smaller backend-specific surface than the current direct runtime.
4. Adapt another concrete backend next, using typed scheduler fields instead of
   old generic payload and metadata encodings. The implementation should cover
   the full `jobdb.WorkflowRuntime` surface, including schedules and list/read
   APIs, before it is considered complete.
5. Only after the `ChapterLog` contract stabilizes, decide whether to publish a
   reusable chapter-log implementation package.

## Non-Goals

- Do not make JobDB core import any external runtime backend.
- Do not expose `pkg/internal/runtimecodec` as public API.
- Do not expose `pkg/jobdb/internal/jobschema` as public API.
- Do not expose `pkg/jobdb/internal/leaseauth` as public API.
- Do not expose the internal chapter store subpackages as-is.
- Do not add cross-store transactions between scheduler state and chapter
  state. Preserve the existing chapter-before-scheduler ordering and
  idempotent recovery model.
- Do not make runtime backends run workflow user code.

## Completion Criteria

- An external runtime implements every method on `jobdb.WorkflowRuntime`.
- An external runtime implements the schema registry path used by runtime core.
- An external runtime passes the public runtime conformance harness with
  schedule, schema, chapter, artifact, list/read, lease, task-completion,
  restart, and conflict cases enabled.
- JobDB core does not expose current internal codec, schema, leaseauth, or
  chapter-store subpackages directly.
