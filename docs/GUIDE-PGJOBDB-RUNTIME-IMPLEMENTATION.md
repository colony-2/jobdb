# Guide: pgjobdb Runtime Implementation

## Status

This is the original implementation guide. The runtime is now implemented in
the separate `/pgjobdb` module. Its public `runtime.Runtime` embeds
`runtimecore.Runtime` and `runtimecore.SchemaRegistry`, and composes the public
Postgres chapter and schema stores. The sections below record the design
constraints and incremental plan; prospective wording reflects that plan.

This guide is pgjobdb-specific, but it is based on the generic runtime
extension design in
[DESIGN-JOBDB-RUNTIME-EXTENSION-SURFACE.md](DESIGN-JOBDB-RUNTIME-EXTENSION-SURFACE.md).
That design resolved the original pgjobdb requests by exposing a narrow public
surface instead of promoting JobDB internals to public API.

The public shape available now is:

- `github.com/colony-2/jobdb/pkg/jobdb/runtimetest`
- `github.com/colony-2/jobdb/pkg/jobdb/runtime/core`
- `github.com/colony-2/jobdb/pkg/jobdb`

`runtimecore.NewRuntime` now returns the complete workflow facade from
`Scheduler`, `ChapterLog`, and `SchemaStore` ports. The pgjobdb adapter supplies
those ports and does not import JobDB private packages.

## Goal

The goal is to keep JobDB core tight while still giving pgjobdb enough public
contract to be a full, conforming runtime:

- JobDB owns workflow semantics.
- pgjobdb owns Postgres durability, indexes, transactions, and locking.
- pgjobdb does not import or copy JobDB private packages.
- Existing consumers continue to depend on the normal `jobdb.WorkflowRuntime`
  API.
- All `jobdb.WorkflowRuntime` APIs are supported. No production method should
  be a stub.

This solves the spirit of the original requests: pgjobdb gets the reusable
semantics it needed, but JobDB does not expose broad internal implementation
details.

## Architecture

Use this shape:

```text
workflow.Engine / jobdb clients
        |
        v
pgjobdb.Runtime
        |
        +-- postgresScheduler      implements runtimecore.Scheduler vocabulary
        +-- postgresChapterLog     implements runtimecore.ChapterLog behavior
        +-- runtimecore.SchemaRegistry
                |
                +-- postgresSchemaStore implements runtimecore.SchemaStore
```

`pgjobdb.Runtime` should satisfy `jobdb.WorkflowRuntime`. It should also
satisfy `jobdb.JobSchemaRegistry`, either directly or by forwarding schema
registry calls to `runtimecore.SchemaRegistry`.

Keep these pieces separate in pgjobdb even if they share a database pool:

- scheduler state and lease transactions;
- chapter and artifact persistence;
- schema persistence;
- public runtime facade and request conversion.

That separation keeps Postgres-specific behavior in pgjobdb while preventing
the runtime facade from becoming a collection of copied JobDB internals.

## Imports

Allowed JobDB imports for pgjobdb runtime implementation:

```go
import (
    "github.com/colony-2/jobdb/pkg/jobdb"
    runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)
```

Allowed test import:

```go
import "github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
```

Do not import these packages from pgjobdb:

- `github.com/colony-2/jobdb/pkg/jobdb/internal/...`
- `github.com/colony-2/jobdb/pkg/internal/runtimecodec`
- `github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema`
- `github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth`
- `github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/...`
- `github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/...`
- `github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite` internals

If pgjobdb needs behavior from one of those packages, that is a signal to add
or extend a narrow public `runtimecore` contract instead of depending on the
private package.

## Request 1: Runtime Conformance Harness

The original request asked for a public harness so pgjobdb can run the same
behavior tests as built-in runtimes. That is now the right starting point for
pgjobdb.

Put pgjobdb database setup helpers in a private test package, for example
`internal/pgjobdbtest`, and keep the actual conformance test near the runtime
package. The test should import only public JobDB packages.

Example:

```go
package pgjobdb_test

import (
    "testing"

    "github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
)

func TestRuntimeConformance(t *testing.T) {
    runtimetest.RunWorkflowRuntimeConformance(t, runtimetest.Harness{
        Name: "pgjobdb",
        Capabilities: runtimetest.Capabilities{
            Leases:         true,
            Schedules:      true,
            SchemaRegistry: true,
            RuntimeStorage: true,
            LongPoll:       true,
        },
        New: func(t testing.TB) runtimetest.Fixture {
            rt, cleanup := newRuntimeForTest(t)
            return runtimetest.Fixture{
                Runtime:        rt,
                WorkerTenantID: "tenant-pgjobdb",
                Cleanup:        cleanup,
            }
        },
    })
}
```

If `pgjobdb.Runtime` does not itself implement `jobdb.JobSchemaRegistry`, set
`Fixture.SchemaRegistry` explicitly. If it does implement the registry, the
harness will discover it from `Fixture.Runtime`.

The final pgjobdb conformance test should enable every capability. Temporary
false flags are useful during incremental development, but they are not a
definition of done for a production pgjobdb runtime.

## Request 2: Runtime Core And Backend Ports

The original request asked for backend ports so pgjobdb could avoid copying
runtime semantics. The solution is the public `runtimecore` package. It does
provide a complete runtime constructor, backend vocabulary, and reusable
workflow semantics.

pgjobdb should implement its Postgres scheduler around the
`runtimecore.Scheduler` concepts:

- `CreateJob`, `GetJob`, `ListJobs`, and `CancelJob`
- `AcquireWork` and `AcquireJobLease`
- `ValidateLease`, `KeepAliveLease`, `CompleteLease`, and `RescheduleLease`
- `GetWaitingTask` and `CompleteTaskWork`
- `UpsertSchedule`, `GetSchedule`, `ListSchedules`, `MutateSchedule`, and
  `ListScheduleRuns`

The scheduler side should own only database facts and atomic mutations:

- creating and reconciling job rows;
- deriving and indexing pollable work;
- acquiring leases under row locks;
- renewing, completing, and rescheduling only the currently live lease;
- guarding task completion against the expected waiting slot;
- recording cancel requests and terminal states;
- storing schedule definitions, generations, next-fire state, and run rows;
- listing jobs, schedules, and schedule runs with stable pagination.

The runtime facade should own public API conversion and call `runtimecore`
for reusable workflow semantics. The pgjobdb adapter only implements the
Postgres ports and constructs the public runtime core.

Do not create a second set of pgjobdb-specific workflow semantics for retry
policy, prerequisite interpretation, schedule occurrence metadata, lease
authorization, chapter format, or schema interpretation. If conformance
requires behavior that is only available privately in JobDB, promote the
smallest useful public helper in `runtimecore`.

## Request 3: Schema Store

Schema validation means JobDB workflow-schema validation, not Postgres DDL
validation.

pgjobdb should implement `runtimecore.SchemaStore` and wrap it with
`runtimecore.NewSchemaRegistry`. The store persists rows; the registry owns
canonicalization, hash calculation, schema document validation, missing and
archived schema behavior, and conversion to public `jobdb.JobSchemaInfo`.

Minimal schema row behavior:

- key rows by `(tenant_id, schema_hash)`;
- store canonical schema JSON returned through `runtimecore.StoredJobSchema`;
- track `ACTIVE` and `ARCHIVED` state;
- track `created_at` and `archived_at`;
- make registration idempotent for an existing `(tenant_id, schema_hash)`;
- return `jobdb.ErrJobSchemaNotFound` for missing rows.

Recommended table shape:

```sql
CREATE TABLE jobdb_schemas (
    tenant_id text NOT NULL,
    schema_hash text NOT NULL,
    schema_json jsonb NOT NULL,
    state text NOT NULL,
    created_at timestamptz NOT NULL,
    archived_at timestamptz,
    PRIMARY KEY (tenant_id, schema_hash)
);

CREATE INDEX jobdb_schemas_list_idx
    ON jobdb_schemas (tenant_id, state, created_at DESC, schema_hash ASC);
```

If `schema_json` is stored as `jsonb`, return normalized JSON bytes from
`GetJobSchema` and `ListJobSchemas`. If byte-for-byte canonical preservation is
important for pgjobdb internals, store the canonical form as `text` or `bytea`
instead. The public registry checks that a stored schema still matches the
registered hash and canonical bytes.

Runtime use should look like:

```go
schemaRegistry, err := runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{
    Store: postgresSchemaStore,
})
if err != nil {
    return nil, err
}
```

On submit and restart:

- call `runtimecore.ResolveActiveSchemaForNewJob`;
- persist the selected schema hash with the job's runtime metadata;
- reject missing schemas with `jobdb.ErrJobSchemaNotFound`;
- reject archived schemas with `jobdb.ErrJobSchemaArchived`.

Before committing chapters:

- validate ordinal `0` with `runtimecore.ValidateFirstChapter`;
- validate ordinary chapters with `runtimecore.ValidateOrdinaryChapter`;
- validate terminal completion chapters with `runtimecore.ValidateLastChapter`;
- return `jobdb.ErrJobSchemaValidation` for schema shape failures.

Archived schemas should stop new submissions that select that schema. Existing
jobs that already carry the schema hash may continue to validate chapters
against the archived schema record.

## Request 4: Chapter Log And Artifacts

The original chapter-store request is best handled as a small chapter-log
boundary, not by exposing the private chapterstore implementation.

pgjobdb should implement `runtimecore.ChapterLog` behavior:

- `Create` creates a job log with initial ordinal `0`;
- `Append` writes exactly the next ordinal and rejects conflicts;
- `ClonePrefix` creates a restart log from an existing prefix and optional
  appended chapter;
- `Get`, `List`, and `Count` read committed chapters;
- `OpenArtifact` opens a committed artifact by job, ordinal, name, and digest.

Use `runtimecore.EncodeChapter` before storing a public `jobdb.Chapter`, and
use `runtimecore.DecodeChapter` when returning a chapter to callers.

Treat `runtimecore.EncodedChapter.Payload` as opaque bytes. Store it and return
it byte-for-byte. pgjobdb should not parse the payload or persist fields that
depend on the private wire format.

Artifact guidance:

- consume `EncodedChapter.ArtifactUploads` only during create, clone, or append;
- store committed artifact descriptors as `jobdb.StoredArtifact`;
- return descriptors in `EncodedChapter.Artifacts` on reads;
- make artifact keys deterministic from job key, ordinal, artifact name, and
  digest;
- ensure a failed scheduler write after a chapter write can be retried without
  corrupting the chapter log.

The transaction boundary may remain chapter-before-scheduler if that matches
JobDB's existing recovery model. Do not add cross-store distributed
transactions unless there is a separate design decision.

## Request 5: Scheduler, Leases, And Completion

The scheduler is where pgjobdb should use Postgres well. Runtime core can
prepare semantic requests, but only the scheduler transaction can prove current
state.

Every lease mutation must re-check these facts in the same database mutation:

- tenant and job ID match;
- lease ID matches;
- worker ID matches;
- lease has not expired;
- job is not archived;
- job has not already reached a terminal state;
- task-wait or route guards still match the expected state.

Return `jobdb.ErrExecutionLeaseLost` when a lease write loses those guards.
Return `jobdb.ErrJobNotFound` for missing jobs. Return `jobdb.ErrConflict` or
`jobdb.NewExistingJobMismatchError(...)` for explicit-ID conflicts where the
existing durable state does not match the request.

`PollWork` and `GetJobLease` should be atomic. Use row locks or equivalent
Postgres primitives so two workers cannot acquire the same live job. Respect
tenant IDs, capabilities, metadata filters, limits, availability timestamps,
and lease durations.

Long polling should be implemented through pgjobdb's own notification or
wait-loop strategy, but the public behavior must match `PollWorkRequest`:
return as soon as work is available or once `LongPollUntil` is reached.

## Request 6: Schedules

All schedule APIs are part of the required runtime surface:

- `UpsertSchedule`
- `GetSchedule`
- `ListSchedules`
- `PauseSchedule`
- `ResumeSchedule`
- `ArchiveSchedule`
- `TriggerSchedule`
- `ListScheduleRuns`

Keep schedule rows in pgjobdb, but preserve JobDB semantics:

- schedule keys are tenant-scoped;
- generation checks enforce optimistic concurrency;
- target data, run policy, and app metadata are snapshotted;
- `SpecHash` changes when the effective schedule spec changes;
- archived schedules stop future runs;
- pause and resume preserve the schedule record and generation behavior;
- manual triggers create schedule-associated jobs;
- schedule runs list jobs associated with that schedule and preserve stable
  pagination.

Schedule metadata is JobDB runtime metadata, not app metadata. Job listing
metadata filters should apply to application metadata only.

## Public Runtime API Checklist

pgjobdb is complete only when `pgjobdb.Runtime` supports every
`jobdb.WorkflowRuntime` method:

- lifecycle: `SubmitJob`, `SubmitRestartJob`, `CancelJob`;
- worker loop: `PollWork`, `GetJobLease`, `CompleteTaskIfWaiting`;
- schedules: `UpsertSchedule`, `GetSchedule`, `ListSchedules`,
  `PauseSchedule`, `ResumeSchedule`, `ArchiveSchedule`, `TriggerSchedule`,
  `ListScheduleRuns`;
- reads: `GetJob`, `ListJobs`;
- chapters: `GetChapter`, `ListChapters`, `PutChapter`;
- artifacts: `OpenArtifact`.

It should also support the public schema registry API:

- `RegisterJobSchema`;
- `GetJobSchema`;
- `ListJobSchemas`;
- `ArchiveJobSchema`.

Avoid partial implementations that return "not supported" for production
paths. If a method is not finished, leave the related conformance capability
disabled only while development is in progress.

## Suggested Implementation Order

1. Add the pgjobdb conformance test harness with database setup under
   `internal/pgjobdbtest`.
2. Implement `runtimecore.SchemaStore` and expose schema registry methods by
   forwarding to `runtimecore.SchemaRegistry`.
3. Implement chapter and artifact persistence behind `runtimecore.ChapterLog`
   behavior using opaque encoded chapter payloads.
4. Implement job creation, explicit-ID reconciliation, `GetJob`, and
   `ListJobs`.
5. Implement polling, targeted leases, keepalive, completion, reschedule, and
   lease loss guards.
6. Implement `PutChapter`, `SubmitRestartJob`, and restart prefix cloning.
7. Implement `CompleteTaskIfWaiting` with waiting-slot guards.
8. Implement schedules and schedule-run listing.
9. Enable all conformance capabilities in CI.

At each step, prefer adding a small public helper to JobDB core over copying a
private package into pgjobdb.

## Acceptance Criteria

pgjobdb is aligned with the JobDB core design when:

- it imports only public JobDB packages;
- its runtime passes `runtimetest.RunWorkflowRuntimeConformance` with all
  production capabilities enabled;
- schema registration and chapter validation go through `runtimecore`;
- chapter payloads are stored opaquely;
- all lease mutations are guarded atomically in Postgres;
- all schedule APIs are implemented;
- existing pgjobdb consumers receive a normal `jobdb.WorkflowRuntime` without
  needing to know about runtimecore internals.
