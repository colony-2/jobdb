# Feature Requests: JobDB Core Support For pgjobdb

## Summary

`github.com/colony-2/pgjobdb` should be able to implement the Postgres-backed
JobDB runtime without importing or copying JobDB private packages.

The clean split should be:

- JobDB core owns workflow semantics: chapters, artifacts, schema validation,
  schedule target semantics, retry/prerequisite behavior, lease authorization,
  and runtime conformance.
- `pgjobdb` owns the Postgres scheduler database implementation: SQL schema,
  stored procedures, leases, schedule rows, job facts, active job state, archive
  state, and typed Go bindings for those scheduler operations.

`pgjobdb.Runtime` composes the public JobDB runtime core and implements
`jobdb.WorkflowRuntime`. JobDB core does not import `pgjobdb`, so applications
choose only the concrete runtime packages they use.

These requests are intentionally narrow. They ask JobDB core to expose the
runtime extension points that are already conceptually core behavior, instead
of asking `pgjobdb` to reimplement or copy that behavior.

## Non-Goals

- Do not make JobDB core import `pgjobdb`.
- Do not move Postgres scheduler policy back into JobDB core.
- Do not preserve old Postgres queue schema compatibility.
- Do not expose low-level internal helper packages as-is.
- Do not add cross-store transaction coordination; keep existing
  chapter-before-scheduler ordering and idempotent recovery behavior.

## Request 1: Public Runtime Conformance Harness — Implemented

Current state:

- `pkg/jobdb/runtimetest` already exports `Harness`, `Fixture`, capability
  flags, and `RunWorkflowRuntimeConformance`.
- Built-in and remote conformance tests already invoke the public harness.
- `pgjobdb` should import and run this package directly. No new core request
  is needed for the harness unless implementation reveals a missing case.

Acceptance criteria:

- `pgjobdb` tests can run the same lifecycle, lease, chapter, artifact,
  idempotency, schedule, metadata, and conflict behavior checks used by built-in
  runtimes.
- Built-in runtimes use the public harness or a thin wrapper around it.
- The harness does not require imports from any `internal` package.

## Request 2: Public Runtime Core With Backend Ports

Current issue:

- The current direct and SQLite runtimes each contain or import private logic
  for workflow runtime semantics: chapter construction, chapter decoding,
  artifact persistence, schema validation, restart validation, retry metadata,
  prerequisite policy, schedule target handling, schedule preflight, and lease
  authorization.
- Exposing each of those as separate helper packages would create a broad,
  fragile surface and would push JobDB semantics into `pgjobdb`.
- `pkg/jobdb/runtime/core` already has public scheduler, chapter-log, and schema
  ports plus chapter codec and schema helpers. It does not yet expose a
  complete `jobdb.WorkflowRuntime` implementation built from those ports.
- The scheduler port now carries typed JobDB facts, routes, task coordinates,
  and payload visibility. Schedule snapshots cross it as opaque JSON after
  core serialization. `NewRuntime` now wires the ports and owns chapter
  reads, artifact access, job reads, job-list projection, and initial job
  submission with explicit ID recovery. Chapter writes now validate live
  leases through the scheduler port. The remaining
  `WorkflowRuntime` operations still need to move into this facade.

Requested core change:

- Extract a public JobDB-owned runtime core package that implements the shared
  `jobdb.WorkflowRuntime` semantics around explicit backend ports.
- The runtime core should own:
  - chapter body encoding/decoding and chapter metadata conversion;
  - artifact upload/download handling and artifact key assignment;
  - job schema resolution and chapter validation;
  - run policy normalization, retry metadata, and backoff calculation;
  - prerequisite condition semantics, including `complete` versus `success`;
  - restart validation and restart-extra chapter handling;
  - schedule target serialization/deserialization;
  - schedule occurrence preflight decisions and cancellation chapters;
  - lease authorization for remote runtime writes.
- The backend ports should let `pgjobdb` provide only the scheduler storage
  operations that are genuinely backend-specific.
- Preserve the existing `ExecutionLease.Payload()` JSON contract by projecting
  immutable run policy and typed task work from the backend. Store only opaque
  application lease payload in the scheduler's `lease_payload` column.
- Preserve archived `JobSummary` fields by reading the final mutable-state
  snapshot in `pgjobdb.jobs_archive` joined with immutable `job_facts`.

Desired addition to the existing `pkg/jobdb/runtime/core` package:

```go
package runtimecore

type Runtime struct {
    // implements jobdb.WorkflowRuntime
}

func NewRuntime(Config) (*Runtime, error)
```

`Config`, `Scheduler`, `ChapterLog`, and `SchemaStore` already exist. Their
types can evolve as the facade is built. JobDB core owns runtime semantics;
`pgjobdb` implements the scheduler port and uses a public JobDB-owned chapter
store implementation or the existing public chapter-log port.

Acceptance criteria:

- `pgjobdb` can expose a `jobdb.WorkflowRuntime` by composing JobDB runtime
  core with its scheduler implementation.
- `pgjobdb` does not need to import `pkg/jobdb/internal/...`,
  `pkg/internal/runtimecodec`, or current direct-runtime internals.
- Schema validation remains a JobDB core responsibility, not a `pgjobdb`
  responsibility.
- Lease authorization remains a JobDB core responsibility, not a `pgjobdb`
  responsibility.
- Schedule target and preflight semantics remain JobDB core responsibilities,
  while schedule rows and scheduled job occurrence fields live in `pgjobdb`.
- Remote completion, reschedule, task completion, and artifact/chapter writes
  continue to reject stale, mismatched, or expired lease writes with
  `jobdb.ErrExecutionLeaseLost` through JobDB-owned runtime code.
- Existing direct, SQLite, toy, and remote runtime behavior remains covered by
  the public conformance harness.

## Request 3: Public Chapter Store Package Or Port — Postgres Adapter Added

Current issue:

- A JobDB runtime needs durable chapter and artifact storage to satisfy
  `GetChapter`, `ListChapters`, `PutChapter`, and `OpenArtifact`.
- The reusable implementation lives under
  `pkg/jobdb/internal/chapterstore/...`, while the public
  `pkg/jobdb/chapterstore/postgres` package now adapts it to the existing
  `runtimecore.ChapterLog` port.
- `pkg/jobdb/schemastore/postgres` now implements the existing
  `runtimecore.SchemaStore` port for the same Postgres database.

Requested core change:

- Use the public Postgres adapter for `pgjobdb.Runtime` and extend its narrow
  `ChapterLog` port only when the composing runtime facade needs more storage
  operations.
- Keep the chapter/artifact model in JobDB core. `pgjobdb` should not define a
  competing chapter format or artifact lifecycle.

Acceptance criteria:

- `pgjobdb` can use a public JobDB-owned chapter/artifact store, or provide
  storage primitives behind a small public port owned by JobDB core.
- Chapter conflict, idempotency, artifact key assignment, artifact cleanup, and
  pagination behavior remain consistent with the existing built-in runtimes.
- No private chapterstore package is copied into `pgjobdb`.

## Submission Order

1. Extend the existing runtime core ports to cover typed scheduler state and
   every `jobdb.WorkflowRuntime` operation.
2. Implement the runtime core facade over those ports.
3. Expose the chapter/artifact store implementation or the storage primitives
   needed by the existing public chapter-log port.
4. Run the already-public `pkg/jobdb/runtimetest` suite against the composed
   runtime.

The goal is not to create many public helper APIs. The goal is to move shared
JobDB runtime semantics into JobDB core and keep `pgjobdb` focused on its
Postgres scheduler database responsibilities.
