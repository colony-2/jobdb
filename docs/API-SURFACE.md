# JobDB Public API Surface

## Status

**Current reference** | Author: Codex | Updated: 2026-09-16

This document defines which importable packages are intentionally public after
the `jobdb` / `workflow` split. The API snapshot should track only packages
listed here as supported public API.

## Supported Public Packages

### `github.com/colony-2/jobdb/pkg/jobdb`

Runtime Go API and REST-compatible data model. This package is public, but no
longer contains the worker SDK or engine builder.

This includes task/job data APIs, artifact APIs, runtime request/response
types, job lifecycle APIs, job-run inspection APIs, list-jobs APIs, first-class
tenant configuration requests/snapshots and administration,
error types, and the `WorkflowRuntime` interface used by runtime
adapters and advanced consumers. `ConsumerRuntime` supplies retained operations
without goal attachment; the worker SDK accepts that interface. See the
[consumer input migration](MIGRATION-CONSUMER-RUNTIME.md) for decorator ports,
retained HTTP constructors and exact function-type changes.

The shared `runtimecore.GoalScheduler` and `runtimetest` consumer inputs likewise
require retained operations only. Core's temporary legacy attachment method
forwards through optional `GoalCandidateMutator`; absence returns an error
wrapping `errors.ErrUnsupported`. Existing backend contracts and legacy HTTP
constructors remain during consumer migration.

Lease history requires `LeaseEventReader` on `WorkflowRuntime` and
`runtimecore.Scheduler`, with the validating `jobdb.ListLeaseEvents` helper and
canonical `LeaseEvent` snapshots. Implementations missing the method intentionally
fail compilation; custom adapters must implement real reads and wrappers must
forward them. See [coordinated migration](MIGRATION-MANDATORY-LEASE-HISTORY.md).
Supplied SQLite and toy runtimes record lease events automatically with existing
constructors and CLI invocations. See [lease history](GUIDE-LEASE-EVENTS.md) for
filters, cursor behavior, manual database upgrades and backend integration.

Fresh submission can select an existing primary goal with `SubmitJob.GoalID`.
The separate optional `JobGoalCapability` and HTTP job-goal discovery endpoint
describe immutable membership, atomic admission and terminal failure support.
Selection support alone does not imply those guarantees; see
[job-goal capabilities](GUIDE-JOB-GOAL-CAPABILITIES.md).
Protected job kinds, saved failure fields, chapter-free inspection and typed
failed output are specified in the
[terminal job read contract](GUIDE-TERMINAL-JOB-RECORDS.md). Reader support alone
does not imply protected publication support.
The optional `PrimaryGoalSelectionCapability`, trusted selection-authority hook,
shared preparation/publication fields and retained complete receipts define
backend support. Unknown support fails safely, and HTTP uses a dedicated
operation. See [primary goal selection](GUIDE-PRIMARY-GOAL-SELECTION.md) for
permissions, retries, compatibility and backend implementer requirements.

Operator recovery uses `WorkflowRuntime.ClearCrashConcern` with
`ClearCrashConcernRequest` to reset a job's consecutive expired-lease count.
See [clearing crash concern](GUIDE-CRASH-CONCERN.md) for CLI, REST, and Go usage.

Execution preservation adds `ExecutionInfrastructureError`, its reason constants,
classification helpers and safe `RemoteExecutionCause`. Optional
`ExecutionProvenanceReader` and its typed request, evidence, limits, usage and issue
values describe retained writer provenance. `CurrentBuildInfo` identifies the
linked JobDB module. These additions leave mandatory runtime/lease/storage ports
and stored encodings unchanged. See [execution infrastructure](GUIDE-EXECUTION-INFRASTRUCTURE.md).

Routing uses `Route{JobType, TaskType}` throughout the public API. See the
[consumer migration guide](MIGRATION-TYPED-ROUTES.md) for the breaking changes.


### `github.com/colony-2/jobdb/pkg/jobdb/runtimetest`

Public runtime conformance harness for external `jobdb.WorkflowRuntime`
implementations.

This package is test infrastructure. It lets a runtime implementation provide a
fixture and capability flags, then run the same lifecycle, lease, chapter,
artifact, idempotency, metadata, and conflict behavior checks used by JobDB's
built-in runtimes.

### `github.com/colony-2/jobdb/pkg/jobdb/clientpayload`

Shared JSON update rules for runtime implementers: lossless RFC 7396 patches,
reset/clear, revision checking, validation, and initial-value identity.

### `github.com/colony-2/jobdb/pkg/jobdb/configuration`

Public tenant configuration storage port, validation/service layer, and memory,
SQLite, and Postgres implementations. External runtime implementations provide
`configuration.Store` to `runtime/core.Config.Configurations`; the service
provides `jobdb.ConfigurationAdmin`. `NewSQLStore` initializes the additive SQL
snapshot table on a caller-owned database.

### `github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres`

Public Postgres chapter and artifact store implementing the runtime core's
`ChapterLog` port. External Postgres runtime implementations can reuse JobDB's
chapter storage behavior without importing internal packages.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/core`

Public backend boundary for external runtime implementers. Optional
`ProvenanceChapterSource` supplies raw committed ceilings and bounded body-only
pages; `ReadExecutionProvenance` shares extraction with native authorization.
Existing mandatory ports do not require the optional source.

This package wires scheduler, chapter-log, schema-store, and configuration-store ports used
to keep runtime semantics in JobDB core while letting a backend own durable
state, artifact persistence, and atomic lease mutations. The schema registry
adapter in this package owns schema canonicalization and validation before it
delegates persistence to a `SchemaStore`.
The scheduler port carries typed job routes, task coordinates, run policy,
application metadata and client payload with its revision.
Client state is separate from typed `ExecutionState`; scheduler mutations accept
explicit client patch/reset updates.
`NewRuntime` composes the backend ports. Its chapter read and artifact methods
use the public chapter log and codec, and its job listing projects native
active and archived rows into the existing `JobSummary` fields.
`GetJob` reads the typed status and the final chapter for archived jobs, with
the existing deferred task-data error behavior.
`SubmitJob` owns first chapter encoding, artifact keys, schema checks, run
policy normalization, prerequisite validation, and explicit job ID recovery.
`PutChapter` checks the current scheduler lease before validating and appending
chapter data and uploaded artifacts.
`SubmitRestartJob` clones a validated chapter prefix, checks retry boundaries,
and optionally appends a cached restart output. An ordinary `LastStepToKeep: 0`
with nil `ExecutionRestart` also accepts a source containing only its valid stored
submission, whether ready or cancelled. Only a specific missing-chapter result
permits this case; other read errors and holes in longer histories still fail.
Present chapter one retains its decode and retry-boundary checks, and other cuts
and logical scope selection keep their existing validation.

The retained submission, original input, metadata, run policy and artifact
contents remain unchanged. Ordinary restart writes no execution header; the
worker loads the retained input and artifacts and writes a fresh header at one.
All retained artifacts remain readable under the destination, including later
retained ordinals. Inherited chapters are not revalidated against a new schema or
payload limit; new extra output and logical restart records still are.
The Go request and HTTP wire shape are unchanged. HTTP users need a repaired
server build; upgrading a client alone does not repair server behavior.
`PollWork` and `GetJobLease` return leases backed by typed scheduler routes;
lease methods expose separate client/framework snapshots and validate ownership before
child submission, reschedule, completion, and chapter writes.

### `github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres`

Public Postgres schema store implementing the runtime core's `SchemaStore`
port. External Postgres runtimes can share JobDB's schema registry persistence
without importing direct runtime internals.

### `github.com/colony-2/jobdb/pkg/workflow`

Higher-level workflow SDK built on top of `pkg/jobdb`. This package owns the
worker-facing API and engine orchestration surface.

This includes `Engine`, `EngineBuilder`, `JobWorker`, `TaskWorker`,
`ExecutionContext`, `TaskContext`, worker loops, replay execution, task discovery,
run-if-leaseable helpers, and worker-level determinism errors.

`TaskResultObserverProvider` is an optional worker capability for synchronous
accepted task-attempt notifications. `ExecutionWorker.ObserveTaskResults` supplies
its factory, and `NewTaskResultObserver` forwards it (a nil factory is a no-op).
The factory runs once per execution/replay invocation outside whole-job retries;
its derived context reaches every attempt. Results notify as consumed after
reconciliation, before task-end observers, catches, retries and later orchestration
on that path. Replay and resume reconstruct fresh state incrementally. Wrappers
must forward the capability. See the [workflow guide](../pkg/workflow/README.md)
for context and callback obligations. Adding the field requires positional
`ExecutionWorker` literals to change; keyed literals remain compatible.
`JobWorker`, runtime interfaces, HTTP contracts and stored formats are unchanged.

`ReadCatchHistory` and its catch history types/constants are supported read-only
inspection APIs for committed v4 full/compact execution history. They expose
opaque boundary/attempt identities, selection, accepted handling and retry
without running application callbacks. See [catch history](CATCH-HISTORY.md)
for states, evidence gaps and compatibility limits.

This package may alias stable lower-level `jobdb` data types for ergonomics,
but moved worker symbols are not re-exported from `pkg/jobdb`.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/remote`

REST-backed runtime client/server adapter. This package is public and is used
by c2j.

Remote lease operations use runtime-minted lease tokens. Poll and targeted
lease responses include a `leaseToken`; lease-mutating HTTP calls must present
that token in `X-JobDB-Lease-Token`. Keepalive returns a fresh token for the
renewed lease.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite`

SQLite-backed runtime. This package is public and is used by c2j for embedded
local execution.

By default this package supports local `blobfs://` artifact storage only.
Provider-backed `BlobStoreURI` values require executable/server code to import
the optional Go CDK registration package.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/toy`

In-memory runtime for tests and local execution. This package is public and is
used by c2j tests and standalone execution paths.

The Postgres runtime is provided by
`github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime` in the pgjobdb module.
The JobDB CLI contains SQLite, toy, and healthcheck commands; pgjobdb's CLI
adds `direct` and `serve`.

The SQLite and toy runtime packages expose lease transport helpers such
as `KeepAliveLeaseByIDWithExpiry` because `remote.NewServer` needs a consistent
adapter surface to renew leases and mint replacement lease tokens. These helper
methods are runtime-adapter API, not application workflow API.

### `github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk`

Optional Go CDK blobstore provider registration package for executable/server
code. Importing this package enables `file://`, `mem://`, `gs://`, `s3://`, and
`azblob://` `BlobStoreURI` values without making those providers transitive
dependencies of embedded SQLite consumers.

## Internal-Only Packages

### `github.com/colony-2/jobdb/pkg/jobdb/internal/...`

Internal implementation and test support. These packages are not part of the
public API snapshot.

### `github.com/colony-2/jobdb/pkg/internal/...`

Shared implementation and test support used by multiple top-level packages.
These packages are not part of the public API snapshot.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/*/internal/...`

Runtime implementation details. These packages are not part of the public API
snapshot.

## Removed From Public Surface

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/testsupport`

This package was test infrastructure for embedded direct-runtime setup. It is
not used by c2j and is not intended for downstream runtime construction. It has
been moved under `pkg/internal/directtestsupport`.

## API Snapshot Packages

The API snapshot should include:

```text
github.com/colony-2/jobdb/pkg/jobdb
github.com/colony-2/jobdb/pkg/jobdb/configuration
github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres
github.com/colony-2/jobdb/pkg/jobdb/runtimetest
github.com/colony-2/jobdb/pkg/jobdb/runtime/core
github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres
github.com/colony-2/jobdb/pkg/workflow
github.com/colony-2/jobdb/pkg/jobdb/runtime/remote
github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite
github.com/colony-2/jobdb/pkg/jobdb/runtime/toy
```

No generated protobuf storage package should be added to this list unless it
is explicitly promoted to public API.

SQLite's public `CheckGoalMigration` and `MigrateGoals` operations accept an
explicit existing filesystem path. `MigrationRequiredError` preserves the source
format, available path and manual-upgrade commands from both constructors, and
matches `ErrMigrationRequired` through `errors.Is`. See
[SQLite manual migration](MIGRATION-IMMUTABLE-GOALS.md).


Job-goal membership is immutable and selected at creation. Toy and SQLite reject
fresh same-goal publication while any retained member is nonterminal. The former
attachment method, HTTP mutation and CLI command are removed after Cortex, c2j and PostgreSQL consumer retirement; member reads and
deprecated empty `OtherGoalIDs` remain. PostgreSQL backend conversion and
terminal-record consumer integration follow shared contract publication. See [the specification](SPEC-Job-Goals.md) and
[manual SQLite upgrade](MIGRATION-IMMUTABLE-GOALS.md) for primary-wins losses,
backups, format refusal, capability limits and required writer exclusion/overlap
drain before deployment activation.

## Explicit project catalog

`pkg/jobdb` adds `ProjectCatalog` and `ProjectAdmission` as separate capabilities.
They do not extend the existing runtime interfaces. Internal tenant fields mean
immutable project IDs; the catalog name is descriptive and never routes resources.
`pkg/jobdb/projectcatalog` supplies memory and caller-owned SQLite/PostgreSQL
storage. See [project catalog design](DESIGN-Project-Catalog.md) for receipts,
explicit installation, conservative deletion and the required runtime integration.

## Stable project identity

`ProjectCatalog`, `ProjectAdmission`, `ProjectExistence` and `ProjectControl` are
optional additive capabilities. Mandatory runtime method sets retain tenant
vocabulary; tenant means immutable project ID. Core Config.Projects,
SQLite/toy WithProjects and shared configuration/chapter/schema-store admission
must be composed for writers; absent admission denies mutations.
`remote.NewProjectRuntime` explicitly selects project HTTP while existing
constructors retain tenant HTTP. SQLite exposes inventory, explicit installation
and fresh-storage initialization. `projectcatalog` supplies memory/SQLite/Postgres
catalog storage. See [project catalog design](DESIGN-Project-Catalog.md).
