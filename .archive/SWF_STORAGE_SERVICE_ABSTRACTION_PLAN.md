# SWF Storage Service Abstraction Plan

## Goal

Decouple `swf` from direct knowledge of `strata` and `pgwf` so those become implementation details behind a single storage/runtime service boundary.

That service should support multiple implementations:

1. `direct` implementation
   Uses the current in-process `strata` client and `pgwf` access.
2. `remote` implementation
   Calls a remote service API. That remote service owns all interaction with `strata` and `pgwf`.
3. `toy` implementation
   Uses an in-memory model for fast tests and local development without external services.

The intended effect is:

- `swf` keeps workflow semantics, replay semantics, task/job execution, and public API.
- storage, chapter persistence, job state persistence, awaits, leases, and artifact materialization move behind one abstraction.
- the current behavior remains available as the default direct implementation.
- `toy` becomes a first-class implementation of the same abstraction instead of a separate conceptual path.

## Current State

Today `swf` is coupled to both systems:

- `pkg/swf/jobs.go`
  - engine builder requires `Strata` config and Postgres DSN
- `pkg/swf/impl/engine.go`
  - holds `*strataclient.Client`, `*gorm.DB`, `*sql.DB`
  - directly creates stories/chapters in `strata`
  - directly calls `pgwf` APIs for scheduling and leasing
- `pkg/swf/internal/backend.go`
  - already defines a useful runner-facing seam, but only for a subset of behavior

That seam is a good start, but it is too narrow and too runner-specific to support a clean remote backend.

## Proposed Direction

Introduce a higher-level abstraction under the engine:

- `WorkflowStore` or `WorkflowRuntime`

`WorkflowRuntime` is the better name if the abstraction covers both storage and scheduling/lease behavior, which it does.

Suggested split:

1. `WorkflowRuntime`
   Owns job lifecycle, chapter persistence, lookup, awaiting, replay reads, and artifact access.
2. `ExecutionLease`
   Represents an active leased execution slot when running workers.

This keeps `swf` talking to one internal dependency instead of separately to `strata` and `pgwf`.

## API Shape

```go
package swf

import (
    "context"
    "encoding/json"
    "io"
    "time"
)

type WorkflowRuntime interface {
    // Job lifecycle
    StartJob(ctx context.Context, req StartJobRequest) (JobHandle, error)
    RestartJob(ctx context.Context, req RestartJobRequest) (JobHandle, error)
    CancelJob(ctx context.Context, req CancelJobRequest) error

    // Worker loop
    PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error)

    // Read APIs
    CheckJobStatus(ctx context.Context, jobKey JobKey) (JobStatus, error)
    GetJobResult(ctx context.Context, jobKey JobKey) (TaskData, error)
    GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error)
    ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)

    // Chapter / replay access
    GetChapter(ctx context.Context, ref ChapterRef) (StoredChapter, error)
    PutChapter(ctx context.Context, req PutChapterRequest) error

    // Artifact access
    GetArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error)
}

type ExecutionLease interface {
    Job() JobHandle
    Capability() string
    Payload() json.RawMessage
    KeepAlive(ctx context.Context) error
    Complete(ctx context.Context, req CompleteExecutionRequest) error
    Reschedule(ctx context.Context, req RescheduleExecutionRequest) error
}

type JobHandle struct {
    JobKey JobKey
}

type ChapterRef struct {
    JobKey   JobKey
    Ordinal  int64
    Attempt  int
    TaskType string
}

type ArtifactRef struct {
    JobKey   JobKey
    Ordinal  int64
    Name     string
    Digest   string
}

type StoredChapter struct {
    Ordinal      int64
    TaskType     string
    ChapterType  string
    PayloadKind  string
    InputHash    string
    CreatedAt    time.Time
    Metadata     json.RawMessage
    Data         json.RawMessage
    Artifacts    []StoredArtifact
}

type StoredArtifact struct {
    Name   string
    Digest string
    Size   int64
}

type ArtifactReader interface {
    Open() (io.ReadCloser, error)
    Size() int64
    Name() string
}

type StartJobRequest struct {
    Job        StartJob
    WorkerID   string
    RequestTime time.Time
}

type RestartJobRequest struct {
    Job      RestartJob
    WorkerID string
}

type CancelJobRequest struct {
    JobKey   JobKey
    Reason   string
    WorkerID string
}

type PollWorkRequest struct {
    WorkerID      string
    Capabilities  []string
    Limit         int
    LongPollUntil *time.Time
}

type PutChapterRequest struct {
    Ref             ChapterRef
    Chapter         StoredChapter
    ArtifactUploads []ArtifactUpload
}

type CompleteExecutionRequest struct {
    Status string
    Detail string
}

type RescheduleExecutionRequest struct {
    NextNeed       string
    WaitUntil      *time.Time
    WaitForJobIDs  []string
    Payload        json.RawMessage
    AlternateNeed  string
    AlternateAfter *time.Duration
}
```

## Final Interface Shape

The earlier `WorkflowRuntime` sketch is directionally right, but it still leaves some ambiguity around runner concerns. The cleaner end state is to have one runtime abstraction with one lease abstraction and no separate runner backend contract.

Suggested final split:

1. `WorkflowRuntime`
   Owns all persistent and scheduling operations.
2. `ExecutionLease`
   Owns the active unit of work currently being executed.
3. optional internal helpers
   Built on top of the runtime, but not a separate backend abstraction.

### Final `WorkflowRuntime`

```go
package swf

import (
    "context"
    "encoding/json"
    "io"
    "time"
)

type WorkflowRuntime interface {
    // Job lifecycle
    StartJob(ctx context.Context, req StartJobRequest) (JobHandle, error)
    RestartJob(ctx context.Context, req RestartJobRequest) (JobHandle, error)
    CancelJob(ctx context.Context, req CancelJobRequest) error

    // Worker / scheduling
    PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error)

    // Job reads
    CheckJobStatus(ctx context.Context, jobKey JobKey) (JobStatus, error)
    GetJobResult(ctx context.Context, jobKey JobKey) (TaskData, error)
    GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error)
    ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)

    // Chapter reads and writes used by live execution and replay
    GetChapter(ctx context.Context, ref ChapterRef) (StoredChapter, error)
    PutChapter(ctx context.Context, req PutChapterRequest) error

    // Artifact reads
    OpenArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error)
}

type ExecutionLease interface {
    Job() JobHandle
    Capability() string
    Payload() json.RawMessage
    KeepAlive(ctx context.Context) error
    Complete(ctx context.Context, req CompleteExecutionRequest) error
    Reschedule(ctx context.Context, req RescheduleExecutionRequest) error
}

type ArtifactUpload struct {
    Name   string
    Size   int64
    Open   func() (io.ReadCloser, error)
}
```

### Why this replaces `RunnerBackend`

Everything `RunnerBackend` currently does fits into one of these categories:

- chapter lookup
  - `GetChapter`
- chapter save
  - `PutChapter`
- await/reschedule behavior
  - `ExecutionLease.Reschedule`
- lease completion
  - `ExecutionLease.Complete`
- keepalive
  - `ExecutionLease.KeepAlive`
- job completion checks used by `AwaitJobs`
  - `CheckJobStatus`
- artifact materialization / fallback
  - `OpenArtifact`
  - `PutChapter` with attached artifact uploads

That means the final system does not need a distinct `RunnerBackend` interface.

### How runner code should use it

Runner code should depend on:

- `WorkflowRuntime`
- `ExecutionLease`
- pure in-process helper functions for envelope conversion, determinism checks, fallback policy, and task/job orchestration

Not on:

- `story.Key`
- `story.Chapter`
- `pgwf.Lease`
- `pgwf.JobDependencies`
- a second internal backend abstraction

### Useful internal helpers after the merge

Some helper structs may still be worthwhile internally, but they should be thin helpers over runtime-native types.

Examples:

```go
type chapterCodec interface {
    DecodeStoredChapter(ch StoredChapter) (chapterEnvelope, error)
    EncodeTaskResult(req EncodeTaskResultRequest) (StoredChapter, error)
}

type awaitPlanner interface {
    ForTime(wakeAt time.Time, currentCapability string, payload json.RawMessage) RescheduleExecutionRequest
    ForJobs(jobIDs []string, currentCapability string, payload json.RawMessage) RescheduleExecutionRequest
}
```

Those helpers do not compete with the runtime abstraction. They are just local logic utilities.

### Mapping from current seams

Current seam:

- `pkg/swf/internal/backend.go:RunnerBackend`
- `pkg/swf/internal/backend.go:Lease`

Target seam:

- `swf.WorkflowRuntime`
- `swf.ExecutionLease`

Migration approach:

1. add adapters so the current runner can call runtime-native methods
2. stop passing `story.Key`, `story.Chapter`, and `pgwf` types through the runner path
3. delete `RunnerBackend` once the runner is fully runtime-native

### Implementation Notes by Runtime

`direct`

- maps `StoredChapter` to `story.Chapter`
- maps `ExecutionLease` to `pgwf.Lease`
- may internally perform artifact upload first, then chapter write

`remote`

- maps runtime methods to OpenAPI calls
- keeps artifact writes chapter-oriented at the service boundary
- server performs the actual `strata` / `pgwf` work

`toy`

- stores `StoredChapter` directly in memory
- implements `ExecutionLease` with in-memory pending work state
- exercises the same runner path without backend-specific adapters

## Design Notes

The API should be expressed in `swf` terms, not `strata` or `pgwf` terms.

Examples:

- use `JobKey`, not `story.Key`
- use `StoredChapter`, not `story.Chapter`
- use `ExecutionLease`, not `pgwf.Lease`
- use `ArtifactRef`, not direct `strata` identifiers

This is important. If the abstraction leaks `story.Key` or `pgwf.JobDependencies`, the remote implementation will be awkward and the boundary will not hold.

## Implementation Model

### 1. Direct Runtime

`directWorkflowRuntime` wraps the current implementation:

- Strata-backed chapter and artifact persistence
- pgwf-backed job lifecycle, scheduling, awaiting, and leases
- local conversion between:
  - `StoredChapter` and `story.Chapter`
  - `ExecutionLease` and `pgwf.Lease`
  - `ArtifactReader` and current `swf.Artifact` handling

This preserves behavior and becomes the reference implementation.

### 2. Remote Runtime

`remoteWorkflowRuntime` talks to a remote service over HTTP/gRPC.

The remote service owns:

- story/chapter creation and retrieval
- artifact upload/download
- job submission / cancellation / status
- lease acquisition / keepalive / complete / reschedule
- await semantics
- list/query operations needed by `swf`

The remote service may still use `strata` and `pgwf` internally, but `swf` no longer knows that.

### 3. Toy Runtime

`toyWorkflowRuntime` provides the same semantic contract in-memory.

It should be treated as an implementation of the same abstraction, not as a parallel engine model.

Responsibilities:

- in-memory job lifecycle
- in-memory chapter persistence
- in-memory artifact storage
- deterministic replay behavior compatible with SWF expectations
- no dependency on `strata`, `pgwf`, or Postgres

Benefits:

- one conceptual model across production and tests
- conformance tests can run against `toy`, `direct`, and `remote`
- fewer one-off test-only execution paths in the engine

## Recommended Service Boundary

Do not expose raw `strata` and `pgwf` semantics over the wire.

The remote API should mirror the `WorkflowRuntime` abstraction closely. That keeps:

- one conceptual model
- one test contract
- one migration target

It also allows the direct and remote implementations to share conformance tests.

## Where This Fits in SWF

### Engine Builder

Current builder requires:

- `WithStrata(...)`
- `WithStrataAPIKey(...)`
- `WithPostgresDSN(...)`

Proposed builder evolution:

```go
type EngineBuilder struct {
    workers         map[string]WorkSet
    maxActive       int
    logger          *slog.Logger
    awaitRecycle    time.Duration

    runtime         WorkflowRuntime

    // legacy direct-runtime config
    strataURI       string
    strataAPIKey    string
    postgresDSN     string
}

func (e *EngineBuilder) WithRuntime(runtime WorkflowRuntime) *EngineBuilder
func (e *EngineBuilder) WithRemoteRuntime(baseURL, apiKey string) *EngineBuilder
```

Build behavior:

1. if `WithRuntime(...)` was provided, use it
2. otherwise construct the current direct runtime from legacy Strata/Postgres config

That keeps backward compatibility while opening the new architecture.

### Engine Internals

Refactor `swfEngineImpl` so it depends primarily on:

```go
type swfEngineImpl struct {
    runtime         swf.WorkflowRuntime
    workers         map[string]*swf.WorkSet
    workerID        string
    activeWorkLimit int
    logger          *slog.Logger
    awaitThreshold  time.Duration
}
```

The current direct-only fields:

- `strata`
- `db`
- `udb`

should move into `directWorkflowRuntime`, not remain on the engine.

### Runner Backend

`pkg/swf/internal/backend.go` should either:

1. be expanded to match the new runtime boundary, or
2. be removed in favor of `WorkflowRuntime` plus small helper adapters

Recommendation: do not keep two overlapping abstractions long term.

`RunnerBackend` was useful as an intermediate seam, but the new runtime should become the single abstraction.

The cleanest merge is:

1. keep `RunnerBackend` only as a temporary adapter layer during migration
2. redefine it in runtime-native terms if needed during the transition
3. remove it once runner/replay talk directly to `WorkflowRuntime` and `ExecutionLease`

Concretely:

- chapter reads/writes should come from `WorkflowRuntime`
- await/reschedule/complete operations should go through `ExecutionLease` and runtime request types
- artifact fallback/materialization should use runtime artifact APIs rather than direct Strata access

If a runner-specific helper is still useful after the migration, it should be a thin internal helper built on top of `WorkflowRuntime`, not a second backend abstraction with its own foreign types.

## Migration Plan

### Step 1: Introduce backend-agnostic APIs and the direct implementation

Add the new `swf`-native abstraction first, without changing behavior:

- add `WorkflowRuntime`, `ExecutionLease`, and the related request/response types in `pkg/swf`
- teach `EngineBuilder` to accept `WithRuntime(...)`
- keep the main engine implementation runtime-oriented and backend-agnostic
- create a separate package for the direct implementation, for example:
  - `pkg/swf/runtime/direct`
- create a separate package for the toy implementation, for example:
  - `pkg/swf/runtime/toy`
- implement `direct` against the current `strata` + `pgwf` behavior
- implement `toy` against the same runtime contract using in-memory state
- make both implementations usable from the same engine abstraction in this phase
- migrate the main engine path so live execution, replay, and worker polling run through the new runtime APIs
- keep leaking public APIs available only as deprecated compatibility shims during this phase
- update tests to exercise the new runtime-based engine path for both `direct` and `toy`
- ensure existing behavior remains intact while running via the new APIs, not the old direct seams

That package should own all direct dependencies on:

- `strata`
- `pgwf`
- `gorm` / direct Postgres access

The direct runtime package may still accept backend-specific objects where useful, for example:

- `*strataclient.Client`
- `*gorm.DB`
- `*sql.DB`
- `*pgwf.Lease`

But those types should not remain part of the main engine package contract.

In the same step, mark the current leaking public APIs as deprecated and route users to the new abstraction. At minimum this includes:

- `swf.Lease`
- `swf.Dependencies`
- `JobKey.ToStoryKey()`
- `JobKeyFromStoryKey(...)`
- `WorkSet.Capabilities`
- `swf.Builder`
- `swf.FromStrataArtifact(...)`
- `swf.ToStrataArtifact(...)`
- `swf.PgwfMetadataPredicates(...)`
- legacy builder configuration that hardcodes direct backends:
  - `WithStrata(...)`
  - `WithStrataAPIKey(...)`
  - `WithPostgresDSN(...)`

Result:

- existing behavior is preserved
- users can migrate to backend-agnostic APIs
- direct backend concerns move into a dedicated package instead of the main engine implementation
- `toy` and `direct` both validate the runtime abstraction from the start

Step 1 is not complete until:

- the engine runs through `WorkflowRuntime` / `ExecutionLease` rather than the old backend-specific seams
- both `pkg/swf/runtime/direct` and `pkg/swf/runtime/toy` are functional
- the relevant test suite passes using the new API path
- users can construct and run engines with the new APIs without depending on leaked `strata` / `pgwf` objects in the main engine package

### Step 2: Remove deprecated leaking APIs

Once the new API has landed and internal code is no longer using the old surface:

- remove deprecated public APIs that expose `strata` or `pgwf`
- remove public helpers whose only purpose is adapting `swf` to `strata` / `pgwf`
- ensure the main `pkg/swf` package no longer exports foreign backend types
- keep any unavoidable backend-specific code isolated inside `pkg/swf/runtime/direct`
- move `toy` behind the same runtime-oriented engine path, or make it a thin engine wrapper around `pkg/swf/runtime/toy`

This is the point where `swf` should present a clean backend-agnostic public API.

Result:

- no public `strata` or `pgwf` objects in the main `swf` API
- backend-specific code is isolated to the direct runtime package
- `toy` participates in the same abstraction boundary as other implementations

### Step 3: Develop an OpenAPI protocol for the remote runtime

Define a wire protocol that mirrors `WorkflowRuntime` closely.

Requirements:

- OpenAPI-first contract
- SWF-native resource model, not raw `strata` / `pgwf` terminology
- endpoints for:
  - job start / restart / cancel
  - poll work / lease keepalive / complete / reschedule
  - job status / result / run details / list jobs
  - chapter read / write
  - artifact upload / download

Recommended approach:

- generate server and client models from the OpenAPI spec
- keep request/response bodies aligned with `swf` runtime types where practical
- define error mapping explicitly so remote failures preserve SWF semantics

Result:

- the remote runtime has a stable, language-independent protocol
- direct and remote implementations can target the same semantic contract

### Step 4: Implement the remote server and client

Build both sides of the remote runtime:

- server implementation
  - owns direct interaction with `strata` and `pgwf`
  - can reuse logic from `pkg/swf/runtime/direct` where appropriate
- client implementation
  - implements `WorkflowRuntime`
  - translates runtime calls into OpenAPI requests
  - handles auth, retries, timeouts, and error translation

Testing should include:

- toy runtime conformance tests
- direct runtime conformance tests
- remote client conformance tests
- server integration tests
- end-to-end tests with an SWF engine using the remote runtime

Result:

- `swf` can run against a remote service with the same runtime contract

## Important Contract Decisions

### 1. Transactions

The direct implementation can preserve current in-process transaction behavior.

The remote implementation cannot share local DB transactions with `swf`.

Therefore the abstraction should treat persistence operations as service operations, not as caller-managed SQL transactions. If some existing code assumes cross-resource local transactionality, that assumption must be removed from the boundary.

### 2. Artifact Uploads

For the remote implementation, artifact upload likely needs one of:

1. direct upload through the runtime API
2. pre-signed upload URLs
3. streaming gRPC

The abstraction above is intentionally neutral. The important semantic rule is that artifacts are committed with their parent chapter, not as an independently durable resource.

That means:

- the public remote API can make `PutChapter` the commit point for artifact bytes
- if large artifact handling requires a multi-step protocol, that protocol should still read as "prepare chapter attachments, then commit chapter", not "create standalone artifacts"

### 3. Await Semantics

Awaits are currently tightly tied to `pgwf` rescheduling.

Those should remain semantic operations in `swf`, but the actual storage/scheduling expression of them belongs in the runtime:

- wait until time
- wait for job completion
- reschedule to next capability

That is the main reason this should be called a runtime, not just storage.

### 4. Replay

Replay must remain read-only.

The runtime contract should make this easy to enforce:

- replay paths use only read APIs
- mutation methods should not be called during replay
- remote service should not need any special-case replay mode beyond honoring read-only usage

## Testing Strategy

Add a conformance-style test suite for `WorkflowRuntime`.

Both implementations should pass the same behavioral tests for:

- start job
- read chapter
- save chapter
- check status
- reschedule / complete lease
- await-by-time
- await-by-job
- list jobs
- artifact round-trip
- replay read expectations

This is the main mechanism that will keep the remote implementation honest.

## Recommendation

Use `WorkflowRuntime` as the primary abstraction name and treat it as the only boundary between `swf` and persistence/scheduling.

Treat the supported implementations as:

- `pkg/swf/runtime/direct`
- `pkg/swf/runtime/remote`
- `pkg/swf/runtime/toy`

The rollout should be:

1. introduce the new backend-agnostic APIs and a separate `direct` runtime package, while deprecating leaking APIs
2. move `toy` and runner internals onto the same abstraction boundary, then remove the deprecated leaking APIs
3. define the OpenAPI protocol for the remote runtime
4. implement the remote server and client

`strata` and `pgwf` should become private dependencies of:

- the `direct` runtime package
- the remote service implementation

The main `swf` engine package should not depend on their concepts once the migration is complete.
