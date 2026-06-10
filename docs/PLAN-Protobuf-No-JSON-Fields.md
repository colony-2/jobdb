# Plan: Remove JSON-Shaped Fields From Storage Protobufs

## Status

**Proposed** | Author: Codex | Date: 2026-06-10

## Goal

Phase two removes JSON-shaped fields from the storage protobuf schema while
keeping existing `swf-go` APIs stable and adding a structured Go API for typed
chapter access.

The current storage proto still carries many fields named `*_json`. That keeps
too much of the old storage model inside the new schema. The next phase should
make SWF-owned data structured protobuf and reserve opaque bytes only for user
application payloads that SWF does not understand.

No backwards compatibility is required. As before, operators delete old jobs and
start with fresh stores before running this version.

## Current Problem Fields

`proto/swf/storage/v1/storage.proto` currently has these JSON-shaped fields:

```text
ChapterRecord.metadata_json
ChapterRecord.input_json
JobStartChapter.payload_json
RestartExtraChapter.payload_json
CustomChapter.payload_json
TaskOutcome.app_payload_json
TaskOutcome.app_error_payload_json
TaskOutcome.system_error_payload_json
TaskOutcome.timeout_payload_json
CustomOutcome.payload_json
SchedulerPayload.visible_payload_json
```

These fields mix three different concepts:

1. User application payload bytes.
2. SWF-owned structured error and metadata state.
3. Public API JSON views used only because current Go interfaces expose
   `json.RawMessage`.

Only the first category should remain opaque in storage. The second category
should be real protobuf messages. The third category should be reconstructed at
the API boundary, not persisted as a JSON-shaped protobuf field.

## Target Shape

### Application and Lease Byte Wrappers

Use role-specific wrappers for caller-owned bytes. Job/task values are
application-defined input/output structs from the caller's perspective, but SWF
storage only preserves the serialized bytes.

```proto
message ApplicationInputBytes {
  bytes data = 1;
}

message ApplicationOutputBytes {
  bytes data = 1;
}

message LeasePayloadBytes {
  bytes data = 1;
}
```

The `Application` prefix is intentional. Bare `InputBytes` and `OutputBytes`
would be concise, but they are too generic in a schema that also has
`InputReference`, task waits, retry state, and scheduler-owned fields. The
`Bytes` suffix makes the generated Go types read as serialization wrappers, not
as the caller's domain structs.

These messages intentionally have no content type, encoding, or JSON-specific
name. Today the public Go API still provides `json.RawMessage`, so the codec may
validate or preserve JSON bytes at the boundary. The storage schema should not
describe those bytes as JSON.

Use these wrappers only when SWF does not interpret the bytes and is preserving
them for an existing public API surface:

```text
JobStartChapter.input        // SubmitJob/PutJob application input bytes
RestartExtraChapter.output   // restart ExtraTaskOutput bytes
TaskOutcome.app_output       // successful task/job output bytes
ChapterRecord.input          // cached TaskData input bytes visible through public task APIs
SchedulerPayload.lease_payload  // RescheduleExecutionRequest.Payload / ExecutionLease.Payload
```

Do not use byte wrappers for SWF-owned state. `RunPolicy`, `TaskWait`,
`JobPrerequisite`, `InputReference`, retry fields, metadata, and SWF error
payloads must be typed protobuf messages because the runtime reads and writes
their fields.

The chapter variants that carry caller-owned bytes should make the role clear:

```proto
message JobStartChapter {
  ApplicationInputBytes input = 1;
}

message RestartExtraChapter {
  ApplicationOutputBytes output = 1;
}
```

### Typed Error Payloads

Replace error payload bytes with SWF-owned messages:

```proto
message AppErrorPayload {
  string message = 1;
  string level = 2;
  map<string, MetadataValue> attrs = 3;
  InputReference input_ref = 4;
  repeated string stacktrace = 5;
}

message SystemErrorPayload {
  string message = 1;
  string component = 2;
  string code = 3;
  bool retryable = 4;
  InputReference input_ref = 5;
  repeated string stacktrace = 6;
}

message TimeoutPayload {
  string kind = 1;
  google.protobuf.Duration after = 2;
  string scope = 3;
  InputReference input_ref = 4;
  bool retryable = 5;
}
```

Then change `TaskOutcome` to:

```proto
message TaskOutcome {
  oneof result {
    ApplicationOutputBytes app_output = 1;
    AppErrorPayload app_error = 2;
    SystemErrorPayload system_error = 3;
    TimeoutPayload timeout = 4;
  }
}
```

### Typed Metadata Values

Replace `metadata_json` with a protobuf value tree:

```proto
message Metadata {
  map<string, MetadataValue> fields = 1;
}

message MetadataValue {
  oneof kind {
    bool bool_value = 1;
    int64 int_value = 2;
    double double_value = 3;
    string string_value = 4;
    MetadataList list_value = 5;
    MetadataMap map_value = 6;
    bool null_value = 7;
  }
}

message MetadataList {
  repeated MetadataValue values = 1;
}

message MetadataMap {
  map<string, MetadataValue> fields = 1;
}
```

This avoids `google.protobuf.Struct` in the storage schema and gives us stable,
explicit conversion rules in Go. The public Go API can still accept
`json.RawMessage` metadata; the codec converts at the runtime boundary.

### Scheduler Payload

Replace `visible_payload_json` with a public lease payload:

```proto
message SchedulerPayload {
  RunPolicy run_policy = 1;
  TaskWait task_wait = 2;
  LeasePayloadBytes lease_payload = 3;
}
```

When the payload is only scheduler state (`run_policy` and/or `task_wait`), do
not duplicate it into `lease_payload`. When a caller provides an arbitrary
public lease payload through `RescheduleExecutionRequest.Payload`, preserve it in
`lease_payload` and return it from `ExecutionLease.Payload()`.

### Chapter Record

The chapter record becomes:

```proto
message ChapterRecord {
  int64 ordinal = 1;
  string task_type = 2;
  string worker_id = 3;
  google.protobuf.Timestamp created_at = 4;
  google.protobuf.Timestamp started_at = 5;
  google.protobuf.Timestamp finished_at = 6;
  string input_hash = 7;
  Metadata metadata = 8;
  ApplicationInputBytes input = 9;
  int32 attempt = 10;
  int32 max_attempts = 11;
  google.protobuf.Timestamp next_attempt_at = 12;
  int64 backoff_millis = 13;
  bool retryable = 14;
  InputReference input_ref = 15;
  RunPolicy run_policy = 16;
  repeated JobPrerequisite prerequisites = 17;

  oneof chapter {
    JobStartChapter job_start = 30;
    JobAttemptOutcomeChapter job_attempt_outcome = 31;
    TaskAttemptOutcomeChapter task_attempt_outcome = 32;
    RestartExtraChapter restart_extra = 33;
  }
}
```

### Structured Go API

Add a second public Go API that is fully structured and mirrors the protobuf
oneof cases. Keep the existing `StoredChapter` API as a legacy/raw adapter for
now, but make the structured API the canonical API for new runtime code.

Go does not have native closed union values, so represent proto `oneof` fields
with concrete SWF-defined structs behind interfaces with unexported marker
methods. Callers can pass one of the SWF-provided concrete types, while external
packages cannot add unsupported custom chapter or outcome variants.

```go
type StructuredWorkflowRuntime interface {
	GetStructuredChapter(ctx context.Context, ref ChapterRef) (StructuredChapterRecord, error)
	ListStructuredChapters(ctx context.Context, req ListChaptersRequest) ([]StructuredChapterRecord, error)
	PutStructuredChapter(ctx context.Context, req PutStructuredChapterRequest) error
}

type PutStructuredChapterRequest struct {
	LeaseID         string
	LeaseToken      string
	Ref             ChapterRef
	Chapter         StructuredChapterRecord
	ArtifactUploads []ArtifactUpload
}

type StructuredChapterRecord struct {
	Ordinal       int64
	TaskType      string
	WorkerID      string
	CreatedAt     time.Time
	StartedAt     *time.Time
	FinishedAt    *time.Time
	InputHash     string
	Metadata      Metadata
	Input         ApplicationInputBytes
	Attempt       int
	MaxAttempts   int
	NextAttemptAt *time.Time
	BackoffMillis int64
	Retryable     *bool
	InputRef      *InputReference
	RunPolicy     *RunPolicy
	Prerequisites []JobPrerequisite
	Body          StructuredChapterBody
	Artifacts     []StoredArtifact
}

type StructuredChapterBody interface {
	structuredChapterBody()
}

type JobStartChapter struct {
	Input ApplicationInputBytes
}

type JobAttemptOutcomeChapter struct {
	Outcome StructuredTaskOutcome
}

type TaskAttemptOutcomeChapter struct {
	Outcome StructuredTaskOutcome
}

type RestartExtraChapter struct {
	Output ApplicationOutputBytes
}
```

Task outcomes follow the same pattern:

```go
type StructuredTaskOutcome struct {
	Result StructuredTaskResult
}

type StructuredTaskResult interface {
	structuredTaskResult()
}

type AppOutputResult struct {
	Output ApplicationOutputBytes
}

type AppErrorResult struct {
	Error AppErrorPayload
}

type SystemErrorResult struct {
	Error SystemErrorPayload
}

type TimeoutResult struct {
	Error TimeoutPayload
}
```

The structured Go API should be implemented in terms of the same internal
protobuf codec used for storage. The legacy `StoredChapter` adapter converts
between `ChapterType`/`PayloadKind`/`Data` and `StructuredChapterRecord`.
Unsupported legacy combinations return validation errors instead of becoming
custom proto variants.

## Implementation Phases

### Phase 2.1: Schema Rewrite

1. Replace all storage proto fields whose names contain `json`.
2. Add `ApplicationInputBytes`, `ApplicationOutputBytes`,
   `LeasePayloadBytes`, `Metadata`, `MetadataValue`, `MetadataList`,
   `MetadataMap`, `AppErrorPayload`, `SystemErrorPayload`, and `TimeoutPayload`.
3. Remove `CustomChapter` and `CustomOutcome`; the proto oneofs should contain
   only SWF-supported chapter and outcome variants.
4. Regenerate opaque Go protobuf code with Edition 2024 builders.
5. Add a proto guard test or script that fails if storage proto field names
   contain `json`, `custom`, or unknown catch-all oneof variants.

Commit this separately.

### Phase 2.2: Codec Conversion

1. Update `pkg/swf/internal/runtimecodec` to convert public `json.RawMessage`
   inputs to typed protobuf values at the boundary.
2. Convert SWF error structs to and from typed protobuf error messages.
3. Convert public metadata JSON objects to `Metadata`.
4. Convert application input/output bytes back to public `json.RawMessage` only
   when returning through existing public APIs.
5. Reject unsupported legacy `StoredChapter` combinations instead of storing
   them as custom proto variants.

Commit this separately.

### Phase 2.3: Structured Go API

1. Add `StructuredWorkflowRuntime`, `PutStructuredChapterRequest`,
   `StructuredChapterRecord`, the chapter-body types, and structured outcome
   result types to `pkg/swf`.
2. Use sealed marker interfaces for `StructuredChapterBody` and
   `StructuredTaskResult` so callers can pass one of the SWF-defined variants
   but cannot introduce custom variants.
3. Add conversion helpers between legacy `StoredChapter` and
   `StructuredChapterRecord`.
4. Implement structured methods in direct, SQLite, toy, and remote runtimes, or
   provide shared adapters where a runtime can implement one API in terms of the
   other.
5. Update the public API snapshot intentionally for this additive API surface.

Commit this separately.

### Phase 2.4: Runtime Adapters

1. Direct runtime:
   - Keep the current compact JSON object carrier for `pgwf` payloads because
     `pgwf` requires JSON object payloads.
   - Keep the current compact JSON object carrier for Strata chapter bodies
     while Strata rejects non-JSON chapter bodies.
   - These carriers are substrate constraints, not protobuf schema fields.
2. SQLite runtime:
   - Store protobuf bytes directly for scheduler payload and wait-list state.
   - Keep public list/lease payload views stable by decoding through the shared
     codec.
3. Remote runtime:
   - Keep REST JSON wire behavior stable for now.
   - Do not add gRPC in this phase.

Commit runtime adapter changes separately from schema/codegen.

### Phase 2.5: Tests and API Guard

1. Keep existing public API tests unchanged where possible.
2. Add focused runtimecodec tests for:
   - typed app/system/timeout error round trips;
   - metadata value tree round trips;
   - application input/output byte preservation;
   - scheduler lease payload preservation;
   - no duplicate scheduler state in `lease_payload`.
3. Add structured Go API tests that write/read each supported chapter oneof
   variant and each supported task-outcome result variant.
4. Update tests that use manual/custom chapters. Either convert them to a
   supported SWF chapter variant when testing storage mechanics, or assert a
   validation error when testing unsupported chapter shapes.
5. Update tests that inspect raw Strata bodies to decode via runtimecodec or the
   public runtime API.
6. Run:

```sh
go test ./...
go run ./cmd/swf-api-snapshot -packages api/packages.txt -check api/swf-go.public.txt
```

7. Re-run c2j against `/src` using a temporary replace directive.

Commit test changes separately when they are not part of the implementation
commit.

## Acceptance Criteria

1. `proto/swf/storage/v1/storage.proto` contains no field names with `json`.
2. `ChapterRecord.chapter` and `TaskOutcome.result` contain only explicit
   SWF-supported oneof variants; there is no custom or unknown catch-all
   variant.
3. SWF-owned error payloads are typed protobuf messages, not opaque byte blobs.
4. SWF-owned metadata is stored as a protobuf value tree, not raw JSON bytes.
5. User application input/output payloads are represented as
   `ApplicationInputBytes` or `ApplicationOutputBytes` and are not named or
   modeled as JSON in the proto.
6. Caller-owned byte wrappers are not used for SWF-owned scheduler, error,
   metadata, retry, prerequisite, or input-reference state.
7. The structured Go API mirrors the proto oneofs with sealed concrete variant
   types for chapters and task outcomes.
8. Existing `swf-go` API signatures remain unchanged; the API snapshot is
   intentionally updated for the additive structured API.
9. Legacy `StoredChapter` adapters reject unsupported chapter types or payload
   kinds instead of storing custom variants.
10. `go test ./...` passes.
11. API snapshot check passes.
12. c2j compile-only validation passes against the local checkout.

## Non-Goals

1. Removing `json.RawMessage` from public Go APIs.
2. Removing JSON from the current REST runtime API.
3. Changing artifact storage or adding artifact content types.
4. Adding gRPC.
5. Supporting old jobs or old protobuf/JSON field shapes.

## Open Implementation Risks

1. Metadata conversion must preserve current equality and filter behavior.
2. `AppErrorPayload.Attrs` may contain heterogeneous values; the value tree must
   cover all shapes current users rely on.
3. Direct runtime still depends on `pgwf` and Strata JSON-object constraints.
   Those carriers can be removed only after those dependencies accept binary
   payloads in the relevant fields.
4. Public `ExecutionLease.Payload()` still returns `json.RawMessage`; arbitrary
   payload bytes must remain valid JSON at that API boundary even though the
   storage proto no longer models them as JSON.
