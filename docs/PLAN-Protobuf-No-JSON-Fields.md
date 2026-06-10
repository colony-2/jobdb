# Plan: Remove JSON-Shaped Fields From Storage Protobufs

## Status

**Proposed** | Author: Codex | Date: 2026-06-10

## Goal

Phase two removes JSON-shaped fields from the storage protobuf schema while
keeping the external `swf-go` API stable.

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

### Opaque Payload Bytes

Use one neutral payload wrapper for caller-owned bytes:

```proto
message OpaquePayload {
  bytes data = 1;
}
```

This message intentionally has no content type, encoding, or JSON-specific name.
Today the public Go API still provides `json.RawMessage`, so the codec may
validate or preserve JSON bytes at the boundary. The storage schema should not
describe those bytes as JSON.

Use `OpaquePayload` only when SWF does not interpret the bytes and is preserving
them for an existing public API surface:

```text
JobStartChapter.payload            // SubmitJob/PutJob payload bytes
RestartExtraChapter.payload        // restart-extra caller payload bytes
CustomChapter.payload              // caller-defined/unknown chapter payload bytes
TaskOutcome.app                    // successful task result bytes
CustomOutcome.payload              // caller-defined/unknown outcome payload bytes
ChapterRecord.input                // cached TaskData bytes visible through public task APIs
SchedulerPayload.external_payload  // RescheduleExecutionRequest.Payload / ExecutionLease.Payload
```

Do not use `OpaquePayload` for SWF-owned state. `RunPolicy`, `TaskWait`,
`JobPrerequisite`, `InputReference`, retry fields, metadata, and SWF error
payloads must be typed protobuf messages because the runtime reads and writes
their fields.

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
    OpaquePayload app = 1;
    AppErrorPayload app_error = 2;
    SystemErrorPayload system_error = 3;
    TimeoutPayload timeout = 4;
    CustomOutcome custom = 5;
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

Replace `visible_payload_json` with an opaque external payload:

```proto
message SchedulerPayload {
  RunPolicy run_policy = 1;
  TaskWait task_wait = 2;
  OpaquePayload external_payload = 3;
}
```

When the payload is only scheduler state (`run_policy` and/or `task_wait`), do
not duplicate it into `external_payload`. When a caller provides an arbitrary
public lease payload through `RescheduleExecutionRequest.Payload`, preserve it in
`external_payload` and return it from `ExecutionLease.Payload()`.

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
  OpaquePayload input = 9;
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
    CustomChapter custom = 34;
  }
}
```

## Implementation Phases

### Phase 2.1: Schema Rewrite

1. Replace all storage proto fields whose names contain `json`.
2. Add `OpaquePayload`, `Metadata`, `MetadataValue`, `MetadataList`,
   `MetadataMap`, `AppErrorPayload`, `SystemErrorPayload`, and `TimeoutPayload`.
3. Regenerate opaque Go protobuf code with Edition 2024 builders.
4. Add a proto guard test or script that fails if storage proto field names
   contain `json`.

Commit this separately.

### Phase 2.2: Codec Conversion

1. Update `pkg/swf/internal/runtimecodec` to convert public `json.RawMessage`
   inputs to typed protobuf values at the boundary.
2. Convert SWF error structs to and from typed protobuf error messages.
3. Convert public metadata JSON objects to `Metadata`.
4. Convert `OpaquePayload` bytes back to public `json.RawMessage` only when
   returning through existing public APIs.
5. Keep custom chapter and custom outcome payloads opaque through
   `OpaquePayload`.

Commit this separately.

### Phase 2.3: Runtime Adapters

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

### Phase 2.4: Tests and API Guard

1. Keep existing public API tests unchanged where possible.
2. Add focused runtimecodec tests for:
   - typed app/system/timeout error round trips;
   - metadata value tree round trips;
   - application payload byte preservation;
   - scheduler payload external payload preservation;
   - no duplicate scheduler state in `external_payload`.
3. Update tests that inspect raw Strata bodies to decode via runtimecodec or the
   public runtime API.
4. Run:

```sh
go test ./...
go run ./cmd/swf-api-snapshot -packages api/packages.txt -check api/swf-go.public.txt
```

5. Re-run c2j against `/src` using a temporary replace directive.

Commit test changes separately when they are not part of the implementation
commit.

## Acceptance Criteria

1. `proto/swf/storage/v1/storage.proto` contains no field names with `json`.
2. SWF-owned error payloads are typed protobuf messages, not opaque byte blobs.
3. SWF-owned metadata is stored as a protobuf value tree, not raw JSON bytes.
4. User application payloads are represented as opaque bytes and are not named
   or modeled as JSON in the proto.
5. `OpaquePayload` is not used for SWF-owned scheduler, error, metadata,
   retry, prerequisite, or input-reference state.
6. External `swf-go` API signatures remain unchanged.
7. `go test ./...` passes.
8. API snapshot check passes.
9. c2j compile-only validation passes against the local checkout.

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
