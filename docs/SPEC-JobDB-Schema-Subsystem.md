# Specification: JobDB Schema Subsystem

## Status

**Implemented design** | Author: Codex | Updated: 2026-06-23

This document describes the implemented JobDB schema subsystem after the package
rename from SWF to JobDB.

## Decision

JobDB should support optional, tenant-local JSON Schemas for validating job
chapter shape. A job may omit a schema entirely; in that case JobDB behaves as
it does today and performs no schema validation.

Schemas are not part of JobDB's core execution semantics. The core runtime still
orders chapters, enforces leases, stores artifacts, schedules work, and tracks
job state. A schema is an opaque validation contract associated with a job.

## Current Implementation Status

Implemented today:

- Public schema model and errors live in `pkg/jobdb/schema.go` and
  `pkg/jobdb/errors.go`.
- Toy, SQLite, direct, and remote runtimes implement `jobdb.JobSchemaRegistry`.
- SQLite and direct runtimes persist tenant-local schema rows in
  `jobdb_schemas`.
- OpenAPI schema endpoints live in `openapi/jobdb-runtime.yaml`; generated
  runtime bindings live in `pkg/jobdb/internal/runtimeapi/zz_generated.go`.
- `SubmitJob` and `SubmitRestartJob` accept `JobSchemaSelector`, with either
  `schemaHash`, inline `schema`, or neither.
- Inline schemas are canonicalized, hashed, compiled, and registered
  tenant-locally with put-if-absent semantics.
- Referenced schemas must exist and be active for new jobs. Archived schemas are
  rejected for new job creation.
- Jobs without schemas continue to work with no schema resolution or validation.
- Resolved schema hashes are stored in the JobDB metadata envelope as
  `internal.schemaHash`.
- `JobInfo`, `JobSummary`, remote `ExecutionLease`, and concrete lease test
  hooks expose `schemaHash`.
- Remote lease tokens carry `schema_hash`, and
  `pkg/jobdb/internal/leaseauth` passes trusted claims through the request
  context.
- Schema-bound writes validate visible chapter records using JSON Schema draft
  2020-12 via `pkg/jobdb/internal/jobschema`.
- Validation runs on `SubmitJob`, `SubmitRestartJob`, `PutChapter`, and
  `CompleteTaskIfWaiting`.
- Schema documents are compiled into a process-local cache keyed by
  `(tenantId, schemaHash)`.

Remaining intentional gaps:

- Schema validation errors are typed, but the public error payload is still a
  plain message rather than a structured JSON-pointer report.
- Schema list APIs are not paginated.
- There are no schema-specific job query indexes beyond exposing `schemaHash`.
- The compiled schema cache is process-local and has no eviction policy.

## Goals

1. Allow different tenants to use different schemas.
2. Let jobs opt into validation by schema hash or inline schema.
3. Keep jobs without schemas fully supported.
4. Validate chapter writes without adding a pgwf/job-row read to remote
   `add_chapter`.
5. Make schema lifecycle explicit: register, get, list, archive.
6. Keep archived schemas usable by already-created mutable jobs.
7. Avoid schema defaults and other mutation behavior.

## Non-Goals

1. No extension-operation schemas in this phase.
2. No schema delete operation.
3. No schema defaults. JobDB never materializes or writes defaulted values.
4. No coupling between workflow execution logic and schema internals.
5. No generated language-specific validators in the first implementation.

## Schema Scope

A JobDB schema defines what visible `ChapterRecord` JSON may look like for a
job. It may constrain:

- chapter ordinal;
- chapter body variant;
- task type;
- metadata shape;
- input/output JSON payload shape;
- artifacts descriptors;
- attempt/retry fields.

It does not define:

- lease ownership;
- polling behavior;
- scheduling behavior;
- artifact byte storage;
- whether a job is ready, waiting, completed, or cancelled.

JSON Schema object fields remain open by default. Extra fields are allowed
unless a schema author explicitly sets `additionalProperties: false` or
`unevaluatedProperties: false`.

## Chapter Zero Versus Later Chapters

Schemas can express different shapes for chapter `0` and non-zero chapters using
ordinary JSON Schema conditionals or `oneOf`.

Example:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "oneOf": [
    {
      "properties": {
        "ordinal": { "const": 0 },
        "body": {
          "properties": { "kind": { "const": "jobStart" } },
          "required": ["kind"]
        }
      },
      "required": ["ordinal", "body"]
    },
    {
      "properties": {
        "ordinal": { "minimum": 1 },
        "body": {
          "properties": {
            "kind": {
              "enum": ["taskAttemptOutcome", "jobAttemptOutcome", "restartExtra"]
            }
          },
          "required": ["kind"]
        }
      },
      "required": ["ordinal", "body"]
    }
  ]
}
```

JobDB does not need a special schema language for this. The schema applies to
the chapter document, and JSON Schema decides which branch matches.

## Schema Identity

Schema identity is a SHA-256 content hash over canonical JSON for the schema
document.

```text
schemaHash = "sha256:" + lowercase_hex(sha256(canonical_json(schema)))
```

The registry key is tenant-local:

```text
(tenantId, schemaHash)
```

The same schema content may produce the same hash in multiple tenants, but
active/archive state is scoped per tenant.

## Registry Lifecycle

Required operations:

- Register schema.
- Get schema by hash.
- List schemas for a tenant.
- Archive schema.

No delete operation exists.

Register is idempotent. Registering the same schema for the same tenant returns
the existing row. Registering a schema whose hash exists with different content
is a conflict, even though SHA-256 collisions are not expected.

Archive is one-way for this phase. An archived schema remains readable and
usable by existing jobs that already reference it. It is rejected for newly
inserted jobs.

## Job Association

A job may specify either:

- `schemaHash`: reference an existing active tenant schema;
- `schema`: inline JSON Schema document; or
- neither.

If `schema` is supplied, JobDB computes its hash and performs a tenant-local
put-if-absent registration. If both `schema` and `schemaHash` are supplied, the
computed hash must equal `schemaHash`.

When a job uses a schema, JobDB stores the resolved hash in immutable stored job
metadata. The canonical internal metadata field is:

```json
{
  "internal": {
    "schemaHash": "sha256:..."
  }
}
```

The metadata helper also recognizes `jobdb_schema_hash`, `schema_hash`, and
`schemaHash` at the root, app, or internal metadata level for compatibility.
That compatibility behavior is not the public API for schema selection.

## Write Enforcement

Schema validation happens only when a job has a schema hash.

### Submit Job

On `SubmitJob`:

1. Resolve the schema reference or inline schema.
2. Reject archived or unknown schema references.
3. Store the resolved schema hash in immutable job metadata.
4. Validate chapter `0` before committing the job.

If no schema is supplied, skip all schema resolution and validation.

### Submit Restart Job

`SubmitRestartJob` accepts the same schema selector. If omitted, the new job has
no schema. If supplied, retained restart chapters and the new start state must
be valid under the new job's schema.

### Add Chapter With Lease

Remote `add_chapter` does not read the pgwf/job table solely to discover the
schema hash.

The path is:

1. Poll or targeted lease acquisition reads the job metadata it already needs.
2. The lease response and signed lease token include the resolved schema hash.
3. `add_chapter` validates the token and extracts `(tenantId, jobId, leaseId,
   schemaHash)`.
4. If `schemaHash` is empty, skip schema validation.
5. If present, load the compiled schema validator from a tenant/hash cache
   backed by the schema registry.
6. Validate the incoming chapter JSON before storing it.

An archived schema is accepted here because the job already chose that schema
when it was created.

### Commit If Waiting

`commit-if-waiting` is lease-less, so it cannot rely on a lease token. It already
has to inspect job wait state to enforce guards. During that read/lock, it
obtains the job's schema hash from metadata and validates the output chapter
before committing it.

### Complete And Reschedule

Complete and reschedule do not validate against the chapter schema unless they
write a visible chapter as part of the operation. If an implementation adds a
visible chapter for either operation, that chapter must be validated using the
job's schema.

## Cache Model

The daemon remains stateless with respect to job execution. A schema document
cache is allowed because schemas are immutable by hash.

Cache key:

```text
(tenantId, schemaHash)
```

Cache value:

- compiled JSON Schema validator.

Archive state is intentionally not part of the validation cache. Archive state
is checked only when selecting a schema for a new job. Existing job writes may
validate against an archived schema by hash.

## Public Go API

Implemented public types in `pkg/jobdb`:

```go
type JobSchemaSelector struct {
    Hash   string
    Schema json.RawMessage
}

type JobSchemaKey struct {
    TenantId   string
    SchemaHash string
}

type JobSchemaInfo struct {
    TenantId    string
    SchemaHash  string
    Schema      json.RawMessage
    State       JobSchemaState
    CreatedAt   time.Time
    ArchivedAt  *time.Time
}

type JobSchemaState string

const (
    JobSchemaStateActive   JobSchemaState = "ACTIVE"
    JobSchemaStateArchived JobSchemaState = "ARCHIVED"
)

type JobSchemaListState string

const (
    JobSchemaListStateActive   JobSchemaListState = "ACTIVE"
    JobSchemaListStateArchived JobSchemaListState = "ARCHIVED"
    JobSchemaListStateAll      JobSchemaListState = "ALL"
)

type RegisterJobSchemaRequest struct {
    TenantId string
    Schema   json.RawMessage
}

type ListJobSchemasRequest struct {
    TenantId string
    State    JobSchemaListState
}

type ListJobSchemasResponse struct {
    Schemas []JobSchemaInfo
}
```

Schema selection is available on job creation structs:

```go
type SubmitJob struct {
    // existing fields
    Schema *JobSchemaSelector
}

type SubmitRestartJob struct {
    // existing fields
    Schema *JobSchemaSelector
}
```

The registry interface is implemented by concrete runtimes and the remote
runtime:

```go
type JobSchemaRegistry interface {
    RegisterJobSchema(ctx context.Context, req RegisterJobSchemaRequest) (JobSchemaInfo, error)
    GetJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
    ListJobSchemas(ctx context.Context, req ListJobSchemasRequest) (ListJobSchemasResponse, error)
    ArchiveJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
}
```

`JobSchemaRegistry` remains separate from `WorkflowRuntime`. Complete runtimes
implement both interfaces; workflow-only fakes do not need schema-listing
methods.

## OpenAPI

Implemented in `openapi/jobdb-runtime.yaml` with generated bindings in
`pkg/jobdb/internal/runtimeapi/zz_generated.go`.

Reusable schemas:

```yaml
JobSchemaHash:
  type: string
  pattern: '^sha256:[0-9a-f]{64}$'

JobSchemaDocument:
  description: JSON Schema draft 2020-12 document used to validate visible JobDB chapter records.
  x-go-type: json.RawMessage

JobSchemaSelector:
  type: object
  additionalProperties: false
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'
    schema:
      $ref: '#/components/schemas/JobSchemaDocument'

JobSchemaState:
  type: string
  enum: [ACTIVE, ARCHIVED]

JobSchemaInfo:
  type: object
  additionalProperties: false
  required: [tenantId, schemaHash, schema, state, createdAt]
  properties:
    tenantId:
      type: string
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'
    schema:
      $ref: '#/components/schemas/JobSchemaDocument'
    state:
      $ref: '#/components/schemas/JobSchemaState'
    createdAt:
      type: string
      format: date-time
    archivedAt:
      oneOf:
        - type: string
          format: date-time
        - type: 'null'
```

Job creation schemas include:

```yaml
SubmitJob:
  properties:
    schema:
      $ref: '#/components/schemas/JobSchemaSelector'

SubmitRestartJob:
  properties:
    schema:
      $ref: '#/components/schemas/JobSchemaSelector'
```

Read models include:

```yaml
JobInfo:
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'

JobSummary:
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'

ExecutionLease:
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'
```

Schema endpoints:

```text
POST /v1/tenants/{tenantId}/schemas
GET  /v1/tenants/{tenantId}/schemas
GET  /v1/tenants/{tenantId}/schemas/{schemaHash}
POST /v1/tenants/{tenantId}/schemas/{schemaHash}/archive
```

Register request:

```yaml
RegisterJobSchemaRequest:
  type: object
  additionalProperties: false
  required: [schema]
  properties:
    schema:
      $ref: '#/components/schemas/JobSchemaDocument'
```

List supports a state filter:

```text
state=ACTIVE | ARCHIVED | ALL
```

Default list behavior is active-only.

## Storage

SQLite and direct/Postgres runtimes use a tenant-local schema table.

Logical columns:

```text
tenant_id
schema_hash
schema_json
state
created_at
archived_at
```

Primary key:

```text
(tenant_id, schema_hash)
```

The direct runtime stores this in JobDB-owned tables, not in pgwf's core queue
schema. The SQLite runtime owns the equivalent table in its local schema setup.

## Validation Errors

Schema validation failure uses a typed JobDB error that maps to HTTP `400`.
Stale/conflicting chapter writes continue to map to `409` only when the write
conflict is lease/ordinal related.

Implemented error:

```go
var ErrJobSchemaValidation = errors.New("job schema validation failed")
```

Current errors include the schema hash, chapter ordinal when available, and the
underlying validator message. A future structured payload could add:

- JSON pointer to the failing location;
- machine-readable validation keyword/detail.

## Implementation Record

Completed:

1. Added public schema types and schema errors in `pkg/jobdb`.
2. Added registry storage to toy, SQLite, and direct runtimes.
3. Added OpenAPI endpoints and regenerated `pkg/jobdb/internal/runtimeapi`.
4. Implemented remote client/server registry methods.
5. Added `SubmitJob` and `SubmitRestartJob` schema selectors.
6. Stored resolved schema hashes in the internal metadata envelope.
7. Added JSON Schema document compilation and compiled-validator caching.
8. Enforced validation on submit, restart submit, chapter append, and
   commit-if-waiting.
9. Exposed `schemaHash` in job and lease read models.
10. Added tests covering no-schema jobs, inline schema registration, archived
    schema behavior, invalid schema registration, remote error mapping, and
    chapter-zero/later-chapter branching.

Verification used during implementation:

```text
go test ./pkg/jobdb ./pkg/jobdb/runtime/remote ./pkg/jobdb/runtime/toy ./pkg/jobdb/runtime/toy/internal/toyimpl ./pkg/jobdb/runtime/sqlite ./pkg/jobdb/runtime/direct/internal/directimpl
```
