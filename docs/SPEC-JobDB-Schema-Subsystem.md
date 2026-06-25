# Specification: JobDB Schema Subsystem

## Status

**Implemented design** | Author: Codex | Updated: 2026-06-25

JobDB supports optional, tenant-local schema validation for visible job
chapters. A job may omit a schema entirely; in that case JobDB performs no
schema resolution, stores no schema hash, and runs no schema validation.

## Decision

Schemas are validation contracts, not execution semantics. JobDB still owns
chapter ordering, leases, artifacts, scheduling, retries, and job state. The
schema subsystem validates the externally visible chapter record shape
associated with a job.

JobDB schema documents are JSON envelopes containing JSON Schema draft 2020-12
fragments:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "description": "Optional human description",
  "chapterShape": {},
  "firstChapterShape": {},
  "lastChapterShape": {}
}
```

`chapterShape` is required. `firstChapterShape` and `lastChapterShape` are
optional role-specific overrides. Raw JSON Schema documents without
`chapterShape` are rejected.

## Scope

A schema shape validates a visible `ChapterRecord` document. It may constrain:

- ordinal;
- chapter body variant;
- task type;
- chapter metadata;
- application input/output JSON;
- artifact descriptors;
- attempt and retry fields.

It does not control:

- lease ownership;
- polling behavior;
- scheduling behavior;
- artifact byte storage;
- whether a job is ready, waiting, completed, or cancelled.

JSON Schema object fields remain open by default. Extra fields are allowed
unless a shape explicitly sets `additionalProperties: false` or
`unevaluatedProperties: false`. JobDB does not apply JSON Schema defaults.

## Shape Selection

JobDB validates different write roles with different shapes:

| Write role | Shape |
| --- | --- |
| First chapter | `firstChapterShape`, falling back to `chapterShape` |
| Ordinary chapter | `chapterShape` |
| Last completion chapter | `lastChapterShape`, falling back to `chapterShape` |

First chapter validation applies to `SubmitJob` chapter `0` and to retained
restart chapter `0`.

Ordinary validation applies to `PutChapter`, `CompleteTaskIfWaiting`, retained
restart chapters with ordinal greater than `0`, and restart extra chapters.

Last validation applies only to the final `jobAttemptOutcome` chapter supplied
to `ExecutionLease.Complete`.

## Schema Identity

Schema identity is a SHA-256 content hash over canonical JSON for the full JobDB
schema envelope:

```text
schemaHash = "sha256:" + lowercase_hex(sha256(canonical_json(schema_envelope)))
```

The registry key is tenant-local:

```text
(tenantId, schemaHash)
```

The same schema content has the same hash in every tenant, but active/archive
state is scoped per tenant.

## Registry Lifecycle

Required operations:

- register schema;
- get schema by hash;
- list schemas for a tenant;
- archive schema.

There is no delete operation.

Register is idempotent. Registering the same schema for the same tenant returns
the existing row. Registering a schema whose hash exists with different content
is a conflict, even though SHA-256 collisions are not expected.

Archive is one-way. Archived schemas remain readable and usable by existing
mutable jobs that already reference them. They are rejected when selected for a
new job.

## Job Association

A job may specify:

- `schemaHash`: reference an existing active tenant schema;
- `schema`: inline JobDB schema envelope; or
- neither.

If `schema` is supplied, JobDB canonicalizes it, computes its hash, validates
and compiles the fragments, and performs tenant-local put-if-absent
registration. If both `schema` and `schemaHash` are supplied, the computed hash
must equal `schemaHash`.

When a job uses a schema, JobDB stores the resolved hash in immutable stored job
metadata:

```json
{
  "internal": {
    "schemaHash": "sha256:..."
  }
}
```

The metadata helper also recognizes historical root/app/internal spellings for
compatibility. Those spellings are not the public schema-selection API.

## Write Enforcement

Validation runs only when a job has a schema hash.

On `SubmitJob`, JobDB resolves the schema, rejects unknown or archived schema
references, stores the resolved hash, and validates chapter `0` with the first
shape before committing the job.

On `SubmitRestartJob`, the new job uses only the provided schema selector. If no
selector is supplied, the restarted job has no schema. Retained chapter `0`
validates as first, retained non-zero chapters validate as ordinary, and any
new restart extra chapter validates as ordinary.

On `PutChapter`, remote append paths use the trusted schema hash carried in the
signed lease token when available. This avoids a job-row read solely to discover
the schema hash. The compiled validator is loaded from a hash-keyed cache backed
by the schema registry, then the chapter validates as ordinary before storage.

On `CompleteTaskIfWaiting`, the implementation already reads/locks wait state.
It obtains the job schema hash from stored metadata during that path and
validates the committed output chapter as ordinary.

On `ExecutionLease.Complete`, the request must include the final visible
`jobAttemptOutcome` chapter. That final chapter validates as last before the
job is completed.

Archived schema state is ignored during existing-job writes. Archive only
blocks new job selection.

## Cache Model

The daemon remains stateless with respect to job execution. A compiled schema
cache is allowed because schema documents are immutable by hash.

Cache key:

```text
schemaHash
```

Cache value:

- compiled `chapterShape`;
- optional compiled `firstChapterShape`;
- optional compiled `lastChapterShape`.

Tenant ID is intentionally not part of the compiled-validator cache key. The
hash guarantees schema bytes, while lifecycle state remains tenant-local.

## Public Go API

Schema selection is represented by:

```go
type JobSchemaSelector struct {
    Hash   string
    Schema json.RawMessage
}
```

`Schema` is the JobDB schema envelope. It is intentionally raw JSON in the Go
API because the fragments are JSON Schema documents.

Registry operations are exposed through:

```go
type JobSchemaRegistry interface {
    RegisterJobSchema(ctx context.Context, req RegisterJobSchemaRequest) (JobSchemaInfo, error)
    GetJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
    ListJobSchemas(ctx context.Context, req ListJobSchemasRequest) (ListJobSchemasResponse, error)
    ArchiveJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
}
```

Complete runtimes implement both `WorkflowRuntime` and `JobSchemaRegistry`.

## OpenAPI

The wire contract is implemented in `openapi/jobdb-runtime.yaml` and generated
into `pkg/jobdb/internal/runtimeapi/zz_generated.go`.

Reusable schemas:

```yaml
JobSchemaHash:
  type: string
  pattern: '^sha256:[0-9a-f]{64}$'

JobSchemaDocument:
  type: object
  required: [chapterShape]
  additionalProperties: false
  description: JobDB schema envelope. Each shape is a JSON Schema draft 2020-12 schema fragment that validates a visible chapter record.
  x-go-type: json.RawMessage
  properties:
    $schema:
      type: string
    description:
      type: string
    chapterShape:
      $ref: '#/components/schemas/JsonSchemaFragment'
    firstChapterShape:
      $ref: '#/components/schemas/JsonSchemaFragment'
    lastChapterShape:
      $ref: '#/components/schemas/JsonSchemaFragment'

JsonSchemaFragment:
  description: JSON Schema draft 2020-12 object or boolean schema fragment.
  x-go-type: json.RawMessage
```

Schema endpoints:

```text
POST /v1/tenants/{tenantId}/schemas
GET  /v1/tenants/{tenantId}/schemas
GET  /v1/tenants/{tenantId}/schemas/{schemaHash}
POST /v1/tenants/{tenantId}/schemas/{schemaHash}/archive
```

Job creation schemas include `schema` selectors with either `schemaHash` or
inline `schema`.

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

Toy runtime stores the same logical records in memory.

## Errors

Schema validation failure uses:

```go
var ErrJobSchemaValidation = errors.New("job schema validation failed")
```

Unknown references use `ErrJobSchemaNotFound`. New job selection of archived
schemas uses `ErrJobSchemaArchived`.

Current validation errors include the schema hash, role, chapter ordinal when
available, and the underlying validator message. Remote schema validation errors
map to HTTP `400`.

## Implementation Status

Implemented:

1. Public schema model and typed schema errors in `pkg/jobdb`.
2. Registry storage in toy, SQLite, direct, and remote runtimes.
3. OpenAPI schema endpoints and generated runtime bindings.
4. `SubmitJob` and `SubmitRestartJob` schema selectors.
5. Tenant-local inline schema registration with put-if-absent semantics.
6. Resolved schema hashes stored in internal job metadata.
7. Schema hash exposure on job and lease read models.
8. Remote lease tokens carrying trusted job, lease, worker, and schema claims.
9. Structured schema envelope parsing with first/ordinary/last role selection.
10. Validation on submit, restart, chapter append, task commit, and lease
    completion final chapter.
11. Process-local compiled-validator cache keyed by schema hash.

Intentional gaps:

- no schema delete operation;
- no schema defaults;
- no extension-operation schemas;
- no structured JSON-pointer validation report;
- no schema-list pagination;
- no schema-specific job query indexes beyond exposed `schemaHash`.
