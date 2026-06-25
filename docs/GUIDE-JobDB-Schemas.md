# Guide: Using JobDB Schemas

JobDB schemas are optional validation contracts for visible job chapters. A job
can omit `Schema` and everything still works: no schema is resolved, no schema
hash is stored, and no schema validation runs.

Use schemas to validate job input/output shape, chapter body variants, metadata,
or artifact descriptors. Do not use schemas to control leasing, polling,
scheduling, retries, state transitions, or artifact storage.

## Schema Document

JobDB does not accept a raw JSON Schema as the schema document. Consumers send a
JobDB schema envelope:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "description": "Example tenant contract",
  "chapterShape": {},
  "firstChapterShape": {},
  "lastChapterShape": {}
}
```

`chapterShape` is required. It validates ordinary chapters. If
`firstChapterShape` is omitted, the first chapter uses `chapterShape`. If
`lastChapterShape` is omitted, the final completion chapter uses
`chapterShape`.

Each shape is a JSON Schema draft 2020-12 object or boolean schema fragment. It
validates the full visible `ChapterRecord` document, not only the application
payload:

```json
{
  "ordinal": 1,
  "createdAt": "2026-06-25T00:00:00Z",
  "taskType": "example-job",
  "body": {
    "kind": "taskAttemptOutcome",
    "outcome": {
      "kind": "success",
      "output": {
        "ok": true
      }
    }
  },
  "artifacts": []
}
```

JSON Schema object fields are open by default. Extra fields are allowed unless a
shape explicitly sets `additionalProperties: false` or
`unevaluatedProperties: false`. JobDB does not apply JSON Schema defaults and
never mutates stored chapters during validation.

## Chapter Roles

JobDB selects a shape by operation:

| Operation | Shape |
| --- | --- |
| `SubmitJob` initial chapter | `firstChapterShape`, falling back to `chapterShape` |
| Retained restart chapter with ordinal `0` | `firstChapterShape`, falling back to `chapterShape` |
| `PutChapter` | `chapterShape` |
| `CompleteTaskIfWaiting` output chapter | `chapterShape` |
| Retained restart chapter with ordinal greater than `0` | `chapterShape` |
| Restart extra chapter | `chapterShape` |
| `ExecutionLease.Complete` final chapter | `lastChapterShape`, falling back to `chapterShape` |

`lastChapterShape` is only for the final chapter supplied to complete the job.
It is not used for ordinary appends, even if the ordinary append has the
highest ordinal so far.

## Example Schema

This schema requires:

- chapter zero input to have `{ "kind": "valid" }`;
- ordinary task output to have `{ "ok": true }`;
- final completion output to have `{ "final": true }`.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "chapterShape": {
    "type": "object",
    "required": ["body"],
    "properties": {
      "body": {
        "type": "object",
        "required": ["kind", "outcome"],
        "properties": {
          "kind": { "const": "taskAttemptOutcome" },
          "outcome": {
            "type": "object",
            "required": ["kind", "output"],
            "properties": {
              "kind": { "const": "success" },
              "output": {
                "type": "object",
                "required": ["ok"],
                "properties": {
                  "ok": { "const": true }
                }
              }
            }
          }
        }
      }
    }
  },
  "firstChapterShape": {
    "type": "object",
    "required": ["ordinal", "body"],
    "properties": {
      "ordinal": { "const": 0 },
      "body": {
        "type": "object",
        "required": ["kind", "input"],
        "properties": {
          "kind": { "const": "jobStart" },
          "input": {
            "type": "object",
            "required": ["kind"],
            "properties": {
              "kind": { "const": "valid" }
            }
          }
        }
      }
    }
  },
  "lastChapterShape": {
    "type": "object",
    "required": ["body"],
    "properties": {
      "body": {
        "type": "object",
        "required": ["kind", "outcome"],
        "properties": {
          "kind": { "const": "jobAttemptOutcome" },
          "outcome": {
            "type": "object",
            "required": ["kind", "output"],
            "properties": {
              "kind": { "const": "success" },
              "output": {
                "type": "object",
                "required": ["final"],
                "properties": {
                  "final": { "const": true }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

## Register And Use

Concrete runtimes and the remote runtime implement `jobdb.JobSchemaRegistry`.

```go
registry, ok := rt.(jobdb.JobSchemaRegistry)
if !ok {
    return fmt.Errorf("runtime does not support job schemas")
}

schema := json.RawMessage(`{
  "chapterShape": {
    "type": "object",
    "required": ["body"]
  }
}`)

info, err := registry.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
    TenantId: "tenant-a",
    Schema:   schema,
})
if err != nil {
    return err
}
```

Attach a schema inline:

```go
handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
    Job: jobdb.SubmitJob{
        TenantId: "tenant-a",
        JobType:  "example-job",
        Data:     jobdb.NewTaskDataOrPanic(map[string]any{"kind": "valid"}),
        Schema:   &jobdb.JobSchemaSelector{Schema: schema},
    },
})
```

Or attach a registered active schema by hash:

```go
handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
    Job: jobdb.SubmitJob{
        TenantId: "tenant-a",
        JobType:  "example-job",
        Data:     jobdb.NewTaskDataOrPanic(map[string]any{"kind": "valid"}),
        Schema:   &jobdb.JobSchemaSelector{Hash: info.SchemaHash},
    },
})
```

If both `Hash` and `Schema` are supplied, JobDB computes the inline schema hash
and requires it to match `Hash`.

## Lifecycle

Registration canonicalizes the envelope, computes a `sha256:<hex>` hash,
compiles the schema fragments, and stores the canonical document for the tenant.
Registering the same schema for the same tenant is idempotent.

```go
got, err := registry.GetJobSchema(ctx, jobdb.JobSchemaKey{
    TenantId:   "tenant-a",
    SchemaHash: info.SchemaHash,
})

active, err := registry.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{
    TenantId: "tenant-a",
})

archived, err := registry.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{
    TenantId:   "tenant-a",
    SchemaHash: info.SchemaHash,
})
```

Archive is one-way. Archived schemas remain readable and remain valid for
already-created mutable jobs. New jobs cannot select an archived schema. There
is no delete operation.

## REST Shape

REST uses `schemaHash` instead of the Go field name `Hash`.

```http
POST /v1/tenants/tenant-a/schemas
Content-Type: application/json

{
  "schema": {
    "chapterShape": {
      "type": "object",
      "required": ["body"]
    }
  }
}
```

Submit with an inline schema:

```json
{
  "job": {
    "jobType": "example-job",
    "data": {
      "data": {
        "kind": "valid"
      },
      "artifacts": []
    },
    "schema": {
      "schema": {
        "chapterShape": {
          "type": "object",
          "required": ["body"]
        }
      }
    }
  }
}
```

Submit with a registered schema:

```json
{
  "job": {
    "jobType": "example-job",
    "data": {
      "data": {
        "kind": "valid"
      },
      "artifacts": []
    },
    "schema": {
      "schemaHash": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  }
}
```

Schema lifecycle endpoints:

```text
GET  /v1/tenants/tenant-a/schemas?state=ALL
GET  /v1/tenants/tenant-a/schemas/{schemaHash}
POST /v1/tenants/tenant-a/schemas/{schemaHash}/archive
```

## Errors

Use typed errors with `errors.Is`:

```go
switch {
case errors.Is(err, jobdb.ErrJobSchemaValidation):
    // The schema document is invalid or a chapter failed validation.
case errors.Is(err, jobdb.ErrJobSchemaArchived):
    // A new job tried to use an archived schema.
case errors.Is(err, jobdb.ErrJobSchemaNotFound):
    // The tenant/hash pair does not exist.
}
```

Schema validation failures map to HTTP `400` over the remote API. Lease and
ordinal conflicts remain conflict errors.

## Operational Notes

- Schema lifecycle is tenant-local, but compiled validators are cached by
  schema hash only. Identical schema bytes share one compiled validator across
  tenants.
- Schema documents are immutable by hash. Register a new schema to change a
  contract.
- The resolved schema hash is stored in JobDB internal job metadata and exposed
  on job and lease read models as `SchemaHash`.
- Remote chapter appends use the signed lease token's schema hash when present,
  so the append path does not need an extra job-row read just to discover the
  schema.
