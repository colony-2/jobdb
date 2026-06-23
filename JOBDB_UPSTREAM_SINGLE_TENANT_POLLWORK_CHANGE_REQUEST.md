# Change Request: Make `pollWork` Singular-Tenant

## Summary

Make `pollWork` explicitly single-tenant by replacing the optional
`tenantIds: string[]` request field with a required `tenantId: string` field.

This is an intentional breaking change. Tenant-less polling, multi-tenant
polling, and one-item-array compatibility should be removed from the upstream
runtime contract.

## Context

The Cloudflare JobDB runtime worker maps JobDB tenants directly to `jobs` project
ids. The underlying scheduler is project-scoped, so efficient polling needs a
specific tenant/project target.

The current upstream OpenAPI contract still models `pollWork` as an optional
single-item tenant array:

```yaml
PollWorkRequest:
  required: [workerId, capabilities, limit]
  properties:
    tenantIds:
      type: array
      description: Optional single-tenant filter for polling. When omitted, polling spans all tenants visible to the runtime.
      minItems: 1
      maxItems: 1
```

That wire shape is awkward for a single-tenant-only contract and still implies
tenant-less or cross-tenant polling may exist. Since breaking changes are
acceptable, the API should move to the shape it actually wants to support.

## New Runtime Contract

`pollWork` requires exactly one non-empty tenant id as `tenantId`.

Conforming runtimes must apply these rules:

- Missing `tenantId`: reject with `400`.
- Empty `tenantId`: reject with `400`.
- Legacy `tenantIds`: reject with `400` as an unknown/unsupported property.
- Any tenant-less/global polling behavior: not supported.
- Exactly one non-empty `tenantId`: poll only that tenant.

The runtime must not infer a tenant from previously observed jobs, worker state,
known tenants, or ambient configuration.

## OpenAPI Change

In `/jobdb/openapi/jobdb-runtime.yaml`, change `PollWorkRequest` to:

```yaml
PollWorkRequest:
  type: object
  additionalProperties: false
  required: [tenantId, workerId, capabilities, limit]
  properties:
    tenantId:
      type: string
      minLength: 1
      description: Required tenant polling target. Polling is scoped only to this tenant.
    workerId:
      type: string
    capabilities:
      type: array
      items:
        type: string
    limit:
      type: integer
    longPollUntil:
      type: string
      format: date-time
    leaseDuration:
      type: string
      description: Duration string controlling how long acquired leases should be held.
    metadataEquals:
      type: array
      description: |
        Metadata equality predicates applied during polling. Multiple
        predicates mean AND across fields; multiple `values` on one
        predicate mean OR on the same field.
      items:
        $ref: '#/components/schemas/MetadataPredicate'
```

Remove `tenantIds` from this schema entirely.

Do not rely on the schema update alone for runtime behavior. The generated Go
server decoder may not reject unknown JSON properties or missing required fields
without explicit validation. The HTTP handler must still validate that
`tenantId` is present, non-empty, and that legacy `tenantIds` is not accepted.

## Go API Change

Change the public runtime request type from:

```go
type PollWorkRequest struct {
    TenantIds []string
    ...
}
```

to:

```go
type PollWorkRequest struct {
    TenantId string
    ...
}
```

All Go runtime implementations should validate `TenantId != ""` before
polling. The direct, SQLite, toy, and remote runtimes should no longer support
zero-tenant polling or multi-tenant fanout.

The generated OpenAPI type should become:

```go
type PollWorkRequest struct {
    TenantId string `json:"tenantId"`
    ...
}
```

No pointer and no `omitempty` should be generated for `TenantId`.

## TypeScript API Change

Regenerate TypeScript API types from the updated OpenAPI schema. The generated
request type should expose:

```ts
tenantId: string
```

and should not expose `tenantIds?: string[]`.

Update all TypeScript runtime/client code that calls `pollWork` to pass
`tenantId`.

## Implementation Plan

1. Update `openapi/jobdb-runtime.yaml` so `PollWorkRequest` requires `tenantId`
   and removes `tenantIds`.
2. Regenerate Go OpenAPI bindings under `pkg/jobdb/internal/runtimeapi`.
3. Regenerate downstream TypeScript API bindings.
4. Update `jobdb.PollWorkRequest` to use `TenantId string`.
5. Update all Go callers and runtime implementations:
   - Direct runtime: pass one project/tenant to the scheduler.
   - SQLite runtime: filter polling by the required tenant.
   - Toy runtime: filter polling by the required tenant.
   - Remote runtime client: always serialize `tenantId`.
   - Remote runtime server: read `tenantId` and reject missing/empty values.
6. Remove remote-client known-tenant fanout and startup no-op behavior for
   omitted tenant polling.
7. Update the worker engine contract. Since the worker loop currently has no
   tenant argument, add an explicit tenant source before it calls `PollWork`.
   Recommended shape:
   - Add `PollTenantId string` or `TenantId string` to `RuntimeBuildOptions`.
   - Add a builder method such as `WithWorkerTenantId(tenantId string)`.
   - Require this option when workers are registered and `Run` may poll work.
   - Pass `TenantId` on every worker-loop `PollWork` call.
8. Update docs and examples so workers are constructed with an explicit polling
   tenant.

## Test Plan

Add or update tests at both HTTP-boundary and runtime-interface levels.

Required rejection tests:

- JSON body missing `tenantId` returns `400`.
- JSON body with `tenantId: ""` returns `400`.
- JSON body with legacy `tenantIds: ["tenant-a"]` returns `400`.
- Go runtime `PollWork` with `TenantId == ""` returns an error.

Required positive tests:

- `pollWork` with `tenantId: "tenant-a"` leases only tenant-a work.
- Work in tenant-b is not leased by a tenant-a poll, even with matching
  capabilities.
- Remote runtime client serializes `tenantId` as a string.
- Worker engine passes its configured tenant id on every poll.

Regression cleanup:

- Delete or rewrite tests that assert tenant-less polling works.
- Delete or rewrite tests that assert remote-client known-tenant fanout works.
- Ensure runtime conformance, remote-runtime conformance, and usage parity
  suites all use explicit tenant ids when polling.

## Compatibility Notes

This is a breaking wire and source API change.

Existing HTTP clients must change:

```json
{
  "tenantIds": ["tenant-a"],
  "workerId": "worker-1",
  "capabilities": ["job-a"],
  "limit": 1
}
```

to:

```json
{
  "tenantId": "tenant-a",
  "workerId": "worker-1",
  "capabilities": ["job-a"],
  "limit": 1
}
```

Existing Go clients must change:

```go
jobdb.PollWorkRequest{TenantIds: []string{"tenant-a"}}
```

to:

```go
jobdb.PollWorkRequest{TenantId: "tenant-a"}
```

Clients that omitted tenant identity must choose a tenant before polling. There
is no upstream-supported replacement for global polling in this contract.

## Acceptance Criteria

- OpenAPI exposes required `tenantId: string` on `PollWorkRequest`.
- OpenAPI no longer exposes `tenantIds` on `PollWorkRequest`.
- Generated Go API type uses required `TenantId string`.
- Generated TypeScript API type uses required `tenantId: string`.
- Public Go `jobdb.PollWorkRequest` uses `TenantId string`.
- Built-in runtimes reject empty tenant ids and poll only the requested tenant.
- Remote client no longer implements omitted-tenant known-tenant fanout.
- Worker engine has an explicit configured poll tenant and passes it to
  `PollWork`.
- HTTP-boundary tests cover missing, empty, and legacy-array rejection.
- Runtime conformance, remote-runtime conformance, and usage parity suites pass
  with explicit tenant polling.
