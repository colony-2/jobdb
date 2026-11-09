# Mandatory lease history

`WorkflowRuntime` and `runtimecore.Scheduler` now embed `jobdb.LeaseEventReader`:

```go
ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error)
```

This intentionally breaks compilation for implementations that omit the method.
Runtime and scheduler owners must implement actual retained-history reads; wrapper
owners must forward requests, normalized filters, pagination, results and errors.
Tenant wrappers must translate request and response tenant IDs consistently.
An empty-success or unsupported stub is not an implementation of this contract.
The narrow reader, helper, request/response types and exported errors remain.

Upgrade custom adapters and consumers together. There is no mixed-version server
compatibility promise: old HTTP endpoint failures return explicit errors rather
than optional-support fallback. Structured validation and permission errors keep
their existing meaning. Storage formats, HTTP payloads and renewal semantics do
not change, and this correction requires no database migration.

JobDB supplies its adapters and shared interfaces. pgjobdb owns its dependency
pin, scheduler and wrappers, and native/HTTP PostgreSQL conformance using its
independent native validity hook after the published dependency is delivered.
Source acceptance and publication do not wait for that deployment.
