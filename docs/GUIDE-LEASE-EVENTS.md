# Canonical lease history

`WorkflowRuntime.ListLeaseEvents(context.Context, ListLeaseEventsRequest)` returns
`ListLeaseEventsResponse{Events, NextPageToken}` for exactly one tenant. It is a
mandatory runtime and `runtimecore.Scheduler` port; rebuild adapters and consumers
together. `POST /v1/tenants/{tenantId}/lease-events/query` is the corresponding
read-only remote operation. Delegated credentials cannot use it.

Events are immutable historical snapshots. `lease_acquired` includes its source
(`poll`, `targeted`, or `task_completion`) and an optional replaced expired lease.
`lease_renewed` includes the previous stored expiry; a shorter accepted renewal is
valid. Event and lease mutations share one writer transaction or engine lock.
Reads, token-only refresh, completion, cancellation and rescheduling do not append
events. Events contain no credentials. Retrying a committed renewal can append
another valid renewal; this is mutation history, not request deduplication.

Arrays combine OR within a dimension and AND across dimensions. Original job type
and exact leased route are independent dimensions. Omitted arrays are unrestricted;
explicit null/empty arrays and arrays over 100 entries before deduplication are
invalid. Current cell names/aliases resolve stable identities, including retired
cells; returned cell names and repositories remain historical. Goal filters include
current additional memberships without changing an event's primary goal. Hidden
jobs require `IncludeHidden`; completed/cancelled history remains available.

Pages default to 100 and accept 1–200. Time bounds are inclusive lower and exclusive
upper. Events are ordered by occurrence descending then numeric sequence descending.
The signed continuation binds normalized filters, tenant, principal/authorized
scope, resolved cells, page size, hidden selection and time bounds. Reauthorize
every page. Invalid or mismatched cursors return `ErrInvalidLeaseEventPageToken`,
separately from `ErrInvalidLeaseEventRequest`. An empty page has `events: []`, no
total and no terminal token.

Pagination observes live statement snapshots. Concurrent commits, goal membership
changes and visibility changes can affect later pages. Refresh the first page to
find newer activity. Traversal is exact for an unchanged dataset, including tied
timestamps; it is not a gap-free export or replication cursor.

SQLite format 12 upgrades format 11 transactionally without replacing jobs, cells,
goals, leases or chapters. Reapplying installation is harmless. No old acquisitions
or diagnostic events are backfilled, so a pre-upgrade lease may have only renewal
events. Toy retains history only for its engine lifetime.

Stop old writers, upgrade storage and deploy all compatible writers before recording
collection activation operationally. Do not infer activation from the oldest event.
This library change does not activate collection in a remote deployment.

## Compatibility and upgrades

This change is not fully backward compatible. Compatibility depends on the boundary:

| Boundary | Effect |
| --- | --- |
| Go implementations | Adding the required `ListLeaseEvents` method to `WorkflowRuntime` and `runtimecore.Scheduler` is a source-breaking interface change. Custom backends, wrappers, mocks and adapters must implement it and rebuild with their dependents. Callers using existing methods on the supplied implementations need no call-site changes. |
| Existing HTTP operations | The query endpoint is additive; existing job detail/list payloads are unchanged. Existing clients can continue using those operations on an upgraded server. |
| New clients with old servers | Older servers do not provide the event endpoint. Treat that failure as an explicit unavailable/incompatible error, never as empty history. There is no optional-port or silent fallback. |
| SQLite data | The supported forward upgrade is additive format 11 to 12 and preserves existing data. Older formats are not covered by this migration. The prior format-11 binary rejects format 12; no downgrade migration is supplied. |
| Writers and recording | Mixed old/new writers are not a supported complete-history rollout: old writers do not record canonical events. Stop old writers, upgrade storage, deploy compatible writers and restart them before declaring collection active. |

Existing acquisitions are not backfilled. A lease acquired before upgrade may have
only renewal events; an empty history does not establish that a job never ran.
Canonical recording also adds work and storage to successful lease mutations, and
an event insertion failure now fails the mutation atomically. Plan capacity for
retained renewals and indexes; no automatic retention expiry is introduced.

These are the agreed compatibility boundaries. Making the new method optional or
supporting mixed-version recording would require a separate contract change.

## Backend integration

The exact required scheduler signature is:

```go
ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error)
```

Core validates/authorizes and forwards this call; it never reconstructs events.
Backend implementations must also validate and authorize their native entry point.
`jobdb.NormalizeLeaseEventRequest(req)` returns a validated canonical copy. Public
`runtime/cellstate` helpers support backend-independent scope and cursors:

```go
(*State).LeaseEventScope(ctx, req) ([]string, error)
LeaseEventBinding(ctx, req, resolvedIDs) string
DecodeLeaseEventCursor(key, binding, token) (*LeaseEventPosition, error)
EncodeLeaseEventCursor(key, binding, LeaseEventPosition{Time: instant, ID: sequence}) string
LeaseEventPage(orderedLookaheadRows, pageSize, key, binding) jobdb.ListLeaseEventsResponse
```

Load the persisted tenant catalog key and minimal cell/alias facts in one batched
read, then execute one set-based event query with all predicates before LIMIT.
Scope helpers only inspect supplied in-memory facts/authority. Never generate a
cursor key on an event read. Use `jobdb-lease-events-v1` domain separation. Native
Postgres bounds must round upward to microsecond precision before comparison,
while binding the original normalized bounds. Persist events inside all scheduler
mutation paths, including borrowed transactions, internal claims and direct
renewal handles. Canonical writes must work with diagnostic tracing disabled.

`runtimetest.RunLeaseEventConformance(t, harnesses ...runtimetest.Harness)` exercises
the public port and also runs from `RunWorkflowRuntimeConformance`. Supply fresh
fixtures. Deployment authentication must be tested at the server boundary, since
client context values are not transmitted as authority. Backend-specific tests
must cover insertion rollback, upgrade/reopen, query budgets/plans, storage
precision and concurrency in addition to this public suite.
