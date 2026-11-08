# Canonical lease history

`jobdb.ListLeaseEvents(ctx, runtime, req)` queries the optional `LeaseEventReader`
capability and returns `ListLeaseEventsResponse{Events, NextPageToken}` for exactly
one tenant. `WorkflowRuntime` and `runtimecore.Scheduler` remain unchanged for
legacy implementations. Existing concrete `ListLeaseEvents` methods remain
available. A wrapper may explicitly forward `LeaseEventReader` to expose it.
`POST /v1/tenants/{tenantId}/lease-events/query` is the corresponding read-only
remote operation. Delegated credentials cannot use it.

Supplied SQLite and toy runtimes record events automatically, including ordinary
CLI startup. No activation flag is required. Legacy/custom backends may omit the
capability and return `ErrLeaseEventsUnsupported`
(check with `errors.Is`), never a successful empty history. An authorized HTTP
request returns 501 with code `lease_events_unsupported`. New clients also map
unrecognized old-server endpoint 404/405 responses to that sentinel, preserving
recognized domain errors and authentication/backend/transport failures.

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

## Compatibility and automatic upgrades

SQLite retains base format 11. Ordinary startup automatically installs the event
table and indexes in one transaction without changing that marker. Repeated
startup preserves existing records and signing keys without synthetic backfill.
Databases already marked
12 by ea45f87 are checked against the known base and event definitions, then
transactionally normalized to 11 without replacing jobs, cells, goals, leases,
chapters, events or signing keys. Unknown formats and inconsistent schemas are
rejected without changing their data or marker. This extension does not introduce
support for unrelated older base formats.

Authentic pre-event format-11 code (f92ae253a5da7a6b81944d0bdf4cadb085772140)
accepts the additive schema and can continue legacy reads and writes. Mixed old/new
writers and pre-upgrade activity produce **partial history**: old writers do not record
events. Existing acquisitions are never backfilled. A pre-upgrade lease may have
only renewal events, and empty history cannot establish that a job never ran.
Toy retains history only for its engine lifetime.

The oldest event is not proof of complete history. Startup and source acceptance
do not require deployment coordination or downstream rebuilds. Consumers adopt the optional feature
independently. Callers holding `WorkflowRuntime` use the public helper or an
explicit optional-interface assertion; concrete-method callers remain compatible.

Event inserts remain atomic with lease mutations: insertion failure rolls back
acquisition or renewal. Failed schema installation rolls back completely and can
be retried after correcting the error; saved data must never be reset to upgrade.

## Backend integration

A scheduler may expose the standalone `jobdb.LeaseEventReader` signature:

```go
ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error)
```

Core validates/authorizes before optional dispatch and forwards this call; it never reconstructs events.
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
the optional public port strictly: missing support fails. General
`RunWorkflowRuntimeConformance` probes with authorized credentials and skips only
absent or unsupported capabilities; other errors fail. Supplied default runtimes pass strict event conformance. Deployment authentication must be tested at the server boundary, since
client context values are not transmitted as authority. Backend-specific tests
must cover insertion rollback, upgrade/reopen, query budgets/plans, storage
precision and concurrency in addition to this public suite.
