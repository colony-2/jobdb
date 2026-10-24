# Lease delegation tokens

Status: Implemented in the runtime HTTP API and Go remote client. c2j adoption
requires updating its JobDB dependency and following the
[integration guide](GUIDE-Lease-Delegation-Tokens.md). This document supersedes the
initial proposal's separate refresh credential and server-side grant records.

A delegation is still a lease: it uses the same `leaseToken`,
`X-JobDB-Lease-Token` header, lease identity, `ExecutionLease` interface, and
version-1 `LeaseCapability` import/export format. Its authority is a signed,
immutable subset of the parent execution lease's authority. There is no
individual delegation revocation. Holders may derive narrower children locally.
See the [implemented attenuation design](DESIGN-Offline-Lease-Delegation.md) for
the token profile and local derivation rules.

## Authority and lifetime

Owners receive a signed `jdb-o1` bundle containing a root macaroon. The Go
`CreateDelegation` method clones and attenuates it locally, returning a `jdb-m1`
child. No HTTP request or server signing key is needed for derivation. The child
contains no owner token or reusable ancestor MAC.

Authenticated grant contents include parent identity, principal/schema binding,
service audience, key ID, root identity and every permission/deadline restriction.
Every descendant must retain or narrow the effective permissions and deadline.
Omitting a deadline inherits the ancestor's deadline; an explicit later deadline
is rejected. Creation checks local syntax and narrowing; server-side parent
liveness is checked at import/use.

Every resource operation and renewal verifies the signature, path binding and
current backend parent identity/liveness. Cancellation, completion, rescheduling,
expiry, or replacement of the parent invalidates the child. Reacquiring the same
job does not revive an old delegation. An unavailable backend fails closed.
Ordinary parent-token rotation does not change the parent execution lease's ID,
so it does not invalidate the child.

The original owner's bearer-token expiration is not the parent's authoritative
lifetime. The owner may have refreshed that token and extended the same backend
lease. The child needs only its own token; it never contacts the owner process or
uses an owner credential.

The public contract is **the selected permissions remain usable until `notAfter`,
provided the exact parent lease remains live**. Without `notAfter`, the parent
lease's lifetime is the only lifetime bound. Equality with the deadline is
expired. Neither credential refresh nor owner renewal can move that deadline.
There is no renewal permission and no caller-selected access TTL.

| Hard deadline | Child authority |
|---|---|
| Absent | Valid while the exact parent remains live |
| Future | Valid while the exact parent remains live and the deadline is not reached |
| Reached | Resource operations and credential refresh are denied |

A child has no separate access-token expiry. Requests verify the macaroon and
current backend parent directly. No token-refresh cache, mutation replay or
per-child heartbeat is needed. `KeepAlive` and `Renew` validate the grant and
return the same token with an updated snapshot, never extending the parent.
The `expiresAt`/`LeaseExpiry()` snapshot is current parent expiry capped by
`notAfter`; it may become stale when the owner extends the parent. Operations
still reach JobDB for authoritative validation. `delegation.notAfter` is the
immutable child authority deadline.

Old token copies remain usable until parent loss or the hard deadline. Snapshot
refresh does not revoke copies. Signing keys must be unique to a trust domain
and shared across replicas. `LeaseTokenAudience` additionally separates services
(default `jobdb`). There is no grant database or extra per-lease storage.

## Permissions

The `permissions` object is an allowlist. Unknown fields are rejected. Omitted
permissions authorize nothing. Selectors compare exact case-sensitive values;
there are no implicit wildcards or namespace prefixes. Names lists have at most
256 items; tokens are limited to 16 KiB.

| Permission | Selectors and effects |
|---|---|
| `jobs.submit` | `jobTypes` and `jobIds`, each `{ "names": [...] }` or `{ "any": true }`. Allows lease-scoped child submission in the parent's tenant. Generated IDs require `jobIds.any`. Explicit IDs must match. No restart submission or inline schema registration; existing schema hashes may be selected. |
| `secrets.resolve` | Array of exact secret names. Resolve the intersection of requested names, grant names, and provider authorization. Denied or unknown names are omitted. An empty intersection returns `{ "secrets": {} }` without invoking the provider. An absent/empty permission denies the operation. Values retain the existing provider lifetime and disclosure checks. |
| `chapters.read` | `{ "ordinals": [...], "ranges": [{ "start": 0, "end": 4 }] }` or `{ "any": true }`. Ranges are inclusive. A single unauthorized ordinal is denied; lists return only authorized chapters. Includes body, metadata and stored artifact descriptors, never artifact bytes. |
| `chapters.write` | Exact `ordinals`, `taskTypes` name selector, and `artifactNames` name selector. Only task outcome chapters (`TaskAttemptOutcome`) are writable. An empty artifact selector permits no uploads. Existing artifact references without fresh uploads are denied. Writes retain ordinary schema, lease and conflict checks. |

An empty name/chapter selector matches nothing. `any` cannot be combined with
finite values for the same selector. There are no permissions for parent
keepalive, completion, rescheduling, configuration reads/writes, restart jobs,
client-payload updates on the parent, schema administration, artifact download,
or job acquisition. Further attenuation needs no additional permission, but
never grants access to the owner-only REST creation endpoint.

Chapter writes cannot write job-start, job-final, restart-extra or execution
control chapters. Authorizing a task outcome nevertheless authorizes its effect
on consumers of that chapter; grant only the ordinals the child should produce.
Submission remains an ordinary job submission, including its input, metadata,
prerequisites and child payload, subject to the selected type and ID.

## REST API

The complete wire contract is in
[`openapi/jobdb-runtime.yaml`](../openapi/jobdb-runtime.yaml).
All paths below start with
`/v1/tenants/{tenantId}/jobs/{jobId}/leases/{leaseId}` and use
`X-JobDB-Lease-Token`.

| Method and suffix | Behavior |
|---|---|
| `POST /delegations` | Owner lease creates a restricted macaroon for direct HTTP clients; the Go SDK uses local derivation. Accepts `permissions` and optional `notAfter`. Returns an `ExecutionLease` with `delegation` metadata. |
| `GET` (no suffix) | Inspect/import a live delegation without renewing. Returns identity, route, expiry and policy, with empty execution state/client payload. Does not expose the owner's execution snapshot. |
| `POST /keepalive` | Existing endpoint: owner tokens extend the parent as before; delegation tokens validate parent liveness and the deadline, then return the same token and a fresh restricted snapshot. |
| `POST /jobs` or `PUT /jobs/{childJobId}` | Existing submission endpoints, with delegated type/ID restrictions. |
| `POST /secrets/resolve` | Existing endpoint with delegated name intersection. |
| `POST /add_chapter` | Existing endpoint with delegated ordinal/type/upload restrictions. |
| `GET /chapters?startOrdinal=0&endOrdinal=4` | Lease-scoped chapter listing; filters unauthorized ordinals. |
| `GET /chapters/{ordinal}` | Lease-scoped chapter read. |

Example creation body:

```json
{
  "notAfter": "2026-10-23T20:00:00Z",
  "permissions": {
    "jobs.submit": {
      "jobTypes": {"names": ["summarize"]},
      "jobIds": {"any": true}
    },
    "secrets.resolve": ["model_api_key"],
    "chapters.read": {"ranges": [{"start": 0, "end": 6}]},
    "chapters.write": {
      "ordinals": [7],
      "taskTypes": {"names": ["summarize"]},
      "artifactNames": {"names": ["summary.json"]}
    }
  }
}
```

Responses containing credentials and lease-scoped responses use
`Cache-Control: no-store`. The API never returns the parent's token.

Delegation authorization occurs before routing and before operation side effects,
including inline schema registration and artifact storage. A delegation header
on an unscoped, admin, dev, or lifecycle route is rejected. This is a capability
restriction inside the existing deployment authentication boundary: an isolated
worker must not also have credentials/network access that authorize unscoped
APIs without a lease. `NewDelegationServerWithOptions` provides a separate worker
ingress that requires a delegated token on every request, including unscoped
routes, and rejects owner tokens. It shares the backend and signing key with the
trusted owner handler. Embedded Go runtimes remain trusted in-process APIs.

## Errors and lease inspection

Delegation errors use `{ "code": "...", "message": "..." }` with sanitized,
credential-free values.

| HTTP/code | Client behavior |
|---|---|
| `401 invalid_delegation_credential` | Signature/identity failure; stop. |
| `403 lease_permission_denied` | Operation/selector disallowed; stop. |
| `403 delegation_deadline_reached` | Hard deadline elapsed; stop. |
| `409 execution_lease_lost` | Parent lost or lease/deadline validity lost during execution; stop. |
| `400 invalid_delegation_request` | Invalid delegated operation body; correct the request. |
| `501 delegation_not_supported` | Backend lacks read-only lease validation. |
| `503 delegation_unavailable` | Parent validation unavailable; no operation dispatched. |

The client does not replay delegated operations automatically. Normal handling
of ambiguous network results and mutation idempotency still applies. Delegated,
secret and configuration requests never follow redirects.

Importing a child inspects the grant against the live parent without renewing
it. `KeepAlive` and `Renew` preserve every restriction and never extend the parent
or child deadline. Even an empty permission allowlist may inspect its own lease
or call keepalive; it still permits no resource operations.

## Implementation and rollout

The built-in core, SQLite and toy backends expose read-only
`ValidateExecutionLeaseByID`. Third-party runtime wrappers must forward this
method (or embed an implementation); unsupported backends fail closed.
The toy backend's child submission validation now uses this read-only method
instead of implicitly extending the parent.

Update the server and Go SDK together. Previous owner and delegation token
formats are rejected; there is no compatibility or conversion path. Existing
public lease operations, capability envelopes and delegation call sites remain.
No database migration or additional server records are needed. c2j integration
is described in the [guide](GUIDE-Lease-Delegation-Tokens.md).

Tests cover local derivation with networking disabled, multilevel attenuation,
server rejection of wider caveats, wire vectors, malformed/tampered chains,
deadline inheritance, parent loss/rotation, secret and chapter restrictions,
concurrent derivation and isolated child processes against worker-only ingress.

## Prior art

The [attenuation design](DESIGN-Offline-Lease-Delegation.md#cryptographic-construction-and-prior-art)
compares macaroons, Biscuit and UCAN. JobDB uses first-party macaroons with typed
permission selectors and authoritative backend lease checks. This is a bearer
capability protocol, not an OAuth implementation.

## Workflow task issuance

`workflow.TaskContext.CreateDelegation(ctx, request)` uses the current execution
lease and caps `NotAfter` to the earliest task-attempt, task-total, enclosing
scope/job, execution-context, parent-capability, or requested deadline. Absent
bounds are ignored. Repeated issuance preserves the engine's absolute budget,
including time already spent preparing the task. A later requested deadline is
capped; permissions still must fit the parent's allowlist.

The method's call context only bounds issuance. The renewable owner's expiry
snapshot does not become a child deadline. Existing retry/recovery policies
select the current attempt's budget; this API does not change timers,
cancellation causes, history, or timeout classification. Closing a task prevents
further issuance but does not revoke already issued children individually.

Issuance uses optional runtime capability `jobdb.LeaseDelegator`; the remote
implementation attenuates locally. Unsupported runtimes and manually constructed
task contexts return `jobdb.ErrLeaseDelegationUnsupported`. Replay cannot issue
credentials. No HTTP or storage changes are required.

The [c2j integration guide](GUIDE-C2J-Task-Delegation.md) describes the owner/child
process boundary and required client integration.
