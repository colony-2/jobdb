# Lease delegation tokens

Status: Implemented in the runtime HTTP API and Go remote client. c2j adoption
requires updating its JobDB dependency and following the
[integration guide](GUIDE-Lease-Delegation-Tokens.md). This document supersedes the
initial proposal's separate refresh credential and server-side grant records.

A delegation is still a lease: it uses the same `leaseToken`,
`X-JobDB-Lease-Token` header, lease identity, `ExecutionLease` interface, and
version-1 `LeaseCapability` import/export format. Its authority is a signed,
immutable subset of the parent execution lease's authority. There is no
individual delegation revocation or further delegation in this release.

## Authority and lifetime

The server issues a `jdb-d1.<payload>.<signature>` token. The signature is
HMAC-SHA256 over the prefix and encoded payload, using the configured lease
signing key. This follows the existing JobDB signing mechanism; it is not JWT.
Old owner-token parsers reject its distinct format instead of treating it as an
unrestricted lease.

Signed claims contain:

- Parent tenant, job, execution lease ID, worker, schema and principal.
- Delegation ID and complete permission selectors.
- Optional absolute `notAfter` hard deadline.
- Access TTL, access expiration, and parent route.

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

Access TTL defaults to 30 seconds and is limited to 1–300 seconds. Every issuance
is bounded by the current backend parent expiry and, when present, `notAfter`.
A nonrenewable child therefore expires at the earliest of those bounds. Setting
a distant hard deadline does not override the access TTL.

**A child can renew only when it has `delegation.renew` AND has no hard deadline.**
The exact parent must also still be live. A grant containing both `notAfter` and
`delegation.renew` is accepted, but renewal is denied, including before the hard
deadline. Omitting a deadline does not implicitly grant renewal.

| Hard deadline | Renewal permission | Child renewal |
|---|---|---|
| Absent | Absent | Denied |
| Present | Absent | Denied |
| Present | Present | Denied |
| Absent | Present | Allowed while parent is live |

For eligible children, the signed lease token doubles as the renewal credential:
access expiry blocks resource operations, but does not erase renewal authority.
An expired token can authenticate `keepalive`. The server preserves the entire
grant and issues a fresh access expiry; it never calls parent keepalive.
No per-child timer or heartbeat is needed. There is no rotation/reuse-detection
protocol: old copies can still renew while the parent is live. Access expiry is
not a revocation boundary for renewable delegations. Signing keys must be unique
to a trust domain and shared across its replicas; there is no grant database.

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
| `delegation.renew` | `{ "maxTtlSeconds": 30 }`, between the requested access TTL and 300. Allows child refresh only without `notAfter`. The original TTL is preserved; refresh cannot broaden permissions or alter bounds. |

An empty name/chapter selector matches nothing. `any` cannot be combined with
finite values for the same selector. There are no permissions for parent
keepalive, completion, rescheduling, configuration reads/writes, restart jobs,
client-payload updates on the parent, schema administration, artifact download,
job acquisition, or issuing more delegations. Those can be separately designed
later rather than inferred from submission or chapter permissions.

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
| `POST /delegations` | Owner lease creates a restricted lease. Accepts `permissions`, optional `ttlSeconds`, optional `notAfter`. Returns an `ExecutionLease` with `delegation` metadata. |
| `GET` (no suffix) | Inspect/import a live delegation without renewing. Returns identity, route, expiry and policy, with empty execution state/client payload. Does not expose the owner's execution snapshot. |
| `POST /keepalive` | Existing endpoint: owner tokens extend the parent as before; eligible delegation tokens refresh only themselves, including after access expiry. |
| `POST /jobs` or `PUT /jobs/{childJobId}` | Existing submission endpoints, with delegated type/ID restrictions. |
| `POST /secrets/resolve` | Existing endpoint with delegated name intersection. |
| `POST /add_chapter` | Existing endpoint with delegated ordinal/type/upload restrictions. |
| `GET /chapters?startOrdinal=0&endOrdinal=4` | Lease-scoped chapter listing; filters unauthorized ordinals. |
| `GET /chapters/{ordinal}` | Lease-scoped chapter read. |

Example creation body:

```json
{
  "ttlSeconds": 30,
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
    },
    "delegation.renew": {"maxTtlSeconds": 30}
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

## Errors and lazy refresh

Delegation errors use `{ "code": "...", "message": "..." }` with sanitized,
credential-free values.

| HTTP/code | Client behavior |
|---|---|
| `401 delegation_access_token_expired` | Only emitted before dispatch. Renew the child, then retry the operation once. |
| `401 invalid_delegation_credential` | Signature/identity failure; stop. |
| `403 lease_permission_denied` | Operation/selector disallowed, or renewal forbidden; stop. |
| `403 delegation_deadline_reached` | Hard deadline elapsed; stop. |
| `409 execution_lease_lost` | Parent lost or access expired during execution; stop. |
| `400 invalid_delegation_request` | Invalid delegated operation body; correct the request. |
| `501 delegation_not_supported` | Backend lacks read-only lease validation. |
| `503 delegation_unavailable` | Parent validation unavailable; no operation dispatched. |

The Go remote client shares an in-memory token cache across its lease operations.
It serializes refreshes for the same cached credential, retries only the exact
pre-dispatch expiry response, and recreates the original request body. It does
not replay operations after ambiguous network errors, arbitrary 401s, provider
errors, or conflicts. Delegation requests never follow redirects. The bounded
cache may evict entries; eviction changes efficiency, not authority.

Creation and ordinary parent renewal remain explicit actions. Importing a valid
child only inspects it; importing an expired renewable child performs the same
lazy refresh cycle. Nonrenewable children can be imported while valid without
requiring renewal permission. `KeepAlive` and `Renew` on a child preserve its
restrictions and never extend its parent.

## Implementation and rollout

The built-in core, SQLite and toy backends expose read-only
`ValidateExecutionLeaseByID`. Third-party runtime wrappers must forward this
method (or embed an implementation); unsupported backends fail closed.
The toy backend's child submission validation now uses this read-only method
instead of implicitly extending the parent.

Upgrade the server and Go client before enabling delegated c2j workers. Existing
owner capabilities and owner import behavior remain compatible. Old clients may
pass a delegated token on allowed operations, but need the new client for lazy
refresh and nonrenewing import. c2j code and CLI flags are maintained outside this
repository; the [guide](GUIDE-Lease-Delegation-Tokens.md) specifies the integration
contract without inventing new CLI flags.

Tests cover toy/SQLite permission enforcement, signed-claim tampering, empty
secret intersections, chapter filtering/writes, deadline/renewal combinations,
concurrent lazy refresh, parent loss/replacement, and a distinct child process
with only a delegated capability. Core validation checks do not renew the parent.

## Prior art and deliberate differences

[OAuth 2.0 RFC 6749 sections 1.5 and 6](https://www.rfc-editor.org/rfc/rfc6749.html#section-1.5)
separates access expiry from continuing refresh authority and requires refreshed
scope to remain within the grant. JobDB adopts that distinction but encodes both
in one signed lease credential for compatibility; this is not an OAuth endpoint.

[Google downscoped credentials](https://docs.cloud.google.com/iam/docs/create-downscoped-short-lived-credentials)
and [Go's ReuseTokenSource](https://pkg.go.dev/golang.org/x/oauth2#ReuseTokenSource)
provide useful models for obtaining restricted tokens on demand and sharing a
cached token source. Here the child calls JobDB itself, with no owner broker.

[Vault token hierarchies](https://developer.hashicorp.com/vault/docs/concepts/tokens#token-hierarchies-and-orphan-tokens)
provide the parent-loss invalidates child model. JobDB deliberately does not
require child periodic heartbeats.

[OAuth security guidance on refresh tokens](https://www.rfc-editor.org/rfc/rfc9700.html#section-4.14)
includes rotation and sender-constraining protections for OAuth clients. This
initial JobDB bearer design does not implement those protections or claim OAuth
conformance. Its renewal authority lasts until parent loss and has no independent
revocation. Do not treat access expiration as invalidating a stolen renewable
credential.
