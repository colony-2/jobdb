# Offline attenuation of lease credentials

Status: Implemented in the runtime HTTP API and Go remote client. See the
[lease API specification](PROPOSAL-Restricted-Lease-Capabilities.md) and
[c2j integration guide](GUIDE-Lease-Delegation-Tokens.md). The public API uses
permissions and an optional deadline; local derivation is internal to the SDK.

## Decision and public contract

A delegated lease authorizes its selected operations until an optional absolute
`notAfter`, provided the exact parent execution lease remains live. Without a
deadline, its lifetime is tied solely to the parent. There is no renewal
permission or caller-selected access TTL. Credential maintenance is internal and
cannot extend the deadline or parent lease.

The implementation uses first-party macaroons to let clients narrow a credential locally, without a
request to JobDB or possession of the server's signing key. A child carries one
attenuated grant proof inside the existing opaque `leaseToken`. It can narrow
that grant again, but cannot remove an ancestor's restrictions.

Keep the existing API:

```go
func (r *Runtime) CreateDelegation(ctx context.Context,
    parent jobdb.ExecutionLease,
    req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error)
```

The request contains only `Permissions` and optional `NotAfter`. Keep
`ExecutionLease`, `LeaseCapability`, `ExportLease`, `ImportLease`, `KeepAlive`,
`Renew` and `X-JobDB-Lease-Token`. Do not introduce a replacement restriction API
or require c2j to branch on token format.

Local derivation cannot check backend liveness. An invalid parent can therefore
be detected at import/use rather than creation. Successful creation has never
guaranteed that the parent will remain live until the next operation; no new
caller workflow is required for this error-timing difference.

No compatibility with previous token formats is required. This design has no
legacy fallback, conversion endpoint or negotiation mechanism. The updated
server and SDK use the new format. Keeping public operations and types stable
does not require preserving old serialized tokens.

## Authority and deadlines

For a token with restriction steps `P1 ... Pn` and optional deadlines `D1 ... Dn`:

```text
effectivePermissions = rootCeiling ∩ P1 ∩ ... ∩ Pn
effectiveDeadline = earliest present deadline, or infinity
valid = authenticated grant
        AND exact parent execution lease is live
        AND serverNow < effectiveDeadline
```

Every request checks the authoritative backend parent using its tenant, job,
execution lease ID and worker identity. Cancellation, completion, rescheduling,
expiry or replacement invalidates every descendant. Reacquiring the same job
does not revive an old child. Owner-token rotation for the same live lease does
not invalidate children or require distributing replacement tokens to them.

Further attenuation may narrow permissions or bring the deadline forward.
Omitting `NotAfter` inherits any ancestor deadline; it never removes one. An
explicit later deadline is rejected. Equality with the effective deadline is
expired, with no clock-skew allowance that extends authority. Server time is
authoritative; changing a client clock cannot bypass a signed deadline.

There is no separate permission for further attenuation. A bearer can already
copy its full credential; deriving a credential with less authority is allowed.
This does not prevent copying, forwarding or collusion. There is no individual
grant revocation API.

## One grant proof, no inherited access window

The macaroon itself remains valid until parent loss or the effective deadline.
JobDB verifies it directly on each request. There is no separate access proof,
short access window, token-refresh cache or automatic mutation replay. Expiry of
an old owner bearer token does not shorten a child's lifetime.

`KeepAlive`/`Renew` remain valid lease operations. For a child they validate its
grant and live parent, then return a restricted lease snapshot. They return
the same token. They never call parent keepalive, reset deadlines, remove
restriction steps or require an owner token. SDK users need no child timer.

The snapshot's `expiresAt`/`LeaseExpiry()` reflects the current known parent
expiry, capped by the effective deadline. It is a snapshot, not a new immutable
bound: the owner may extend the parent afterward. A locally constructed handle
may initially have an older parent-expiry snapshot. It must still allow an
operation or inspection to reach JobDB, which checks current liveness. The
immutable child lifetime bound is `Delegation().NotAfter`.

Do not bind the grant to the expiration of an old owner access token. The grant
is bound to the backend lease identity, and that live record remains necessary
for every operation. This is what lets a separate child process continue working
while the owner refreshes its own lease independently.

## Cryptographic construction and prior art

Macaroons use chained MACs so a bearer can append restrictions without knowing
the issuer's root key. The issuer verifies the resulting chain. This fits the
existing JobDB HMAC trust model and avoids placing reusable ancestor bearer
signatures in the child.
[Original macaroon paper](https://research.google/pubs/macaroons-cookies-with-contextual-caveats-for-decentralized-authorization-in-the-cloud/).

Biscuit is an alternative when public-key verification by independent services
or a richer authorization language is needed. JobDB currently verifies its own
credentials and has typed permission selectors, so those features are not
required here.
[Biscuit specification](https://doc.biscuitsec.org/reference/specifications).

UCAN provides principal-to-principal certificate-style delegation. Its ancestor
time bounds also illustrate why an expiring owner access credential should not
be the lifetime root for children that must survive owner-token rotation.
[UCAN delegation time bounds](https://github.com/ucan-wg/delegation#time-bounds).

The implementation pins `gopkg.in/macaroon.v2` at v2.1.0 for cloning, first-party
caveats, binary encoding and signature verification. JobDB validates every caveat
and rejects third-party caveats before using the library's `VerifySignature`.
[Go macaroon API](https://pkg.go.dev/gopkg.in/macaroon.v2).
The profile has a deterministic wire vector and adversarial tests. These checks
do not constitute an independent cryptographic audit.

## Credential contents and role separation

The implemented formats are:

| Prefix | Contents |
|---|---|
| `jdb-o1.` | Base64url canonical owner-claim JSON, a dot, and base64url HMAC-SHA256 over the prefix and encoded payload. Claims include the root grant. |
| `jdb-m1.` | Base64url, unpadded, canonical macaroon V2 binary. The root grant has no caveats; a usable child has one to eight. |

Previous owner and delegation formats are rejected. Root grants are accepted
only as derivation material inside an authenticated owner credential, never as
unrestricted child credentials. Owner tokens are limited to 32 KiB; child/root
macaroons to 16 KiB.

An owner bundle contains its ordinary owner access credential plus a root grant
macaroon for local derivation. JobDB supplies these together during normal lease
acquisition, so no extra request is needed. Owner operations continue to validate
the owner credential, including its expiry. The root grant cannot authorize
parent keepalive, completion, configuration or administration.

The root macaroon's authenticated identifier contains:

- Profile version, key ID and purpose `jobdb.delegation.grant`.
- Configured service/trust-domain ID; never derive it from an incoming Host header.
- Random root ID and the exact tenant/job/execution-lease/worker identity.
- Principal and schema binding required by existing authorization.
- Fixed ceiling profile `jobdb.resource-actions.v1`. Deadlines are in restriction steps.

The root ceiling covers only the existing resource actions: job submission,
secret resolution, chapter reads and restricted chapter writes. Existing backend,
schema and secret-provider authorization still applies. The root has no short
access expiry that descendants inherit.

Each first-party caveat is a complete typed restriction step, logically:

```json
{
  "kind": "restrict",
  "version": 1,
  "id": "locally-generated-random-id",
  "permissions": {
    "chapters.read": {"ordinals": [0, 1]},
    "secrets.resolve": ["model_api_key"]
  },
  "notAfter": "2026-10-23T20:00:00Z"
}
```

The step ID is an attribution label, not a uniqueness or authorization guarantee.
The effective identity includes the authenticated root and full restriction path.
A step's permissions are a complete allowlist, not a patch. Missing actions deny
access. A child credential must contain at least one restriction step.

Derivation clones the root/child macaroon and adds a restriction caveat. The
result contains the authenticated root identifier, restriction caveats and final
MAC, but no reusable ancestor MAC or owner bearer credential. Construct the
child envelope from scratch; never serialize the full owner bundle as ancestry.
Changing a prefix must never turn a child credential into an owner credential.

Restriction JSON uses the field order `kind`, `version`, `id`, `permissions`,
`notAfter`; absent optional fields are omitted. Root fields are `version`,
`purpose`, `audience`, `keyId`, `id`, `parent`, `ceiling`. The parent fields are
optional `principal`, then `tenantId`, `jobId`, `leaseId`, `workerId`, and optional
`schemaHash`. Purpose is `jobdb.delegation.grant.v1` and version is 1.

Canonical JSON is exactly Go `encoding/json.Marshal` output for the profile
structs in `attenuation.go`: compact JSON, struct field order, integer ordinals,
standard JSON string escaping including HTML escapes, UTC RFC3339Nano deadlines,
and no alternate null/empty representations of omitted fields. Array order is
preserved. Decoding must round-trip to identical bytes. This rejects duplicate
or unknown fields, malformed selectors, unknown versions and alternate encodings.
Macaroon binary must also round-trip exactly; trailing bytes, V1 binary, location
fields and third-party caveats are rejected. No discharge service or remote key
lookup is involved. `TestAttenuationWireVector` fixes a complete interoperable
example with a public test key, root, restriction and expected encoded token.

## Permission composition

Evaluate the root ceiling and every restriction step conjunctively. Do not trust
a client-supplied summary or authorize only the last step.

| Field | Composition |
|---|---|
| Job types and job IDs | Intersect exact-name sets; `any` is the universal set |
| Secret names | Intersect exact names, then provider authorization |
| Chapter reads | Intersect ordinal sets and inclusive ranges |
| Chapter writes | Intersect ordinals, task types and artifact names independently; task-outcome-only checks remain |
| Hard deadline | Earliest present deadline; omission inherits existing bounds |

The SDK rejects requested widening. The server independently validates the full
policy sequence and rejects invalid widening as well as enforcing the
intersection. A later `any` cannot undo an earlier finite allowlist. Normalize
chapter ranges without expanding large ranges into individual ordinals.
Inspect/renew responses derive effective policy and deadline from verified claims.

## Local derivation and server verification

`CreateDelegation` takes a consistent snapshot of the input handle, validates
local syntax and attenuation, clones the macaroon, appends the new caveat and
returns an ordinary restricted remote lease handle. It performs no HTTP request,
parent refresh or hidden fallback. Honor context cancellation and avoid mutating
the original handle. An empty permission set remains valid but permits no
resource operations.

A holder can inspect the HMAC-based policy locally but cannot authenticate its
root without the server key. Local success is therefore construction, not server
validation. The recipient uses the existing import/use path to validate it. The
child handle must redact owner execution state and payloads exactly as current
server-created delegations do.

For every server request:

1. Enforce size limits and the allowed credential role before routing.
2. Parse the bounded profile and verify the complete macaroon under its configured
   purpose-specific key, including service binding.
3. Validate all restriction steps and compute effective permissions/deadline.
4. Bind the request path to the authenticated lease identity and check the exact
   live backend parent with `ValidateExecutionLeaseByID`.
5. Enforce the deadline and requested operation/selectors before side effects.
6. Dispatch through existing scoped authorization paths. Preserve checks around
   secret disclosure, chapter reads and backend mutations.

Fail closed on unavailable backend validation. Retain the existing rejection of
owner tokens, missing tokens and unscoped/admin requests at the worker ingress.
Do not use local creation as an authorization bypass for any server operation.

Keep the existing owner-authorized `POST /delegations` endpoint for direct HTTP
clients. It produces the same restricted macaroon and lifetime semantics. It is
not required by SDK local derivation and is not a fallback for failed attenuation.
Further attenuation does not grant a child access to this owner-only endpoint.

## Server storage and key lifecycle

There is no new per-parent or per-delegation database record. Verification uses
only the presented token, configured signing material and the existing backend
parent lease record. No grant registry, child expiry updater, certificate store
or stored refresh token is needed. The server does not track issuance to decide
whether a child is valid.

The macaroon key is HKDF-SHA256 of `LeaseTokenSigningKey`, with nil salt, info
`jobdb.delegation.grant.v1` + a zero byte + audience, and 32 output bytes. Key ID
is lowercase hex of the first eight bytes of SHA256 of that derived key.
`ServerOptions.LeaseTokenAudience` selects the audience (default `jobdb`); replicas
must agree on both key and audience. Owner claims are also audience-bound.
Only the configured key is accepted; unknown key IDs are rejected. There is no
remote key discovery or retained-keyring API in this implementation.

Owner renewal issues a new root for the same live lease. Existing child roots remain valid without server
records. Retiring a signing key invalidates its lineages; ordinary owner-token
rotation does not. Keep the intended key-retention policy explicit.

An owner bundle contains delegation authority that lasts while the backend
parent lives, even after its owner access credential expires. A retained old
bundle can still derive children within that authority. It cannot use the expired
owner access credential to extend the parent or complete the job. Similarly,
old child copies remain valid until parent loss or their effective deadline.
Refreshing a snapshot does not revoke earlier copies.

Offline creation is not observable to JobDB until use. Audit verified root/path
identity, parent lease, action and result; do not claim a complete inventory of
issued children or trust client-reported creation times. Never log a macaroon,
MAC value or complete capability. Preserve explicit `Encode` and redacted display.

Bound the encoded child token to 16 KiB, restriction depth to eight steps, and
selectors to the existing 256-entry limits. Check limits before expensive parsing
and locally before returning a child. Renewal does not reset depth. Do not flatten
or re-root a child to bypass limits; an owner may explicitly create a new branch.

## c2j and other clients

The [integration guide](GUIDE-Lease-Delegation-Tokens.md) remains the caller model.
After updating the JobDB SDK, the owner makes the same call:

```go
child, err := rt.CreateDelegation(ctx, owner, jobdb.CreateDelegationRequest{
    NotAfter: &operationDeadline,
    Permissions: jobdb.DelegationPermissions{
        Secrets: []string{"model_api_key"},
        Read: &jobdb.DelegationChapters{Ordinals: []int64{0, 1}},
    },
})
```

Export only the child capability through the existing private handoff. The
isolated operation process uses `DecodeLeaseCapability` and `ImportLease`, then
its ordinary lease-dependent operations. Give it the worker ingress URL through
trusted configuration. It needs no owner credential, owner process, callback or
credential-refresh timer. The owner continues maintaining its main lease.

`KeepAlive`/`Renew` continue working for generic lease consumers, even when no
credential replacement is necessary. SDK callers do not need to understand
macaroons, token versions or internal refresh strategies. The child can call the
same `CreateDelegation` method to hand narrower authority to another isolated
process without contacting its own issuer.

## Validation

`TestAttenuation*`, `TestDelegation*` and the existing lease/secret/runtime suites
cover the following. Update SDK and server together; delegation call sites stay
the same. No backend migration or delegation-record storage is needed.

- Derive children and grandchildren through `CreateDelegation` with networking
  disabled. Neither owner bearer tokens nor reusable ancestor MACs appear in them.
- Exercise omitted deadlines, earlier deadlines, inherited deadlines and rejected
  later deadlines. Operations and keepalive fail at/after the effective deadline.
- Use a child after owner-token rotation and an old parent-expiry snapshot, while
  the exact backend parent remains live. No owner callback or token is needed.
- Cancel, complete, expire, reschedule or replace the parent; all descendants fail.
  Child operations and keepalive leave the parent expiry unchanged.
- Tamper with identity, permissions, deadline, purpose, version, MAC or caveat
  order; strip restrictions or splice sibling chains; verification fails.
- Evaluate all selector intersections, chapter filtering, secret-provider bounds
  and artifact restrictions; an empty grant authorizes no resource operations.
- Reject cross-service, cross-lease, wrong-worker and unknown-key credentials.
  Verify shared-key replicas work without exchanging delegation records.
- Test key rotation, redaction, byte/depth limits, malformed encodings and concurrent
  handle snapshots. A caller-chosen step ID cannot alias cached authority.
- Run a distinct c2j-like child process against delegation-only ingress. Import,
  allowed reads/writes/submissions and keepalive work with only its child token;
  owner and unscoped APIs remain inaccessible.
