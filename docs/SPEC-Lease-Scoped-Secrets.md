# Specification: Lease-Scoped Secret Resolution

Status: Implemented, with first-class Go consumption and storage-independent provider configuration.

This specifies the REST and provider APIs for
[the original feature request](../FEATURE_REQUEST_JOBDB_LEASE_SCOPED_SECRETS.md).
It changes the original all-or-nothing authorization behavior: a successful
response contains the allowed subset of the requested secrets. Operational
failures still fail the whole request.

## Contract

A valid execution lease lets its holder request secrets for that job. JobDB
validates the lease and calls a configured provider with the tenant, job,
principal, application metadata, and requested secret names. The provider
decides which secrets are allowed and returns their values.

JobDB does not implement a second secret authorization policy, interpret c2j
recipes, or accept provider paths or credentials from the requester.

**JobDB core never caches provider output.** Each valid resolution request
calls the provider again. A provider may implement its own caching; freshness,
rotation, and authorization consistency within that cache are its responsibility.

A rudimentary SQLite development secret store is specified below, including
administrative HTTP endpoints and CLI commands available in SQLite server mode.
The provider is independent of workflow storage; other backend modes do not
expose this development administration API.

## REST API

```http
POST /v1/tenants/{tenantId}/jobs/{jobId}/leases/{leaseId}/secrets/resolve
X-JobDB-Lease-Token: <lease token>
Content-Type: application/json

{"names":["github_token","registry_password"]}
```

Operation ID: `resolveSecretsWithLease`.

The lease token must match the tenant, job, and lease in the URL. Existing
deployment authentication requirements also apply; this endpoint adds no
separate secret-read credential for the runner.

The body contains only required `names`. Unknown body fields, including
`principal`, are rejected. Principal comes exclusively from the validated,
signed lease token; see the principal semantics below.
Application metadata is loaded from the job by JobDB, not accepted in this body.

`names` must be a nonempty array of distinct, nonempty strings. Names are opaque,
case-sensitive logical identifiers. JobDB neither normalizes them nor interprets
them as provider paths. There is no wildcard or list-all operation. An empty
request is invalid, not a request for all secrets.

The HTTP implementation must bound request and response sizes. Limits are
configured through runtime `SecretLimits` (default 64 KiB requests, 1 MiB
responses, 10-second provider timeout) and checked
before provider invocation for requests and before response transmission for
results. Oversized requests receive `413`; oversized provider results fail as
an invalid provider response without transmitting values.

### Success

```http
HTTP/1.1 200 OK
Content-Type: application/json
Cache-Control: no-store

{
  "secrets": {
    "github_token": {"value": "example-value"}
  }
}
```

The response is a map keyed by requested name. Each entry has:

- `value`: required UTF-8 string; an explicitly returned empty string is valid.
- `version`: optional opaque provider version identifier.
- `expiresAt`: optional RFC 3339 UTC expiration timestamp.

Binary values are outside this first contract. JobDB must reject invalid UTF-8
provider strings rather than silently changing their contents during encoding.

Only requested secrets that the provider permits are included. Denied or unknown
names are omitted without distinguishing those cases. If none are allowed or
known, return `200` with `{"secrets":{}}`.

Omission is different from an empty value. Callers requiring every requested
secret must compare the returned keys with their request and fail the invocation
if required names are missing. JobDB does not substitute empty values.

JobDB preserves supplied version and expiration metadata. It must not return a
result containing a value whose declared expiration has already passed at the
final response check; that request fails with `secrets_unavailable`. An absent
expiration is not a promise of indefinite validity.

### Errors

Errors contain no secret values, provider messages, external secret paths, or
request credentials:

```json
{"code":"secrets_unavailable","message":"Secret resolution is unavailable."}
```

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | `invalid_secret_request` | Invalid JSON, fields, or names. |
| 413 | `secret_request_too_large` | Request exceeds the configured limit. |
| 409 | `execution_lease_lost` | Missing/invalid token, identity mismatch, expired or inactive lease, or cancellation requested. |
| 503 | `secrets_unavailable` | Provider temporarily unavailable, provider deadline exceeded, or returned credential expired. |
| 501 | `secrets_not_supported` | No provider is configured. |
| 500 | `secret_resolution_failed` | Unexpected provider error or invalid provider response. |

Deployment authentication may reject the request before this endpoint executes.
Within the endpoint, invalid lease identity and missing jobs use the same lease
failure response. Per-name denials are successful omissions, not HTTP errors.

All endpoint responses, including errors, use `Cache-Control: no-store`.
An error response never includes a `secrets` map or partial values.

## Provider API

Go contract in `pkg/jobdb`, available to provider implementations:

```go
type SecretProvider interface {
    ResolveSecrets(context.Context, SecretResolveRequest) (SecretResolveResult, error)
}

type SecretResolveRequest struct {
    TenantID    string
    JobID       string
    Principal   *string // nil when the signed lease token has no principal claim
    AppMetadata json.RawMessage
    Names       []string
}

type SecretResolveResult struct {
    Secrets map[string]SecretValue
}

type SecretValue struct {
    Value     string     `json:"value"`
    Version   string     `json:"version,omitempty"`
    ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

// Recognized through errors.Is; its wrapped error text is never exposed.
var ErrSecretsUnavailable = errors.New("secret provider unavailable")
```

The provider is the authority for resolving names and deciding access using
the supplied context. Provider configuration and backend credentials stay on
the server. The lease token is not passed to the provider.

Provider behavior:

1. Return a map containing only allowed, known names from `Names`.
2. Omit denied or unknown names. A nil map and nil error mean an empty result.
3. Return `ErrSecretsUnavailable`, optionally wrapped, for a transient failure.
   Backend outages and lookup failures must not be disguised as denials or
   missing names.
4. Return another error for an unexpected failure. If a result and error are
   both returned, JobDB discards the entire result.
5. Respect context cancellation and deadlines. Calls may be concurrent, so
   provider implementations must be safe for concurrent use.
6. Do not mutate a returned result after returning. A caching provider must
   return a stable snapshot suitable for serialization.

JobDB validates that every returned name was requested. An unexpected key is
an invalid provider response: fail the whole call and expose no values. This
validation enforces the API shape; JobDB does not second-guess the provider's
authorization decision for requested names.

Provider batch resolution does not promise that all values came from one
transactional snapshot of the external store. Each value may carry its own
version and expiration.

## Tenant, job, principal, and application metadata

`TenantID` and `JobID` come from the validated lease and authoritative job
identity. Both are nonempty. These use JobDB's existing `(TenantId, JobId)`
identity; no additional identity hierarchy or job-context lookup is required.

`Principal` comes exclusively from an optional `principal` claim in the signed
lease token, after JobDB validates its signature, identity binding, and expiry.
It is an opaque string asserted by the trusted token issuer, not a field the
requester can supply or override. JobDB passes the verified value unchanged to
the provider, which decides what access that principal has for the tenant and
job. A signed principal establishes its provenance; it does not itself grant
access to secrets.

Principal-bearing lease tokens are a future extension. Until that extension is
available, and whenever a valid token lacks the claim, JobDB passes `nil` to the
provider. It must not infer a principal from a worker ID, request header, job
metadata, or client payload. An absent claim is distinct from a present empty
string, which is passed as a non-nil pointer to that string. The provider decides
how to handle either case; nil does not imply elevated access.

When principal-bearing tokens are supported, renewal must preserve the signed
principal, including its absence, using trusted lease state or validated claims.
The requester cannot change it through renewal or secret resolution. Child jobs,
restarts with a new job ID, and schedule-created jobs use their own lease and
issuer-established claims. The same principal does not automatically inherit
another job's secret grants.

An embedded adapter uses equivalent trusted lease identity supplied by its
issuer, passing nil when no principal is associated with that lease. It must
not expose a caller-controlled principal override on the resolution operation.

`AppMetadata` contains the application's metadata stored on the identified job
(`StoredJob.AppMetadata`, or the `app` portion of `JobMetadataEnvelope`). JobDB
loads it for the validated tenant and job before calling the provider and passes
a copy of the JSON value without interpreting application-specific fields.
Absent metadata is represented by nil. Failure to load metadata fails the
request without calling the provider; it must not be treated as absent metadata.
The copy isolates stored state from any provider mutation.

This field excludes JobDB's internal metadata, client payload, and task inputs.
The resolution request cannot override it. Stored application metadata may have
been supplied by the job submitter; storage alone does not make its contents
trusted identity or permission claims. The provider decides how to use those
fields in its authorization policy. Child and restarted jobs supply their own
stored application metadata.

## Lease checks and in-flight requests

JobDB checks the token and current scheduler lease before calling the provider.
Reject expired, released, superseded, completed, or cancellation-requested
executions. A valid signed token alone is insufficient if the scheduler no
longer recognizes the lease.

After the provider returns, JobDB checks the request token and current lease
again before releasing values. No scheduler lock is held during provider I/O.
Resolution does not renew or acquire a lease.

For example: a lease is valid when a lookup starts, but the job is cancelled
while the provider is responding. The final check discards the result and
returns `execution_lease_lost`. This is the in-flight race the second check
addresses; it is separate from caching.

The guarantee is validity at that final check. JobDB cannot revoke bytes already
sent or guarantee that a lease stays valid while a response travels over the
network. This requires no transaction with the external secret store.

Renewal retains existing token behavior. A replacement token does not itself
invalidate an unexpired older token for the same active lease. If the token
used for an in-flight request expires before the final check, the request fails;
the caller can issue a new request with its renewed token. The new request
calls the provider again.

Provider calls receive the HTTP request context bounded by a configured lookup
timeout and the request token's expiry. JobDB does not automatically retry
provider calls. Callers may retry transient failures with a valid lease; retries
may return different values or cause another credential issuance.

## Runtime caching, workflow caching, and diagnostics

The runtime/server never caches successful results, empty results, or per-name denials, and
does not coalesce independent requests into one shared provider result. Output
exists only as transient data needed to validate and return that request.

Consequently, every independently admitted request invokes the provider, even
when its lease and requested names match a previous request. A provider may
cache internally, so this contract does not guarantee that repeated requests
observe rotation immediately. Expiration metadata never authorizes core caching.

Workflow `JobContext` and `TaskContext` cache values and omissions by name for
one live job attempt. They request only unresolved names from the lease/provider,
share successful concurrent reads, and never cache failed calls. Whole-job retries
and resumptions start fresh. Expired cached values fail without refreshing in the
same attempt. See the [workflow cache contract](MIGRATION-Workflow-Read-Caching.md).

Responses bypass chapters, task inputs/results, client payloads, artifacts,
job inspection, exports, and replay history. The read-only replay runtime
rejects resolution without invoking the provider. c2j resolves secrets only
for an actual uncached invocation, and owns transient injection and output
sanitization.

JobDB uses fixed public error messages and stable codes. It must not interpolate
provider error text, unwrap errors into diagnostic output, or attach raw
provider errors to durable workflow failures. Client errors likewise exclude
raw HTTP bodies, including malformed responses and proxy-generated errors.

Secret-bearing result types must redact ordinary diagnostic formatting; explicit
REST serialization intentionally exposes their values. Request credentials and
response bodies must be excluded from application and transport logs and traces.
Auditing may record requested/returned names, job and lease identity, time, and
outcome, but not values or credentials. Providers are responsible for applying
the same discipline to their own diagnostics.

Every holder of the lease credential, including Pulse, shares its authority to
request secrets. Once a runner receives plaintext, this API cannot prevent that
runner or executed code from copying it into output or artifacts.

## Go consumption and composition

`WorkflowRuntime.ResolveSecrets(ctx, ResolveSecretsRequest)` and
`ExecutionLease.ResolveSecrets(ctx, names)` are required interface methods.
The runtime operation is the verified transport entry point; embedded callers
use the issued lease. There are no optional secret capabilities or helper
functions that probe a runtime or lease by type assertion.

`workflow.JobContext` and `workflow.TaskContext` both expose:

```go
ResolveSecrets(context.Context, []string) (jobdb.SecretResolveResult, error)
```

The contexts use the current execution lease and bound each call by the caller
and worker invocation. A finished or timed-out invocation cannot retrieve
secrets even if the job still has a live lease. Reads write no chapters and
retain no provider output for replay. Offline replay rejects direct reads;
resolving inside a task lets replay reuse the task outcome without calling the
provider. Callers must check for required names in the returned subset.

`SecretProvider` depends only on its request/result contract, never on a
scheduler, chapter store, runtime, lease token, or storage-specific handle.
Runtime code checks live leases and copies stored application metadata; the
provider owns authorization policy and retrieval. Shared broker code applies
limits, safe errors, and final lease checks consistently across backends.

Provider configuration is independent of job storage:

- SQLite accepts `Config.SecretProvider` or `WithSecretProvider`.
- Runtime core accepts `Config.SecretProvider` alongside its storage ports.
- The in-memory runtime accepts `toy.WithSecretProvider`.
- Remote Go consumers use the provider configured on the server.

Without a provider, every runtime returns `ErrNoSecretProvider` for an otherwise
valid secret read. Invalid or lost leases are rejected first. Injected providers
remain caller-owned; runtimes never close them. Applications can share a provider
across runtimes or configure different providers over the same kind of storage.

The SQLite secrets implementation is public in `pkg/jobdb/secrets/sqlite`.
`sqlitesecrets.Open` returns an encrypted store implementing `SecretProvider`
and offering `Set`, `Remove`, and `Close`. Its only database dependency is its
own secrets file. The SQLite runtime's `DevSecretsDB` and `DevSecretsKeyFile`
configuration is a convenience that creates and owns this provider. Supplying
those paths together with an injected provider is a configuration error.

## SQLite development secret store

This is an optional development/prototype provider backed by a separate SQLite
store. The CLI selects it only in SQLite mode. The Go provider is independent of
workflow storage and can be injected into any supported runtime. It supplies a
simple way to populate tenant secrets and
exercise the normal lease-scoped resolution API. It is not a production secret
manager or a required capability of other runtime implementations.

### Storage and provider behavior

Store values in a dedicated SQLite file configured with `--dev-secrets-db` (`-s`).
This file is separate from the workflow database and is not part of job exports
or workflow backups. Selecting the SQLite workflow backend alone does not enable
the provider; the secret-store configuration must be explicit. This storage
configuration does not control registration of the administration routes, which
are always available in SQLite mode.

Each entry is keyed by `(tenant_id, name)` and contains an encrypted UTF-8 value.
Use authenticated encryption with a fresh nonce per write and bind ciphertext
to tenant and name as authenticated associated data. The encryption key comes
from `--dev-secrets-key-file` (`-k`), containing a base64-encoded 32-byte key managed
outside the SQLite file and its backups. The key is required, never generated
implicitly on startup, and must not be stored in the secret database. Use
AES-256-GCM for this initial implementation. Encrypt values before passing them
to SQLite so journal/WAL files do not receive plaintext values. Tenant IDs and
names are not encrypted.

Create the store with owner-only file access. Initialization must detect a wrong
key, including for an empty initialized store, using an authenticated format/key
check record. A missing or wrong key fails startup or the operation safely; it
must not silently recreate the store. Key rotation tooling and historical secret
versions are outside this first implementation.

The development provider's authorization policy is deliberately simple:
**any valid lease for a job in a tenant may resolve the requested secrets stored
for that tenant.** It ignores `Principal`, `AppMetadata`, and `JobID` for access
decisions; the broker still supplies them and validates the lease normally.
There are no per-job or per-principal grants in this provider.

The provider returns existing requested names for `TenantID` and omits missing
names. Empty stored values are valid. It never queries another tenant's values
and never returns unrequested names. A storage or decryption failure fails the
whole resolution call without partial values. This provider does not cache:
each call reads committed store state. A lookup begun after a successful set or
remove observes the change; an overlapping lookup may observe the previous
committed value. Previously delivered plaintext cannot be revoked by removal.

The initial store has no credential expiry or version selection. Returned
`SecretValue` entries contain `value` only. Storing administrative values here
does not authorize the broker to persist resolution responses elsewhere.

### Development administration HTTP API

Register these routes automatically whenever JobDB runs in SQLite mode,
including the CLI's default SQLite mode. No enablement flag or admin token is
required:

```http
POST /dev/v1/tenants/{tenantId}/secrets/set
Content-Type: application/json

{"name":"github_token","value":"example-value"}
```

Atomically insert or replace the tenant/name entry. Return `204 No Content`.
Repeating the operation with the same value has the same logical effect.

```http
POST /dev/v1/tenants/{tenantId}/secrets/remove
Content-Type: application/json

{"name":"github_token"}
```

Atomically remove the tenant/name entry. Return `204 No Content`, including when
the entry does not exist. Removal does not require the tenant to have a live job.
Setting a value likewise does not require an existing job.

Both routes use the existing runtime HTTP listener, which defaults to loopback
in the CLI. They require neither an admin token nor a lease token. Any caller
that can reach this development API can set or remove values for the named
tenant. The normal resolution endpoint continues to require a valid lease.

On a non-SQLite runtime, these routes return `404`. Supplying development-store
flags to a non-SQLite backend fails startup instead of being ignored. In SQLite
mode, routes remain registered even when secret storage is not configured;
operations then return `503` with a fixed `dev_secrets_not_configured` error and
perform no mutation. Store/key configuration remains a storage prerequisite,
not an API enablement or authentication requirement.

Tenant and name must be nonempty; names follow the resolution API's opaque,
case-sensitive semantics. `value` must be present and a UTF-8 JSON string, with
an empty string permitted. Reject unknown fields. Bound body and value sizes;
validate before mutation. Set/remove use transactions and acknowledge success
only after commit.

Use `400` for invalid input, `413` for oversized input, `503` for unconfigured
storage or transient store failures, and `500` for
other store failures. Use fixed, sanitized error codes/messages with no request
body, value, key, token, or raw SQLite error text. Every response uses
`Cache-Control: no-store`; no success or failure response echoes the value.
Request bodies are excluded from logs and traces.

There is no administrative get/list endpoint in this first version. Runners
read through the normal lease-scoped resolution endpoint.

### CLI

Add commands under `jobdb secrets` as clients of the development administration
API. They require a running SQLite server; they do not open
or modify a database file directly.

```sh
jobdb secrets set github_token --tenant tenant-a \
  --value-file ./github-token.txt

jobdb secrets set github_token -t tenant-a -f ./github-token.txt

jobdb secrets remove github_token -t tenant-a
```

- `set NAME` adds or replaces a value; `add` is an alias for `set`.
- `remove NAME` removes it, succeeding if it is already absent.
- `--tenant` (`-t`) is required for both commands.
- `--url` (`-u`) defaults to `http://127.0.0.1:9047` and selects the runtime server.
- `set` requires `--value-file PATH` (`-f PATH`); `--value-file -` or `-f -` reads
  stdin. Preserve the
  exact UTF-8 content, including newlines. Do not accept a secret value as a
  positional argument or `--value` flag, or prompt with terminal echo enabled.
- Neither command prints the value. Success output may identify
  the operation, tenant, and name. Failures return a nonzero exit status and
  use sanitized errors, including when the server sends an unexpected body.

CLI help must label these commands as SQLite development/prototype utilities.
Show both long and single-letter options in help. The CLI needs neither an admin
token nor the store's encryption key.

### OpenAPI and compatibility boundary

The lease-scoped resolution endpoint belongs in the main runtime OpenAPI
contract. The development set/remove endpoints do not.

Their complete HTTP contract is in the separate
`openapi/jobdb-dev-secrets.yaml`, with a development/prototype title, description,
and operation tags. Keep it out of the
main runtime client generation and runtime conformance requirements. Document
that the routes are automatically available in SQLite mode and have no token
authentication; their separate specification marks their development scope.

The main provider API stays backend-independent. The development store is a
SQLite-specific implementation, and other providers need not expose any
administrative HTTP surface. The SQLite routes and commands are implemented.
Other backend modes default to `no secret provider configured` for resolution.
Library users can inject any provider independently of storage. Additional
built-in providers are deferred.

## Acceptance criteria

- A valid lease can request secrets without a separate runner secret-manager
  credential. The provider receives the correct tenant, job, principal,
  application metadata, and requested names.
- Allowed subsets return `200`; denied and unknown names are indistinguishably
  omitted. An empty subset serializes as an empty object, never null.
- Explicit empty values remain distinguishable from missing keys.
- Invalid requests and invalid leases do not invoke the provider.
- A request cannot target a different job or tenant through its lease token.
- A principal in a validated signed lease token reaches the provider unchanged.
  A token without the claim passes nil, including existing tokens. A present
  empty string remains distinguishable from an absent claim.
- A request-body principal is rejected; headers, metadata, and client payload
  cannot override the signed claim or supply a fallback. Tampering with a token's
  principal invalidates its signature and prevents provider invocation.
- When principal-bearing tokens are introduced, renewal preserves the principal
  or its absence. Embedded resolution uses only equivalent trusted lease identity.
- Application metadata comes from the identified job, cannot be overridden by
  the resolution request, and excludes internal metadata and client payload.
  Absent metadata is nil; loading failures do not invoke the provider, and
  provider mutation cannot change the stored metadata.
- Provider errors, extra result keys, oversized results, and expired returned
  credentials expose no partial values.
- Lease loss or cancellation during provider I/O prevents release of values at
  the final check. Renewed tokens work without changing existing token semantics.
- Repeated valid requests each invoke the provider. Different returned values
  are reflected without a JobDB cache, including after a previous empty result.
- Provider and transport errors containing test secrets never leak them through
  public errors, logs, traces, or stored workflow failures.
- Resolution performs no workflow persistence. Replay makes zero provider calls.
- REST and embedded adapters obey the same provider and lease contract.
- The SQLite development store persists encrypted values across restarts;
  missing/wrong keys fail safely, and plaintext values never reach SQLite files,
  including journals and WAL files.
- CLI set/add and remove operate through the development HTTP API for the named
  tenant. Replacement, empty values, exact newline preservation, idempotent
  removal, tenant isolation, and fresh reads after committed changes are tested.
- Development administration routes are registered automatically in SQLite mode,
  including default CLI mode, without an enablement flag or admin token. Other
  backends return `404`; unconfigured SQLite secret storage returns a sanitized
  `503` without mutation. Resolution still requires a valid lease.
- CLI short options (`-t`, `-u`, `-f`) behave identically to their long forms;
  SQLite storage options also provide `-s` and `-k` aliases.
- The development provider's tenant-wide access policy is explicit in its help
  and documentation; principal and metadata do not grant narrower protection.
- Development administration appears only in its separate OpenAPI document and
  is not required of other runtime backends. CLI and HTTP diagnostics never
  expose values or encryption keys.

- Custom and SQLite secret providers work with SQLite and in-memory job storage,
  through embedded and HTTP leases; runtime core accepts the same contract.
- Job and task contexts resolve directly, enforce invocation lifetime, and do
  not store values in history or retrieve them during replay.
- Closing a runtime leaves its injected provider usable by its owner.
