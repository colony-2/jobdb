# Proposal: Tenant Configuration Store

Status: Implemented in JobDB. Postgres runtime wiring is supplied as a companion
`pgjobdb` patch; see implementation notes below.

## Recommendation

Add a built-in, tenant-scoped configuration store on every JobDB backend, with
administration routes in the main public OpenAPI document and a separate
lease-scoped endpoint that returns the tenant's complete configuration.

Fetch configuration only when a client explicitly calls a retrieval method.
Runtime, lease, and administration calls read fresh snapshots from storage,
through the server for remote clients. Workflow contexts cache the first
successful snapshot for one live job attempt, sharing concurrent reads.
Workers do not declare requested names, and applications need not manage HTTP
requests.

The separate read provides explicit authorization, freshness, and failure
behavior. Do not prefetch configuration during startup, lease acquisition,
renewal, or chapter retrieval, or attach it to stored chapters or job-start
records. Workers that never request configuration perform no configuration reads.

This complements [lease-scoped secrets](SPEC-Lease-Scoped-Secrets.md).
Configuration is ordinary application data stored by JobDB, not a secret
provider. Unlike the SQLite development secret store, configuration storage and
administration are standard capabilities on all supported backends.

## Scope and data model

- Identity is `(tenantId, name)`. Configuration does not require an existing job
  or active lease to be administered.
- Names are nonempty, case-sensitive UTF-8 strings, treated as opaque keys.
  Dots and slashes do not create paths, namespaces, or inheritance.
- Values are JSON values: strings, numbers, booleans, objects, arrays, or null.
  A present null, empty string, false, and zero are distinct from an absent key.
  Preserve JSON numbers without conversion through floating-point Go values;
  public Go values can use `json.RawMessage`.
- There is one configuration map per tenant, with no job, principal, environment,
  or global override layers. Every valid execution lease in that tenant can read
  the entire map, including task leases and supplied leases.
- Each read returns a coherent snapshot, with one opaque tenant revision that
  identifies the whole map. Revisions support equality checks and conditional
  writes; they are not timestamps or retrievable historical versions.
- An unconfigured tenant returns an empty object and a valid initial revision.
  Reading it need not create a database record.
- Configuration is not a place for credentials. It uses ordinary backend
  persistence and backups, with no additional application-level encryption or
  secret-provider authorization. Values are not automatically expanded as
  templates, exported as environment variables, or resolved as secret references.

Keep the initial store small enough to return as one response. Proposed defaults:
1,024 entries per tenant, 256 UTF-8 bytes per name, 64 KiB per encoded value, and
1 MiB per encoded snapshot, including its envelope, and 1 MiB per request body.
JSON nesting is bounded to 128 levels including the snapshot envelope (126
levels inside a value). Bound input bodies as well.
Enforce aggregate limits atomically on writes; never truncate or paginate a
worker snapshot. Limits are deployment configuration, shared across server
instances. If lowered below existing data, reads fail explicitly until an admin
reduces the data or restores the limits; removal must remain possible.

## Public REST administration

Register all routes on every backend, without a feature flag, external provider,
separate database, or development mode. Describe and generate them from
[`openapi/jobdb-runtime.yaml`](../openapi/jobdb-runtime.yaml), with a
`configuration` tag. They are not `/dev` routes or a separate OpenAPI document.

“Always available” means registered and backed by the selected runtime. These
routes use the deployment's existing administrative authentication and tenant
authorization boundary; they do not require a lease or introduce a new admin
token format. The current OpenAPI contract declares bearer authentication, but
the hosting deployment must enforce its access policy. Route registration alone
does not provide that enforcement. A lease token must not by itself authorize
administrative reads or writes. A deployment running without access controls
exposes administration to any caller that can reach its listener.

| Method and path | Operation ID | Result |
| --- | --- | --- |
| `GET /v1/tenants/{tenantId}/configuration` | `getTenantConfiguration` | `200`, complete snapshot and `ETag`. |
| `POST /v1/tenants/{tenantId}/configuration/set` | `setTenantConfigurationValue` | Upsert one name; `204` and resulting `ETag`. |
| `POST /v1/tenants/{tenantId}/configuration/remove` | `removeTenantConfigurationValue` | Remove one name; `204` and resulting `ETag`, including when absent. |

Set/remove bodies follow the secret administration convention, keeping opaque
names out of URL path segments:

```http
POST /v1/tenants/tenant-a/configuration/set
Content-Type: application/json
If-Match: "cfg-42"

{"name":"build.defaults","value":{"region":"eu-west-1","parallelism":4}}
```

```http
POST /v1/tenants/tenant-a/configuration/remove
Content-Type: application/json

{"name":"build.defaults"}
```

`name` is required for both; `value` is required for set, including when null.
Reject unknown envelope fields, invalid UTF-8, malformed JSON, and duplicate
JSON object keys, including inside values. Arbitrary fields inside a stored
object value are allowed. Validate the tenant and limits before committing.

Writes are atomic and acknowledged only after commit. Without `If-Match`,
concurrent writes serialize and the last committed write to a name wins.
With `If-Match`, require one exact revision ETag from an earlier read or write;
a mismatch returns `412 configuration_revision_mismatch` without mutation.
The condition applies to the entire tenant map, so unrelated edits can conflict.
Check the condition before deciding whether a write is a no-op.

Actual changes advance the tenant revision. Setting a JSON-equivalent value
or removing an absent name leaves it unchanged. Use the repository's canonical
JSON equality rules. Revisions must not be reused after deletion and recreation
of entries, including removal of the final entry. Retain the tenant revision
record when its map becomes empty. A retried conditional write may return `412`
if its first attempt committed; the caller can reread to reconcile.

One set can atomically replace an entire structured value, which is sufficient
for a group of settings that must change together. Atomic batches across names,
whole-map replacement, watches, and historical-version reads are deferred.

## CLI

Add commands under `jobdb config` as clients of the public administration API.
They work with a running server on any supported backend and do not open or
modify database files directly.

```sh
jobdb config set codex_api_base_url -t tenant-a -f ./codex.url

jobdb config set build.defaults --tenant tenant-a \
  --value-file ./build-defaults.json --json

jobdb config add build.defaults -t tenant-a -f ./build-defaults.json -j

printf '%s' 'false' | jobdb config set enablePreview -t tenant-a -f - -j

jobdb config remove build.defaults -t tenant-a
```

- `set NAME` adds or replaces a value; `add` is an alias with the same upsert
  behavior, including when the name already exists.
- `remove NAME` removes the value, succeeding if it is already absent.
- `--tenant` (`-t`) is required for both commands.
- `--url` (`-u`) defaults to `http://127.0.0.1:9047` and selects the runtime server.
- `set` requires `--value-file PATH` (`-f PATH`); `--value-file -` reads stdin.
  By default, store the exact UTF-8 input as a JSON string, preserving whitespace
  and newlines; empty input is an empty string. Do not infer types from content:
  `false`, `0`, `null`, and JSON-looking text are all strings without `--json`.
- `set --json` (`-j`) parses exactly one UTF-8 JSON value, not a request envelope
  or a map of entries. Strings in this mode must be JSON-quoted. Reject empty
  input, trailing non-whitespace data, and invalid or duplicate-key JSON before
  sending a mutation; preserve numbers without floating-point loss.
- Enforce value size limits on the JSON encoding, including string escaping.
  Existing JSON-input CLI commands must add `--json` (`-j`); the REST and Go
  configuration APIs continue to accept JSON values unchanged.
- Both commands accept optional `--if-match ETAG`, passing the quoted ETag
  unchanged to `If-Match`. A revision conflict fails without an automatic
  unconditional retry.
- Use the deployment's normal administrative authentication, without a lease
  token or any secret-store key. No backend or enablement flag is required.
- Success output may identify the operation, tenant, name, and resulting ETag,
  but does not print the value. Failures return a nonzero exit status with
  sanitized diagnostics, including for unexpected server responses.

CLI help must show both long and short flags and explain default string input
and explicit JSON input with boolean and object examples. These are standard commands on all backends, not
SQLite development utilities.

## Lease-scoped worker read

```http
GET /v1/tenants/tenant-a/jobs/job-123/leases/lease-456/configuration
X-JobDB-Lease-Token: <lease token>
```

Operation ID: `getConfigurationWithLease`. There is no body, requested-name list,
principal override, or version selector. Normal deployment authentication also
applies, with no additional configuration-store credential for the worker.

```http
HTTP/1.1 200 OK
Content-Type: application/json
Cache-Control: no-store
ETag: "cfg-42"

{
  "revision": "cfg-42",
  "values": {
    "build.defaults": {"region":"eu-west-1","parallelism":4},
    "enablePreview": false,
    "optionalLabel": null
  }
}
```

The admin GET uses exactly the same snapshot schema. `revision` and `values`
are required; the envelope rejects unknown fields. `values` maps names to the
existing public JSON-value representation, and is `{}`, never null, when empty.
ETag is the quoted revision. This version always returns a full `200` snapshot;
it does not implement conditional GET or `304` responses.

Authorization follows the secrets proposal's lease checks:

1. Validate the signed token, its expiry, and its tenant/job/lease binding.
2. Validate current backend ownership and execution eligibility. Expired,
   released, superseded, completed, or cancellation-requested leases fail.
3. Read one committed tenant snapshot, including its revision, atomically.
4. Recheck the request token and current lease before returning values. If the
   lease was lost during the read, discard the result.

Use trusted issued lease identity for embedded callers. Raw job/lease IDs or a
caller-provided worker ID are not sufficient authority. The read neither
acquires nor renews a lease. Bound it by the request context, a configured read
timeout, and token expiry. Keep scheduler locks out of configuration-store I/O.
Validity is guaranteed at the final check; already delivered values cannot be
revoked. Renewal retains the existing token semantics described for secrets.

## Freshness and execution behavior

The server reads current committed state from storage on every admitted read,
with no configuration-result cache. A read begun after an acknowledged mutation
observes that mutation or a later one. An overlapping read may see the preceding
or following snapshot, but never a mixture of values and revisions from different
commits.

The endpoint is **not pinned to a lease or job**. Two reads with the same valid
lease may return different revisions. Lease renewal does not freeze settings;
there is no durable per-lease snapshot record.

The SDK convenience layer provides these semantics:

- Fetch the entire map only in response to an explicit retrieval call. Creating
  a client, context, or execution session performs no configuration read.
- Lease and administration clients issue a fresh read per call. Workflow job and
  task contexts share the first successful complete snapshot for one live job
  attempt, including empty snapshots. Concurrent first reads share a successful
  fetch; failures are not cached.
- Task retries, lease renewal, and waits within the invocation retain the snapshot.
  Whole-job retries, suspension/resumption, process restart, and lease loss create
  a fresh scope, even if the durable job attempt number is unchanged.
- Return caller-owned copies of the map and raw JSON bytes. Caller mutations
  cannot change the cached snapshot or another caller's result.
- Validate lease authority on every worker read. Calls after known lease loss
  or release fail without returning previously retrieved values.

For example, worker A retrieves revision 42, an admin commits revision 43, and
A calls `GetConfiguration(ctx)` again in the same execution session. That call
retrieves revision 43 or a later committed revision. A's previously returned
snapshot still contains revision 42. Child jobs and schedule occurrences perform
no configuration reads until their code explicitly requests them.

An unavailable store fails a requested read. It must not look like an empty map
or cause execution to silently use defaults. A worker that never uses
configuration does not need the extra request.

## Go job and task context access

Expose explicit snapshot retrieval on both `workflow.JobContext` and
`workflow.TaskContext`, so worker code does not need a runtime reference, lease
token, tenant argument, or HTTP client:

```go
// Add to the existing workflow.JobContext interface.
GetConfiguration(ctx context.Context) (ConfigurationSnapshot, error)

// Add a method to the existing workflow.TaskContext struct.
func (tc TaskContext) GetConfiguration(ctx context.Context) (ConfigurationSnapshot, error)

// In workflow/aliases.go, following SecretResolveResult and SecretValue.
type ConfigurationSnapshot = jobdb.ConfigurationSnapshot
```

Both methods return the complete configuration of the executing job's tenant.
Tenant identity comes from the runner's trusted lease binding, not a mutable
context field such as `TaskContext.JobKey`. They expose no administrative write
operation or tenant override. For example, the same access pattern works in a
job worker or task worker (`work` is its supplied workflow context, and `ctx` is
the caller's standard Go context):

```go
snapshot, err := work.GetConfiguration(ctx)
if err != nil {
    return nil, err
}
raw, present := snapshot.Values["build.defaults"]
if !present {
    return nil, fmt.Errorf("missing configuration: build.defaults")
}
var defaults struct {
    Region      string `json:"region"`
    Parallelism int    `json:"parallelism"`
}
if err := json.Unmarshal(raw, &defaults); err != nil {
    return nil, err
}
```

The example is a task-worker fragment; a job worker uses the same lookup and
decoding with its own return type. Typed decoding is application-owned. A
missing map key is distinct from a present JSON null, and no implicit defaults
or string conversions are applied.

Wire both context APIs to the runner's current lease read capability and its
attempt-local cache. Inline tasks and copies of a `TaskContext` share the job's
snapshot. A separately leased invocation starts a fresh scope. Each returned map
and its `json.RawMessage` bytes must be isolated from the cache, backend state,
and other returned snapshots.

Follow the existing secrets invocation boundaries in
[`workflow/secrets.go`](../pkg/workflow/secrets.go) and
[`worker_runner.go`](../pkg/workflow/worker_runner.go). Wire an unexported
`getConfiguration` callback into `TaskContext`, as with `resolveSecrets`, and
implement the job method on `workerRunner`. Bound reads by both the caller's
context and the enclosing job/task invocation, even if the caller supplies
`context.Background()` (normalize nil as the secrets helper does). Task reads
also honor invocation/total timeouts. Captured contexts become unusable after
their invocation ends, even if the parent job's lease remains live.

Each independent read also honors session cancellation and the configured read
timeout. Cancelling one caller does not cancel another independent read. Check
invocation/lease state again after I/O and discard a late result after timeout,
cancellation, yield, or lease loss. Every remotely admitted call makes a server
round trip and checks live lease validity before and after the storage read.
The SDK uses the current renewed lease token for each request; renewal itself
does not fetch configuration.

Use the same Go error conventions as secrets: missing/unbound lease capability
(including a zero-value or manually constructed `TaskContext`) and rescheduled
execution return `jobdb.ErrExecutionLeaseLost`. Invocation cancellation or
timeout returns the analogous `jobdb.ErrConfigurationUnavailable`, with no
snapshot. Replay contexts and `replayReadOnlyRuntime.GetConfiguration` use
`replayMutationError("get configuration")`, preserving
`workflow.ErrReplayShouldNeverMutate` for `errors.Is`, without calling storage.
None of these cases may fall back to an administrative read or an empty success.

Implement the method on every concrete job context, including the replay
context, and update internal mocks and examples. Adding it to `JobContext` is a
source compatibility change for external implementations of that interface;
document the required method in the migration notes. The `TaskContext` addition
does not require callers to change its existing constructor.

## Replay and durable decisions

Live configuration is execution context, not automatically part of job history.
Do not inject it into chapters, task inputs/results, client payload, or exported
job records. Do not change existing task input hashes behind the caller's back.
Read-only replay must never query the live configuration store; a request for
live configuration during replay fails explicitly.

This has a deliberate consequence: configuration is not automatically frozen
for a job's lifetime. Applications must not let an unrecorded live lookup change
workflow branching during replay. For settings that determine durable decisions,
capture the relevant values through an ordinary recorded task result or explicit
job input, and use that recorded data on resume/replay. Recording only the
revision is insufficient because this store does not retain historical values.

Settings used only inside uncached task execution may be retrieved explicitly
when needed. Multiple lookups during one task may return different revisions;
code needing a consistent set of values should use one returned snapshot for
that operation. Completed tasks remain cached when configuration changes. If a
configuration value is intended to affect task cache identity, the application must explicitly
include the selected, durably captured value in task input. Operational settings
that are allowed to vary between attempts need not be part of that identity.

Unlike secrets, configuration values may be deliberately recorded as ordinary
application data when needed. JobDB simply does not persist fetched snapshots
into workflow history automatically.

## Why not deliver it with chapters or lease acquisition?

Current `getChapter` and `listChapters` routes are job-scoped read APIs, also used
by inspection and replay; they do not require a lease token. Chapter ordinal zero
is a durable job-start record created at submission. “First chapter retrieval”
is not a server-side execution event: clients can repeat reads, select ranges,
read in another order, or already have cached history.

| Delivery option | Tradeoff |
| --- | --- |
| Persist in the job-start chapter | Defines submission-time configuration and records all values forever; different semantics from live execution settings. |
| Add to ordinary chapter responses | Couples live tenant data to history reads and needs new authorization and replay rules; a transient envelope still has these issues. |
| Add to `pollWork` / `getJobLease` | Repeats the map across polled jobs, enlarges lease responses, and couples configuration failure to lease acquisition; supplied-lease imports need a separate path anyway. |
| Separate lease-scoped GET | One fresh read per explicit retrieval call, with uniform behavior for all acquisition paths. Recommended. |

Automatic bootstrap delivery is outside this contract: configuration is fetched
only when requested through its retrieval API, independently of chapter reads
and lease acquisition.

## Backend and Go integration

Use a dedicated backend port for tenant configuration, following the separation
already used by `runtime/core` for scheduler, chapters, and schema storage.
The port needs snapshot read and atomic set/remove with optional expected
revision; shared code should own JSON validation, limits, and public errors.
Backend transactions must include the revision check and aggregate-size check.

| Backend / adapter | Implementation |
| --- | --- |
| SQLite | A dedicated table in the existing runtime database stores each tenant's values and revision together in one snapshot row. Mutations replace that row in one write transaction. No development-secret database or key flags. |
| Toy | Tenant maps and revisions protected by a mutex, with deep copies at API boundaries. State has the backend's existing in-memory lifetime. |
| Runtime core / Postgres | Add a public configuration-store port to core wiring. Implement it in the separately maintained `pgjobdb` backend with tenant-level mutation serialization and coherent transactional reads. |
| Remote | Implement admin operations and lease reads through generated HTTP APIs. Every retrieval makes a new request; no local configuration cache or fallback map. |

The Postgres implementation lives outside this repository, as documented in the
[README](../README.md#postgres); supporting SQLite and toy alone does not fulfill
the all-backend requirement. Configuration belongs in normal backend backups,
but deleting a job or exporting its history does not delete or export the tenant
configuration store.

### Match the implemented secrets access APIs

The current secrets implementation adds methods directly to
[`WorkflowRuntime` and `ExecutionLease`](../pkg/jobdb/runtime.go), with matching
job/task methods and workflow result aliases. Configuration should follow that
required-interface pattern, not introduce an optional lease capability or
type-assertion helper. Use the same verb at every Go layer: `GetConfiguration`.
`Get` describes reading the whole stored map; secrets retain `ResolveSecrets`
because they resolve a requested subset through a provider.

| Layer | Existing secrets API | Proposed configuration API |
| --- | --- | --- |
| Runtime | `ResolveSecrets(ctx, ResolveSecretsRequest)` | `GetConfiguration(ctx, GetConfigurationRequest)` |
| Execution lease | `ResolveSecrets(ctx, names)` | `GetConfiguration(ctx)` |
| Job context | `ResolveSecrets(ctx, names)` | `GetConfiguration(ctx)` |
| Task context | `ResolveSecrets(ctx, names)` | `GetConfiguration(ctx)` |
| Workflow result alias | `SecretResolveResult = jobdb.SecretResolveResult` | `ConfigurationSnapshot = jobdb.ConfigurationSnapshot` |
| Lease HTTP operation | `resolveSecretsWithLease` | `getConfigurationWithLease` |

Public types in `pkg/jobdb/configuration.go` and additions to the existing
interfaces in `pkg/jobdb/runtime.go`:

```go
type ConfigurationSnapshot struct {
    Revision string                     `json:"revision"`
    Values   map[string]json.RawMessage  `json:"values"`
}

// Transport request, analogous to ResolveSecretsRequest without Names.
type GetConfigurationRequest struct {
    JobKey     JobKey
    LeaseID    string
    LeaseToken string
}

type WorkflowRuntime interface {
    // ...existing methods...
    GetConfiguration(context.Context, GetConfigurationRequest) (ConfigurationSnapshot, error)
}

type ExecutionLease interface {
    // ...existing methods...
    GetConfiguration(context.Context) (ConfigurationSnapshot, error)
}
```

Mirror [`ResolveSecretsRequest`](../pkg/jobdb/secrets.go)'s redacted diagnostic
formatting and JSON marshaling for `GetConfigurationRequest`, since it carries
a bearer capability. The configuration snapshot remains ordinary JSON data.
The request is a Go transport type, not a GET body: the remote adapter puts
identity in the URL and `LeaseToken` in `X-JobDB-Lease-Token`.

Follow [`remote/secrets.go`](../pkg/jobdb/runtime/remote/secrets.go): the remote
lease constructs the request from its trusted identity and current token, and
the runtime adapter makes the HTTP call. The server validates that token and
passes verified claims to the backend runtime method. Local runtime methods
must require those trusted claims rather than trust raw request IDs/token text;
embedded callers use their issued lease's method. This is the same boundary
implemented by [`secretsbroker.FromRequest`](../pkg/jobdb/internal/secretsbroker/broker.go).
Configuration does not need the secrets provider's names, principal, or job
metadata inputs: its authorization is tenant-wide after lease validation.

Mirror [`leaseSession.ResolveSecrets`](../pkg/workflow/lease_session.go): capture
the current renewed lease under the session lock, release the lock during the
read, then recheck session state before returning. Slow configuration reads must
not block heartbeats. Shared invocation-context plumbing can be extracted from
the secrets helper; configuration should not invoke the secret provider or reuse
secret-specific result validation/error codes.

Add a separate public administration interface for `GetTenantConfiguration`,
`SetTenantConfigurationValue`, and `RemoveTenantConfigurationValue`. Keep those
methods distinct from lease-authorized `GetConfiguration`; do not expose them
through job/task contexts. Unlike the optional SQLite development-secret admin
capability, all configuration backends must provide administration. Core wiring
requires the store; missing wiring is a startup error, never an ephemeral
fallback. Register every route unconditionally on supported servers.

Adding methods to `WorkflowRuntime`, `ExecutionLease`, and `JobContext` is a
source compatibility change, as with the implemented secrets methods. Update
all backend implementations, wrappers, mocks, and third-party migration notes,
including `pgjobdb`; do not hide missing implementations behind type assertions.

## Errors and OpenAPI requirements

Every configuration response, including administration and errors, uses
`Cache-Control: no-store`. Use the existing public error envelope with fixed
messages; do not echo values, tokens, request bodies, or raw backend errors.
Do not log response bodies or mutation values by default.

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | `invalid_configuration_request` | Invalid tenant, name, JSON, envelope, or conditional-write header. |
| 413 | `configuration_too_large` | Request, value, entry count, or resulting tenant map exceeds limits; no mutation. |
| 409 | `execution_lease_lost` | Lease read with missing/invalid token, binding mismatch, missing job, expiry, cancellation, or inactive lease. |
| 412 | `configuration_revision_mismatch` | Admin write's expected revision is stale. |
| 503 | `configuration_unavailable` | Transient storage failure or read timeout while the lease remains valid. |
| 500 | `configuration_read_too_large` | Existing snapshot exceeds current read limits; no partial values. |
| 500 | `configuration_failed` | Unexpected storage failure or invalid stored data. |

Deployment authentication/authorization may also return `401` or `403` before
dispatch. There is no missing-configuration `404`, disabled-store `501`, or
per-name denial: empty tenants succeed and every valid tenant lease reads all
names. Document exact request/response schemas, required lease headers, ETags,
conditional-write behavior, and error responses in the main OpenAPI document.

## Implementation sequence and acceptance

1. Add public value/request types, shared validation, backend port, and main
   OpenAPI schemas/routes; regenerate the remote bindings.
2. Implement SQLite and toy storage and core wiring; coordinate the corresponding
   `pgjobdb` implementation before claiming backend parity.
3. Implement admin handlers and embedded/remote lease reads, including both
   live-lease checks and forwarding through supplied/renewed lease wrappers.
4. Add `GetConfiguration(ctx)` on job and task contexts with a shared live
   job-attempt cache backed by the current lease, and add replay guards. Wire inline
   and separately leased task paths, update context implementations/mocks, and document the
   `WorkflowRuntime`, `ExecutionLease`, and `JobContext` interface changes.
   Existing chapter and lease acquisition wire formats remain unchanged.
5. Add `jobdb config set`/`add` and `remove` through the administration API, with
   file/stdin JSON input, long/short flags, and conditional-write support.
6. Run shared conformance tests on embedded and HTTP adapters covering:
   tenant isolation; empty maps; JSON/null fidelity; persistence where supported;
   no-op and conditional mutations; concurrent revisions and size limits;
   coherent snapshots; fresh reads after commits; stale/cancelled leases and
   lease loss during reads; repeated lease calls issuing independent reads; and replay
   making zero live configuration reads.

Also verify that all administration routes appear in generated public clients
and are registered on each backend without an enablement flag, and that missing
storage cannot silently yield an empty successful result. Tests should demonstrate
the chosen update semantics: a second direct lease read sees an acknowledged
edit, while workflow context reads retain their live job-attempt snapshot. Starting
jobs/tasks, importing or renewing leases, and reading chapters must make zero
configuration reads unless application code explicitly requests configuration.

Go context tests must exercise both job and task access on embedded and remote
runtimes, including supplied leases. Verify automatic tenant binding, one storage
read per successful initial fetch shared by job/inline-task and concurrent
callers; isolation of returned values; independent cancellation; cached empty
snapshots; fresh reads after failures and whole-job retries/resumptions; renewal;
rejection after known lease loss; and
explicit failures for replay and unbound contexts without any live store read.
Follow the existing [secrets context tests](../pkg/workflow/secrets_test.go) for
captured contexts after invocation completion, task timeout while its job lease
remains live, cancellation with a late result, and exact `errors.Is` behavior.
Add the configuration equivalent of the
[slow secret read / renewal test](../pkg/workflow/lease_session_test.go) to prove
storage I/O does not hold the heartbeat lock. Verify direct runtime calls with
raw identity but no trusted claims fail, while issued embedded leases and
verified HTTP calls succeed.

CLI tests must cover set/add replacement, idempotent removal, required tenant,
server URL selection, equivalent long/short flags, file and stdin input, JSON
type and number preservation, rejection of invalid input before mutation,
conditional-write conflicts, and nonzero sanitized errors. Exercise the same
HTTP commands against each backend; no command may depend on SQLite storage or
development-secret configuration.

## Implementation notes

The public `configuration.Store` port and `configuration.Service` provide shared
validation and administration. `configuration.MemoryStore` is toy storage;
`configuration.NewSQLStore` supports SQLite and Postgres with an additive
`jobdb_configuration` table. The snapshot is one JSON byte document per tenant,
so a read cannot mix revisions and values even at READ COMMITTED. Postgres uses
`BYTEA` to preserve arbitrary JSON numbers, and locks the tenant row for updates.
SQLite acquires its write lock before reading the row for an update.

Configure bounds through `jobdb.ConfigurationLimits`, SQLite/core config fields,
or `toy.WithConfigurationLimits`. Remote clients can use
`remote.WithConfigurationLimits` when raising server defaults. All replicas must
agree on limits. CLI clients use the defaults; `JOBDB_BEARER_TOKEN` optionally
supplies the deployment's normal bearer credential.

The [companion pgjobdb patch](pgjobdb-configuration.patch) wires the public SQL
store into the separately maintained runtime and updates its integration tests.
It was tested against pgjobdb commit
`2befb710b55c3ce4aee0a6f1d01f5a117fdbdee8` using a Go workspace containing both
repositories. Apply it when upgrading pgjobdb to this JobDB API; the Postgres
binary must be rebuilt to expose the configuration routes. No released pgjobdb
binary is changed by updating this repository alone.

Validation covers the complete JobDB test suite, configuration race tests, and
real Postgres storage plus embedded/HTTP runtime conformance. The companion's
configuration and native-core tests pass. Its broader workflow suite has existing
timeouts in `TestChapterConstraintsAcrossEngines` and
`TestRestartPrerequisitesCheckedAtRestartExtra`; both were reproduced using the
unmodified repositories at their pre-change commits.
