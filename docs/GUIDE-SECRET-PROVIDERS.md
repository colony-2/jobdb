# Implementing and running secret providers

Implement `jobdb.SecretProvider` to resolve secrets in process, or serve the same
implementation from a separate executable using `secretplugin.Serve`. A provider
collection calls every member and selects the first returned entry for each name.
The built-in SQLite secrets store and external plugins can be used together.

See the [complete, compilable development provider](../examples/secret-provider/main.go)
and the [implementation design](PROPOSAL-Secret-Provider-Plugins.md).

## Provider contract

```go
type SecretProvider interface {
    ResolveSecrets(context.Context, SecretResolveRequest) (SecretResolveResult, error)
}

type SecretResolveRequest struct {
    TenantID    string
    JobID       string
    Principal   *string
    AppMetadata json.RawMessage
    Names       []string
    Lease       SecretLeaseContext
}

type SecretLeaseContext struct {
    LeaseID            string
    WorkerID           string
    LeaseExpiresAt     time.Time
    AuthorizedNotAfter time.Time
}
```

The broker supplies verified tenant, job, principal, and lease identity. Job
application metadata comes from the stored job and can contain submitter-provided
data; it cannot extend lease authority. The provider never receives a lease token.

Return only requested names you authorize. Omit denied or unknown names, return
an empty map if none apply, and preserve empty strings as valid values. Backend
failures are errors, not omissions: return `jobdb.ErrSecretsUnavailable` for
transient failures. Any error discards the entire response. Raw error messages
are removed by the SDK; log neither credentials nor backend response bodies.

Calls can be concurrent. Honor the supplied context throughout backend work,
return an owned snapshot, and do not mutate results after returning. The SDK
passes deadlines/cancellation over gRPC and limits concurrent backend calls.

## Mint credentials bounded by the lease

`LeaseExpiresAt` describes the current backend lease. `AuthorizedNotAfter`
also accounts for the presenting capability's expiry and is the upper bound
for credential issuance. After renewal, an older handle/token can still have an
earlier `AuthorizedNotAfter`. Use the renewed handle for renewed authority.

A dynamic provider should:

1. Authorize the requested name for the tenant, job, principal, and intended use.
2. Reject issuance if the verified lease identity or authority deadline is absent.
3. Ask the issuer for an expiry no later than `AuthorizedNotAfter`, allowing for
   clock precision, skew, and any expiration leeway used by the credential verifier.
4. Check the actual expiry returned by the issuer, then return:

```go
jobdb.SecretValue{
    Value: credential,
    ExpiresAt: &actualExpiry,
    LeaseBound: true,
}
```

**The credential itself must enforce that expiry.** Changing `ExpiresAt` in the
response cannot shorten a signed token or revoke an external password. The
broker rejects incorrect metadata, but cannot inspect arbitrary credential
formats. The [example](../examples/secret-provider/main.go) signs a JWT whose
`exp` equals the returned expiry, and its test checks that verification fails
after the presenting capability's deadline.

`ctx.Deadline()` limits how long resolution may take; it is not the lifetime of
the credential. A provider cache must retain authorization and lifetime checks
for the current request. Do not reuse a token across tenants or lease attempts
merely because the secret name matches. Expiry does not provide immediate
revocation of credentials already delivered when a job is canceled early.

Static values can retain `LeaseBound: false`, with optional `ExpiresAt`. This is
how SQLite development passwords coexist with short-lived external tokens.
All contributions, including lower-priority values, are validated. The broker
rechecks the live lease after provider work, so a revoked lease cannot receive
a late successful response.

## Turn a Go provider into a plugin

The executable initializes its own backend and calls:

```go
import secretplugin "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin"

func main() {
    provider := initializeProvider() // Your implementation of jobdb.SecretProvider.
    secretplugin.Serve(provider)
}
```

The SDK implements protobuf conversion, safe error encoding, gRPC admission,
and the HashiCorp handshake. Initialize before `Serve`, and write startup
diagnostics to stderr: stdout is reserved for the handshake until `Serve` takes
over. The JobDB CLI forwards provider stdout/stderr to its server log, tagged
with the provider ID and stream. Providers should not print credentials.
Provider-specific configuration belongs in files or explicitly passed environment
variables. Do not pass credentials as command-line arguments.

The transport uses local subprocesses, gRPC, automatic mutual TLS, and application
protocol version 1. The [protobuf contract](../proto/jobdb/secrets/plugin/v1/provider.proto)
is separate from job history. A plugin is trusted executable code, not a sandbox.
For non-Go implementations, the protobuf service alone is insufficient: they
must also implement the go-plugin handshake and control/health protocol.

## Development: local SQLite plus the example plugin

Build the example from the JobDB repository:

```sh
mkdir -p bin .dev-secrets
go build -o "$PWD/bin/jobdb-example-secret-provider" ./examples/secret-provider
openssl rand 32 > .dev-secrets/signing.key
chmod 600 .dev-secrets/signing.key
```

Create `.dev-secrets/issuer.json`:

```json
{
  "tenant": "dev",
  "audience": "my-development-service",
  "signing_key_file": "signing.key"
}
```

This example authorizes `dev_token` for live jobs in the configured tenant and
issues an HS256 JWT. The consuming development service must verify its signature,
issuer, audience, expiry, and appropriate job/tenant identity. Adapt the policy
and issuer for your production backend.

Create `.dev-secrets/providers.json`, replacing `/absolute/path/to/jobdb` with
your checkout path:

```json
{
  "version": 1,
  "max_parallel": 4,
  "development_admin_provider": "local",
  "log_selections": true,
  "providers": [
    {
      "id": "local",
      "type": "sqlite",
      "path": "../jobdb.db.secrets.db"
    },
    {
      "id": "issuer",
      "type": "plugin",
      "command": "/absolute/path/to/jobdb/bin/jobdb-example-secret-provider",
      "args": ["--config", "/absolute/path/to/jobdb/.dev-secrets/issuer.json"]
    }
  ]
}
```

Run the server and manage local values using the normal commands:

```sh
jobdb sqlite --db ./jobdb.db --secret-providers-file .dev-secrets/providers.json
# In another terminal:
jobdb secrets set database_password --tenant dev --value-file ./password.txt
```

Jobs can resolve `database_password` and `dev_token` in one request. Setting a
local `dev_token` overrides the plugin's entry because `local` comes first.
Removing it exposes the external entry on the next fresh/uncached resolution.
The CLI's `set/remove` operations target only `local`; writes never reach plugins.

Configuration rules:

- `version` must be 1. Member IDs must be unique, 1–128 characters, start with an
  ASCII letter or digit, and otherwise contain letters, digits, `_`, `-`, or `.`.
- `sqlite.path` is relative to the provider file's directory; `:memory:` is supported.
  Pointing at the normal `.secrets.db` path reuses existing development secrets.
- `plugin.command` must be absolute. `args` are passed directly, without a shell;
  argument paths are not rewritten, so use absolute configuration paths.
- Optional `pass_env` lists host environment names to pass. Other host variables
  are not inherited; plugin protocol variables cannot be overridden this way.
- Optional `sha256` pins the expected executable digest as 64 hex characters.
- `development_admin_provider`, when supplied, must name a SQLite member.
  Without it, configured SQLite runtimes retain disabled admin responses (503),
  and toy has no admin routes (404). Without a provider file, existing defaults
  and SQLite development administration are unchanged.
- `log_selections` emits job, tenant, secret name, and selected provider ID only.
  It defaults to false. Provider selection does not imply a later broker check
  accepted the result; these are selection diagnostics, not delivery receipts.

An explicit provider file selects exactly its members. There is no automatic
SQLite fallback, and a failed plugin is never treated as an empty response.

### Startup and provider diagnostics

The CLI reports each provider as it starts and becomes ready, including its ID,
type, priority (1 wins first), and whether it handles development administration.
The collection summary reports `development_admin_provider=none` when set/remove
is disabled. Without a provider file, SQLite reports its automatic development
provider; toy reports that no providers are configured.

Plugin stdout and stderr appear as `secret provider output` log entries with
`provider`, `stream`, and `output` fields. Startup stderr is included even if the
plugin fails to start. Startup failures also include the transport diagnostic
in the server log. These diagnostics are always enabled by the CLI, independently
of `log_selections`, and continue after automatic plugin restarts. Output may be
split into chunks; partial lines are emitted immediately once received.

These are operator logs, not job history or secret-resolution responses. Returned
errors remain sanitized, and JobDB does not log secret response values or plugin
arguments/environment. A plugin's own diagnostic text is forwarded as written.

## Embed a collection in another host

Use `secretplugin.Start(ctx, ProcessConfig{Command: absolutePath, Args: args})`
to obtain a `*secretplugin.Process` implementing `SecretProvider`. The context
bounds startup only. Keep the process handle and close it on every exit path.
Set `ProcessConfig.Logger` to a `*slog.Logger` to receive subprocess output and
startup diagnostics in an embedded host; nil keeps those diagnostics disabled.
Use a logger with provider ID attributes to distinguish multiple plugins.

```go
collection, err := secretproviders.New([]secretproviders.Member{
    {ID: "local", Provider: localSQLiteStore},
    {ID: "external", Provider: pluginProcess},
}, secretproviders.Options{MaxParallel: 4})
if err != nil { return err }

runtime, err := sqlite.NewFromConfig(ctx, sqlite.Config{
    DBPath: "jobs.db", SecretProvider: collection,
})
```

The same collection works with `toy.WithSecretProvider` and
`runtimecore.Config.SecretProvider`. Runtimes and collections do not close
injected providers. Drain requests, close your runtime, then close the stores
and plugin handles you opened. `Process.Close()` is idempotent, cancels active
calls, and prevents restart.

All members are called, even if an earlier provider supplies every name. Results
are merged by configured order, not completion order; an empty value counts as
present. An omission grants nothing but cannot veto another provider's grant.
Any error or invalid response discards all values, including errors from shadowed
providers. Successful lookup therefore requires all members to be available.

Each selected entry carries `SecretValue.ProviderID`, assigned by the collection.
Value, version, expiry, lease binding, and source stay together. Existing workflow
caches preserve the source with the value; `ResolveSecretsFresh` refreshes both.
See [lease-bound secrets and fresh reads](GUIDE-LEASE-BOUND-SECRETS.md).
There are no new secret chapters and offline replay still rejects secret reads.

## Limits and failure handling

Defaults are a 10-second lookup timeout, 64 KiB name-list request limit, 1 MiB
JSON result limit, 8 MiB protobuf message limit (including metadata), 10-second
process startup timeout, and 32 concurrent calls per process. Collection
parallelism defaults to 4 and also limits active member calls across its requests.
Queueing consumes the caller's deadline. Both SDK sides independently enforce
their limits; increase both `ProcessConfig` and `ServeOptions` when needed.

A crashed child makes active calls fail with a safe error. Later lookups restart
it through a serialized path with backoff from 250 ms to 30 seconds on repeated
startup failures. The SDK does not retry the failed resolution: providers may
issue credentials or write audits. Configuration is fixed for the process
handle's lifetime; hot reload and reattachment are not supported.

## Development checks

Run provider, CLI, example, and integration coverage:

```sh
go test ./pkg/jobdb/secrets/... ./pkg/jobdb/cli ./examples/secret-provider
go test -race ./pkg/jobdb/secrets/... ./pkg/jobdb/cli ./examples/secret-provider
go test -tags=integration ./...
```

The subprocess suite covers actual handshakes, deadline propagation, crashes,
restart, safe failures, mixed-provider lookup over SQLite/toy and HTTP/embedded
transports, old capabilities after renewal, and workflow cache refreshes. The
CLI suite checks local administration and cleanup on startup/listener failure.

To regenerate the plugin wire code with the tested tool versions:

```sh
# buf.gen.yaml invokes pinned Go and gRPC code generators through go run.
go run github.com/bufbuild/buf/cmd/buf@v1.66.0 generate --path proto/jobdb/secrets/plugin/v1
go generate ./pkg/jobdb/internal/runtimeapi
```
