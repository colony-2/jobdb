# SQLite development secrets

JobDB's SQLite backend automatically opens a development secrets database next
to the main database: `jobdb.db` uses `jobdb.db.secrets.db`. No secret-store flags
or encryption key are required. Values are stored as plaintext, and new files
use mode `0600`. In-memory SQLite runtimes use an in-memory secrets store.
Other backends default to `no secret provider configured`; library users can
inject any `jobdb.SecretProvider` independently of job storage.

```sh
jobdb sqlite --db jobdb.db
# Or use the default SQLite mode:
jobdb --db jobdb.db
```

The same naming rule applies to SQLite DSNs and caller-owned database handles.
The secrets file remains separate from the workflow database and job exports.

In another terminal, add or replace a value and later remove it:

```sh
jobdb secrets set github_token -t tenant-a -f ./github-token.txt
jobdb secrets remove github_token -t tenant-a
```

`add` is an alias for `set`. `-f -` reads stdin, and values retain exact UTF-8
content, including trailing newlines. Empty values are supported. The CLI never
prints values and does not accept them as command-line arguments.

| Short | Long | Meaning |
| --- | --- | --- |
| `-t` | `--tenant` | Required tenant for set/remove |
| `-f` | `--value-file` | Required value file for set; `-` reads stdin |
| `-u` | `--url` | Runtime URL; defaults to `http://127.0.0.1:9047` |

SQLite automatically registers the development admin API on its runtime HTTP
listener, with no admin token or lease required. Anyone able to reach that
listener can set/remove tenant secrets. The default store is ready immediately.
With an injected provider, these routes return `503 dev_secrets_not_configured`;
manage that provider directly. Other backends return `404`.
See the separate [development OpenAPI contract](../openapi/jobdb-dev-secrets.yaml).

Runners read through the normal lease API:

```http
POST /v1/tenants/tenant-a/jobs/JOB/leases/LEASE/secrets/resolve
X-JobDB-Lease-Token: TOKEN
Content-Type: application/json

{"names":["github_token"]}
```

```json
{"secrets":{"github_token":{"value":"example"}}}
```

Job and task workers resolve secrets directly through their workflow context:

```go
result, err := taskContext.ResolveSecrets(ctx, []string{"github_token"})
if err != nil {
    return nil, err
}
token, allowed := result.Secrets["github_token"]
if !allowed {
    return nil, errors.New("required credential unavailable")
}
// Use token.Value for the external call; keep it out of workflow output.
```

`JobContext.ResolveSecrets` has the same signature. Workers holding an embedded
or remote `ExecutionLease` can call `lease.ResolveSecrets(ctx, names)` directly.
There are no optional capability assertions or resolution helper functions.
Both context methods use the current lease, including renewal, and reject reads
after their invocation ends. Offline replay rejects secret reads. Put reads
inside tasks to allow replay to reuse task outcomes without retrieving secrets.

The development provider permits any valid lease in the tenant to read that
tenant's requested secrets. It does not apply per-principal or per-job policies.
Missing names are omitted; callers must check for required names. Each direct
lease/provider call reads committed store state. Workflow contexts cache values
for a job execution attempt, as described in
[workflow read caching](MIGRATION-Workflow-Read-Caching.md). Secret responses
never become workflow chapters. Replay rejects resolution.

JobDB checks lease validity before and after provider I/O. Cancellation, release,
or expiry prevents further reads, but cannot revoke values already delivered.
Provider requests include job application metadata and the signed token's
optional principal (`nil` for current tokens without that claim). REST callers
cannot supply a principal. Provider errors and client transport errors are
sanitized before reaching workflow failure reporting.

SQLite library users need only their normal `sqlite.Config` or caller-owned
SQLite handle. Optional `SecretLimits` default to
64 KiB per resolution request, 1 MiB per response/admin set request, and a
10-second provider timeout. Remote Go clients currently use the same default
request/response bounds. This is development/prototype storage, not a production
secret manager.

## Independent provider configuration

Implement `jobdb.SecretProvider.ResolveSecrets(context.Context,
jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error)` to supply another
provider. It receives tenant, job, verified principal (or nil), stored application
metadata, and requested names. It owns both access policy and retrieval. Return
only the allowed subset; any error discards all values. Providers must honor
cancellation and support concurrent calls. JobDB applies common request/result
bounds, safe errors, and live-lease checks around every provider call.

Inject a caller-owned provider with:

- `sqlite.Config{SecretProvider: provider}` or `sqlite.WithSecretProvider(provider)`.
- `toy.WithSecretProvider(provider)`.
- `runtimecore.Config{SecretProvider: provider}` alongside the storage ports.

Configure `SecretLimits` in either runtime config, or use
`WithSecretLimits` for option-based constructors. Remote clients use the provider
configured on their server, through the same lease and workflow context API.

The SQLite development provider is public at
`github.com/colony-2/jobdb/pkg/jobdb/secrets/sqlite` (package `sqlitesecrets`):

```go
provider, err := sqlitesecrets.Open(ctx, "secrets.db", jobdb.SecretLimits{})
if err != nil {
    return err
}
defer provider.Close()

runtime := toy.New(toy.WithSecretProvider(provider))
// The same provider can be injected into SQLite or runtime core.
```

Injected providers stay caller-owned: closing a runtime does not close them.
Without an injected provider, the SQLite runtime opens and owns its default
secrets store and closes it with the runtime. An injected provider takes precedence.
The CLI keeps its SQLite-only administration routes and does not configure a
provider for other backend modes.

## Moving from the encrypted development store

Remove `--dev-secrets-db` / `-s` and `--dev-secrets-key-file` / `-k` from server
commands, and remove `DevSecretsDB` / `DevSecretsKeyFile` from Go configuration.
Direct users of `sqlitesecrets.Open` must remove the key-file argument.

Repopulate the automatic store using `jobdb secrets set` and the original secret
values. Old encrypted files are not migrated or modified; opening one as the new
store fails with a safe format error. Keep any old file at the automatic path
under a different name before starting the runtime.
