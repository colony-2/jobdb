# SQLite development secrets

JobDB's SQLite backend can store tenant secrets in a separate encrypted SQLite
file. Other backends currently return `no secret provider configured` for secret
resolution. The provider interface is available for future integrations.

Create an encryption key once and keep it outside database backups:

```sh
umask 077
openssl rand -base64 32 > dev-secrets.key
jobdb sqlite --db jobdb.db -s dev-secrets.db -k dev-secrets.key
```

Keep the same key when restarting. A missing or incorrect key is an error.
The key file contains a base64-encoded 32-byte key. Tenant IDs and secret names
are plaintext; values use AES-256-GCM, including before writes reach SQLite's
journal. The secret database is created with mode `0600`.

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
| `-s` | `--dev-secrets-db` | Server's separate secret database |
| `-k` | `--dev-secrets-key-file` | Server's encryption key file |

SQLite automatically registers the development admin API on its runtime HTTP
listener, with no admin token or lease required. Anyone able to reach that
listener can set/remove tenant secrets. Without store/key configuration the
routes return `503 dev_secrets_not_configured`. Other backends return `404`.
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

Go workers holding an embedded or remote lease use:

```go
result, err := jobdb.ResolveLeaseSecrets(ctx, lease, []string{"github_token"})
```

The development provider permits any valid lease in the tenant to read that
tenant's requested secrets. It does not apply per-principal or per-job policies.
Missing names are omitted; callers must check for required names. Every call
reads committed store state. Secret responses are never cached or persisted by
JobDB and never become workflow chapters. Replay rejects resolution.

JobDB checks lease validity before and after provider I/O. Cancellation, release,
or expiry prevents further reads, but cannot revoke values already delivered.
Provider requests include job application metadata and the signed token's
optional principal (`nil` for current tokens without that claim). REST callers
cannot supply a principal. Provider errors and client transport errors are
sanitized before reaching workflow failure reporting.

SQLite library users configure `DevSecretsDB`, `DevSecretsKeyFile`, and optional
`SecretLimits` on `sqlite.Config` passed to `NewFromConfig`. Limits default to
64 KiB per resolution request, 1 MiB per response/admin set request, and a
10-second provider timeout. Remote Go clients currently use the same default
request/response bounds. This is development/prototype storage, not a production
secret manager.
