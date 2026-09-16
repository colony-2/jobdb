# jobdb

`jobdb` is a runtime server for durable jobs. The installed `jobdb` command
serves the JobDB runtime REST API over HTTP using one of the available storage
backends.

Use the server when you want a standalone runtime process that workers and other
clients can talk to over the remote runtime protocol.

## Installation

Install the CLI with npm:

```bash
npm install -g @colony2/jobdb
```

Verify the command is available:

```bash
jobdb --help
```

## Container Image

Tagged releases publish a multi-platform image for Linux AMD64 and ARM64:

```text
ghcr.io/colony-2/jobdb:<release-tag>
```

The image has a fixed `jobdb serve` entrypoint for stateless production use. It
always stores runtime records in Postgres and artifact bytes larger than the
inline threshold in S3, Google Cloud Storage, or Azure Blob Storage. The
supported container interface cannot select SQLite, the toy runtime, local
filesystem storage, or memory storage. Those modes remain available when using
the installed `jobdb` binary outside the container.

Required configuration:

| Variable | Default | Description |
| --- | --- | --- |
| `JOBDB_POSTGRES_DSN` | none | Postgres connection string. |
| `JOBDB_BLOB_STORE_URI` | none | Remote `s3://`, `gs://`, or `azblob://` bucket/container URI. |
| `JOBDB_LEASE_TOKEN_SIGNING_KEY` | none | Base64-encoded signing key containing at least 32 random bytes. |
| `JOBDB_LEASE_TOKEN_SIGNING_KEY_FILE` | none | Mounted file containing the signing key; use instead of the direct variable. |
| `JOBDB_LISTEN` | `0.0.0.0:8080` | HTTP listen address. |
| `JOBDB_MAX_INLINE_ARTIFACT_BYTES` | `4096` | Largest artifact size retained inline in Postgres. |

All replicas in one deployment must use the same signing key. Generate one and
store it in the deployment's secret manager rather than in an image or manifest:

```bash
openssl rand -base64 32
```

For example, with Postgres and the S3 bucket already available:

```bash
docker run --rm \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --publish 8080:8080 \
  --env JOBDB_POSTGRES_DSN \
  --env JOBDB_BLOB_STORE_URI='s3://jobdb-artifacts?region=us-east-1' \
  --env JOBDB_LEASE_TOKEN_SIGNING_KEY \
  ghcr.io/colony-2/jobdb:<release-tag>
```

The image runs as UID/GID `65532:65532`, declares no volume, and supports a
read-only root filesystem. It exposes `GET /healthz` and includes an automatic
container health check. Operators can also check any JobDB server directly:

```bash
jobdb healthcheck http://jobdb.example:8080
```

Use an exact release tag or image digest in production. Provider credentials
are resolved by the cloud SDKs; prefer workload identity, instance/task roles,
or mounted secrets over credentials embedded in the blob URI.

## Quick Start

Run the default SQLite-backed server:

```bash
jobdb --listen 127.0.0.1:9047 --db jobdb.db
```

This starts the runtime API at `http://127.0.0.1:9047`. SQLite is the default
backend and persists runtime state in `jobdb.db`; large artifacts are stored in a
blob bucket URL that defaults to a local `blobfs://` directory at `<db>.blobs`.

The explicit SQLite subcommand is equivalent:

```bash
jobdb sqlite --listen 127.0.0.1:9047 --db jobdb.db
```

Stop the server with `Ctrl-C` or `SIGTERM`; the command shuts the HTTP server
down before closing backend resources.

## Backend Options

### SQLite

SQLite is the default embedded durable backend.

```bash
jobdb sqlite \
  --listen 127.0.0.1:9047 \
  --db ./jobdb.db \
  --blob-store-uri 'file:///var/lib/jobdb/blobs'
```

Flags:

- `--db`: SQLite database path. Defaults to `jobdb.db`.
- `--blob-store-uri`: blob bucket URL for large artifacts. The `jobdb`
  executable includes Go CDK providers, so it supports `file://`, `gs://`,
  `s3://`, and `azblob://`; defaults to local `blobfs://` at `<db>.blobs`.
- `--blob-dir`: legacy directory shortcut for local large artifacts. Ignored
  when `--blob-store-uri` is set.
- `--sqlite-dsn`: SQLite DSN. Overrides `--db` and `JOBDB_SQLITE_DSN`.
- `--listen`: HTTP listen address. Defaults to `127.0.0.1:9047`.

Environment:

- `JOBDB_SQLITE_DSN`: SQLite DSN used when `--sqlite-dsn` is not set.

### Toy

The toy backend is in-memory. It is useful for local experiments and tests, not
for durable execution.

```bash
jobdb toy --listen 127.0.0.1:9047
```

### Direct

The direct backend uses JobDB's `pkg/jobdb/runtime/direct` adapter with the
`github.com/colony-2/pgjobdb/pkg/pgjobdb` scheduler for Postgres job records,
and a blobstore URI for large artifact bytes. It installs or verifies the
`pgjobdb` schema on startup.

The first start requires a brand-new empty Postgres database. Existing `pgwf`
or JobDB chapter data cannot be adopted; provision a new database for this
release.

```bash
JOBDB_POSTGRES_DSN='postgres://user:pass@localhost:5432/jobdb?sslmode=disable' \
  jobdb direct --blob-store-uri 's3://jobdb-artifacts?region=us-east-1' --listen 127.0.0.1:9047
```

Flags:

- `--postgres-dsn`: Postgres DSN for `pgjobdb` state.
- `--blob-store-uri`: blob bucket URL for large artifacts. The `jobdb`
  executable includes Go CDK providers, so it supports `file://`, `gs://`,
  `s3://`, and `azblob://`; defaults to local `blobfs://`.
- `--listen`: HTTP listen address. Defaults to `127.0.0.1:9047`.

Environment:

- `JOBDB_POSTGRES_DSN`: Postgres DSN used when `--postgres-dsn` is not set.

Blob URL examples:

- Local filesystem: `file:///var/lib/jobdb/blobs` or legacy
  `blobfs:///var/lib/jobdb/blobs`.
- Google Cloud Storage: `gs://jobdb-artifacts?prefix=prod/`.
- Amazon S3: `s3://jobdb-artifacts?region=us-east-1&prefix=prod/`.
- Azure Blob Storage: `azblob://jobdb-artifacts?prefix=prod/`.

Credential resolution is handled by the Go CDK provider drivers, so `jobdb`
does not need separate credential flags:

- GCS uses Application Default Credentials. Use
  `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, or
  attached Google Cloud service account credentials in VM/container
  environments.
- S3 uses the AWS SDK for Go v2 configuration chain. Provide `AWS_REGION` and
  credentials through environment variables, shared `~/.aws/config` and
  `~/.aws/credentials` profiles, or attached instance/task roles.
- Azure Blob Storage uses Go CDK's Azure driver. Provide
  `AZURE_STORAGE_ACCOUNT` with `AZURE_STORAGE_KEY`, a connection string, a SAS
  token, or Azure default credentials such as environment credentials, Azure
  CLI credentials, or managed identity.

Library embedders of `runtime/sqlite` or `runtime/direct` only get `blobfs://`
support by default. Import
`github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk` from executable/server
code to enable Go CDK provider URI registration.

Library users can import `github.com/colony-2/jobdb/pkg/jobdb/runtime/direct`
for Postgres or import another runtime implementation. The public
`github.com/colony-2/jobdb/pkg/jobdb` and `runtime/core` packages do not
import pgjobdb. Only the direct package and the JobDB CLI select pgjobdb.

References:

- [Go CDK blob storage guide](https://gocloud.dev/howto/blob/)
- [Go CDK URL opener concepts](https://gocloud.dev/concepts/urls/)
- [Go CDK GCS driver credentials](https://pkg.go.dev/gocloud.dev/blob/gcsblob)
- [AWS SDK for Go v2 configuration](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html)
- [Google Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials)
- [Go CDK Azure Blob driver credentials](https://pkg.go.dev/gocloud.dev/blob/azureblob)
- [Azure Identity credential chains for Go](https://learn.microsoft.com/en-us/azure/developer/go/sdk/authentication/credential-chains)

## Runtime API

The server exposes the JobDB runtime REST API. The wire contract is documented
in [openapi/jobdb-runtime.yaml](openapi/jobdb-runtime.yaml).

Go clients normally use the remote runtime adapter:

```go
runtime, err := remoteruntime.New("http://127.0.0.1:9047", nil)
```

See [pkg/jobdb/README.md](pkg/jobdb/README.md) for the Go runtime API, data
types, and runtime package reference.

## Go Workflow Workers

Workflow workers are intentionally documented separately from the server. If you
are writing job workers, task workers, or a process that runs worker loops, use
the `pkg/workflow` package.

See [pkg/workflow/README.md](pkg/workflow/README.md).

## Development

The CLI source lives in `cmd/jobdb`. To run it directly from a checkout:

```bash
go run ./cmd/jobdb --listen 127.0.0.1:9047 --db jobdb.db
```

Run the full test suite:

```bash
go test ./...
```

Useful references:

- [pkg/jobdb/README.md](pkg/jobdb/README.md): runtime API, data types, and backend packages.
- [pkg/workflow/README.md](pkg/workflow/README.md): workflow SDK, workers, and engines.
- [docs/MIGRATION-SWF-GO-TO-JOBDB.md](docs/MIGRATION-SWF-GO-TO-JOBDB.md): concise import migration from `swf-go`.
- [docs/API-SURFACE.md](docs/API-SURFACE.md): supported public packages.
- [docs/SPEC-OpenAPI-Runtime-Contract.md](docs/SPEC-OpenAPI-Runtime-Contract.md): runtime REST contract notes.
