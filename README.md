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

The direct backend uses Postgres for job and chapter records, and a blobstore
URI for large artifact bytes. It installs or verifies the `pgwf` schema on
startup.

```bash
JOBDB_POSTGRES_DSN='postgres://user:pass@localhost:5432/jobdb?sslmode=disable' \
  jobdb direct --blob-store-uri 's3://jobdb-artifacts?region=us-east-1' --listen 127.0.0.1:9047
```

Flags:

- `--postgres-dsn`: Postgres DSN for `pgwf` state.
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
