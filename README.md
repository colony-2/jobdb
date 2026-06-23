# jobdb

`jobdb` is a runtime server for durable jobs. The main entry point in this repo
is `cmd/jobdb`, which serves the JobDB runtime REST API over HTTP using one of
the available storage backends.

Use the server when you want a standalone runtime process that workers and other
clients can talk to over the remote runtime protocol.

## Quick Start

Run the default SQLite-backed server:

```bash
go run ./cmd/jobdb --listen 127.0.0.1:9047 --db jobdb.db
```

This starts the runtime API at `http://127.0.0.1:9047`. SQLite is the default
backend and persists runtime state in `jobdb.db`; large artifacts are stored in a
blob directory that defaults to `<db>.blobs`.

The explicit SQLite subcommand is equivalent:

```bash
go run ./cmd/jobdb sqlite --listen 127.0.0.1:9047 --db jobdb.db
```

Stop the server with `Ctrl-C` or `SIGTERM`; the command shuts the HTTP server
down before closing backend resources.

## Backend Options

### SQLite

SQLite is the default embedded durable backend.

```bash
go run ./cmd/jobdb sqlite \
  --listen 127.0.0.1:9047 \
  --db ./jobdb.db \
  --blob-dir ./jobdb.blobs
```

Flags:

- `--db`: SQLite database path. Defaults to `jobdb.db`.
- `--blob-dir`: directory for large artifacts. Defaults to `<db>.blobs`.
- `--sqlite-dsn`: SQLite DSN. Overrides `--db` and `JOBDB_SQLITE_DSN`.
- `--listen`: HTTP listen address. Defaults to `127.0.0.1:9047`.

Environment:

- `JOBDB_SQLITE_DSN`: SQLite DSN used when `--sqlite-dsn` is not set.

### Toy

The toy backend is in-memory. It is useful for local experiments and tests, not
for durable execution.

```bash
go run ./cmd/jobdb toy --listen 127.0.0.1:9047
```

### Direct

The direct backend uses Postgres-backed `pgwf` for job state and an embedded
Strata daemon for chapter and artifact storage. It installs or verifies the
`pgwf` schema on startup.

```bash
JOBDB_POSTGRES_DSN='postgres://user:pass@localhost:5432/jobdb?sslmode=disable' \
  go run ./cmd/jobdb direct --listen 127.0.0.1:9047
```

Flags:

- `--postgres-dsn`: Postgres DSN for `pgwf` state.
- `--listen`: HTTP listen address. Defaults to `127.0.0.1:9047`.

Environment:

- `JOBDB_POSTGRES_DSN`: Postgres DSN used when `--postgres-dsn` is not set.

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

Run the full test suite:

```bash
go test ./...
```

Useful references:

- [pkg/jobdb/README.md](pkg/jobdb/README.md): runtime API, data types, and backend packages.
- [pkg/workflow/README.md](pkg/workflow/README.md): workflow SDK, workers, and engines.
- [docs/API-SURFACE.md](docs/API-SURFACE.md): supported public packages.
- [docs/SPEC-OpenAPI-Runtime-Contract.md](docs/SPEC-OpenAPI-Runtime-Contract.md): runtime REST contract notes.
