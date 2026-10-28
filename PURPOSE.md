# JobDB purpose

JobDB provides the runtime contracts, storage-backed execution services, and Go
worker SDK for durable jobs and tasks. It supports both embedded execution and a
standalone HTTP server, allowing workflow code to use the same runtime interface
across local and remote deployments.

## Responsibilities and interfaces

- [pkg/jobdb](pkg/jobdb/README.md) owns the backend-independent Go runtime API
  and data model: job submission and lifecycle, work leases, durable run history,
  artifacts, schedules, goals and dependencies, tenant configuration, and
  lease-scoped secret access. Its `WorkflowRuntime` and `ExecutionLease`
  interfaces connect workers, clients, and runtime implementations.
- [pkg/workflow](pkg/workflow/README.md) owns the higher-level worker SDK and
  execution engine: job and task workers, worker loops, durable task calls,
  waits and child jobs, retries, recovery, and read-only history replay and
  determinism inspection. Application code supplies the actual workers.
- [runtime/sqlite](pkg/jobdb/runtime/sqlite) provides durable embedded storage;
  [runtime/toy](pkg/jobdb/runtime/toy) provides in-memory execution for tests and
  experiments. [runtime/remote](pkg/jobdb/runtime/remote) implements the HTTP
  client/server adapter using the [runtime REST contract](openapi/jobdb-runtime.yaml).
- [runtime/core](pkg/jobdb/runtime/core) supplies shared runtime semantics and
  public backend ports for scheduler, chapter/artifact, schema, and configuration
  persistence. Backend implementations own durable state and atomic lease
  mutations behind these ports.
- [cmd/jobdb](cmd/jobdb) and [pkg/jobdb/cli](pkg/jobdb/cli) provide the standalone
  SQLite or toy runtime server, health checks, and runtime administration commands.
  [Blob storage](pkg/jobdb/blobstore) supports artifact persistence, with optional
  Go CDK provider registration for executable/server consumers.
- The repository maintains the public contracts and compatibility guidance in
  [docs/API-SURFACE.md](docs/API-SURFACE.md), runtime conformance support in
  [pkg/jobdb/runtimetest](pkg/jobdb/runtimetest), and tests for runtime and workflow
  behavior.

## Ownership boundaries

The separate [pgjobdb repository](https://github.com/colony-2/pgjobdb) owns the
Postgres runtime and typed scheduler composition, its `direct` and `serve`
commands, and its container image. JobDB owns the shared runtime ports and
reusable [Postgres chapter/artifact store](pkg/jobdb/chapterstore/postgres),
[Postgres schema store](pkg/jobdb/schemastore/postgres), and
[configuration storage service](pkg/jobdb/configuration), including SQL storage.
The presence of these components does not make the Postgres runtime part of the
base JobDB CLI.

Consumers such as c2j compose these interfaces into their own workflows. JobDB
owns execution infrastructure and semantics; application-specific job behavior,
cell catalogs, routing policy, and build/review orchestration belong to those
consumers. Backend and deployment owners provide their storage, credentials, and
operational configuration.

Supported public interfaces are identified in the API surface reference;
internal implementation packages and generated storage encodings are not public
extension contracts. See the [README](README.md) for server operation and the
package references above for runtime and worker usage.
