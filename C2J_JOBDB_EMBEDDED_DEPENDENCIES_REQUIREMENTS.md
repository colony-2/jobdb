# Requirements: Keep JobDB Blob Provider Code Out Of c2j

## Status

Draft requirements.

## Problem

c2j depends on `github.com/colony-2/jobdb v0.0.13`. The default c2j binary only
needs two JobDB runtime modes:

- remote HTTP JobDB through `github.com/colony-2/jobdb/pkg/jobdb/runtime/remote`
- embedded local JobDB through `github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite`

The embedded mode is for local testing, development, and self-contained CLI
flows. It stores state under `~/.c2j/embed/default` using a local SQLite
database and local blob files. c2j does not run the production JobDB server and
does not need in-process S3, GCS, Azure Blob, or generic Go CDK object-store
drivers.

Today those provider packages enter the c2j binary import graph through this
chain:

```text
cmd/c2j/internal/swfruntime
  -> github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite
  -> github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore
  -> gocloud.dev/blob
  -> gocloud.dev/blob/s3blob
  -> gocloud.dev/blob/gcsblob
  -> gocloud.dev/blob/azureblob
```

This means the default c2j binary can compile and link code from cloud
blob-store providers such as:

- `gocloud.dev`
- `cloud.google.com/go/storage`
- `github.com/aws/aws-sdk-go-v2/service/s3`
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`

This is dependency bloat for c2j. It also increases binary build cost, security
review surface, and the chance that unrelated cloud SDK code affects local c2j
builds.

## Goals

- Default c2j binaries must not compile or link cloud object-store provider
  packages.
- `c2j submit`, `c2j list`, `c2j run`, and `c2j run one` must keep working with
  `--embed`.
- Remote HTTP JobDB mode must keep working without any local object-store
  provider dependencies in c2j.
- JobDB's full server or direct-runtime deployments may still support S3, GCS,
  Azure Blob, and other Go CDK blob providers through an explicit dependency.
- The dependency boundary must be visible in the Go package import graph used to
  build `./cmd/c2j`, not only at runtime configuration.

## Non-Goals

- Do not remove JobDB artifact or chapter blob support.
- Do not add cloud object-store support to c2j embedded mode.
- Do not change the JobDB remote HTTP protocol.
- Do not require c2j users to provide cloud credentials for embedded mode.

## JobDB Requirements

### 1. Split local blob storage from cloud provider storage

JobDB must provide a lightweight blob-store path for local embedded runtimes that
does not import Go CDK blob drivers or cloud SDKs.

The local path must support the existing embedded c2j storage model:

- SQLite row storage
- filesystem-backed large artifacts
- default blob directory derived from the SQLite DB path
- no external credentials
- no provider registration side effects

The existing `blobfs://` behavior is enough for c2j embedded mode. It must be
available without importing `gocloud.dev/blob` or any provider package.

### 2. Move Go CDK provider support behind an explicit package boundary

The package that blank-imports these providers must not be imported by
`runtime/sqlite` on the default c2j path:

- `gocloud.dev/blob/azureblob`
- `gocloud.dev/blob/fileblob`
- `gocloud.dev/blob/gcsblob`
- `gocloud.dev/blob/memblob`
- `gocloud.dev/blob/s3blob`

Provider-backed storage should move to an explicit package surface, for example:

- `github.com/colony-2/jobdb/blobstores/gocdk`
- `github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk`

The exact package shape is flexible. The important requirement is that the
default `./cmd/c2j` package graph does not import it directly or indirectly.

### 3. Keep provider packages out of the default package graph

The fix should use ordinary package boundaries first. Build tags are not
required for the initial implementation. The requirement is about what the Go
compiler sees for a default c2j build.

For a normal build with no optional provider tags, this command must not list
cloud provider packages:

```sh
go list -deps ./cmd/c2j
```

Provider code may remain in the same Go module as JobDB core. A separate module
is not required for this requirement as long as provider packages are not
imported by the packages that c2j builds.

### 4. Keep the embedded SQLite API local by default

`runtime/sqlite` must continue to work for the c2j embedded use case with no
caller-supplied blob configuration.

Acceptable behaviors:

- `Config{DBPath: ...}` derives a local blob directory and opens it with the
  lightweight filesystem blob store.
- `Config{BlobDir: ...}` uses the lightweight filesystem blob store.
- `Config{BlobStoreURI: "blobfs://..."}` uses the lightweight filesystem blob
  store.

Object-store URIs such as S3, GCS, or Azure must require an explicit optional
provider package import. They must not work by default through hidden imports in
`runtime/sqlite`.

Do not change `runtime/sqlite.Config` or `runtime/direct.Config` as part of this
dependency-boundary fix. `BlobStoreURI` should remain the compatibility surface;
the implementation behind it should become local-only unless an optional
provider registration package is imported by the executable.

### 5. Make unsupported provider use fail clearly

If a program imports only the lightweight JobDB runtime and passes an object
store URI, JobDB must fail with a clear error such as:

```text
unsupported blob store scheme "s3": import/configure the JobDB Go CDK blob provider package
```

It must not silently fall back to local storage, inline storage, or a different
provider.

### 6. Preserve remote runtime lightness

`runtime/remote` must not import any concrete storage providers. Storage
provider selection is a server-side concern for remote JobDB.

### 7. Add JobDB dependency-boundary tests

JobDB should include a minimal-consumer test or script that imports only:

- `github.com/colony-2/jobdb/pkg/jobdb`
- `github.com/colony-2/jobdb/pkg/workflow`
- `github.com/colony-2/jobdb/pkg/jobdb/runtime/remote`
- `github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite`

For that consumer, `go list -deps` must not include:

- `gocloud.dev/blob/s3blob`
- `gocloud.dev/blob/gcsblob`
- `gocloud.dev/blob/azureblob`
- `github.com/aws/aws-sdk-go-v2/service/s3`
- `cloud.google.com/go/storage`
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`

Provider packages should have separate tests that prove S3, GCS, Azure Blob,
file, and memory providers still work when the optional provider surface is
imported.

## c2j Requirements

### 1. Keep c2j imports on the lightweight JobDB path

c2j default code may import these JobDB packages:

- `github.com/colony-2/jobdb/pkg/jobdb`
- `github.com/colony-2/jobdb/pkg/workflow`
- `github.com/colony-2/jobdb/pkg/jobdb/runtime/remote`
- `github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite`

c2j tests may also import `github.com/colony-2/jobdb/pkg/jobdb/runtime/toy`.

c2j must not import JobDB direct Postgres runtime packages or optional blob
provider packages in default builds.

### 2. Make embedded mode explicitly local

The c2j embedded opener should construct the SQLite runtime using local storage
configuration only. Prefer `DBPath` and, if needed, `BlobDir` over provider-style
URIs.

For `embed:///`, c2j must not accept or synthesize S3, GCS, Azure Blob, or other
remote object-store configuration.

### 3. Keep remote mode as HTTP-only

For `http://host/tenant` and `https://host/tenant`, c2j must use the JobDB
remote runtime. It must not import or configure server-side storage providers.

### 4. Add c2j dependency checks

c2j should add a CI check that runs after updating to the fixed JobDB version:

```sh
go list -deps ./cmd/c2j
```

The dependency list for `./cmd/c2j` must not contain:

- `gocloud.dev/blob/s3blob`
- `gocloud.dev/blob/gcsblob`
- `gocloud.dev/blob/azureblob`
- `github.com/aws/aws-sdk-go-v2/service/s3`
- `cloud.google.com/go/storage`
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`

## Recommended Design

The preferred fix is in JobDB:

1. Keep the filesystem blob store in JobDB core with no Go CDK dependency.
2. Change `runtime/sqlite` to use that local store directly for default,
   `BlobDir`, and `blobfs://` configuration.
3. Move Go CDK bucket opening and provider blank imports to an optional package
   that c2j does not import.
4. Update the JobDB server/direct-runtime entrypoints that need object-store
   support to import the optional provider registration package explicitly.
5. Release a new JobDB version.
6. Update c2j to that version and verify `go list -deps ./cmd/c2j`.

This keeps the public c2j behavior unchanged while making production object
storage an explicit JobDB server dependency rather than a transitive dependency
of every embedded runtime consumer.

## Acceptance Criteria

- After updating c2j to the fixed JobDB version, `go list -deps ./cmd/c2j` does
  not include Go CDK blob provider packages or S3/GCS/Azure object-store SDK
  packages.
- `c2j submit --embed`, `c2j list --embed`, `c2j run --embed`, and
  `c2j run one --embed` continue to pass their existing tests.
- c2j remote HTTP tests continue to pass.
- JobDB tests prove provider-backed blob stores still work when the optional
  provider registration package is imported.
- Unsupported object-store URIs fail clearly in lightweight embedded builds.

## Resolved Design Notes

- `BlobStoreURI` remains on `runtime/sqlite.Config` and `runtime/direct.Config`
  for this change.
- Provider-backed storage is enabled by importing an explicit registration
  package, for example `github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk`.
