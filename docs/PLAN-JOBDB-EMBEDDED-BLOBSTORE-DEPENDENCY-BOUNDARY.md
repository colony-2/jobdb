# Plan: Keep Advanced Blob Backends Out Of Embedded JobDB Consumers

## Goal

Make local embedded JobDB usage lightweight by default while keeping S3, GCS,
Azure Blob, Go CDK `file://`, and Go CDK `mem://` support available to the
`jobdb` executable and other callers that explicitly opt in.

This plan follows `C2J_JOBDB_EMBEDDED_DEPENDENCIES_REQUIREMENTS.md`: the
important boundary is the Go package import graph. A program that embeds only
`pkg/jobdb`, `pkg/workflow`, `runtime/remote`, and `runtime/sqlite` must not
compile or link cloud blob providers. The `cmd/jobdb` executable may import the
advanced provider package because serving a full JobDB runtime is its job.

## Current State

The dependency leak is concentrated in one package:

```text
pkg/jobdb/runtime/sqlite
  -> pkg/jobdb/internal/chapterstore/blobstore
  -> gocloud.dev/blob
  -> gocloud.dev/blob/{s3blob,gcsblob,azureblob,fileblob,memblob}
```

`pkg/jobdb/internal/chapterstore/blobstore` currently contains both:

- lightweight filesystem storage in `fs.go`;
- Go CDK bucket wrapping and provider blank imports in `cdk.go` and
  `factory.go`.

Because Go compiles all files in an imported package, every importer of the
local filesystem helper also sees the Go CDK providers. Today,
`go list -deps ./pkg/jobdb/runtime/sqlite` includes `gocloud.dev/blob/s3blob`,
`gocloud.dev/blob/gcsblob`, `gocloud.dev/blob/azureblob`,
`github.com/aws/aws-sdk-go-v2/service/s3`, `cloud.google.com/go/storage`, and
`github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`. `runtime/remote` is
already clean.

## Design Decision

Use a normal package boundary first. Build tags are not necessary for the
initial fix.

Do not change the public runtime config structs as part of this fix.
`BlobStoreURI` stays on the existing configs and options, but its default
provider set changes:

- local `blobfs://...`, `BlobDir`, and derived default blob directories work
  without any optional imports;
- non-local provider URIs such as `s3://...`, `gs://...`, `azblob://...`, and
  Go CDK `file://...` require explicitly importing the provider package;
- without that provider import, non-local schemes fail clearly instead of silently
  working through hidden provider imports.

This preserves the runtime config API while making the dependency boundary
visible in the import graph.

## Target Package Shape

Keep the existing internal blobstore package as the runtime-facing opener:

```text
github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore
```

It should contain:

- the filesystem `blobfs://` implementation;
- `OpenURI`, which supports `blobfs://` by default;
- a small internal registration hook for optional provider packages;
- a clear unsupported-scheme error when no registered opener owns a URI.

Move Go CDK support into an explicit optional package:

```text
github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk
```

That package owns:

- `gocloud.dev/blob`;
- blank imports for `azureblob`, `fileblob`, `gcsblob`, `memblob`, and
  `s3blob`;
- the CDK-backed `Store` implementation;
- side-effect registration with the internal blobstore opener.

Programs that want provider-backed `BlobStoreURI` values import this package.
Programs that embed only `runtime/sqlite` do not.

## Implementation Steps

1. Split local and provider blobstore code.

   Move or wrap the existing filesystem implementation so the local package has
   no Go CDK imports. Remove Go CDK imports from
   `pkg/jobdb/internal/chapterstore/blobstore`. Keep a local URI parser that
   recognizes `blobfs://...`, creates the directory, and rejects unsupported
   schemes with an error like:

   ```text
   unsupported blob store scheme "s3": import/configure github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk
   ```

2. Keep runtime configs unchanged.

   Leave `pkg/jobdb/runtime/sqlite.Config`,
   `pkg/jobdb/runtime/direct.Config`, and `WithBlobStoreURI` structurally
   unchanged. They should continue to pass `BlobStoreURI` to the internal
   blobstore opener.

   The behavior of that opener changes:

   - `blobfs://...` works by default;
   - `BlobDir` or derived `<db>.blobs` still resolve to local `blobfs://...`;
   - `s3://...`, `gs://...`, `azblob://...`, `file://...`, and `mem://...`
     work only when the importing program has imported
     `pkg/jobdb/blobstore/gocdk`;
   - otherwise, non-local schemes fail with an actionable unsupported-scheme
     error.

3. Make direct runtime lightweight by default too, without config changes.

   `pkg/jobdb/runtime/direct` should keep the same config struct and still
   default to local `blobfs://jobdb-blobs`. Provider-backed storage becomes
   explicit by import, not by a new config field.

   This is slightly broader than the c2j requirement, but it matches the
   principle that embedding a runtime package should not automatically import
   production object-store SDKs.

4. Wire advanced providers only in `cmd/jobdb`.

   Blank-import `github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk` from
   `cmd/jobdb`.

   Existing `jobdb sqlite --blob-store-uri ...` and
   `jobdb direct --blob-store-uri ...` code can keep constructing the same
   config structs. The import is enough to register provider-backed URI
   handling for the executable.

   This means `go list -deps ./cmd/jobdb` can include Go CDK and cloud SDKs by
   design, while embedded library consumers do not.

5. Update docs and API snapshot.

   Update `README.md`, `pkg/jobdb/README.md`, and `docs/API-SURFACE.md` to say:

   - embedded SQLite defaults to local filesystem artifact storage;
   - provider URIs require importing `blobstore/gocdk`;
   - the `jobdb` executable includes that provider package for server-style
     deployments.

   The API snapshot should not change for config struct shape. Regenerate it
   only if comments or public symbols are intentionally tracked by the snapshot
   tooling.

6. Release and validate c2j.

   After releasing a fixed JobDB version, update c2j and run:

   ```bash
   go list -deps ./cmd/c2j
   ```

   The c2j dependency list must not contain Go CDK provider packages or
   S3/GCS/Azure SDK packages. The `--embed` workflows should continue using
   `DBPath`/`BlobDir` local storage only.

## Regression Tests

Add a dependency-boundary test that builds a minimal external consumer in a
temporary module. This avoids accidentally counting JobDB's own test imports
while matching how c2j consumes JobDB.

Test location:

```text
pkg/jobdb/internal/dependencytest/dependency_boundary_test.go
```

Test behavior:

1. Create a temp module with:

   ```go
   package main

   import (
       _ "github.com/colony-2/jobdb/pkg/jobdb"
       _ "github.com/colony-2/jobdb/pkg/workflow"
       _ "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
       _ "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
   )

   func main() {}
   ```

2. Use a `replace github.com/colony-2/jobdb => <repo root>` directive.

3. Run:

   ```bash
   go list -deps .
   ```

4. Fail if any of these packages appear exactly:

   ```text
   gocloud.dev/blob/s3blob
   gocloud.dev/blob/gcsblob
   gocloud.dev/blob/azureblob
   github.com/aws/aws-sdk-go-v2/service/s3
   cloud.google.com/go/storage
   github.com/Azure/azure-sdk-for-go/sdk/storage/azblob
   ```

Also add focused runtime tests:

- `runtime/sqlite` accepts `Config{DBPath: ...}`, `Config{BlobDir: ...}`, and
  `Config{BlobStoreURI: "blobfs://..."}` without importing/configuring Go CDK;
- `runtime/sqlite` fails clearly for `Config{BlobStoreURI: "s3://..."}` when no
  provider registration package is imported;
- `runtime/direct` is covered by the same shared blobstore opener and a
  dependency-graph check;
- `blobstore/gocdk` tests prove provider schemes are registered and at least
  `mem://` or `file://` round-trips through the shared blobstore contract.

The dependency-boundary test is the key regression guard. The provider tests
should live under the optional provider package so they do not mask accidental
imports from lightweight packages.

## Acceptance Criteria

- `go list -deps ./pkg/jobdb/runtime/sqlite` no longer lists Go CDK provider
  packages or S3/GCS/Azure SDK packages.
- The minimal-consumer dependency test passes.
- `runtime/remote` remains free of concrete storage providers.
- `cmd/jobdb` still supports provider-backed `--blob-store-uri` by importing
  the optional Go CDK provider package explicitly.
- Unsupported provider URIs fail with an actionable error in lightweight
  embedded builds.
- Existing embedded SQLite behavior using local blob files remains unchanged.
