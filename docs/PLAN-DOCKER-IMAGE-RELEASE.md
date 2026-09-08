# Plan: Release a Stateless Production JobDB Container Image

## Goal

Extend the existing tagged release so every JobDB release also publishes a
lean, multi-architecture OCI image at:

```text
ghcr.io/colony-2/jobdb
```

The image's supported startup path must run JobDB only as a stateless service:

```text
JobDB HTTP server -> Postgres records + remote object storage
```

Postgres, a supported remote blob-store URI, and externally managed lease-token
signing material are required at startup. The container must not offer a
backend switch, SQLite configuration, or local artifact storage through its
normal command or environment interface.

The image will contain the same `jobdb` binary already produced for the rest of
the release. That binary may continue to include SQLite, toy, direct, and local
filesystem support for non-container use; the container entrypoint constrains
how it is invoked.

## Current State

The repository already has most release prerequisites:

- `.github/workflows/release.yaml` creates a SemVer tag from `main` after tests
  pass, checks out that exact tag, and runs GoReleaser Pro.
- The release job grants `packages: write` for GHCR and `id-token: write`, which
  can also support keyless image signing.
- `.goreleaser.yaml` builds `cmd/jobdb` for Linux and macOS on AMD64 and ARM64
  with `CGO_ENABLED=0` and stripped linker flags.
- The current stripped Linux AMD64 binary is fully static and approximately
  51 MiB. Its SQLite implementation and cloud SDKs dominate that size.
- `cmd/jobdb` handles `SIGTERM` and performs a bounded graceful HTTP shutdown.

The current CLI is not suitable as an unconstrained image command:

- its root command defaults to SQLite;
- it exposes SQLite and toy subcommands;
- its direct runtime falls back to local `blobfs://` storage when no blob URI
  is supplied;
- it defaults to the loopback-only address `127.0.0.1:9047`;
- it has no operational health endpoint or built-in health-check command;
- `remote.NewServer` creates a random lease-token signing key per process, so a
  token minted by one replica cannot be validated by another replica or a
  replacement process.

## Design Decisions

### Reuse the existing release binary

Do not add a container-specific Go binary or a second GoReleaser build. Add a
stateless `serve` subcommand to the existing `cmd/jobdb` executable and use this
fixed image entrypoint:

```dockerfile
ENTRYPOINT ["/jobdb", "serve"]
```

Do not set a replaceable `CMD`. Arguments supplied through normal Docker or
Kubernetes `args` are appended after `serve`; because `serve` accepts no
positional arguments or backend/storage flags, values such as `sqlite`, `toy`,
`--db`, or `--blob-dir` fail instead of selecting local operation.

The root command currently defines several persistent flags which Cobra will
inherit into `serve`. Preserve that parsing behavior for compatibility with the
other commands, but have `serve` explicitly reject every inherited flag whose
`Changed` value is true. It is not sufficient to silently ignore a parsed
local-mode flag, because that gives operators false confidence about their
deployment.

The `serve` subcommand must always:

- construct the direct/Postgres runtime;
- require and validate a remote blob-store URI;
- load external lease-token signing material;
- read its small configuration surface from environment variables;
- reject local-backend environment variables when they are non-empty.

The existing root, `sqlite`, `toy`, and `direct` commands retain their current
non-container behavior. An operator who explicitly replaces the OCI entrypoint
can invoke any functionality present in the binary; preventing that is not a
security boundary an image can provide. The supported image interface itself
must not provide an argument or environment path to local operation.

### Require Postgres and an allowlisted remote blob provider

The `serve` command must fail before opening its HTTP listener unless these are
present and valid:

```text
JOBDB_POSTGRES_DSN
JOBDB_BLOB_STORE_URI
JOBDB_LEASE_TOKEN_SIGNING_KEY or JOBDB_LEASE_TOKEN_SIGNING_KEY_FILE
```

Allow only these blob URI schemes:

| Scheme | Provider |
| --- | --- |
| `s3://` | Amazon S3 or a configured S3-compatible remote service |
| `gs://` | Google Cloud Storage |
| `azblob://` | Azure Blob Storage |

Reject an empty URI, a path without a scheme, and local or process-memory
schemes including `blobfs://`, `file://`, and `mem://`. Require the parsed URI
to identify a non-empty bucket or container. Apply the allowlist before runtime
construction so the direct runtime's local fallback is unreachable.

The binary can retain all current Go CDK providers. A provider-package split is
not needed for the first image because reusing the exact existing release
binary is more valuable than removing the comparatively small local provider
code. Runtime validation, not link-time exclusion, enforces the image contract.

### Externalize lease-token signing state

Add a configurable remote-server constructor that accepts lease-token signing
material. Keep `remote.NewServer` backward compatible for existing embedders,
but do not use its random-key fallback from `jobdb serve`.

Require either:

- `JOBDB_LEASE_TOKEN_SIGNING_KEY`: a base64-encoded key containing at least 32
  random bytes; or
- `JOBDB_LEASE_TOKEN_SIGNING_KEY_FILE`: a mounted-secret file containing that
  value.

Reject setting both. All replicas in one deployment must receive the same key
so in-flight lease tokens remain valid across load balancing and container
replacement. Never log the key, include it in an error, bake it into an image
layer, or derive it from the Postgres DSN.

The first implementation can document coordinated rotation after the maximum
outstanding token lifetime. A multi-key verification ring for seamless rotation
can be added later if required.

### Treat the container as stateless

Do not declare a data volume, create `/var/lib/jobdb`, or expose database/blob
path settings. The image must run with a read-only root filesystem. SDK
temporary files can use `/tmp`; a locked-down deployment should mount that path
as a small tmpfs.

Database records, schema state, and artifact bytes must survive arbitrary
container replacement because they live in Postgres and remote object storage.
In-flight lease tokens remain valid because every replica receives the same
external signing key. Correctness must not depend on a writable root
filesystem, container hostname, or particular replica.

### Use Distroless static Debian 13

Use this final base, pinned to a tested multi-architecture index digest:

```text
gcr.io/distroless/static-debian13:nonroot@sha256:<digest>
```

The JobDB binary is fully static and does not need a libc-bearing base.
Distroless static adds approximately 2 MiB while providing maintained CA
certificates, timezone data, `/tmp`, and non-root identity metadata. It has no
shell or package manager. This is preferable to maintaining those runtime
files manually in a custom `scratch` image.

Run explicitly as UID/GID `65532:65532`, use an exec-form entrypoint, and pin
the base digest. Automate digest update proposals and verify the upstream
keyless signature before accepting a new base. Do not publish a debug-flavored
image under a production JobDB tag.

### Use port 8080

The image defaults to:

```text
JOBDB_LISTEN=0.0.0.0:8080
```

Port `8080` is a conventional unprivileged web-service port. It lets the
Distroless non-root user bind without root, `CAP_NET_BIND_SERVICE`, or a
network-namespace sysctl. Set `EXPOSE 8080` and use port `8080` in the health
check and documentation.

This is an image default only. Preserve the existing non-container CLI default
of `127.0.0.1:9047` for backward compatibility.

### Publish one multi-architecture image

Publish a single OCI image index for:

```text
linux/amd64
linux/arm64
```

Tag it as follows:

| Tag | Behavior |
| --- | --- |
| `vX.Y.Z` | Canonical release tag matching the immutable Git tag. |
| `latest` | Updated only for a stable release, never a prerelease. |

Do not publish mutable branch tags. Production examples should recommend an
exact release tag or, for strict reproducibility, the image digest.

## Container Configuration Contract

The fixed `jobdb serve` entrypoint should support only this configuration:

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `JOBDB_POSTGRES_DSN` | yes | none | Postgres connection string for job, chapter, and scheduler state. |
| `JOBDB_BLOB_STORE_URI` | yes | none | `s3://`, `gs://`, or `azblob://` bucket URI for artifact bytes. |
| `JOBDB_LEASE_TOKEN_SIGNING_KEY` | conditional | none | Base64-encoded active signing key containing at least 32 random bytes. |
| `JOBDB_LEASE_TOKEN_SIGNING_KEY_FILE` | conditional | none | Mounted file containing the same base64 value; use instead of the direct variable. |
| `JOBDB_LISTEN` | no | `0.0.0.0:8080` | HTTP listen address used by the server and automatic health check. |
| `JOBDB_MAX_INLINE_ARTIFACT_BYTES` | no | `4096` | Threshold at which artifact bytes move from Postgres to object storage. |

The inline threshold of 4096 bytes is specific to `jobdb serve` and therefore
the released container. Do not change the library runtime's existing default or
the behavior of other CLI commands as part of this work.

The `serve` subcommand should not expose flags for these settings. Environment
configuration keeps the fixed Docker entrypoint unambiguous and lets the health
process observe the same listen address as the server.

Reject these local/backend settings when non-empty rather than silently
ignoring them:

```text
JOBDB_BACKEND
JOBDB_DB_PATH
JOBDB_SQLITE_DSN
JOBDB_BLOB_DIR
```

Fail fast with actionable errors for:

- a missing Postgres DSN;
- a missing, malformed, local, memory-backed, or unknown blob URI;
- a missing, malformed, too-short, or conflicting signing-key source;
- an invalid listen address;
- an invalid inline-artifact threshold;
- any positional arguments or local/backend environment settings supplied to
  `serve`.

Do not bake required values or cloud credentials into an image layer. Inject
Postgres and signing credentials from secrets. Use provider workload identity,
task/instance roles, or mounted credentials, and avoid embedding access keys in
the blob URI.

## Standard Health and Lifecycle Behavior

Add `GET /healthz` at the shared executable HTTP-server boundary so it is
available from the normal `jobdb` command in every serving mode. Keep this
operational route outside `openapi/jobdb-runtime.yaml`.

Add a root-level `jobdb healthcheck` command to the existing binary. It should:

- accept zero or one positional JobDB base URL, for example
  `jobdb healthcheck http://jobdb:8080`;
- read `JOBDB_LISTEN` and otherwise use the executable's existing listen
  default when no URL argument is supplied;
- replace wildcard hosts such as `0.0.0.0` or `::` with a loopback address;
- request `/healthz` on the derived port with a short fixed timeout;
- exit zero only for a successful response;
- reject malformed URLs, unsupported schemes, and more than one argument;
- never construct a runtime, open Postgres, or open a blob bucket.

Do not add a configurable health-check URL or separate health port. The Docker
health check runs `/jobdb healthcheck`, and the image-level `JOBDB_LISTEN`
default makes it automatically probe `http://127.0.0.1:8080/healthz`. Because
`serve` accepts its listen address only through the environment, the server and
health subprocess cannot drift. The optional positional URL is for operators,
CI, and external diagnostics; the image's own health check does not use it.

The endpoint becomes reachable only after configuration validation, Postgres
connectivity, `pgwf` installation/verification, and remote bucket construction
succeed. Give Docker's health check a start period longer than the existing
45-second backend setup timeout.

Retain direct signal delivery and the current bounded `SIGTERM` shutdown. The
health endpoint is a process/startup signal, not a continuous write test
against Postgres and every cloud dependency.

## Implementation Steps

### 1. Add shared health and configurable token signing

Refactor only enough shared server code to support the new behavior without
duplicating it:

- wrap every `cmd/jobdb` HTTP server with `/healthz`;
- add the root `healthcheck` command;
- introduce a remote-server option for a caller-supplied lease-token signing
  key;
- preserve `remote.NewServer` and its current ephemeral-key behavior for source
  compatibility;
- use the configured constructor from `jobdb serve`.

Add tests proving:

- the health route becomes reachable only after runtime construction;
- the health command derives IPv4 and IPv6 loopback URLs automatically;
- an optional HTTP or HTTPS base URL targets another JobDB instance and has
  `/healthz` appended consistently;
- malformed URLs, unsupported schemes, and extra arguments fail;
- non-2xx responses and timeouts fail;
- a token issued by one handler instance validates on a second instance with
  the same key and fails with a different key;
- existing CLI commands and remote-server construction remain compatible.

### 2. Add the stateless `serve` subcommand

Add `serve` to `cmd/jobdb`. It must accept no positional arguments and no
backend, persistence, blob-path, listen, or threshold flags. Resolve only the
documented environment variables into a typed configuration, validate it, and
then invoke the direct runtime.

Account explicitly for the root command's existing persistent flags. Add tests
that set each inherited flag before and after the `serve` subcommand and prove
the command rejects it rather than applying or silently ignoring it.

Add focused tests for:

- required Postgres, remote blob, and signing configuration;
- acceptance of `s3`, `gs`, and `azblob` URIs;
- rejection of `blobfs`, `file`, `mem`, missing schemes, empty bucket names,
  and unknown schemes;
- a default listen address of `0.0.0.0:8080` in the image environment;
- a default inline threshold of 4096;
- explicit valid overrides of listen address and threshold;
- rejection of local/backend environment variables;
- rejection of arguments such as `sqlite`, `toy`, `--db`, and `--blob-dir`;
- direct and file-based signing keys, mutual exclusion, base64 decoding,
  minimum key length, and secret-safe errors;
- graceful shutdown.

Do not split Go CDK providers or alter existing SQLite, toy, or direct command
behavior for this image work.

### 3. Add the Distroless Dockerfile

Add a Dockerfile designed for GoReleaser's temporary artifact context:

1. Start from pinned `static-debian13:nonroot`.
2. Copy `$TARGETPLATFORM/jobdb` to `/jobdb`; do not rebuild source.
3. Set UID/GID `65532:65532` explicitly.
4. Set `JOBDB_LISTEN=0.0.0.0:8080`.
5. Expose `8080`.
6. Configure an exec-form health check using `/jobdb healthcheck`.
7. Set `ENTRYPOINT ["/jobdb", "serve"]` and no `CMD`.
8. Do not add a volume, local database directory, shell, package manager, or
   local-backend configuration.

Add release-specific OCI metadata through GoReleaser rather than hard-coding
it in the Dockerfile.

### 4. Configure GoReleaser to reuse build ID `jobdb`

Extend `.goreleaser.yaml` with `dockers_v2` filtered to the existing `jobdb`
build. Do not add another Go build. The Dockerfile copies the exact Linux
binary already produced for archives and other release consumers.

Configure:

- image `ghcr.io/colony-2/jobdb`;
- exact Git tag and stable-only `latest`;
- Linux AMD64 and ARM64 platforms;
- attached SBOM generation;
- OCI source, description, license, version, revision, and creation metadata;
- the release Dockerfile.

Use current `dockers_v2`, which reuses GoReleaser artifacts and creates one
multi-platform index with Buildx. Validate with `goreleaser check`. Pin the
GoReleaser action to a tested version instead of `latest` so upstream changes
cannot break an unrelated JobDB release.

### 5. Update the release workflow

Before the existing GoReleaser step:

1. Initialize Docker Buildx.
2. Log in to `ghcr.io` as `${{ github.actor }}` using the short-lived
   `${{ secrets.GITHUB_TOKEN }}`.
3. Install a pinned Cosign version.

Keep `packages: write` and `id-token: write`; no long-lived registry credential
is needed. Configure keyless signing of the pushed image by digest and treat a
signature failure as a release failure. Keep attached SBOM and provenance
generation enabled.

The existing tag checkout remains the common source for binary archives, npm,
Homebrew, the GitHub release, and the image. After the first publish, verify
GHCR linked the package to this repository and make it public if public releases
are intended.

### 6. Add non-publishing image CI

Add an isolated pull-request job that stages the same
`$TARGETPLATFORM/jobdb` context expected by GoReleaser and builds the image
without pushing it.

Use ephemeral Postgres and an S3-compatible remote object store such as MinIO
to verify:

1. Missing any required storage or signing setting exits before listening.
2. Each prohibited local URI scheme exits non-zero.
3. Supplying `sqlite`, `toy`, `--db`, or `--blob-dir` as normal container
   arguments fails rather than changing modes.
4. The server listens on port 8080, initializes Postgres, and becomes healthy
   with an allowed remote URI.
5. Artifacts at 4096 bytes remain inline and artifacts above 4096 bytes use the
   remote object store; include the exact boundary cases.
6. A submitted job and remote artifact remain available after destroying the
   container and starting a replacement against the same services.
7. Lease tokens minted through one replica validate through another replica
   using the same signing secret.
8. The container runs as UID/GID `65532:65532` with `--read-only`, no data
   volume, and only a small `/tmp` tmpfs.
9. `SIGTERM` reaches the process directly and produces a clean bounded exit.
10. The production image contains no shell or package manager.
11. AMD64 runs successfully and ARM64 at least builds and can be inspected when
    an ARM64 execution runner is unavailable.

Also run `goreleaser check`. Because every merge to `main` currently auto-tags
a release, land the command, health support, Dockerfile, tests, workflow, and
documentation atomically after all non-publishing checks pass.

### 7. Document and verify deployment

Add a container guide linked from `README.md`. Show only Postgres plus remote
blob-store deployments; do not include a SQLite or local-volume example.

The basic shape should be:

```bash
docker run --name jobdb \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --publish 8080:8080 \
  --env JOBDB_POSTGRES_DSN \
  --env JOBDB_BLOB_STORE_URI='s3://jobdb-artifacts?region=us-east-1' \
  --env JOBDB_LEASE_TOKEN_SIGNING_KEY \
  ghcr.io/colony-2/jobdb:vX.Y.Z
```

Document:

- all supported variables and remote URI schemes;
- the 4096-byte inline threshold;
- provider identity and credential setup;
- Postgres and signing-key secret injection;
- the requirement that replicas share a signing key and the initial rotation
  procedure;
- non-root and read-only-root operation;
- automatic health checks and graceful shutdown;
- optional external checks using `jobdb healthcheck http://jobdb:8080`;
- exact tags, digest pinning, SBOM/provenance, and signature verification;
- that the installed CLI still supports local development modes while the
  image's fixed entrypoint intentionally does not;
- that replacing the OCI entrypoint is outside the supported image contract;
- the lack of built-in TLS termination and caller authentication, requiring a
  trusted network plus an authenticated TLS proxy or service mesh.

For the first published tag:

1. Confirm all release channels report the same version and commit.
2. Confirm the image index contains AMD64 and ARM64 manifests.
3. Confirm the image's `/jobdb` is the same released Linux binary, rather than a
   separate Docker build.
4. Confirm the exact tag and stable `latest` resolve to the same digest.
5. Run with a read-only root against staging Postgres and object storage.
6. Confirm local options cannot be selected through normal image arguments or
   environment variables.
7. Verify OCI metadata, attached SBOM/provenance, and Cosign signature.
8. Confirm anonymous pull succeeds if the image is intended to be public.
9. Record compressed/unpacked size and vulnerability scan results as the
   regression baseline.

Rollback by deploying the prior immutable tag or digest. Do not delete or
rewrite old release tags.

## Security and Production Boundaries

- The supported image entrypoint always uses Postgres plus an allowlisted
  remote object store.
- There is no backend switch, local path, or replaceable `CMD` in the image
  interface.
- The container owns no persistent volume and supports read-only-root
  operation.
- The process runs as a fixed non-root identity on unprivileged port 8080.
- Distroless supplies maintained runtime metadata without a shell or package
  manager.
- The image contains CA roots but no database, signing, or cloud credentials.
- A shared external signing key makes in-flight lease tokens replica-independent.
- Provider allowlisting prevents configuration from silently selecting an
  ephemeral local store.
- An operator with permission to replace an image entrypoint can invoke other
  commands included in the shared binary. Container runtime admission policy,
  not duplicate build artifacts, is the appropriate control for that privilege.
- Distroless does not remove vulnerabilities in linked Go modules. Preserve
  dependency scanning and the attached SBOM.
- JobDB still requires a trusted network or authenticated TLS proxy/service
  mesh; the image is not directly Internet-safe.

## Acceptance Criteria

- A stable release publishes signed AMD64/ARM64 images at
  `ghcr.io/colony-2/jobdb:vX.Y.Z` and `:latest` from the same tag and existing
  `jobdb` build as all other release artifacts.
- A prerelease does not move `latest`.
- The image has a fixed `/jobdb serve` entrypoint and no replaceable `CMD`.
- Normal container arguments cannot select SQLite, toy, a DB path, or a local
  blob directory.
- Startup requires a Postgres DSN, a non-local `s3://`, `gs://`, or
  `azblob://` URI, and exactly one valid external signing-key source.
- `blobfs://`, `file://`, `mem://`, empty, malformed, and unknown blob URIs are
  rejected before listening.
- The default listener is `0.0.0.0:8080` and the health command automatically
  probes its loopback equivalent without a health URL setting; callers may
  optionally supply one base URL as a positional argument.
- The `jobdb serve` inline-artifact default is exactly 4096 bytes.
- The image runs without a persistent volume as UID/GID `65532:65532` on a
  read-only root filesystem.
- State, artifacts, and in-flight lease validity survive container replacement
  when the same external services and signing key are supplied.
- The image has working CA certificates, health checks, and graceful
  `SIGTERM` shutdown.
- The final image contains no shell or package manager and adds only the small
  Distroless static runtime to the existing binary.
- The published image includes OCI metadata, attached SBOM/provenance, and a
  verifiable keyless signature.
- Pull-request CI exercises Postgres plus remote object storage without pushing
  an image.
- Existing CLI archives, npm, Homebrew, macOS builds, and local development
  modes remain unchanged.

## Out of Scope

- Supporting SQLite, toy, `blobfs://`, `file://`, `mem://`, or persistent
  container volumes through the image's normal entrypoint.
- Preventing an operator who controls the container specification from
  replacing the image entrypoint.
- Application-level authentication or TLS termination in JobDB.
- Kubernetes manifests, a Helm chart, or an operator.
- Separate images for each provider or backend.
- Mutable branch/nightly tags.
- Changing library-level inline-artifact defaults or runtime data formats.

## References

- [Distroless project documentation](https://github.com/GoogleContainerTools/distroless)
  describes the static Debian 13 non-root image, its footprint, supported
  architectures, signing, and debug variants.
- [Distroless static image contents](https://github.com/GoogleContainerTools/distroless/blob/main/base/README.md)
  documents its CA certificates, timezone data, passwd metadata, and `/tmp`.
- [GoReleaser Docker v2](https://www.goreleaser.com/customization/package/dockers_v2/)
  documents reuse of released binaries, `$TARGETPLATFORM`, multi-architecture
  Buildx publication, OCI metadata, and attached SBOMs.
- [GitHub's container registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
  documents GHCR authentication, repository association, visibility, and OCI
  source labels.
- [GoReleaser Docker signing](https://goreleaser.com/customization/sign/docker_sign/)
  documents signing published images and manifests by digest with Cosign.
