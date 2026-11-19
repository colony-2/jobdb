# JobDB local verification

`verify.yaml` uses the `c2.build-hook/v1` include contract. It deliberately binds
only the API version, candidate, and base, without an input schema: the caller's
request, target directory, documents, and absent/null/value-artifact settings are
accepted but never interpreted as shell code. Checks always cover the whole repo.

| Workflow behavior | Native task |
| --- | --- |
| `test.yaml`: Linux ARM64 Go tests with integration tag | `test_integration`: `go test -count=1 -p=2 -v -tags=integration -exec "python3 $PWD/scripts/verify-test-exec.py" ./...` |
| `test.yaml`: workflow/provider and shared/embedded runtime race check | `test_races`: `GOMAXPROCS=2 go test -count=1 -timeout=20m -p=1 -parallel=2 -v -race ./pkg/workflow ./pkg/jobdb/secrets/... ./pkg/jobdb/cli ./examples/secret-provider ./pkg/jobdb/runtime/remote ./pkg/jobdb ./pkg/jobdb/runtime/core ./pkg/jobdb/runtime/sqlite ./pkg/jobdb/runtime/toy/... ./pkg/jobdb/runtimetest` |
| Crash-concern live state, expiry and same-job recovery | `test_integration`: three repetitions of the built-in and HTTP `crash_concern_excludes_ready_work` conformance cases, with a five-minute package budget |
| Infrastructure preservation, provenance and synchronous legacy renewal | `test_races`: `go test -count=1 -p=1 -timeout=20m -v -race ./pkg/jobdb/runtime/core ./pkg/jobdb/runtime/sqlite ./pkg/jobdb/runtime/remote -run 'TestInfrastructure\|TestProvenance\|TestLegacyKeepAlive'` |
| Completion fallback, input recovery, ignored SDK failures and HTTP source identity | `test_races`: ten race-instrumented repetitions of `TestInfrastructureCompletionSource`, `TestExecutionUncertainFinalCompletion`, `TestInfrastructureSDKFailures`, `TestInfrastructureHTTPFallbackRetainsKnownSource`, `TestInfrastructureHTTPOperationSource`, `TestInfrastructureHTTPArtifactBodySource` and `TestExecutionRecoveryInputSnapshots` in workflow and HTTP |
| Generated runtime inventories and public API snapshot | `test_integration`: checksum both generated HTTP inventories before/after `go generate`, then check the combined public API snapshot |
| Published-module receipt acceptance and failure cases | `test_integration`: Python standard-library unit fixtures for `verify-published-module.py`; actual module availability is checked after publication |
| Tools/platform metadata | `check_environment` |
| Local result and durable evidence | `summarize` |

Every operation is const and independently declares pinned tools. The existing
Nixpkgs pin supplies Go 1.26.8; CI requests 1.26.1. This deliberately records a
compatible patch version rather than claiming exact CI parity. Go's race detector
requires CGO and the declared native GCC toolchain. Build/package parallelism is bounded (two for integration, one for races) to
avoid compiling other packages during timing-sensitive workflow race tests. The
integration suite keeps Go’s native CPU-aware default. The race task sets
`GOMAXPROCS=2` for the Go command, compiler tools and test subprocesses to bound
thread pressure. Its explicit `-timeout=20m` prevents the full workflow package
from hitting Go’s default ten-minute limit while tests are still progressing.
The upstream `-parallel=2` limit also bounds parallel test fixtures. Remote runtime
and public JobDB package coverage remain included. No tests are excluded, and
race instrumentation remains enabled.

The race selection also includes `pkg/jobdb/runtimetest`, covering the shared
retained-input expiry oracle and compatibility failure probes under race
instrumentation. The existing SQLite and HTTP selections exercise a scheduler
without attachment and full shared conformance through attachment-free consumer
decorators. Live fixture VH-039 proves a failure in the newly added shared race
package fails verification while preserving all four evidence artifacts.
The ten-repeat selection also includes input-snapshot recovery across toy,
SQLite and HTTP. Its lease stays valid until the append fault fires; expiry is
then explicit, keeping recovery independent of storage and HTTP speed.

Integration first retains current main's generated-file checks for both legacy and
consumer HTTP inventories and its public API snapshot check. The snapshot includes
`ConsumerRuntime`, legacy `WorkflowRuntime` and the additive preservation APIs.

The integration audit includes four embedded-PostgreSQL packages:
`chapterstore/postgres`, `internal/chapterstore/postgres`, `configuration`, and
`schemastore/postgres`. The Go `-exec` launcher isolates their binaries beneath the
operation's scratch TMPDIR. When invoked as root it drops supplementary groups,
GID and UID to `nobody` before startup. A non-root worker keeps its identity.
PostgreSQL downloads its normal native 17.5.0 archive and uses a separate cache
beneath its scratch working directory. The launcher removes the inherited home
variable only in those test processes so embedded-postgres cannot write to the
worker's home. Other tests keep their source working directory and fixture access.
The workflow/provider race command uses a twenty-minute per-package timeout in
both CI and the native hook. The native hook also exercises core, SQLite and HTTP
infrastructure/provenance races, including synchronous legacy renewal shutdown.
It then repeats completion fallback, ignored SDK failure and HTTP origin regressions ten
times under the race detector. These include ordinary provider errors and valid
server origins as controls. The HTTP operation matrix covers writes, waits,
completion, rescheduling and artifacts, including body-carried ordinals before
transport classification and interrupted artifact responses.

After full integration, the hook repeats crash-concern conformance three times
across toy, SQLite and their HTTP adapters. The fixture uses two-second leases
and a one-minute context per backend: the former 100 ms lease could expire during
storage or HTTP assertions on a loaded worker. It still waits for authoritative
expiry on every acquisition and checks ACTIVE before expiry, READY or CRASH_CONCERN
after expiry, blocked acquisition at the threshold, guarded clearing and recovery
of the same job. If a listing omits backend expiry, the test waits for ACTIVE to
end within its context: capability expiry can precede backend expiry. Unexpected
non-ACTIVE states still fail immediately. This adds no retry of a failed case.

Environment failures block dependent suites. Ordinary integration failures still
allow the independent race suite to run. All mandatory results, timeout flags,
diagnostic errors, required logs, and aggregate report generation gate a pass.
Cancellation is not caught. Full logs are exported as native artifact references;
`summary.md` records outcomes, tool versions, candidate/base, and observed skips.
Scoped Git transport remains intact; enclosing job identity and opt-in remote
runtime/Postgres settings are cleared before testing.

Timeouts belong to command inputs: two minutes for environment/reporting and
thirty minutes for each test suite, with a twenty-minute Go package timeout for
the race suite. Durable states have no wall-clock deadline,
so suspension or delayed result collection cannot expire an already successful
command. Command timeouts and all aggregate evidence requirements still gate a pass.

Held regardless of available optional infrastructure: Linux AMD64 and macOS
matrix jobs, matrix-wide certification, tagging, GoReleaser, cross compilation,
signing, registry/npm/Homebrew publication, and deployment. No Docker, hosted
service, cloud credentials, privileged mounts, or remote CI are invoked.

Contract regression checks:

```sh
c2j test run --file tests/verify-hook/verify.test.json
c2j test run --file tests/verify-hook/absent.test.json
c2j test run --file tests/verify-hook/live.test.json --include-live --parallelism 1 --case-timeout 10m
```

The live suite uses disposable source fixtures to run real commands, load the
candidate hook, assert const preservation, and demonstrate integration, runtime
race, repeated completion source, repeated SDK and repeated HTTP source failures without merging or publishing.
The fixtures include stable generated files, a deterministic Go generator, and a
small stand-in snapshot checker. Generation and stale-snapshot failure fixtures
verify those gates independently while the real repository generation/snapshot
commands are checked separately. They do not certify actual API compatibility.
They also load the real receipt unit fixtures with a minimal example input;
those controlled subprocess checks do not establish module availability.
The repeated crash-concern failure fixture passes the initial integration sweep,
fails only at count three, and requires failed verification, successful independent
races, all four evidence artifacts and an unchanged fixture checkout.
Application-suite results must also be checked on the actual candidate; fixture success alone does not certify JobDB's tests.

For managed child-job validation, a successful preliminary command does not
establish that hook restore artifacts reach the resolver. A bounded child with
an explicit const `git rev-parse HEAD` preparation succeeded at that command but
failed at `recipe_hook:resolve` with `hook workspace restore artifact missing`.
This is a caller/runtime artifact-routing limitation, before hook commands run.
Use the local candidate-loading suites above for source acceptance; do not treat
submission or the preparation command as verification success. Runtime owners
must repair restore-artifact delivery before claiming that child route works.

`publish.yaml` checks normal module consumption after the managed Git publication.
It passes the actual full `merged_hash` to `scripts/verify-published-module.py`;
the helper requires the checkout to match that commit. It creates an external
consumer and an empty module cache, disables workspace mode, clears `GOFLAGS`,
and resolves and downloads JobDB through the normal Git route without replacements.
Both origins must identify Git, a nonblank URL and the publication commit, and
both versions must agree. Download `Sum` and `GoModSum` must be valid `h1:`
SHA-256 checksums, and the module archive must exist in the fresh cache.
The exported `module-consumption.md` records the full commit, resolved and
downloaded versions, complete unmodified origin objects, both Go checksums and
the archive's byte count and SHA-256. The archive checksum is separate from Go's
content checksum. Resolution or download failure fails the
hook; the helper does not merge, push, substitute another commit or retry.

This check uses the same scoped Git transport available for private module
resolution in the verification hook. It needs no publishing credential or JobDB
secret. The managed flow retains Git publication; this hook verifies availability
of its result. A failed check does not undo a publication that already completed.

Receipt acceptance and failure fixtures run without publishing:

```sh
python3 -m unittest discover -s scripts -p 'test_verify_published_module.py' -v
```

They check exact identities, origins, versions, checksums, archives, isolation and
readable failures using controlled command responses. Actual module availability
still requires the configured post-publication check.

The completion source fixture passes integration and the initial races, then fails
only the ten-repeat completion regression. It requires a failed verdict, all four
evidence artifacts and a clean fixture checkout. The actual regressions cover
lost acknowledgements after terminal writes, known zero/positive ordinals, absent
chapters and authoritative typed origins (including explicitly unknown sources).
