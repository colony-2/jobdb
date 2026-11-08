# JobDB local verification

`verify.yaml` uses the `c2.build-hook/v1` include contract. It deliberately binds
only the API version, candidate, and base, without an input schema: the caller's
request, target directory, documents, and absent/null/value-artifact settings are
accepted but never interpreted as shell code. Checks always cover the whole repo.

| Workflow behavior | Native task |
| --- | --- |
| `test.yaml`: Linux ARM64 Go tests with integration tag | `test_integration`: `go test -count=1 -p=2 -v -tags=integration -exec "python3 $PWD/scripts/verify-test-exec.py" ./...` |
| `test.yaml`: workflow/provider race check | `test_races`: `go test -count=1 -p=1 -v -race ./pkg/workflow ./pkg/jobdb/secrets/... ./pkg/jobdb/cli ./examples/secret-provider` |
| Tools/platform metadata | `check_environment` |
| Local result and durable evidence | `summarize` |

Every operation is const and independently declares pinned tools. The existing
Nixpkgs pin supplies Go 1.26.8; CI requests 1.26.1. This deliberately records a
compatible patch version rather than claiming exact CI parity. Go's race detector
requires CGO and the declared native GCC toolchain. Build/package parallelism is bounded (two for integration, one for races) to
avoid compiling other packages during timing-sensitive workflow race tests. The
Go runtime keeps its native CPU-aware default; no tests are excluded.

The integration audit includes four embedded-PostgreSQL packages:
`chapterstore/postgres`, `internal/chapterstore/postgres`, `configuration`, and
`schemastore/postgres`. The Go `-exec` launcher isolates their binaries beneath the
operation's scratch TMPDIR. When invoked as root it drops supplementary groups,
GID and UID to `nobody` before startup. A non-root worker keeps its identity.
PostgreSQL downloads its normal native 17.5.0 archive and uses a separate cache
beneath its scratch working directory. The launcher removes the inherited home
variable only in those test processes so embedded-postgres cannot write to the
worker's home. Other tests keep their source working directory and fixture access.
No test assertions, package selection, or GitHub Actions steps are changed.

Environment failures block dependent suites. Ordinary integration failures still
allow the independent race suite to run. All mandatory results, timeout flags,
diagnostic errors, required logs, and aggregate report generation gate a pass.
Cancellation is not caught. Full logs are exported as native artifact references;
`summary.md` records outcomes, tool versions, candidate/base, and observed skips.
Scoped Git transport remains intact; enclosing job identity and opt-in remote
runtime/Postgres settings are cleared before testing.

Held regardless of available optional infrastructure: Linux AMD64 and macOS
matrix jobs, matrix-wide certification, tagging, GoReleaser, cross compilation,
signing, registry/npm/Homebrew publication, and deployment. No Docker, hosted
service, cloud credentials, privileged mounts, or remote CI are invoked.

Contract regression checks:

```sh
c2j test run --file tests/verify-hook/verify.test.json
c2j test run --file tests/verify-hook/absent.test.json
c2j test run --file tests/verify-hook/live.test.json --include-live --parallelism 1
```

The live suite uses disposable source fixtures to run real commands, load the
candidate hook, assert const preservation, and demonstrate a failing test without
merging or publishing. Application-suite results must also be checked on the
actual candidate; fixture success alone does not certify JobDB's tests.

For managed child-job validation, a successful preliminary command does not
establish that hook restore artifacts reach the resolver. A bounded child with
an explicit const `git rev-parse HEAD` preparation succeeded at that command but
failed at `recipe_hook:resolve` with `hook workspace restore artifact missing`.
This is a caller/runtime artifact-routing limitation, before hook commands run.
Use the local candidate-loading suites above for source acceptance; do not treat
submission or the preparation command as verification success. Runtime owners
must repair restore-artifact delivery before claiming that child route works.
