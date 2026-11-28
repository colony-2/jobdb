# JobDB local verification

`verify.yaml` uses the `c2.build-hook/v1` include contract. It deliberately binds
only the API version, candidate, and base, without an input schema: the caller's
request, target directory, documents, and absent/null/value-artifact settings are
accepted but never interpreted as shell code. Checks always cover the whole repo.

| Workflow behavior | Native task |
| --- | --- |
| `test.yaml`: native Linux Go tests with integration tag | `test_integration`: `go test -count=1 -p=2 -timeout=20m -v -tags=integration -exec "python3 $PWD/scripts/verify-test-exec.py" ./...` |
| `test.yaml`: workflow/provider and shared/embedded runtime race check | `test_races`: `GOMAXPROCS=2 go test -count=1 -timeout=25m -p=1 -parallel=2 -v -race ./pkg/workflow ./pkg/jobdb/secrets/... ./pkg/jobdb/cli ./examples/secret-provider ./pkg/jobdb/runtime/remote ./pkg/jobdb ./pkg/jobdb/runtime/core ./pkg/jobdb/runtime/sqlite ./pkg/jobdb/runtime/toy/... ./pkg/jobdb/runtimetest ./pkg/jobdb/projectcatalog ./pkg/jobdb/internal/projectwire` |
| Immutable-goal migration, admission and protected records | `test_integration`: focused goal/terminal/manual-migration/availability/restart checks; `test_races`: three repetitions of independent goal writers and controlled closure/admission/wait commit orders |
| Crash-concern live state, expiry and same-job recovery | `test_integration`: three repetitions of the built-in and HTTP `crash_concern_excludes_ready_work` conformance cases, with a five-minute package budget |
| Infrastructure preservation, provenance and synchronous legacy renewal | `test_races`: `go test -count=1 -p=1 -timeout=20m -v -race ./pkg/jobdb/runtime/core ./pkg/jobdb/runtime/sqlite ./pkg/jobdb/runtime/remote -run 'TestInfrastructure\|TestProvenance\|TestLegacyKeepAlive'` |
| Completion fallback, input recovery, accepted-prefix replay, ignored SDK failures and HTTP source identity | `test_races`: ten race-instrumented repetitions of `TestInfrastructureCompletionSource`, `TestExecutionUncertainFinalCompletion`, `TestInfrastructureSDKFailures`, `TestInfrastructureHTTPFallbackRetainsKnownSource`, `TestInfrastructureHTTPOperationSource`, `TestInfrastructureHTTPArtifactBodySource`, `TestExecutionRecoveryInputSnapshots` and `TestReplayPayloadAcceptedPrefix` in workflow and HTTP |
| Generated runtime inventories and public API snapshot | `test_integration`: checksum legacy, consumer and project HTTP inventories plus project route/envelope plans before/after `go generate`, then check the combined public API snapshot |
| Extended metadata reads | `test_integration`: separate HTTP checks for toy/SQLite attestation and first-write CAS, a backend that cannot attest, and old-peer responses; read-scope regression checks |
| Recognizable job IDs and historical keys | `test_integration`: run the allocator vectors, direct and HTTP allocation, grouped concurrency and recovery, exact-key replay, and SQLite goal conversion before the full sweep |
| Cell retirement and restore | `test_integration`: verify retirement regressions, direct and HTTP restore on toy and SQLite, SQLite reopen, optional-port compatibility and both bundled serving commands before the full sweep |
| Published-module receipt acceptance and failure cases | `test_integration`: Python standard-library unit fixtures for `verify-published-module.py`; actual availability requires an explicit check of a published commit |
| Immutable-goal external consumer checks | `test_integration`: rejection fixtures for `check-published-goals.py` and public toy/SQLite consumer tests in the complete integration sweep; external acquisition/execution follows publication |
| Tools/platform metadata | `check_environment` |
| Local result and durable evidence | `summarize` |

Every operation is const and independently declares pinned tools. The existing
Nixpkgs pin supplies Go 1.26.8; CI requests 1.26.1. This deliberately records a
compatible patch version rather than claiming exact CI parity. Go's race detector
requires CGO and the declared native GCC toolchain. Build/package parallelism is bounded (two for integration, one for races) to
avoid compiling other packages during timing-sensitive workflow race tests. The
integration suite keeps Go’s native CPU-aware default. The race task sets
`GOMAXPROCS=2` for the Go command, compiler tools and test subprocesses to bound
thread pressure. Its explicit `-timeout=25m` bounds each initial-sweep package and avoids Go’s
default ten-minute limit while tests are still progressing. HTTP took 18.6
minutes in the returned run before the longer all-of fixture waits; twenty-five
minutes allows those waits and leaves headroom for worker load.
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

The accepted-prefix replay fixture uses a ten-second lease for its initial live
execution, matching the existing oversized-payload execution fixture. Its former
one-second lease could expire during race-instrumented HTTP payload rejection
while a terminal mutation blocks renewal. Replay still checks the exact retained
prefix, terminal payload failure, read-only operations and unchanged durable
history. Live fixture VH-042 passes the broad sweep and fails only when this
test is selected with count ten, proving the repeated gate retains its failure
and all four evidence artifacts. Lease expiry and renewal failure tests retain
their own deliberately short authority windows.
The repeated-failure fixtures also supply the toy runtime package selected by
the hook, so each reaches its intended injected failure rather than stopping at
a missing fixture package.

Integration retains the legacy and consumer generated-file checks and public API
snapshot check, and adds the project HTTP inventory and compiled envelope/route plans. The snapshot includes
`ConsumerRuntime`, legacy `WorkflowRuntime` and the additive preservation APIs.

The integration audit includes five embedded-PostgreSQL packages:
`chapterstore/postgres`, `internal/chapterstore/postgres`, `configuration`, and
`schemastore/postgres`, and `projectcatalog`. The Go `-exec` launcher isolates their binaries beneath the
operation's scratch TMPDIR. When invoked as root it drops supplementary groups,
GID and UID to `nobody` before startup. A non-root worker keeps its identity.
PostgreSQL downloads its normal native 17.5.0 archive and uses a separate cache
beneath its scratch working directory. The launcher removes the inherited home
variable only in those test processes so embedded-postgres cannot write to the
worker's home. Other tests keep their source working directory and fixture access.
The workflow/provider race command uses a twenty-minute per-package timeout in
CI; the broader native initial sweep uses twenty-five minutes. The native hook also exercises core, SQLite and HTTP
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

Timeouts belong to command inputs: two minutes for environment/reporting,
thirty minutes for integration and ninety minutes for the race operation.
The returned 45-minute race run timed out during SQLite's initial sweep, after
workflow took 760.523 seconds and HTTP took 1116.582 seconds. It had not reached
either preservation command. The aggregate budget allows cold compilation,
three long serial suites (workflow, HTTP and SQLite), the smaller packages and
the goal-race command and both preservation commands; Bash records elapsed time for each command. Every
assertion, test selection and thread limit remains intact. Initial-sweep packages
have a twenty-five-minute limit; preservation commands keep twenty-minute limits, and repeated goal races keep
the upstream ten-minute limit. Integration retains its twenty-minute package limit.
The shared availability fixture gives the dependencies-first all-of case ten
seconds to complete its three producers and denial/read probes before observing
the time gate alone. The returned HTTP/SQLite race failure took 3.71 seconds,
exceeding the former three-second fixture window. Explicit pre-availability rejection probes remain. A context-aware timer then
waits for eligibility before the five-second acquisition budget starts, avoiding
the former 10 ms HTTP/SQLite polling loop that delayed other cases' setup. The
matrix's parent authority lasts five minutes and each case has a thirty-second
context. Dependency-first/time-first ordering, all acquisition/authority choices,
future-observation assertions and real-time eligibility checks remain required.
Durable states have no wall-clock deadline,
so suspension or delayed result collection cannot expire an already successful
command. Command timeouts and all aggregate evidence requirements still gate a pass.

The hook accepts native Linux amd64 or arm64, matches the Go host toolchain to
the worker, and records the target of each suite. If the environment gate fails,
the report retains the failure and marks both suites not run instead of treating
their absent logs as a reporting failure. A pass still requires both suites and
all four artifacts.

Held regardless of available optional infrastructure: other platform matrix
jobs, matrix-wide certification, tagging, GoReleaser, cross compilation,
signing, registry/npm/Homebrew publication, and deployment. No Docker, hosted
service, cloud credentials, privileged mounts, or remote CI are invoked.

Contract regression checks:

Integration explicitly runs `TestGoalCandidatesPreservesTerminalFailure` before
the complete suite. It invokes the CLI against a loopback runtime and checks the
protected failure discriminator, complete snapshot, ordinary row, project identity
and pagination through both selector spellings. The live hook fixture
`VH-054-goal-candidate-cli-failure` fails only that focused invocation and requires
rejected integration, successful independent races and all four evidence artifacts.

```sh
c2j test run --file tests/verify-hook/verify.test.json
c2j test run --file tests/verify-hook/absent.test.json
c2j test run --file tests/verify-hook/platform.live.test.json --include-live
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

The optional `scripts/verify-published-module.py` checks an explicitly supplied,
already-published full commit. Upstream removed the repository's publish hook;
this helper does not run automatically after publication. For a read-only check,
run it from a checkout whose HEAD matches the published commit:

```sh
python3 -B scripts/verify-published-module.py FULL_PUBLISHED_HASH module-consumption.md
```

It creates an external consumer with an empty module cache, disables workspace
mode, clears `GOFLAGS` and uses `GOPROXY=direct`. Public and private GitHub modules
use the operator-managed Git transport. There is no public Go proxy workaround
or override of inherited `GONOPROXY`/`GONOSUMDB`. Verify actual retrieval rather
than treating a gateway configuration change as proof of availability.

Both origins must identify Git, a nonblank URL and the publication commit, and
both versions must agree. Download `Sum` and `GoModSum` must be valid `h1:`
SHA-256 checksums, and the module archive must exist in the fresh cache.
The report retains complete origins, versions, checksums and archive facts.
It then runs executable public examples, external consumer tests and linked
`CurrentBuildInfo` assertions without replacements. A failed check records the
failure; it does not merge, push, substitute another commit or retry.

Receipt acceptance and failure fixtures run without publishing:

```sh
python3 -B -m unittest discover -s scripts -p 'test_verify_published_module.py' -v
```

They check exact identities, origins, versions, checksums, archives, isolation,
direct routing despite an inherited public proxy, preserved transport settings
and readable failures using controlled command responses. Live fixture `VH-040-published-verifier-regression-failure`
proves that a failing receipt regression rejects verification while independent
races and all four evidence artifacts survive. Actual availability requires a
real fresh-consumer check of the exact published commit.

There is no repository publish hook in the current lifecycle. The recipe owns
Git publication. Do not restore the removed publish hook or `.c2j` files.
The immutable-goal consumer check described below runs after actual publication.

The completion source fixture passes integration and the initial races, then fails
only the ten-repeat completion regression. It requires a failed verdict, all four
evidence artifacts and a clean fixture checkout. The actual regressions cover
lost acknowledgements after terminal writes, known zero/positive ordinals, absent
chapters and authoritative typed origins (including explicitly unknown sources).

The focused migration and repeated goal-race live fixtures pass the broad sweep
and fail only at the newly added focused command. They require failed verification,
all four diagnostic artifacts and unchanged fixture source. They test the hook's
gates; the actual migration and independent-writer behavior is checked separately
against JobDB source.

Live fixture VH-053 makes only the new external-checker unit gate fail. It requires
failed verification, the diagnostic marker, all four artifacts and unchanged
fixture source. This proves a checker-fixture failure cannot be silently omitted.
The published-module consumer README gives the post-publication command and
explains the fresh caches, exact origins/checksums and actual API execution.

The integration hook also runs `scripts/testdata/published-append-consumer`
explicitly because Go excludes `testdata` from `./...`. This exercises the public
SQLite/HTTP cancellation and semantic append fixture against the candidate. The
publication verifier copies that same fixture into its separate downloaded-module
consumer and requires its behavioral checks to pass, alongside linked build
identity and the existing public examples. Candidate execution establishes source
behavior; the later external execution establishes published behavior.

Live fixture VH-041 forces the public consumer test to fail. It requires failed
verification, successful independent races, all four evidence artifacts and an
unchanged fixture checkout. Use the documented ten-minute case budget for these
live fixtures; their isolated Go caches require fresh compilation.


Project catalog verification covers receipt replay after deletion, competing creates,
query-bound cursors, durable admission seals, SQLite process termination and
PostgreSQL borrowed handles/search paths. The catalog package runs in the full
integration sweep and the race suite. These checks do not certify runtime writer
coverage or any production migration. Live fixture `VH-040-project-catalog-race-failure` verifies that a catalog
race failure fails the hook and retains all four evidence artifacts.

Project identity-envelope regression tests run under race instrumentation in
`pkg/jobdb/internal/projectwire`. Live fixture VH-041 injects a failure only in
that package and requires failed verification with all four evidence artifacts.
Generated project models, route inventory and envelope plans are hashed before
regeneration, alongside the retained legacy and consumer output. The public API
snapshot includes runtime admission composition and explicit SQLite maintenance.

The existing race commands retain upstream's 25-minute initial package budget, 90-minute
operation budget, elapsed-time recording and broad ten-repeat replay selection.
An additional exact HTTP external-input command repeats that boundary ten times.
Its live failure fixture injects only for that exact filter, preserving the
upstream broad-selection failure fixture independently. All live fixtures include
both the upstream published-append consumer and the project-specific packages.

The project append contract explicitly documents HTTP 403 `lease_permission_denied`
on ordinary and lease-authorized start-or-append. Integration runs
`TestProjectAppendPermissionDeniedResponses` against both generated HTTP clients,
checking typed JSON403 fields, retained body/status, one request, lease headers
and malformed error-body rejection. The project client package also runs in the
initial race sweep. Live fixture `VH-055-project-append-permission-decoding-failure`
proves failure of this focused regression rejects integration while independent
races complete and all four artifacts survive.

Retained-read denial verification runs `TestRetainedRead` explicitly before the
broad integration sweep. Its public-only remote test fixture closes and reopens
SQLite, then checks selected metadata, chapter list/detail, original-producer
artifact lookup and reader opening with bare/wrapped denials, revocation and
restored access. The initial remote race sweep also exercises this fixture.

The hook separately runs `scripts/testdata/retained-read-compatibility`, which Go
excludes from `./...`. It compiles an isolated helper at exact baseline commit
`f885723cc03ce9c8b52365390dca9601109d2c7e` (canonical tag `v0.0.94`), checks its
resolution/download origins and linked dependency, and tests the old HTTP client
against the candidate ConsumerServer. It also creates real baseline storage:
current format-14 constructors refuse its format 11 without conversion. That
reopening compatibility is reported as failed; the assessment checks unchanged
files and successful reopening by the baseline reader. HTTP behavior and the
assessment itself are required gates. This does not certify whole-module API or
storage compatibility with the baseline.

Live fixtures VH-056 and VH-057 inject failures into the focused retained-read
command and the baseline-client command. Each requires a failed hook result,
independently successful races, all four evidence files and an unchanged fixture
checkout. VH-031 retains the passing control.

The published-module verifier copies the same public remote fixture and baseline
helper into its external consumer. It reads named Go JSON test events and rejects
missing, skipped or failed cases, even when the process exits zero. Its receipt
lists actual retained-read/client results and the failed old-storage assessment.
Run it against the actual managed publication commit after publication; source
checks and hook fixture results do not identify that future commit.


Resource backoff has a dedicated required selection in `test_integration`, including
the external public fixture excluded by `./...`. `scripts/check-resource-tests.py`
reads the real Go JSON output and requires all 63 prepublication resource IDs plus
C03 retained-read controls, SQLite durable conformance and toy memory conformance.
It rejects empty selections, failed or skipped test cases and missing IDs. The
public toy forwarding package itself has no test files. Python negative controls
exercise this inventory checker before the feature selection.

The existing complete integration and race commands remain intact. `test_races`
also repeats the real resource selection three times with `-race`, including
competing claims, the held-transaction acknowledgment barrier, rollback boundaries,
format-13 migration, actual storage reopen and HTTP authority checks. Feature
fixtures inject backend clock/RNG; workflow fixtures use separate original task
deadlines and a 30-second transport lease to tolerate instrumentation overhead.

The public-module verifier now includes the resource consumer, frozen required
interfaces and old execution decoder. Its SQLite fixture closes and reopens real
storage behind a stable HTTP authority and checks immutable receipt replay and
exact large int64 values. These local fixture checks do not establish publication.
After managed publication, run the existing verifier with the exact published
full commit; it resolves the version in a new external module with `GOWORK=off`,
an empty module cache and no replacements or overlays, and records the actual
version, origin, checksums and named public test results.

The integration hook checks project installation inspection and the supported
SQLite offline migration commands before the long sweeps. The CLI fixtures now
exercise real formats 11, 12 and 13, require target format 14 and resource tables,
and retain read-only checking, repeat-migration and no-runtime-initialization
assertions. This catches stale format expectations at the public command boundary.

Before the complete race sweep, the hook repeats the full availability-route
matrix and `TestResourceBackoffAvailabilityClockBoundaryB09b` three times under
the race detector. Ready-route fixtures use an explicit elapsed cutoff rather
than the immediate wall-clock boundary; delayed, dependency and alternate-route
checks retain their own gates and all discovery/claim/poll assertions. The new
SQLite test injects the backend clock, checks an ordinary nil-wait reschedule
exactly at its cutoff, rewinds one nanosecond to require future status and denied
acquisition, then restores eligibility. It also checks the elapsed-cutoff control.
Missing ready rows now log detail and unfiltered-list state before failing.
The returned race failure was not reproduced in 100 ordinary and 100
race-instrumented repetitions of its original case. Clock regression explains
the deterministic boundary behavior; the historical worker clock was not observed,
so it is not an established cause of that failure. No retries or eventual-success
assertions replace the original checks, and the complete race sweep is unchanged.


Resource deadline regression verification repeats the exact
`B09b_deadline_keeps_resource_gate`, four `B11a_zero_credit_deadline_*` and two
`B11_zero_credit_engine_timeout_external_*` shared backend cases three times under
race instrumentation before the unchanged complete race sweep. SQLite and toy
use injected backend clocks/RNG. The cases reject early external output and
append, retain job-only deadline admission at zero credit, preserve it through
reopen and another handler crash, and run real local/external workflow timeout
recovery without repeating the task callback or input builder. Original engine
timers remain real; those two cases wait for their recorded five-second total
deadlines separately from injected scheduler time. The resource inventory checker
requires every exact new case on both backends, in addition to all original IDs,
and its negative controls reject omission of any one case. Existing generated,
API, legacy-client, complete integration/race and repeated checks are preserved.
