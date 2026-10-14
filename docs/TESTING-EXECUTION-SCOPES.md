# Execution-scope scenario coverage

The versioned engine tests use public worker/run/replay APIs and real toy,
SQLite, and REST runtimes. Recovery tests cancel workers and reclaim expired
leases. Fault injection targets durable append boundaries; it does not replace
invocation matching with mocked task results.

| Area | Positive and negative coverage |
| --- | --- |
| Invocation identity | Cached results skip builders/workers; changed site, task type, contract, or command fails; swallowing a mismatch cannot produce success. |
| Input lifetime | Task retries reuse snapshots; builder failures rebuild; worker mutations do not alter retry input; recovery before/after the first input-bearing outcome has different expected behavior. |
| Commit uncertainty | Lost acknowledgements at header, activation, scope, attempt, result, catch, handler, decision, and end records are reconciled without repeating committed work; final-result publication and scheduler completion can recover independently. |
| Scope retry | Completed children repeat in a new enclosing attempt; recovery consumes them in the original attempt. |
| Timeouts | Interrupted callbacks and tasks, late calls/results, task retries, durable deadlines, timeout after the last child committed, nested budget enforcement, and catch-handler budget exhaustion. Delayed storage explicitly exhausts admission budgets; cancellation tests wait for callbacks to start before expiring them. Nested replay propagates recorded ancestor timeouts without consuming them early, while rejecting unrelated outcomes. |
| Catch | Ordered matching, handled output, rewritten failure, propagation, handler failure, invalid disposition, recovery inside handlers, and changed predicates after recorded selection. |
| External work | Explicit inputs despite intervening control records; cache-only replay misses; malformed/missing generation, ordinal, and route rejection without writes; stale completion rejection; deadline-driven replacement reuses input and changes generation. Independent scheduler/chapter ports also exercise versioned completion. |
| Timed alternates | Local A bypass, leased A execution, B eligibility, human answers before/after due and racing pickup, cancellation before/after claim, lease loss before/after B dispatch, unchanged wait/input through recovery, sticky B retries and backoff (including an attempt expired before B pickup), lost dispatch/result acknowledgements, invalid declarations, changed replay contracts, hard deadlines, and replay without registered implementations. |
| Restart | Patched commands/inputs, immutable source history, retained-prefix reuse, unsupported targets, idempotent submission, conflicting patches, expired source deadlines, repeated forks, and recovery before/after suffix commit with unchanged continuation budgets. |
| Versions | Named consumer versions, copied registration maps, invalid entries, unsupported/missing pattern versions, preserved initial provenance and newer worker activations, supplied leases, and inspection. Version 1 continuation, external completion, and restart retain version 1; new dispatch fields require version 2. |
| Artifacts | Producer-authored JSON references resolve live and after replay across transports; same-name input/output artifacts and names colliding with generated input names remain distinct. Outcome ordinals stay correct across worker replacement, in-process waits, durable waits, repeated yields, and external requeue. |
| Failed diagnostics | Failed task JSON and artifacts survive history and replay observers, including task retry; failure status is preserved and temporary output files are cleaned. |
| Task identity | Logical invocation IDs survive unfinished recovery and task retries. Task attempts increment only on retry; enclosing scope and whole-job retries create new invocation IDs. |
| Cache ownership | Task and nested scope activity share configuration/secrets snapshots; whole-job retries reset caches; secrets are absent from durable history. |
| Child waits | Default-policy deadlines with task-level and execution-level joins across toy, SQLite, and HTTP/SQLite; multiple children; success/failure/cancellation; worker replacement; replay without workers rerunning or writes. Unfinished joins wake at deadlines through both poll and direct acquisition, expired deadlines clear on reschedule, and SQLite persists them across reopen. Controlled-clock tests check before/at/after deadline and preserve submission not-before times. |
| Yield confirmation | Both contexts expose confirmed suspension through wrapped errors; rejected requests, lease loss, cancelled calls, transport failures before commit, and lost responses after commit never report success. Successful handoffs are leaseable on the new route without completed task outcomes. Gated task/orchestration callers prove Run returns its receipt without waiting for user code; scheduler read failures cannot hide it. Metadata validation/copying, cached outcomes, recovery/replay, and late acknowledgements after cancellation are covered. |
| Terminal failures | Non-retryable markers and named error types survive serialization; catches can preserve or rewrite retryability; prerequisites succeed/reject; swallowed live mismatches archive a failed job. |
| Declaration validation | Missing invocation fields, invalid commands, negative timeout/retry values, and incomplete catch rules reject before callbacks execute. |
| Frozen history | `pkg/workflow/testdata/execution-v1.json` is generated from real execution and replayed through all three runtimes after its historical deadlines. Keep this fixture immutable after the first release. |

Run the focused scenarios with:

```sh
go test -race ./pkg/workflow ./pkg/jobdb ./pkg/jobdb/runtime/core -run '^TestExecution' -count=1
```

Match the Go version in [the CI workflow](../.github/workflows/test.yaml) and run
its commands before committing:

```sh
go test -v -tags=integration ./...
go test -race ./pkg/workflow
```

Also run `go test ./...` and the full race suite before cutover. Existing API
conformance and regression tests remain enabled during coexistence. The new
scenarios add scope/catch/restart protocol cases beyond the older task-only API;
passing them does not replace c2j's recipe/compiler integration tests during its
migration.
