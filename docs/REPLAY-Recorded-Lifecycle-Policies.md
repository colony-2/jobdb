# Recorded lifecycle policies

JobDB owns timeout and retry decisions. A consumer changing its defaults must
not prevent inspection of recorded work or redefine the policy of work already
in progress.

## Behavior

- Existing scope and task occurrences use their recorded total timeout, attempt
  timeout, and retry policy. Recorded outcomes, timeout errors, retry decisions,
  and deadlines remain authoritative.
- Recovery binds the same policy before reaching unfinished work. Removing a
  timeout cannot extend an existing deadline; adding one cannot shorten it.
  Future attempts in that occurrence retain its captured retry/timing policy.
- A purely organizational scope can be omitted from compact history. When
  reconstructed over existing history it remains unlimited and non-retrying,
  even if the current declaration adds timeouts or retries. Its descendants
  still retain their recorded identities and policies.
- A new occurrence at the live frontier captures the current declaration.
  A restart retains ancestor policies and rebases their deadlines through the
  existing continuation mechanism. Work re-entered at the restart target uses
  new declarations where the prior declarations were not retained.
- Read-only replay never evaluates live timers, runs task workers, or rewrites
  stored policies. Completed tasks use their stored inputs and results.

Structural matching remains strict: scope/task paths, task types, task contract
versions, commands, catch rule identities/order, restart-hook presence, and task
alternate declarations. A change to timeout or retry defaults is not equivalent
to a change in the consumer's task contract or control flow.

Declarations must still be valid. This change does not allow malformed options,
change the execution/storage versions, or require a database rewrite.

## Regression protection

`pkg/workflow/execution_policy_replay_test.go` exercises old producer settings
against different reader/recovery settings, rather than only replaying with the
same configuration that wrote the history. Coverage includes:

- Removed, added, and changed scope/task timeout defaults, with and without a
  stored scope declaration or restart hook.
- Completed results and completed retry sequences; unchanged history and no
  extra input construction or task execution.
- Actual scope/task total and attempt timeouts using virtual time, then replay
  after the original deadlines have passed with timeout defaults removed.
- Suspended external work, original deadlines on recovery, new declarations for
  newly entered scopes, and expiry after defaults are removed.
- Recorded retry budgets controlling recovery despite changed retry defaults.
- Restart continuation deadlines derived from retained ancestor policies.
- Rejection of changed commands, task types, task contract versions, catch rules,
  and restart-hook shape in the descriptor regression tests.

The fixed `testdata/execution-recorded-policy-v4.json` fixture was captured using
JobDB commit `0bd5f2c`, before this fix. It contains synthetic input/output only,
a one-hour scope total timeout, a thirty-minute scope attempt timeout, and a
one-minute task invocation timeout. The regression reader supplies no timeouts.
Do not regenerate this fixture with the reader under test to make a failure
pass: it is a historical compatibility contract. The import test expands its
records into chapters and adds the terminal job outcome through the runtime.

Transport cases cover toy, SQLite, and HTTP. Virtual-time deadline tests use
toy so they do not depend on wall-clock scheduling. The historical fixture,
removed-default tests, and recovery tests fail against `0bd5f2c` and pass with
the policy binding fix.

Useful checks:

```sh
go test ./pkg/workflow -run 'TestExecutionRecordedPolicy|TestExecutionReplayDescriptorDiagnostics' -count=1
go test -tags=integration ./... -count=1 -timeout=240s
go test -race ./pkg/workflow -count=1 -timeout=300s
```

## Validation (2026-10-29)

Passed the focused policy/descriptor tests, full JobDB integration suite, full
workflow race suite, and public API snapshot check using the CI Go 1.26.1
toolchain. Selected current c2j scope/timeout and story reconstruction tests
also passed with a local JobDB replacement:

```sh
go test ./pkg/story/... ./pkg/worker/compiler \
  -run 'TestExecutionScopes|TestExecutionTimeout|TestExecuteOp|TestUntimedOp|TestStoryReplay|TestJobRunStoryReplay' \
  -count=1 -timeout=180s
```

This is selected c2j coverage, not a claim that every historical c2j job is
compatible. The captured job has the remaining incompatibility below.

## Captured c2j/Cortex incident

Read-only replay of job `3LIsi58lrjR29QBCS9dMcUbaUBK` using c2j `0c9f073` now
passes the scope that previously failed with a one-hour-versus-zero timeout
mismatch. Its original and migrated scope descriptors are equivalent.

Replay then reveals a separate task-contract mismatch: the recorded
`command_execution:command_execution` call declares contract version 1, while
current c2j declares version 2. c2j commit `59351c6` changed that declaration.
JobDB deliberately continues to reject this mismatch. Full historical story
replay therefore still needs consumer compatibility handling; the JobDB policy
fix alone does not make this captured job readable in the latest Cortex.
