# Unfinished task inspection through replay

## Agreed design

JobDB owns logical invocation identity and lifecycle interpretation. Replay
reconstructs orchestration through scopes until it reaches an unfinished task.
Recorded outcomes, timeout boundaries, catch selections, and retry decisions
remain authoritative. Replay never activates live timers or executes workers.
An unfinished task in an active job is logically running; this is not a claim
about instantaneous worker liveness.

No new execution records, tables, or lifecycle writes are required. Reuse stored
task input when present; otherwise invoke the task's replay-safe input builder.
Input builders are orchestration reconstruction and must be deterministic and
side-effect-free. Worker execution, configuration/secrets access, scheduling, and
other mutations remain forbidden during inspection. Completed tasks continue to
use recorded inputs and outputs without invoking builders.

The original requests' blanket prohibition on input-builder invocation is
superseded by this agreed reconstruction contract.

## Implementation steps

1. Extend JobDB replay inspection to expose unfinished invocation/attempt
   identity, task type, input, and logical state. Emit a start observation without
   an end observation; preserve existing completed-event behavior and identities.
   Leave unknown execution timestamps unset and ordinals as storage coordinates.
2. Permit replay-only reconstruction of missing scope/task/attempt declarations
   at the history frontier. Continue through scope bodies without live admission
   or timers. Keep recorded descriptor checks, exceptional boundaries, and all
   durable operations strict. Stop at waits and unavailable control decisions.
3. Reconstruct unfinished local input; reuse external wait input and prior retry
   input. Never run workers, persist reconstructed declarations, or manufacture
   outcomes. Respect inspection cancellation and propagate reconstruction errors.
4. Update c2j's story projection to show active attempt input immediately, clear
   previous attempt outputs, preserve logical selection identity through
   completion, and consume JobDB state. Use a separate working copy and supply
   an applicable patch if no c2j checkout is available.
5. Document the public replay/input-builder contract and update the API snapshot
   if required by repository checks.

## Verification

- Add real-runtime tests for toy, SQLite, and remote adapters, using explicit
  barriers and a separate inspection engine. Cover a first local task with only
  deferred scopes, a later task reconstructed from prior output, external waiting
  and leased execution, and a running retry with a completed failed attempt.
- Assert input and stable identity on repeated inspection and after completion;
  no unfinished output/end event; no worker calls, history writes, or scheduler
  mutations caused by inspection; unchanged compact-history volume.
- Cover nested scope policies, recorded scope timeouts, retry/backoff boundaries,
  cancellation, builder failure, and replay after deadlines. Completed histories
  must still skip builders and enforce deterministic matching.
- Add c2j recorder tests and real-JobDB story integration tests for active input,
  prior-attempt isolation, waiting state, and identity across completion.
- Run focused regressions first, then the full JobDB suite and race checks for
  affected workflow tests. Run c2j story tests against the modified JobDB.

## Status

Implemented and verified.

- JobDB now reconstructs unfinished scope/task declarations and input, reports
  logical task state through start events and cache misses, and preserves the
  existing durable execution protocol and compact-history volume.
- Regression coverage lives in `pkg/workflow/execution_unfinished_test.go` and
  `pkg/workflow/replay_frontier_test.go`. The barrier matrix includes 15
  local/external/retry scenarios across toy, SQLite, and remote adapters, plus
  waiting/cancellation and reconstruction failure/read-only guards. Existing
  timeout, catch, restart, determinism, and compact-history regressions pass.
- The c2j changes were implemented in `/tmp/c2j-in-progress` against
  `v0.0.105-0.20261027024848-fbcd5cd24a8a`. No working c2j checkout was supplied.
  [c2j-in-progress-story.patch](c2j-in-progress-story.patch) contains the recorder
  changes, recorder regressions, adjusted projection assertions, and eight real
  `GetJobRunStory` integration scenarios covering SQLite/remote execution and
  local/external tasks with and without retries. `git apply --check` succeeds
  against that source version. Apply it in c2j and use the updated JobDB dependency;
  no release or dependency version was fabricated here.

Verification completed:

```sh
GOTOOLCHAIN=go1.26.1 go test -tags=integration ./...
go test -race ./pkg/workflow ./pkg/jobdb/secrets/... ./pkg/jobdb/cli ./examples/secret-provider
go test -race ./pkg/workflow -run 'TestExecutionUnfinished|TestReplayFrontier|TestReplayUnfinished' -count=1
GOTOOLCHAIN=go1.26.1 go run ./cmd/jobdb-api-snapshot -packages api/packages.txt -check api/jobdb.public.txt
```

The full integration suite also passed with the environment's default Go 1.27.1.
API snapshot generation uses CI's Go 1.26.1 because the snapshot tool's type
loader cannot read this environment's Go 1.27 export data.

In the c2j working copy, a temporary modfile replaces JobDB with `/src`; its
module files remain unchanged. These checks pass:

```sh
go test -modfile=/tmp/c2j-in-progress.mod ./pkg/story/...
go test -race -modfile=/tmp/c2j-in-progress.mod ./pkg/story/...
go test -modfile=/tmp/c2j-in-progress.mod ./pkg/worker/compiler ./pkg/executionruntime ./pkg/template
```

Regression sensitivity was verified with read-only Go overlays: replacing the
JobDB task/history implementation with its original version makes all 15 new
barrier cases fail; replacing the c2j recorder with its original version makes
the new local story integration case fail. Both pass with the fixes restored.

The running state is deliberately a logical inference from the job snapshot.
Unrecorded start timestamps remain unknown. Replay still stops at future waits
and unavailable durable control decisions; it does not invent catch selections,
retry decisions, or task outcomes. Input builders must obey the documented
reconstruction contract.
