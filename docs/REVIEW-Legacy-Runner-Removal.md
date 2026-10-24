# Legacy runner removal

Reviewed c2j main at `165adbd2be0dbf6f8a7cc404849bd77001d2a051` on
2026-10-24 before removing the sequential workflow runner.

## c2j callers

The production entry points already select the versioned execution engine:

- `pkg/worker/compiler/job_worker.go`: `NewRecipeJobWorker` returns
  `jobworkflow.ExecutionWorker`, whose callback takes `ExecutionContext`.
  `NewRecipeWorkerWithOptions` registers that worker and its task implementations.
- `cmd/c2j/internal/runjob/service.go`: live runs and replay use that factory
  through `newStoryJobWorker`.
- `cmd/c2j/internal/runjob/supplied_lease.go`: supplied-lease execution also uses
  `newStoryJobWorker`.
- `pkg/story/live/recorder.go` and `restart_context.go`: story reconstruction
  and restart replay wrap the same execution worker.
- `pkg/recipetest/execution_scope.go` and the recipe test executor use
  `ExecutionWorker` or worksets from the same compiler factory.

No Go references to JobDB's `JobContext`, `WithTaskOptions`, `TaskOptionsFor`, or
legacy `TaskAlternate` were found. c2j's own contextual `JobContext` data type is
unrelated. Uses of `jobworkflow.JobWorker` as a registration interface remain
valid because their values are execution workers.

A compilation check of c2j's compiler, story/live, runjob, and recipetest packages
fails identically against JobDB before and after this removal: c2j's
`validationJobContext` lacks the already-required `ResolveSecretsFresh` method.
The reviewed c2j revision pins JobDB v0.0.46. This is an existing dependency
migration issue, not evidence of a legacy runner dependency; it prevents claiming
a successful end-to-end c2j build against current JobDB.

## Other consumers

The sibling pgjobdb checkout still has legacy JobContext implementations in
`pkg/pgjobdb/runtime/integration/`, its copied test harness/conformance fixtures,
and `docs/repros/jobdb-workflow-stalls/main.go`. Its task-alternate tests also use
`WithTaskOptions`. These fixtures/reproductions need migration when pgjobdb
updates its JobDB dependency. No changes to the separate pgjobdb repository are
included here.

## Removal and validation coverage

`JobWorker.Run` now receives `ExecutionContext`. `ExecutionWorker` remains its
callback implementation. Registration APIs and c2j's factory calls retain their
shapes. There is one invocation/scope execution path; the old task retry loop,
job retry loop, ordinal cursor, timers, and Goexit-based handoff are removed.
Legacy `JobContext` and data-attached alternate options are removed as well.
The low-level runtime and chapter APIs remain available.

Internal workers and runtime conformance fixtures now use task declarations with
stable sites and input builders. Tests asserting the retired sequential chapter
layout, manually overwriting those chapters, or comparing their physical
ordinals with the execution engine are retired. Relevant current coverage is:

- `execution_compact_test.go`: compact histories, recovery, task-boundary
  restarts, and committed outcomes.
- `execution_integration_test.go`: retries, nested scopes, deadlines, input
  snapshots, replay, restart budgets, artifacts, and child joins across runtimes.
- `execution_failure_test.go`: non-retryable errors, prerequisite admission,
  deterministic-history failures, uncertain completion, and external results.
- `execution_alternate_test.go`: alternate dispatch, claims, lease replacement,
  expiry, retries, cancellation, and recovery.
- `execution_handoff_test.go` and `supplied_lease_integration_test.go`: handoff,
  renewal failure, cancellation, and supplied leases.
- `TestExecutionReplayPreservesObserverHistoryAndExpiredBudgets`: identical
  observer events and immutable history when replaying after execution budgets
  have expired, without rerunning task workers.

Both direct `JobWorker` implementations and callback `ExecutionWorker`
registrations are exercised by the timeout, secrets, and handoff suites.
