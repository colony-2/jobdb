# Usage API Parity Test Plan

## Goal

Add comprehensive usage tests that verify the refactor preserved behavior across the two supported SWF usage APIs:

- `swf.SWFEngine`
- `swf.WorkflowRuntime`

This plan does **not** attempt to test removed Strata/pgwf-leaking APIs. The compatibility target is the current public SWF surface.

## What "Old" and "New" Mean

For the purpose of this plan:

- "old API" means the downstream usage pattern centered on `swf.SWFEngine`
- "new API" means the refactored direct usage pattern centered on `swf.WorkflowRuntime`

That maps to current code as:

- engine creation via [pkg/swf/jobs.go](/src/pkg/swf/jobs.go)
- runtime interface via [pkg/swf/runtime.go](/src/pkg/swf/runtime.go)
- existing shared test harness via [pkg/swf/internal/swftest/harness.go](/src/pkg/swf/internal/swftest/harness.go)

## Current Baseline

The repo already has strong conformance coverage:

- runtime conformance in [pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go](/src/pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go)
- engine conformance in [pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go)
- additional engine semantics in [pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go)
- package-level integration tests in [pkg/swf](/src/pkg/swf)

What is still missing is a dedicated **parity** layer that drives the same scenario through both APIs and compares normalized outcomes.

## Target Test Structure

Add a new shared package:

- `pkg/swf/internal/swftest/usageparity`

Recommended file layout:

- `pkg/swf/internal/swftest/usageparity/parity_helpers.go`
- `pkg/swf/internal/swftest/usageparity/lifecycle_parity_test.go`
- `pkg/swf/internal/swftest/usageparity/execution_parity_test.go`
- `pkg/swf/internal/swftest/usageparity/job_run_parity_test.go`
- `pkg/swf/internal/swftest/usageparity/list_jobs_parity_test.go`
- `pkg/swf/internal/swftest/usageparity/artifact_parity_test.go`
- `pkg/swf/internal/swftest/usageparity/construction_parity_test.go`

Do not replace the existing conformance suites. The new package should sit above them and assert that both public usage styles produce the same results.

## Core Design

Each parity test should run the same scenario twice:

1. through `swf.SWFEngine`
2. through `swf.WorkflowRuntime`

Each run should emit a normalized observation object. The test should compare the two observation objects, not just whether both runs succeeded.

Recommended normalized observation types:

```go
type ScenarioObservation struct {
    FinalStatus    swf.JobStatus
    Result         *NormalizedResult
    JobRun         *NormalizedJobRun
    Chapters       []NormalizedChapter
    ListedJobs     []NormalizedJobSummary
    WaitingTask    *NormalizedWaitingTask
    Artifacts      []NormalizedArtifact
    ErrorKind      string
    ErrorContains  string
}
```

Normalization rules:

- ignore lease IDs
- ignore worker IDs unless the scenario is explicitly about worker behavior
- ignore timestamp precision unless ordering is the behavior under test
- compare payload kinds and decoded payloads, not raw serialized envelopes where possible
- compare artifact names, digests, sizes, and bytes
- compare `GetJobRun` structure after removing incidental timestamps

## Reuse Existing Harnesses

Do not build a second setup layer. Reuse:

- `swftest.BuiltInRuntimeHarnesses()`
- `swftest.BuiltRuntimeHarness`
- `swftest.MustWorkSet(...)`
- existing helper workers in [pkg/swf/internal/swftest/harness.go](/src/pkg/swf/internal/swftest/harness.go)

Add only the missing parity-specific helpers:

- `RunViaEngine(...)`
- `RunViaRuntime(...)`
- `ObserveEngineScenario(...)`
- `ObserveRuntimeScenario(...)`
- `CompareObservations(...)`

## Concrete Rollout

### Phase 1: Add the parity harness

Files:

- `pkg/swf/internal/swftest/usageparity/parity_helpers.go`
- optionally `pkg/swf/internal/swftest/parity_types.go` if shared normalization helpers are useful outside the new package

Work:

- define the normalized observation structs
- add helpers to run the same scenario through engine and runtime APIs
- add comparison helpers with stable diffs
- add a small "smoke parity" test to prove the harness works

Done condition:

- one simple workflow can be executed through both APIs and compared in one test

### Phase 2: Add lifecycle parity coverage

File:

- `pkg/swf/internal/swftest/usageparity/lifecycle_parity_test.go`

Scenarios:

- start job with generated ID
- start job with explicit `JobID`
- cancel pending/in-flight job
- restart completed job from valid boundary
- restart invalid cases:
  - negative `LastStepToKeep`
  - missing next chapter
  - retry-chain boundary violation

Primary observations:

- returned job key
- final status
- final result / error kind
- `ListJobs` summary for the target job

Done condition:

- all current lifecycle behavior reachable from either API is covered by a parity test

### Phase 3: Add execution parity coverage

File:

- `pkg/swf/internal/swftest/usageparity/execution_parity_test.go`

Scenarios:

- basic sequential workflow
- external task completion
- `AwaitJobs` at job level
- `AwaitJobs` at task level
- `AwaitDuration`
- task retry then success
- job retry then success
- determinism failure after worker change

Primary observations:

- final status/result
- pending state while mid-flight
- waiting task handle presence and contents
- chapter sequence and payload kinds

Done condition:

- execution semantics are compared through both APIs for both `toy` and `direct`

### Phase 4: Add `GetJobRun` parity coverage

File:

- `pkg/swf/internal/swftest/usageparity/job_run_parity_test.go`

Scenarios:

- completed run with inputs and outputs
- lazy output loading
- failed output
- cancelled output
- pending runtime view
- replay after external task completion

Primary observations:

- normalized `GetJobRunResponse`
- `GetOutput(...)` behavior and error class

Done condition:

- job-run shape and downstream semantics match between engine-driven and runtime-driven usage

### Phase 5: Add artifact and chapter parity coverage

File:

- `pkg/swf/internal/swftest/usageparity/artifact_parity_test.go`

Scenarios:

- task output artifacts on success
- task output artifacts on failure
- job output artifacts on failure
- explicit runtime `PutChapter` + `GetChapter` + `OpenArtifact`
- artifact cleanup visible behavior

Primary observations:

- artifact bytes, digests, sizes, names
- chapter artifact references
- output retrieval behavior after cleanup

Done condition:

- artifacts and stored chapter behavior are parity-tested rather than only surface-tested

### Phase 6: Add list/query parity coverage

File:

- `pkg/swf/internal/swftest/usageparity/list_jobs_parity_test.go`

Scenarios:

- filter by tenant
- filter by status
- filter by job IDs
- filter by job type
- filter by job/task tuple
- filter by singleton key
- metadata predicate filtering
- pagination and ordering
- active vs archived store routing

Primary observations:

- normalized `ListJobsResponse`

Done condition:

- query semantics are parity-tested rather than only integration-tested through one surface

### Phase 7: Add construction and registration parity coverage

File:

- `pkg/swf/internal/swftest/usageparity/construction_parity_test.go`

Scenarios:

- build via `NewEngineBuilder().WithRuntime(...).BuildEngine()`
- direct runtime use without engine
- toy runtime use without engine
- `RegisterWorkers(...)` after engine construction
- `WithMaxActive(...)` and `WithAwaitRecycleThreshold(...)` smoke coverage

Primary observations:

- construction success/failure
- observable runtime behavior after registration/build options

Done condition:

- all supported downstream construction paths have usage-level coverage

## Relationship to Existing Tests

The new parity suite should **reuse** and then gradually absorb overlapping behavior checks from:

- [pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go](/src/pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go)
- [pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go)
- [pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go)

Do not delete those suites immediately. The migration order should be:

1. add parity tests
2. confirm parity tests fully cover the scenario
3. remove redundant conformance cases only when the parity version is clearly stronger

## Priorities

### P0

- basic sequential success
- explicit job ID
- cancel
- restart valid and invalid boundaries
- external task completion
- failed job output
- pending runtime view
- list jobs by status

### P1

- job retry parity
- task retry parity
- `AwaitJobs`
- `AwaitDuration`
- artifact error-path behavior
- metadata filter parity
- pagination parity

### P2

- construction options parity
- registration-after-build parity
- determinism edge cases
- direct-only synthesized next-attempt views if they become runtime-portable

## Expected Deliverables

Code deliverables:

- new `usageparity` test package
- parity helpers and normalization types
- initial P0 test set
- follow-up P1/P2 sets

Documentation deliverables:

- update [WORKFLOW_RUNTIME_TEST_CONSOLIDATION_PLAN.md](/src/WORKFLOW_RUNTIME_TEST_CONSOLIDATION_PLAN.md) to point at the parity suite once it exists
- update migration guides to point at executable examples if any parity helpers become the canonical sample code

## CI and Execution

Recommended test commands:

```bash
go test ./pkg/swf/internal/swftest/usageparity -count=1
go test ./pkg/swf/internal/swftest/... -count=1
go test ./pkg/swf -count=1
go test ./... -count=1
```

Recommended CI order:

1. `usageparity`
2. `runtimeconformance`
3. `engineconformance`
4. `pkg/swf`
5. full repo

That makes public-behavior regressions fail early.

## Done Criteria

This plan is complete when:

- every major user-visible workflow behavior has parity coverage across `SWFEngine` and `WorkflowRuntime`
- both built-in runtimes (`direct`, `toy`) run the same parity matrix
- existing conformance suites are reduced to surface-specific checks rather than duplicated behavior coverage
- future runtimes can opt into parity testing by only wiring a harness entry
