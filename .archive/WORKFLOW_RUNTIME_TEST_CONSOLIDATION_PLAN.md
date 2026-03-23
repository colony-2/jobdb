# Workflow Runtime Test Consolidation Plan

## Goal

Now that `swf.WorkflowRuntime` is the standard backend boundary, consolidate duplicated runtime-behavior tests into shared conformance suites.

The main outcomes should be:

- less duplicated coverage across `pkg/swf/impl` and `pkg/swf/toy`
- a clear split between:
  - runtime conformance tests
  - engine semantic tests
  - backend-specific tests
- a reusable test harness that future `runtime/remote` can run unchanged

## Why Consolidation Is Now Possible

Before the runtime facade correction, `direct` and `toy` were not really implementing the same operational API.

Now they do:

- job lifecycle goes through `WorkflowRuntime`
- worker leasing goes through `PollWork` + `ExecutionLease`
- chapter access goes through `GetChapter` / `PutChapter`
- artifact access goes through `OpenArtifact`, while artifact writes are part of `PutChapter`

That gives us a stable contract to test once and run against multiple implementations.

## Current Duplication

There are several obvious behavior pairs already duplicated between direct and toy tests.

Examples:

- restart behavior
  - `pkg/swf/impl/restart_job_test.go`
  - `pkg/swf/toy/toy_test.go`
- job run detail / pending runtime view
  - `pkg/swf/impl/job_run_details_test.go`
  - `pkg/swf/toy/toy_test.go`
- chapter / artifact persistence behavior
  - `pkg/swf/impl/artifact_key_test.go`
  - `pkg/swf/toy/chapter_metadata_test.go`
  - `pkg/swf/toy/toy_test.go`
- await-jobs and pending-runtime state
  - `pkg/swf/impl/await_jobs_test.go`
  - `pkg/swf/impl/await_jobs_task_test.go`
  - `pkg/swf/toy/toy_test.go`

There is also a second kind of duplication:

- engine-level integration tests in `pkg/swf`
- backend-specific behavior tests in `pkg/swf/impl`
- toy semantic mirrors in `pkg/swf/toy`

Those should not all collapse into one suite, but they should be organized around clearer ownership.

## Target Test Structure

### 1. `pkg/swftest/runtimeconformance`

Add a shared runtime conformance package for tests that should pass for every `WorkflowRuntime`.

Recommended contents:

- lifecycle suite
  - start job
  - restart job
  - cancel job
  - check status
  - get result
- listing suite
  - `ListJobs`
  - pagination
  - status/store filtering
- chapter suite
  - `PutChapter`
  - `GetChapter`
  - stored metadata round-trip
- artifact suite
  - `PutChapter` with attached artifact uploads
  - `OpenArtifact`
  - artifact descriptor round-trip
- worker leasing suite
  - `PollWork`
  - `ExecutionLease.KeepAlive`
  - `ExecutionLease.Complete`
  - `ExecutionLease.Reschedule`
- replay-read suite
  - replay paths only use runtime reads
  - expected missing-data behavior

This package should be runtime-first, not engine-first.

### 2. `pkg/swftest/engineconformance`

Add a shared engine parity package for behavior that should be true for any engine built from a runtime.

Recommended contents:

- basic workflow success path
- task error envelope behavior
- job error envelope behavior
- chapter write-once constraints
- artifact cleanup user-visible behavior
- completion status surface behavior
- restart semantics visible through `SWFEngine`
- `GetJobRun` parity at the engine API

This is where the current duplicated direct/toy semantic tests should move when they are not runtime-storage-specific.

### 3. Keep backend-specific tests local

Some tests should remain in backend-specific packages.

Keep in `pkg/swf/impl`:

- transaction propagation
- pgwf lease expiry / crash concern behavior
- keepalive stop behavior tied to pgwf implementation details
- direct envelope storage format details
- Strata-specific fallback artifact behavior
- any tests asserting direct DB or Strata side effects

Keep in `pkg/swf/toy`:

- inline execution guarantees specific to toy
- custom job ID generator behavior
- pending-task queue implementation details specific to toy
- any intentionally toy-only shortcuts or limitations

Keep in `pkg/swf`:

- public types and helpers that are runtime-independent
  - artifact constructors
  - determinism error types
  - pure request/filter/page-token helpers

## Recommended Harness Design

Create a small runtime harness interface in `pkg/swftest`.

Example shape:

```go
type RuntimeHarness struct {
    Name string

    NewRuntime func(t *testing.T) swf.WorkflowRuntime
    BuildEngine func(t *testing.T, runtime swf.WorkflowRuntime, workers ...swf.WorkSet) swf.SWFEngine

    SupportsLeases bool
    SupportsRuntimeStorage bool
}
```

For direct:

- construct runtime via `pkg/swf/runtime/direct`
- use `pkg/swf/runtime/direct/testsupport`
- keep Strata/pgwf setup inside the direct runtime package

For toy:

- construct runtime via `pkg/swf/runtime/toy`

If a capability is not yet implemented consistently enough for both runtimes, mark it explicitly in the harness and skip only that suite.

## Proposed Consolidation Phases

### Phase 1: Build the conformance harness

- create `pkg/swftest/runtimeconformance`
- create shared harness helpers in `pkg/swftest`
- register `direct` and `toy`
- move the existing runtime construction tests into this structure

Done condition:

- both runtimes can be instantiated and exercised from one shared test entrypoint

### Phase 2: Consolidate direct/toy runtime duplication

Move the clearest runtime-parity tests first:

- restart behavior
- `GetJobRun` behavior that depends only on public engine/runtime output
- chapter metadata preservation
- artifact lookup / stored artifact round-trip
- pending runtime state surfaced through job views

Done condition:

- duplicated direct/toy tests are replaced by one shared suite where semantics are intended to match

### Phase 3: Consolidate engine semantic parity

Move current `pkg/swf` integration tests that are really runtime-agnostic engine tests into shared engine parity suites.

Candidates:

- basic workflow integration
- error workflow integration
- chapter constraints across runtimes
- artifact cleanup across runtimes

Done condition:

- cross-runtime engine behavior is covered by shared suites, not ad hoc duplicated tests

### Phase 4: Trim and reclassify remaining tests

After shared coverage exists:

- remove redundant direct/toy mirrors
- rename remaining backend-specific tests to make their scope explicit
- document why each remaining package-local suite is not shared

Done condition:

- every remaining package-local test is clearly either:
  - runtime-independent public API coverage
  - direct-only backend detail coverage
  - toy-only implementation detail coverage

## Initial Candidate Moves

### Move to runtime conformance first

- direct/toy restart boundary validation
- chapter read/write round-trip
- artifact open/round-trip
- lease reschedule/complete/keepalive basics
- list-jobs filter behavior where both runtimes support the same semantics

### Move to engine conformance next

- `GetJobRunCompleted`
- `GetJobRunPendingRuntime`
- `GetJobRunGetOutputFailed`
- basic workflow success
- task/job error envelope behavior at the engine API

### Keep local for now

- `TestCheckJobStatusUsesContextTransaction`
- `TestJobCrashConcernAfterRepeatedLeaseExpirations`
- `TestRunnerStopsKeepAliveOnExit`
- `TestAwaitUntilRecycle`
- direct envelope round-trip tests
- toy custom ID and inline-execution tests

## Risks

### Risk 1: accidental lowest-common-denominator semantics

Shared tests should capture intended contract, not erase valid implementation differences.

Mitigation:

- define contract expectations explicitly in the suite
- keep backend-specific assertions out of conformance tests

### Risk 2: hiding important direct-runtime behavior

Direct runtime has real DB/lease behavior that toy does not model fully.

Mitigation:

- keep direct-only tests for lease expiry, crash concern, DB transaction usage, and Strata-specific behavior

### Risk 3: unstable harness boundaries

If the runtime contract keeps moving, shared tests will churn.

Mitigation:

- finalize the runtime API first
- only then move large blocks of coverage

## Recommended Order of Work

1. create the runtime harness and conformance package in `pkg/swftest`
2. move the already-duplicated direct/toy restart and `GetJobRun` tests first
3. add shared runtime storage and artifact round-trip suites
4. add shared lease/poll suites
5. move cross-runtime engine integration tests out of `pkg/swf` into engine conformance
6. delete redundant mirrored tests only after the shared suites are proven stable

## Success Criteria

The consolidation is successful when:

- `direct`, `toy`, and later `remote` can all run the same runtime conformance suite
- engine parity across runtimes is covered by shared suites in `pkg/swftest`
- package-local tests are mostly backend-detail tests, not duplicated behavior tests
- adding `runtime/remote` requires mostly harness wiring, not copying large test files

## Bottom Line

Yes, there is meaningful test consolidation available now.

The standard `WorkflowRuntime` API gives us a real contract for shared conformance tests, and the current test layout already shows enough duplication to justify extracting shared suites immediately.
