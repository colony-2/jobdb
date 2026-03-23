# Usage API Parity Test Inventory

This file turns the parity plan into an implementation inventory.

## New Package

- `pkg/swf/internal/swftest/usageparity`

## New Shared Helpers

### `pkg/swf/internal/swftest/usageparity/parity_helpers.go`

Add:

- `type ScenarioObservation`
- `type NormalizedResult`
- `type NormalizedJobRun`
- `type NormalizedJobAttempt`
- `type NormalizedTaskRun`
- `type NormalizedChapter`
- `type NormalizedJobSummary`
- `type NormalizedArtifact`
- `func ObserveViaEngine(...)`
- `func ObserveViaRuntime(...)`
- `func CompareObservations(t *testing.T, got, want ScenarioObservation)`
- `func NormalizeJobRun(...)`
- `func NormalizeListJobs(...)`
- `func NormalizeArtifacts(...)`
- `func NormalizeStoredChapter(...)`

Helper requirements:

- support both `toy` and `direct`
- never compare lease IDs directly
- allow per-scenario tolerances for timestamps/order where needed

## Scenario Inventory

## 1. Construction parity

File:

- `pkg/swf/internal/swftest/usageparity/construction_parity_test.go`

Tests:

- `TestEngineAndRuntimeConstructionParityAcrossBuiltInRuntimes`
- `TestEngineBuildOptionsSmokeParityAcrossBuiltInRuntimes`
- `TestRegisterWorkersAfterBuildParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go](/src/pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go)
- [pkg/swf/runtime_engine_test.go](/src/pkg/swf/runtime_engine_test.go)

What this adds:

- explicit comparison of supported downstream construction paths
- assurance that engine-construction wrappers do not alter behavior relative to direct runtime use

## 2. Basic lifecycle parity

File:

- `pkg/swf/internal/swftest/usageparity/lifecycle_parity_test.go`

Tests:

- `TestStartJobParityAcrossBuiltInRuntimes`
- `TestExplicitJobIDParityAcrossBuiltInRuntimes`
- `TestCancelJobParityAcrossBuiltInRuntimes`
- `TestRestartJobParityAcrossBuiltInRuntimes`
- `TestRestartValidationParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go)
- [pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go)

What to compare:

- returned job key
- final status
- final result or error kind
- `ListJobs` summary for target job
- `GetJobRun` high-level status

## 3. Sequential execution parity

File:

- `pkg/swf/internal/swftest/usageparity/execution_parity_test.go`

Tests:

- `TestSequentialWorkflowParityAcrossBuiltInRuntimes`
- `TestTaskFailureParityAcrossBuiltInRuntimes`
- `TestJobFailureParityAcrossBuiltInRuntimes`
- `TestDeterminismFailureParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/basic_workflow_integration_test.go](/src/pkg/swf/basic_workflow_integration_test.go)
- [pkg/swf/error_workflow_integration_test.go](/src/pkg/swf/error_workflow_integration_test.go)
- [pkg/swf/chapter_constraints_test.go](/src/pkg/swf/chapter_constraints_test.go)

What to compare:

- final result
- final status
- chapter sequence
- chapter payload kinds
- decoded outputs at each step

## 4. External task parity

File:

- `pkg/swf/internal/swftest/usageparity/execution_parity_test.go`

Tests:

- `TestExternalTaskCompletionParityAcrossBuiltInRuntimes`
- `TestPendingTaskHandleShapeParityAcrossBuiltInRuntimes`
- `TestReplayAfterExternalTaskCompletionParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/basic_workflow_integration_test.go](/src/pkg/swf/basic_workflow_integration_test.go)
- [pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go)

What to compare:

- waiting task presence
- waiting task metadata
- input seen by external completer
- post-completion chapter/result state

## 5. Await parity

File:

- `pkg/swf/internal/swftest/usageparity/execution_parity_test.go`

Tests:

- `TestAwaitJobsParityAcrossBuiltInRuntimes`
- `TestTaskAwaitJobsParityAcrossBuiltInRuntimes`
- `TestAwaitDurationParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go)
- [pkg/swf/worker_runner_test.go](/src/pkg/swf/worker_runner_test.go)

What to compare:

- intermediate status (`PENDING_JOBS`, `AWAITING_FUTURE`)
- `ListJobs.WaitFor`
- resumed result after dependency/time elapses

## 6. Retry parity

File:

- `pkg/swf/internal/swftest/usageparity/execution_parity_test.go`

Tests:

- `TestJobRetryParityAcrossBuiltInRuntimes`
- `TestTaskRetryParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_additional_conformance_test.go)
- [pkg/swf/worker_runner_test.go](/src/pkg/swf/worker_runner_test.go)

What to compare:

- final result
- chapter/attempt count
- `GetJobRun` attempt structure

Notes:

- direct-only synthesized retry views should remain in direct-specific tests unless toy/runtime semantics are intentionally aligned

## 7. `GetJobRun` parity

File:

- `pkg/swf/internal/swftest/usageparity/job_run_parity_test.go`

Tests:

- `TestCompletedJobRunParityAcrossBuiltInRuntimes`
- `TestLazyOutputLoadParityAcrossBuiltInRuntimes`
- `TestFailedGetOutputParityAcrossBuiltInRuntimes`
- `TestCancelledGetOutputParityAcrossBuiltInRuntimes`
- `TestPendingRuntimeViewParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go](/src/pkg/swf/internal/swftest/engineconformance/engine_conformance_test.go)

What to compare:

- normalized job-run tree
- `GetOutput(...)` result/error class
- pending runtime fields

## 8. Artifact parity

File:

- `pkg/swf/internal/swftest/usageparity/artifact_parity_test.go`

Tests:

- `TestArtifactSuccessPathParityAcrossBuiltInRuntimes`
- `TestArtifactStorageOnTaskErrorParityAcrossBuiltInRuntimes`
- `TestArtifactStorageOnJobErrorParityAcrossBuiltInRuntimes`
- `TestArtifactCleanupVisibleBehaviorParityAcrossBuiltInRuntimes`
- `TestRuntimeArtifactRoundTripParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/artifact_cleanup_integration_test.go](/src/pkg/swf/artifact_cleanup_integration_test.go)
- [pkg/swf/artifact_cleanup_dual_engine_integration_test.go](/src/pkg/swf/artifact_cleanup_dual_engine_integration_test.go)
- [pkg/swf/artifact_storage_on_error_test.go](/src/pkg/swf/artifact_storage_on_error_test.go)
- [pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go](/src/pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go)

What to compare:

- artifact names
- digests
- sizes
- bytes
- chapter artifact references
- output retrieval behavior after cleanup

## 9. Chapter parity

File:

- `pkg/swf/internal/swftest/usageparity/artifact_parity_test.go`

Tests:

- `TestStoredChapterRoundTripParityAcrossBuiltInRuntimes`
- `TestChapterMetadataRoundTripParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go](/src/pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go)

What to compare:

- ordinal
- task type
- chapter type
- payload kind
- input hash
- metadata payload
- stored artifact refs

## 10. List/query parity

File:

- `pkg/swf/internal/swftest/usageparity/list_jobs_parity_test.go`

Tests:

- `TestListJobsStatusParityAcrossBuiltInRuntimes`
- `TestListJobsJobTypeParityAcrossBuiltInRuntimes`
- `TestListJobsJobTaskParityAcrossBuiltInRuntimes`
- `TestListJobsJobIDParityAcrossBuiltInRuntimes`
- `TestListJobsSingletonParityAcrossBuiltInRuntimes`
- `TestListJobsMetadataPredicateParityAcrossBuiltInRuntimes`
- `TestListJobsPaginationParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/list_jobs_integration_test.go](/src/pkg/swf/list_jobs_integration_test.go)
- [pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go](/src/pkg/swf/internal/swftest/runtimeconformance/runtime_conformance_test.go)

What to compare:

- normalized job summaries
- ordering
- next page token presence/absence

## 11. Completion status parity

File:

- `pkg/swf/internal/swftest/usageparity/lifecycle_parity_test.go`

Tests:

- `TestCompletionStatusParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/completion_status_integration_test.go](/src/pkg/swf/completion_status_integration_test.go)

What to compare:

- final archived status
- final `GetJobResult` error kind
- output envelope kind where relevant

## 12. Prerequisite parity

File:

- `pkg/swf/internal/swftest/usageparity/lifecycle_parity_test.go`

Tests:

- `TestPrerequisiteSuccessAndCompleteParityAcrossBuiltInRuntimes`
- `TestRestartPrerequisiteCheckParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/prerequisites_integration_test.go](/src/pkg/swf/prerequisites_integration_test.go)

What to compare:

- prerequisite wait behavior
- success vs complete semantics
- restart-extra prerequisite failure behavior

## 13. Multi-engine / worker behavior parity

File:

- `pkg/swf/internal/swftest/usageparity/construction_parity_test.go`

Tests:

- `TestMultiEngineSharedRuntimeParityOnDirectRuntime`
- `TestWorkerRegistrationParityAcrossBuiltInRuntimes`
- `TestMaxActiveSmokeParityAcrossBuiltInRuntimes`

Current related coverage:

- [pkg/swf/basic_workflow_integration_test.go](/src/pkg/swf/basic_workflow_integration_test.go)
- [pkg/swf/worker_engine_test.go](/src/pkg/swf/worker_engine_test.go)

Notes:

- true multi-engine distribution is only meaningful on `direct`
- keep direct-specific scheduling/lease assertions in direct runtime tests
- parity here should assert SWF-level outcomes, not pgwf lease internals

## Proposed Execution Order

## Batch 1: immediate value

Implement first:

- `construction_parity_test.go`
- `lifecycle_parity_test.go`
- `execution_parity_test.go`

Start with these tests:

- `TestEngineAndRuntimeConstructionParityAcrossBuiltInRuntimes`
- `TestStartJobParityAcrossBuiltInRuntimes`
- `TestExplicitJobIDParityAcrossBuiltInRuntimes`
- `TestCancelJobParityAcrossBuiltInRuntimes`
- `TestRestartJobParityAcrossBuiltInRuntimes`
- `TestSequentialWorkflowParityAcrossBuiltInRuntimes`
- `TestExternalTaskCompletionParityAcrossBuiltInRuntimes`

Reason:

- these cover the highest-risk downstream workflows with the least normalization complexity

## Batch 2: shape and query parity

Implement next:

- `job_run_parity_test.go`
- `list_jobs_parity_test.go`

Start with:

- `TestCompletedJobRunParityAcrossBuiltInRuntimes`
- `TestPendingRuntimeViewParityAcrossBuiltInRuntimes`
- `TestListJobsStatusParityAcrossBuiltInRuntimes`
- `TestListJobsJobTaskParityAcrossBuiltInRuntimes`

## Batch 3: artifacts, retries, prerequisites

Implement last:

- `artifact_parity_test.go`
- remaining lifecycle and execution parity tests

Start with:

- `TestRuntimeArtifactRoundTripParityAcrossBuiltInRuntimes`
- `TestArtifactStorageOnTaskErrorParityAcrossBuiltInRuntimes`
- `TestJobRetryParityAcrossBuiltInRuntimes`
- `TestTaskRetryParityAcrossBuiltInRuntimes`
- `TestPrerequisiteSuccessAndCompleteParityAcrossBuiltInRuntimes`

## Tests That Should Stay Outside the Parity Suite

Keep direct-only:

- [pkg/swf/runtime/direct/internal/directimpl/envelope_test.go](/src/pkg/swf/runtime/direct/internal/directimpl/envelope_test.go)
- lease/pgwf-specific behavior
- Strata-specific storage side effects

Keep package-local unit tests:

- [pkg/swf/worker_runner_test.go](/src/pkg/swf/worker_runner_test.go)
- [pkg/swf/worker_engine_test.go](/src/pkg/swf/worker_engine_test.go)
- pure artifact and determinism unit tests in [pkg/swf](/src/pkg/swf)

Reason:

- those tests validate implementation mechanics, not public usage parity

## Success Criteria

The inventory is complete when:

- every P0 and P1 scenario has a parity test
- every parity test runs against both built-in runtimes unless explicitly marked direct-only
- overlapping conformance cases can be reduced or removed without losing behavioral coverage
- downstream examples in migration guides are all exercised by executable tests
