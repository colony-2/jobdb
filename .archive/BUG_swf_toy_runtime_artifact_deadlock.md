# Bug: `runtime/toy` can deadlock when persisting jobs that include artifacts

## Summary

In `github.com/colony-2/swf-go v0.0.0-20260316043542-40b3a335da97`, the toy runtime appears able to deadlock when a job persists output that includes artifacts.

The consumer uses only the public engine path:

```go
engine, err := swf.NewEngineBuilder().
    WithRuntime(toyruntime.New()).
    PlusWorkers(jobWorker, taskWorkers...).
    BuildEngine()
```

and then:

```go
go engine.Run(ctx)
jobKey, _ := engine.StartJob(...)
err := swf.WaitForJobToComplete(ctx, 30*time.Second, jobKey, engine)
```

The job never reaches a visible terminal state. `WaitForJobToComplete(...)` hangs until timeout.

## Version

- `github.com/colony-2/swf-go v0.0.0-20260316043542-40b3a335da97`

## Scenario

The failure shows up in jobs with artifact-bearing flows, for example:

- a task writes git/thin-pack style artifacts
- a later step passes those artifacts through
- job completion persists artifacts as part of task/job output handling

This does not look like a consumer API misuse. The consumer is using the supported `WorkflowRuntime` + `BuildEngine()` path and waiting via public `SWFEngine` APIs.

## Minimal Reproducer Shape

1. Build an engine on `runtime/toy`.
2. Register workers.
3. Start a job whose execution produces artifacts.
4. Wait for completion with `swf.WaitForJobToComplete(...)`.

Example shape:

```go
rt := toyruntime.New()
engine, err := swf.NewEngineBuilder().
    WithRuntime(rt).
    PlusWorkers(jobWorker, taskWorkers...).
    BuildEngine()
if err != nil {
    panic(err)
}

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go engine.Run(ctx)

jobKey, err := startArtifactBearingJob(engine)
if err != nil {
    panic(err)
}

if err := swf.WaitForJobToComplete(ctx, 30*time.Second, jobKey, engine); err != nil {
    // Repro: this times out instead of returning job completion/failure.
    panic(err)
}
```

## Observed Behavior

- Job execution appears to make progress.
- The wait path never observes completion.
- Goroutine dumps show multiple goroutines blocked on toy-runtime locking while artifact persistence/open is happening.

Representative stack fragments:

```text
github.com/colony-2/swf-go/pkg/swf/runtime/toy/internal/toyimpl.(*ToyEngine).getJobRecord
github.com/colony-2/swf-go/pkg/swf/runtime/toy/internal/toyimpl.(*Runtime).CheckJobStatus
github.com/colony-2/swf-go/pkg/swf.WaitForJobToComplete
```

```text
github.com/colony-2/swf-go/pkg/swf/runtime/toy/internal/toyimpl.(*ToyEngine).OpenStoredArtifact
github.com/colony-2/swf-go/pkg/swf/runtime/toy/internal/toyimpl.(*ToyEngine).PutStoredArtifacts
github.com/colony-2/swf-go/pkg/swf.persistTaskDataChapter
github.com/colony-2/swf-go/pkg/swf.(*workerRunner).persistJobOutcome
```

```text
github.com/colony-2/swf-go/pkg/swf/runtime/toy/internal/toyimpl.(*Runtime).PollWork
github.com/colony-2/swf-go/pkg/swf.(*workerEngine).Run
```

## Expected Behavior

- Artifact-bearing jobs should reach a terminal state.
- `swf.WaitForJobToComplete(...)` should return success or failure.
- The toy runtime should not deadlock while persisting or reopening artifacts.

## Suspected Root Cause

This looks like a lock-order / mutex reentrancy problem inside the toy runtime:

- one path holds the toy engine lock during persistence
- artifact persistence reopens a runtime-backed artifact
- another goroutine attempts status polling or work polling
- all of them end up blocked behind the same lock

## Upstream Areas Likely Involved

- `pkg/swf/runtime/toy/internal/toyimpl/toy.go`
- `pkg/swf/runtime/toy/internal/toyimpl/runtime.go`
- `pkg/swf/worker_runtime_support.go`
- `pkg/swf/worker_runner.go`

## Impact

- Artifact-heavy tests on `runtime/toy` can hang indefinitely.
- Consumers cannot rely on toy-runtime completion semantics for artifact-bearing workflows.
