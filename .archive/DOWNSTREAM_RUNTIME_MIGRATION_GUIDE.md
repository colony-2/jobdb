# Downstream Migration Guide: Runtime-Based SWF Construction

## Summary

SWF engine construction is now runtime-based.

Downstream code should:

- construct a `swf.WorkflowRuntime`
- pass it to `EngineBuilder.WithRuntime(...)`
- call `BuildEngine()`

`swf` itself no longer exposes direct `strata` or `pgwf` construction/configuration hooks.

## Current Preferred Construction

### Direct runtime from config

```go
import (
    "log/slog"
    "time"

    "github.com/colony-2/swf-go/pkg/swf"
    directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
)

runtime, err := directruntime.NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey)
if err != nil {
    return err
}

engine, err := swf.NewEngineBuilder().
    WithRuntime(runtime).
    WithLogger(slog.Default()).
    WithAwaitRecycleThreshold(5 * time.Minute).
    PlusWorkers(jobWorker, taskWorkers...).
    BuildEngine()
if err != nil {
    return err
}
```

### Direct runtime from existing clients

```go
import (
    "github.com/colony-2/swf-go/pkg/swf"
    directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
)

runtime := directruntime.New(gormDB, strataClient)

engine, err := swf.NewEngineBuilder().
    WithRuntime(runtime).
    PlusWorkers(jobWorker, taskWorkers...).
    BuildEngine()
if err != nil {
    return err
}
```

### Toy runtime

```go
import (
    "github.com/colony-2/swf-go/pkg/swf"
    toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
)

engine, err := swf.NewEngineBuilder().
    WithRuntime(toyruntime.New()).
    PlusWorkers(jobWorker, taskWorkers...).
    BuildEngine()
if err != nil {
    return err
}
```

### Third-party runtime

Any downstream runtime can implement `swf.WorkflowRuntime` directly:

```go
type MyRuntime struct {
    ...
}

func (r *MyRuntime) StartJob(ctx context.Context, req swf.StartJobRequest) (swf.JobHandle, error) { ... }
func (r *MyRuntime) RestartJob(ctx context.Context, req swf.RestartJobRequest) (swf.JobHandle, error) { ... }
func (r *MyRuntime) CancelJob(ctx context.Context, req swf.CancelJobRequest) error { ... }
func (r *MyRuntime) PollWork(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) { ... }
func (r *MyRuntime) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) { ... }
func (r *MyRuntime) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) { ... }
func (r *MyRuntime) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) { ... }
func (r *MyRuntime) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) { ... }
func (r *MyRuntime) GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.StoredChapter, error) { ... }
func (r *MyRuntime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error { ... }
func (r *MyRuntime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) { ... }

engine, err := swf.NewEngineBuilder().
    WithRuntime(&MyRuntime{}).
    PlusWorkers(jobWorker, taskWorkers...).
    BuildEngine()
```

There is no separate runtime-side `BuildEngine(...)` hook anymore. `EngineBuilder.BuildEngine()` always builds the shared worker engine on top of the provided `WorkflowRuntime`.

## Old To New Mapping

Removed builder/configuration APIs:

- `EngineBuilder.WithPostgresDSN(...)`
- `EngineBuilder.WithStrata(...)`
- `EngineBuilder.WithStrataAPIKey(...)`
- `EngineBuilder.Build(...)`
- `swf.Builder`

Replacement:

- `runtime/direct.NewFromConfig(...)` or `runtime/direct.New(...)`
- `runtime/toy.New()`
- `EngineBuilder.WithRuntime(...)`
- `EngineBuilder.BuildEngine()`

## Public API Changes

### Removed backend-leaking types and helpers

These old public APIs are gone from `swf`:

- `swf.Lease`
- `swf.Dependencies`
- `JobKey.ToStoryKey()`
- `JobKeyFromStoryKey(...)`
- `WorkSet.Capabilities`
- `swf.FromStrataArtifact(...)`
- `swf.ToStrataArtifact(...)`
- `swf.PgwfMetadataPredicates(...)`

Current guidance:

- keep using `swf.JobKey`, not Strata story keys
- use `swf.Artifact` and SWF artifact constructors, not Strata artifact bridge helpers
- use `swf.MetadataPredicates(...)` only if you are explicitly working with SWF metadata filters
- treat worker capability routing as internal runtime/engine behavior, not downstream API surface

### Runtime facade

`swf.WorkflowRuntime` is now the real backend facade, not just a constructor seam.

That means downstream code can either:

- keep using `swf.SWFEngine`
- use `swf.WorkflowRuntime` directly for job lifecycle, polling, chapter access, and artifact access

Example:

```go
handle, err := runtime.StartJob(ctx, swf.StartJobRequest{
    Job: swf.StartJob{
        TenantId: "tenant-a",
        JobType:  "example-job",
        Data:     swf.NewTaskDataOrPanic(map[string]any{"x": 1}),
    },
    RequestTime: time.Now().UTC(),
})
if err != nil {
    return err
}

status, err := runtime.CheckJobStatus(ctx, handle.JobKey)
if err != nil {
    return err
}
```

## Migration Notes

### If you only built engines

Most downstream users only need to change construction:

Old mental model:

- SWF builder owned backend wiring

New mental model:

- backend wiring lives in the runtime implementation
- `swf.EngineBuilder` only configures shared worker-engine behavior

### If you implemented custom backends

Implement `swf.WorkflowRuntime`.

Do not add backend-specific client types to the public `swf` surface.
Keep backend-specific clients, handles, and storage adapters inside your runtime package.

### If you still have Strata or `pgwf` usage

Keep that code in runtime-specific packages such as:

- `pkg/swf/runtime/direct`
- your own custom runtime package

It should not be part of application-facing workflow code built on top of `swf`.

## Bottom Line

The downstream migration target is now:

1. choose or implement a `swf.WorkflowRuntime`
2. construct the engine with `WithRuntime(...).BuildEngine()`
3. keep `strata` / `pgwf` details out of downstream workflow code

There is no compatibility `Build(...)` path and no concrete-runtime `BuildEngine(...)` requirement anymore.
