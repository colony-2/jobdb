# WorkflowRuntime Facade Migration Guide

## Purpose

This guide is for users who adopted the first, incorrect `WithRuntime(...)` refactor and need to align with the current model.

The old intermediate state treated `WorkflowRuntime` like an engine-construction seam.
The current model treats `WorkflowRuntime` as the real backend facade.

## The Incorrect Intermediate Model

The earlier refactor effectively behaved like this:

```go
type WorkflowRuntime interface {
    BuildEngine(workers []swf.WorkSet, opts swf.RuntimeBuildOptions) (swf.SWFEngine, error)
}
```

That was wrong because it left worker execution and backend behavior mixed together.

## The Current Model

`WorkflowRuntime` is now the actual storage / scheduling / artifact facade:

```go
type WorkflowRuntime interface {
    StartJob(ctx context.Context, req swf.StartJobRequest) (swf.JobHandle, error)
    RestartJob(ctx context.Context, req swf.RestartJobRequest) (swf.JobHandle, error)
    CancelJob(ctx context.Context, req swf.CancelJobRequest) error

    PollWork(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error)

    CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error)
    GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error)
    GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error)
    ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error)

    GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.StoredChapter, error)
    PutChapter(ctx context.Context, req swf.PutChapterRequest) error

    OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error)
}
```

`ExecutionLease` is now part of that runtime boundary:

```go
type ExecutionLease interface {
    Job() swf.JobHandle
    Capability() string
    Payload() json.RawMessage
    KeepAlive(ctx context.Context) error
    Complete(ctx context.Context, req swf.CompleteExecutionRequest) error
    Reschedule(ctx context.Context, req swf.RescheduleExecutionRequest) error
}
```

## What Downstream Code Should Do Now

### Normal engine users

If you already do this:

```go
engine, err := swf.NewEngineBuilder().
    WithRuntime(runtime).
    PlusWorkers(jobWorker, taskWorkers...).
    BuildEngine()
```

and then use `SWFEngine`, you usually do not need further code changes.

### Custom runtime implementers

If you implemented the earlier builder-only `WorkflowRuntime`, that code is not source-compatible.

You now need to implement the full facade above.

## Important Current Difference

There is no concrete-runtime `BuildEngine(...)` hook anymore.

The current engine build path is:

- downstream provides any `swf.WorkflowRuntime`
- `swf.EngineBuilder.BuildEngine()` constructs the shared worker engine on top of that runtime

So the migration is:

- remove any custom `BuildEngine(...)` requirement from your runtime type
- move backend behavior into the `WorkflowRuntime` methods themselves

## Before And After

### Before: runtime as engine factory

```go
type MyRuntime struct{}

func (r *MyRuntime) BuildEngine(
    workers []swf.WorkSet,
    opts swf.RuntimeBuildOptions,
) (swf.SWFEngine, error) {
    ...
}
```

### After: runtime as backend facade

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
```

## Recommended Migration Order

1. Keep engine call sites on `WithRuntime(...).BuildEngine()`.
2. Delete any custom runtime-side engine factory assumptions.
3. Implement the real `WorkflowRuntime` facade.
4. Move backend-specific logic out of engine wrappers and into the runtime.
5. Add conformance tests around lifecycle, polling, chapters, artifacts, and leases.

## Bottom Line

The first runtime refactor was conceptually wrong.

The current shape is:

- `WorkflowRuntime` is the backend boundary
- `SWFEngine` is the workflow execution surface built on top of it
- `EngineBuilder.BuildEngine()` builds the shared worker engine for any `WorkflowRuntime`
