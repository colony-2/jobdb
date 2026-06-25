# pkg/workflow

`pkg/workflow` is the higher-level Go SDK for writing JobDB workers. It builds
on `pkg/jobdb` and runs job workers, task workers, replay, external task waits,
and worker loops against any `jobdb.WorkflowRuntime`.

Use this package when you are writing workflow code. Use
[pkg/jobdb](../jobdb/README.md) when you need the lower-level runtime API or
runtime implementations.

## Core Concepts

- A `JobWorker` is the top-level workflow. It orchestrates task calls, waits,
  child jobs, and final output.
- A `TaskWorker` is a reusable unit of work called by a job.
- An `Engine` binds workers to a `jobdb.WorkflowRuntime`.
- The runtime can be local (`sqlite`, `toy`, `direct`) or remote
  (`runtime/remote`) against a running `jobdb` server.

## Quick Start Against A Server

Install and start a server:

```bash
npm install -g @colony2/jobdb
jobdb --listen 127.0.0.1:9047 --db jobdb.db
```

Then run workers against it through the remote runtime adapter:

```go
package main

import (
    "context"
    "log"

    "github.com/colony-2/jobdb/pkg/jobdb"
    remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
    "github.com/colony-2/jobdb/pkg/workflow"
)

type DataProcessingJob struct{}

func (DataProcessingJob) Name() string { return "data_processing" }

func (DataProcessingJob) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
    result, err := ctx.DoTask(jobdb.DefaultRunPolicy(), "validate", input)
    if err != nil {
        return nil, err
    }
    return ctx.DoTask(jobdb.DefaultRunPolicy(), "transform", result)
}

type ValidateTask struct{}

func (ValidateTask) Name() string { return "validate" }

func (ValidateTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
    return input, nil
}

type TransformTask struct{}

func (TransformTask) Name() string { return "transform" }

func (TransformTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
    return input, nil
}

func main() {
    ctx := context.Background()

    runtime, err := remoteruntime.New("http://127.0.0.1:9047", nil)
    if err != nil {
        log.Fatal(err)
    }

    engine, err := workflow.NewEngineBuilder().
        WithRuntime(runtime).
        WithWorkerTenantId("tenant-a").
        PlusWorkers(DataProcessingJob{}, ValidateTask{}, TransformTask{}).
        BuildEngine()
    if err != nil {
        log.Fatal(err)
    }

    go engine.Run(ctx)

    input := jobdb.NewTaskDataOrPanic(map[string]any{"value": 42})
    jobKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
        TenantId: "tenant-a",
        JobType:  "data_processing",
        Data:     input,
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("started job: %s", jobKey)
}
```

## Worker Interfaces

```go
type JobWorker interface {
    Name() string
    Run(workflow.JobContext, jobdb.JobData) (jobdb.JobData, error)
}

type TaskWorker interface {
    Name() string
    Run(workflow.TaskContext, jobdb.TaskData) (jobdb.TaskData, error)
}
```

Jobs use `workflow.JobContext`:

```go
func (j MyJob) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
    output, err := ctx.DoTask(jobdb.DefaultRunPolicy(), "task-name", input)
    if err != nil {
        return nil, err
    }

    if err := ctx.AwaitDuration(jobdb.Duration(5 * time.Minute)); err != nil {
        return nil, err
    }

    return output, nil
}
```

Tasks use `workflow.TaskContext`:

```go
func (t MyTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
    raw, err := input.GetData()
    if err != nil {
        return nil, err
    }

    var payload MyPayload
    if err := json.Unmarshal(raw, &payload); err != nil {
        return nil, err
    }

    return jobdb.NewTaskData(payload)
}
```

## Engine Configuration

Build an engine over any `jobdb.WorkflowRuntime`:

```go
engine, err := workflow.NewEngineBuilder().
    WithRuntime(runtime).
    WithWorkerTenantId("tenant-a").
    WithMaxActive(10).
    WithLogger(logger).
    WithAwaitRecycleThreshold(5 * time.Minute).
    PlusWorkers(jobWorker, taskWorkerA, taskWorkerB).
    BuildEngine()
```

Workers can also be registered after the engine is built:

```go
workset, err := workflow.AsWorkSet(jobWorker, taskWorkerA, taskWorkerB)
if err != nil {
    return err
}
err = engine.RegisterWorkers(workset)
```

## Runtime Choices

Workflow workers can use any runtime implementation:

- Remote server: `remoteruntime.New("http://127.0.0.1:9047", nil)`.
- SQLite embedded: `sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{...})`.
- Toy in-memory: `toyruntime.New()`.
- Direct Postgres: `directruntime.NewFromConfig(directruntime.Config{...})`.

For server operation and backend flags, see the root
[README](../../README.md). For runtime package details, see
[pkg/jobdb/README.md](../jobdb/README.md).

## Data, Artifacts, Policies, And Errors

The workflow SDK uses `pkg/jobdb` data types for payloads, artifacts, run
policies, schedules, and runtime errors.

Common examples:

```go
data := jobdb.NewTaskDataOrPanic(map[string]any{"n": 1})

policy := jobdb.RunPolicy{
    Retry: jobdb.RetryPolicy{
        InitialInterval:    jobdb.Duration(100 * time.Millisecond),
        BackoffCoefficient: 2,
        MaximumAttempts:    3,
    },
    InvocationTimeout: jobdb.AsDuration(30 * time.Second),
    TotalTimeout:      jobdb.AsDuration(10 * time.Minute),
}
```

Worker-returned errors are stored as job or task failures. Use
`jobdb.NewSystemError` for infrastructure failures and regular Go errors for
application failures.

## External Task Completion

Jobs can wait on task capabilities that are completed by external systems. The
engine exposes discovery helpers such as `FindTasksWaitingForCapability`,
`FindTasksWaiting`, and `GetWaitingTask`; returned `TaskHandle` values can be
completed with `Finish`.

```go
handles, err := engine.FindTasksWaitingForCapability(ctx, "approval-job", "approve", []string{"tenant-a"})
if err != nil {
    return err
}
for _, handle := range handles {
    output := jobdb.NewTaskDataOrPanic(map[string]any{"approved": true})
    if err := handle.Finish(ctx, output); err != nil {
        return err
    }
}
```

## Replay And Inspection

`workflow.ReplayRunRequest` and `Engine.ReplayJobRun` can replay persisted
history for inspection and determinism checks. Runtime-level job-run read models
come from `pkg/jobdb`.
