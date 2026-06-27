# pkg/jobdb

`pkg/jobdb` is the Go runtime API and data model for JobDB. It owns the
low-level contract used by runtime implementations and by the HTTP runtime
adapter.

This package does not define job workers, task workers, or the worker engine.
For worker processes, use [pkg/workflow](../workflow/README.md).

## What Lives Here

- `WorkflowRuntime`: backend-agnostic runtime interface.
- Job lifecycle requests: `SubmitJobRequest`, `SubmitRestartJobRequest`,
  `CancelJobRequest`, `JobHandle`, `JobKey`, and `JobInfo`.
- Runtime work leasing: `PollWorkRequest`, `GetJobLeaseRequest`,
  `ExecutionLease`, `CompleteExecutionRequest`, and
  `RescheduleExecutionRequest`.
- Data and artifacts: `TaskData`, `JobData`, `Artifact`, `ArtifactKey`,
  `ArtifactRef`, and artifact constructors.
- Durable run history: chapters, job-run read models, task attempts, outcomes,
  and runtime error payloads.
- Schedules and list-jobs request/response types.

## Runtime Packages

The runtime implementations live below `pkg/jobdb/runtime`.

### `runtime/sqlite`

Durable embedded runtime backed by SQLite plus Go CDK blob storage.

```go
runtime, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{
    DBPath:       "jobdb.db",
    BlobStoreURI: "file:///var/lib/jobdb/blobs",
})
if err != nil {
    return err
}
defer runtime.Close(context.Background())
```

Use this when you want local durable execution without running Postgres or an
external artifact service. Set `BlobStoreURI` to a Go CDK bucket URL such as
`gs://bucket`, `s3://bucket?region=us-east-1`, or `azblob://container` when
artifacts should live outside the local filesystem. Credential lookup is handled
by the Go CDK provider drivers; see the repository README for provider-specific
environment variables, VM role behavior, and references.

### `runtime/remote`

HTTP client/server adapter for the JobDB runtime REST API.

Create a client for a running `jobdb` server:

```go
runtime, err := remoteruntime.New("http://127.0.0.1:9047", nil)
if err != nil {
    return err
}
```

Serve any `jobdb.WorkflowRuntime` over HTTP:

```go
handler := remoteruntime.NewServer(runtime)
err := http.ListenAndServe("127.0.0.1:9047", handler)
```

The REST wire contract is defined in
[../../openapi/jobdb-runtime.yaml](../../openapi/jobdb-runtime.yaml).

### `runtime/toy`

In-memory runtime for tests and short-lived local experiments.

```go
runtime := toyruntime.New()
```

The toy runtime is not durable.

### `runtime/direct`

Postgres direct runtime. It stores job records and chapter records in Postgres
and stores large artifact bytes through a configured blobstore URI.

```go
runtime, err := directruntime.NewFromConfig(directruntime.Config{
    PostgresDSN:  postgresDSN,
    BlobStoreURI: "s3://jobdb-artifacts?region=us-east-1",
})
if err != nil {
    return err
}
```

`jobdb direct` wraps this runtime and serves it over the remote runtime API.

## Runtime Usage

Submit a job through any runtime:

```go
input := jobdb.NewTaskDataOrPanic(map[string]any{"n": 1})

handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
    Job: jobdb.SubmitJob{
        TenantId: "tenant-a",
        JobType:  "example-job",
        Data:     input,
    },
    RequestTime: time.Now().UTC(),
})
if err != nil {
    return err
}
```

Poll and complete work at the runtime layer:

```go
leases, err := runtime.PollWork(ctx, jobdb.PollWorkRequest{
    TenantId:      "tenant-a",
    WorkerID:      "worker-a",
    Capabilities:  []string{"example-job"},
    Limit:         1,
    LeaseDuration: 30 * time.Second,
})
if err != nil {
    return err
}
if len(leases) == 0 {
    return nil
}

lease := leases[0]
chapter := jobdb.Chapter{
    Ordinal:   1,
    TaskType:  lease.Capability(),
    CreatedAt: time.Now().UTC(),
    Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
        Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
    }},
}
err = lease.Complete(ctx, jobdb.CompleteExecutionRequest{
    Status:  "success",
    Chapter: &chapter,
})
```

Most applications should not implement worker execution directly against leases.
Use [pkg/workflow](../workflow/README.md) for job and task workers.

## Data And Artifacts

`TaskData` is the runtime payload container. It carries JSON data and optional
artifacts.

```go
data, err := jobdb.NewTaskData(map[string]any{
    "userId": 123,
    "action": "process",
})
```

Artifacts represent file-like data that can be stored with job and task inputs
or outputs:

```go
artifact, err := jobdb.NewArtifactFromFile("report.csv", "/tmp/report.csv")
data, err := jobdb.NewTaskData(payload, artifact)
```

Artifacts are assigned runtime keys when persisted. Lazy artifacts can be
materialized later through a type that implements `jobdb.ArtifactGetter`.

## Error Types

The runtime layer exposes structured error types for application, system, and
timeout failures:

- `AppError`
- `SystemError`
- `TimeoutError`
- `JobFailedError`
- `NonRetryableError`

Helpers such as `IsAppError`, `IsSystemError`, `IsTimeoutError`, and
`IsJobFailed` are available for classification.

## Schedules And Listing

The runtime API includes first-class schedule and job listing types:

- `UpsertScheduleRequest`, `ScheduleSpec`, `ScheduleTarget`, and schedule
  mutation/list request types.
- `ListJobsRequest`, `ListJobsResponse`, `JobSummary`, and metadata filters.

The server exposes these through the same remote runtime API used for job
lifecycle and leasing.
