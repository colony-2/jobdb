# pkg/jobdb

`pkg/jobdb` is the Go runtime API and data model for JobDB. It owns the
low-level contract used by runtime implementations and by the HTTP runtime
adapter.

This package does not define job workers, task workers, or the worker engine.
For worker processes, use [pkg/workflow](../workflow/README.md).

The named-cell release requires a fresh execution store. Submit requests need
`Cell`; creating an ordinary cell with its first job also needs `CellRepo` and
creation authority. Embedded runtimes take trusted policy through
`jobdb.WithCellAuthority`; HTTP embeddings supply it in authenticated middleware.
See [named cells](../../docs/SPEC-Tenant-Cells.md) for root bootstrap and policy scope.

## What Lives Here

- `WorkflowRuntime`: backend-agnostic runtime interface.
- `ConfigurationSnapshot`, `GetConfigurationRequest`, and `ConfigurationAdmin`:
  tenant configuration access. `ExecutionLease.GetConfiguration` reads a fresh
  snapshot; the runtime transport method requires verified lease authority.
- Job lifecycle requests: `SubmitJobRequest`, `SubmitRestartJobRequest`,
  `CancelJobRequest`, `JobHandle`, `JobKey`, and `JobInfo`.
- Runtime work leasing: `PollWorkRequest`, `GetJobLeaseRequest`,
  `ExecutionLease`, `CompleteExecutionRequest`, and
  `RescheduleExecutionRequest`.
- Data and artifacts: `TaskData`, `JobData`, `Artifact`, `ArtifactKey`,
  `ArtifactRef`, and artifact constructors.
- Durable run history: chapters, job-run read models, task attempts, outcomes,
  and runtime error payloads.
- List-jobs request/response types.

## Runtime Packages

The runtime implementations live below `pkg/jobdb/runtime`.

### `runtime/sqlite`

Durable embedded runtime backed by SQLite plus local blob storage by default.

```go
runtime, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{
    DBPath:  "jobdb.db",
    BlobDir: "/var/lib/jobdb/blobs",
})
if err != nil {
    return err
}
defer runtime.Close(context.Background())
```

Use this when you want local durable execution without running Postgres or an
external artifact service. `blobfs://` works without optional provider imports.
Executable/server code that wants Go CDK URLs such as `file://`, `gs://bucket`,
`s3://bucket?region=us-east-1`, or `azblob://container` must import
`github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk` to register those
providers. Credential lookup is handled by the Go CDK provider drivers; see the
repository README for provider-specific environment variables, VM role behavior,
and references.

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

### Postgres runtime

The Postgres runtime lives in
`github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime`. The pgjobdb CLI adds
`direct` and `serve` commands to JobDB's base CLI and includes Go CDK provider
registration. Library embedders that pass provider-backed `BlobStoreURI`
values must import `github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk`
from their executable/server code.

## Runtime Usage

Submit a job through any runtime:

```go
input := jobdb.NewTaskDataOrPanic(map[string]any{"n": 1})

handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
    Job: jobdb.SubmitJob{
        Cell: "example", CellRepo: "github.com/example/workflows",
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

Secret providers receive verified lease identity and expiry in
`SecretResolveRequest.Lease`. Individual `SecretValue` results may opt into
lease-bound expiry with `LeaseBound`. Workflow contexts expose
`ResolveSecretsFresh` to refresh the current execution attempt's cache; bootstrap
can use `workflow.SecretResolverFromContext` inside `BeforeRun`.
See [lease-bound secrets and fresh resolution](../../docs/GUIDE-LEASE-BOUND-SECRETS.md).

## Crash Concern And Ready Work

`PollWork`, `GetJobLease`, and external-task completion claims require `READY`.
`CRASH_CONCERN` jobs are excluded, including when an alternate route becomes due.
SQLite and toy assign five remaining crash credits. The first acquisition keeps
five; successful expired execution reclaims install 4/3/2/1/0. The final live
zero-credit lease remains `ACTIVE`; another acquisition after its expiry is
blocked. Reads, expiry and renewals neither debit nor refill credit. Ordinary
successful rescheduling assigns five; resource backoff assigns its independently
bound allowance. Child submission and append to an existing job preserve the
parent's balance.

SQLite format 14 persists the countdown and resource state. Existing formats
11/12/13 require the supported explicit offline migration; legacy format-13
counts become `max(5-oldCount,0)` once, preserving outstanding leases. Toy keeps
memory semantics only. PostgreSQL migration and configurable-default delivery
belong to pgjobdb. See [resource backoff](../../docs/GUIDE-RESOURCE-BACKOFF.md).

Operators can call `WorkflowRuntime.ClearCrashConcern` or
`jobdb jobs clear-crash-concern -t TENANT JOB_ID` to restore the default crash budget
and restore normal scheduling eligibility. Clearing preserves history and other
scheduling constraints, and rejects live leases, completed, cancelled, or hidden
jobs. See the [CLI, REST, and Go recovery guide](../../docs/GUIDE-CRASH-CONCERN.md).

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
- `JobCancelledError`
- `NonRetryableError`

Helpers such as `IsAppError`, `IsSystemError`, `IsTimeoutError`, and
`IsJobFailed` are available for classification.

Cancelled results from `GetJobRunResponse.GetOutput` and the workflow runner's
`JobRunOutcome.JobError` return `*JobCancelledError`. Use
`errors.Is(err, jobdb.ErrJobCancelled)` to classify cancellation and
`errors.As(err, &cancelled)` with `var cancelled *jobdb.JobCancelledError` to read
`cancelled.Detail`. The detail preserves the exact stored cancellation reason,
including whitespace and newlines; an empty reason stays empty. `Error()` includes
a nonempty reason and otherwise returns `"job cancelled"`. Cancellation remains
distinct from `JobFailedError` and takes precedence over prior attempt failures.

## Job Listing

The runtime API includes `ListJobsRequest`, `ListJobsResponse`, `JobSummary`,
and metadata filters.

The server exposes these through the same remote runtime API used for job
lifecycle and leasing.

`JobSummary.CompletionStatus` and `CompletionDetail` expose the persisted terminal
scheduler outcome without reading history or replaying the workflow. Known
categories are `success`, `failed_app`, `failed_system`, `failed_timeout`, and
`cancelled`; other values are preserved. Empty status means unavailable, and empty
detail is valid. Scheduler `Status` and its filters are unchanged: `COMPLETED`
alone does not imply success. Retry attempts and cancellation requests alone do
not establish a final outcome. The fields also appear in `GetJobRun`'s job summary. Remote JSON uses optional `completionStatus` and
`completionDetail` fields; older servers may omit them.
