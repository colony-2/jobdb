# SWFEngine Lease Acquisition Proposal

## Recommendation

Split the engine surface:

- add `GetJobLease(...)` directly to `swf.SWFEngine`
- do **not** add raw `PollWork(...)` to `swf.SWFEngine`
- instead, enhance the existing waiting-task discovery API with a request-based method that can carry metadata filters

## Proposed Shape

```go
type taskRunApi interface {
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error)
	FindTasksWaiting(ctx context.Context, req FindTasksWaitingRequest) ([]TaskHandle, error)
	GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
}

type jobLeaseApi interface {
	GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error)
}

type SWFEngine interface {
	jobRunApi
	taskRunApi
	jobLeaseApi
	loopWorkerApi
	jobsListApi

	RegisterWorkers(workset *WorkSet) error
	GetArtifact(tenantId string, key ArtifactKey) (Artifact, error)
}

type FindTasksWaitingRequest struct {
	JobType        string
	TaskType       string
	TenantIds      []string
	MetadataFilter MetadataFilter
	Limit          int
}
```

`FindTasksWaitingForCapability(...)` stays as the convenience wrapper:

```go
func (e *runtimeEngine) FindTasksWaitingForCapability(
	ctx context.Context,
	jobType string,
	taskType string,
	tenantIds []string,
) ([]TaskHandle, error) {
	return e.FindTasksWaiting(ctx, FindTasksWaitingRequest{
		JobType:   jobType,
		TaskType:  taskType,
		TenantIds: tenantIds,
	})
}
```

## Why `GetJobLease` Belongs On `SWFEngine`

`GetJobLease(...)` exposes a specific capability that the current external engine API cannot express:

- claim one known job exclusively
- get back an `ExecutionLease`
- drive completion/reschedule through the existing lease methods

That is a good fit for an advanced external API because it is explicit and narrow.

## Why Raw `PollWork` Should Not Go On `SWFEngine`

`PollWork(...)` is a broad worker/runtime primitive.
Putting it on `SWFEngine` would blur the line between:

- the managed worker loop: `engine.Run(ctx)`
- task-centric manual flows: `FindTasksWaitingForCapability(...)`, `GetWaitingTask(...)`
- raw lease polling against any matching capability

That makes the engine surface harder to reason about, especially because manual polling would directly compete with `Run(ctx)` for the same work.

## Why A Request-Based Waiting-Task API Is Better

For engine users, the existing “manual task completion” abstraction is `TaskHandle`, not `ExecutionLease`.

If the main new engine-level need is metadata-aware discovery, the natural extension is:

- keep returning `TaskHandle`
- add `MetadataFilter`
- optionally add `Limit`

That fits the current external API much better than dropping raw `PollWork(...)` onto `SWFEngine`.

It also composes cleanly with the existing runtime-backed implementation because it can continue to build on `ListJobs(...)` plus `GetChapter(...)`, or later optimize underneath without changing the public engine contract.

## Important Semantic Distinction

These two engine APIs would intentionally have different semantics:

- `FindTasksWaiting(...)`
  - discovery-oriented
  - returns `TaskHandle`
  - no lease is acquired
- `GetJobLease(...)`
  - claim-oriented
  - returns `ExecutionLease`
  - exclusive ownership semantics come from the runtime

That distinction is useful.
Trying to make one engine method cover both discovery and leasing would make the API less clear.

## What I Would Not Do

I would not:

- add `PollWork(...)` directly to `SWFEngine`
- rename `GetJobLease(...)` to something engine-specific
- mutate `FindTasksWaitingForCapability(...)` in place by adding more positional parameters

The request-based additive method is cleaner than extending the existing positional signature.

## Bottom Line

Recommended external engine change:

```diff
 type taskRunApi interface {
     FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error)
+    FindTasksWaiting(ctx context.Context, req FindTasksWaitingRequest) ([]TaskHandle, error)
     GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
 }

+type jobLeaseApi interface {
+    GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error)
+}
+
 type SWFEngine interface {
     jobRunApi
     taskRunApi
+    jobLeaseApi
     loopWorkerApi
     jobsListApi
     ...
 }
```

So:

- `GetJobLease(...)` is added directly
- generic polling is represented externally by an enhanced waiting-task API, not raw `PollWork(...)`
