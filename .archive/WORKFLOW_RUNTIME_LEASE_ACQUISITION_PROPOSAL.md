# WorkflowRuntime Lease Acquisition Proposal

## Scope

This proposal targets the current `swf.WorkflowRuntime` interface in `pkg/swf/runtime.go`.
There is no separate exported `swf.Runtime` type today, so the concrete review surface is `swf.WorkflowRuntime`.

Goals:

- expose poll-time metadata predicates backed by `pgwf.GetWorkWithOptions`
- expose targeted lease acquisition for a known job backed by `pgwf.GetJobLease` / `pgwf.GetJobLeaseWithOptions`
- keep the SWF surface backend-agnostic and additive

Non-goals:

- mirror pgwf helper names one-for-one in `swf`
- expose pgwf option structs directly
- broaden worker polling in unrelated ways in the same change

## Proposed Shape

```go
type WorkflowRuntime interface {
	// Job lifecycle
	SubmitJob(ctx context.Context, req SubmitJobRequest) (JobHandle, error)
	SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error)
	CancelJob(ctx context.Context, req CancelJobRequest) error

	// Worker loop
	PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error)
	GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error)
	CompleteTaskIfWaiting(ctx context.Context, req CompleteTaskIfWaitingRequest) error

	// Read APIs
	GetJob(ctx context.Context, jobKey JobKey) (JobInfo, error)
	ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)

	// Chapter / replay access
	GetChapter(ctx context.Context, ref ChapterRef) (StoredChapter, error)
	ListChapters(ctx context.Context, req ListChaptersRequest) ([]StoredChapter, error)
	PutChapter(ctx context.Context, req PutChapterRequest) error

	// Artifact access
	OpenArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error)
}

type PollWorkRequest struct {
	WorkerID      string
	Capabilities  []string
	Limit         int
	LongPollUntil *time.Time

	// Optional. Zero means runtime default.
	LeaseDuration time.Duration

	// Optional equality predicates applied before a lease is granted.
	// Reuses the existing concrete predicate type instead of exposing
	// backend-specific filter types.
	MetadataEquals []MetadataPredicate
}

type GetJobLeaseRequest struct {
	JobKey        JobKey
	WorkerID      string
	Capabilities  []string

	// Optional. Zero means runtime default.
	LeaseDuration time.Duration
}
```

## Why This Shape

### 1. Keep `PollWork` as the only generic polling entry point

`PollWorkRequest` is already the SWF request/options carrier for lease acquisition.
Adding `PollWorkWithOptions(...)` at the SWF layer would duplicate the existing pattern instead of extending it.

The direct runtime mapping stays simple:

```go
pgwf.GetWorkWithOptions(..., pgwf.GetWorkOptions{
	TenantIDs:      nil,
	LeaseSeconds:   durationToLeaseSeconds(req.LeaseDuration),
	MetadataEquals: toPgwfPredicates(req.MetadataEquals),
})
```

### 2. Add one targeted-lease method instead of forcing callers through `PollWork`

Getting a lease for one known job is a different operation than generic polling.
Trying to represent it by overloading `PollWorkRequest` with an optional `JobKey` makes the request shape ambiguous and complicates implementations.

A separate method keeps the contract obvious:

```go
lease, err := runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
	JobKey:       jobKey,
	WorkerID:     workerID,
	Capabilities: []string{"my-job:task-a"},
})
```

### 3. Use `time.Duration` in SWF, not integer seconds

`swf` already uses Go-native time types in its request structs.
The pgwf-backed runtime can round up to whole seconds internally.

### 4. Use `[]MetadataPredicate`, not pgwf types and not `MetadataFilter`

Why not pgwf types:

- `WorkflowRuntime` should not leak pgwf types into `swf`

Why not `MetadataFilter`:

- `WorkflowRuntime` request structs are plain data carriers
- `MetadataFilter` is an interface-backed builder shape, which is awkward for a backend boundary and for the remote API mirror
- callers that prefer the builder can still derive concrete predicates with `swf.MetadataPredicates(filter)`

One implementation note: the current pgwf `GetWorkWithOptions` path only accepts string metadata values.
I would treat that as a pgwf-backed runtime limitation, not as a reason to introduce a second SWF-specific predicate type just for leasing.

## Recommended Semantics

`GetJobLease` should return:

- `nil, nil` when the job does not currently produce a lease
- a non-nil `ExecutionLease` when it does
- a non-nil error only for validation or backend failures

That preserves the pgwf single-round-trip behavior and keeps the method useful for "try to lease this job now" flows.
If a caller needs to distinguish "job missing" from "job exists but is not leaseable", it can pair this with `GetJob(...)`.

## Deliberately Out Of Scope

I would not add tenant scoping to `PollWorkRequest` in this change even though pgwf already supports it.
That capability predates these new pgwf functions, and bundling it here makes review noisier without helping the two requested use cases.

If you want full pgwf `GetWorkOptions` parity on the SWF side, `TenantIds []string` can be added later in the same request struct without disturbing this shape.

## Compatibility

This is additive for runtime callers:

- existing `PollWork(...)` call sites continue to compile unchanged
- callers that do not care about metadata-filtered polling or targeted leasing do nothing

It is source-breaking for custom runtime implementers because `WorkflowRuntime` gains one new method:

- `GetJobLease(ctx, req GetJobLeaseRequest) (ExecutionLease, error)`

## Expected Follow-On Changes

- `pkg/swf/runtime/direct/internal/directimpl/runtime.go`
  - thread `PollWorkRequest.MetadataEquals` and `PollWorkRequest.LeaseDuration` into `pgwf.GetWorkWithOptions`
  - implement `GetJobLease` via `pgwf.GetJobLeaseWithOptions`
- `pkg/swf/runtime/toy/internal/toyimpl/runtime.go`
  - filter lease candidates by `MetadataEquals`
  - add a keyed lease path for `GetJobLease`
- runtime conformance tests
  - poll with metadata predicates
  - targeted job lease success / miss cases
- `openapi/swf-runtime.yaml`
  - extend poll request schema
  - add a targeted lease endpoint if the remote API is meant to continue mirroring `WorkflowRuntime`

## Summary

Recommended API delta:

```diff
 type WorkflowRuntime interface {
     ...
     PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error)
+    GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error)
     ...
 }

 type PollWorkRequest struct {
     WorkerID      string
     Capabilities  []string
     Limit         int
     LongPollUntil *time.Time
+    LeaseDuration time.Duration
+    MetadataEquals []MetadataPredicate
 }

+type GetJobLeaseRequest struct {
+    JobKey        JobKey
+    WorkerID      string
+    Capabilities  []string
+    LeaseDuration time.Duration
+}
```

That gives SWF direct access to both new pgwf capabilities without pushing pgwf-specific APIs into the public `swf` surface.
