# Pulse: discover work using available routes

## Bug

Job `c2/3KkoeEhs5Am8sDhDa8H87RzzTNA` in the `problem6` capture remained
unclaimed after its external-input wait became eligible for timeout processing
at 2026-10-16 00:46:59 UTC. Its stored route was
`recipe / input:collect_user_input`; its due alternate was the job-only
`recipe` route. Both `PollWork` and `GetJobLease` could acquire the alternate on
an isolated copy of the database.

In the inspected Pulse revision `437b420`, discovery and acquisition disagree:

- `internal/c2j/embedded.go`: `Embedded.List` discovers jobs through
  `joblist.ListExecutionJobs` and copies the summary's `NextRoute`.
- `internal/c2j/jobs.go`: `Job.Ready` requires that stored route to match a
  configured supported route. A job-only worker therefore skips this wait.
- `internal/c2j/embedded.go`: `Embedded.Claim` supplies only the listed route to
  `GetJobLease`, which can also fail when an alternate has become effective.

This code path explains how a running Pulse controller can miss the job. The
capture does not contain Pulse's deployed revision or request logs.

## JobDB change

`ListJobsRequest.AvailableRoutes` (HTTP `availableRoutes`) filters jobs by current
lease eligibility using the same route and due-alternate rules as acquisition.
A job-only route has an empty task type; it is not a wildcard for all task types.

```go
supported := []jobdb.Route{{JobType: "recipe"}}
page, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{
    TenantIds:       []string{tenant},
    AvailableRoutes: supported,
    PageSize:        100,
    PageToken:       token,
})
```

Semantics:

- Routes within `AvailableRoutes` are alternatives (OR). All other supplied
  filters, including `JobTasks`, intersect it (AND), before pagination.
- Omitted or empty `AvailableRoutes` preserves ordinary listing behavior.
- Only currently leaseable jobs are returned. Active leases, future availability,
  unresolved dependencies without an elapsed wait deadline, cancellation,
  terminal jobs, and crash-concern jobs are excluded.
- A due alternate replaces the primary route for availability matching, exactly
  as in the scheduler. Listing neither claims work nor commits the alternate.
- `NextRoute` remains the stored route. `ExecutionState.TaskWait` remains intact.
  Existing external-input discovery through `JobTasks` continues to work.
- Availability is evaluated when each page is read. Results are advisory, not a
  reservation or a stable snapshot across pages. Acquisition rechecks atomically.

The HTTP list request body can include:

```json
{
  "availableRoutes": [{"jobType": "recipe"}],
  "pageSize": 100
}
```

## Required Pulse changes

1. Supply the configured supported routes as `AvailableRoutes` on every discovery
   request, including subsequent pages. Retain resource validation, local cooldown,
   duplicate-launch protection, and provisioning admission.
2. Remove the local `NextRoute` capability check from this discovery path. Do not
   reject an available result because its stored task route is unsupported.
3. Pass the configured supported routes to `GetJobLease`, not the listed route:

   ```go
   lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
       JobKey:        candidate.JobKey,
       WorkerID:      workerID,
       Routes:        supported,
       LeaseDuration: leaseDuration,
   })
   ```

4. Treat a nil lease as an ordinary lost race. Use the acquired lease's route and
   client payload for execution; the listing can have become outdated.
5. Upgrade the JobDB server as well as the client dependency before relying on
   this filter. An older server may ignore an unknown field; it does not provide
   the new discovery guarantee. Custom runtime-core scheduler adapters must
   implement `ListJobsRequest.AvailableRoutes` at `Now` before pagination, using
   their acquisition eligibility rules. The core forwards the filter; it cannot
   implement it by post-filtering a scheduler page.

There is no need for Pulse to inspect chapters, calculate deadlines, or resolve
alternate routes itself. `PollWork` remains an alternative for controllers that
want discovery and acquisition in one operation.

## c2j integration

Read-only inspection confirmed that `ListExecutionJobs` preserves the entire
`jobdb.ListJobsRequest`, including on subsequent pages, and does not filter by
`NextRoute`. This holds at Pulse's pinned c2j revision `66010fb85af6` and c2j HEAD
`6a25847a90d0303f700bbade1bda7f67d858ce48` inspected on 2026-10-16. No c2j wrapper
logic change is required for Pulse's path. Pulse must build against the updated
JobDB module and use a server implementing the filter.

The separate `joblist.Client.List` / `joblist.Query` API does not expose
`AvailableRoutes`. Pulse does not use that API for discovery. See the
[c2j integration note](NOTE-C2J-AVAILABLE-ROUTE-DISCOVERY.md) for the optional
extension and suggested regression tests.

Normal `JobDB.Engine.Run` polling already uses scheduler-aware `PollWork`.
Supplied-lease execution should use the acquired lease as before. Pending-input
UI/API readers should continue using ordinary listing or `JobTasks`, without
`AvailableRoutes`, so an open input remains visible until claimed or completed.

## Regression coverage

JobDB's `RunAvailableRoutesConformance` runs on toy and SQLite, directly and over
HTTP. It covers primary/job/task routes, future and due alternates, delayed work,
dependencies and their deadlines, active/cancelled/completed jobs, intersections,
tenant isolation, validation, pagination, unchanged pending-input information,
repeated read-only discovery, and agreement with both acquisition APIs. Lease
expiry/crash-concern conformance also checks availability filtering.

Pulse should add an integration regression that yields a job to an unsupported
input task with a due job-only alternate, runs discovery and admission with only
`recipe` supported, and successfully exports the job-only lease. Also cover a
future alternate, an external answer or competing claim between listing and
acquisition, and a cooldown/provisioning rejection without duplicate launches.
