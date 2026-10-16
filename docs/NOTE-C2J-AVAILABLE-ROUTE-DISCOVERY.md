# c2j: available-route discovery review

## Finding

**No c2j logic change is required to fix Pulse's missed timeout pickup.** The
confirmed consumer bug is in Pulse's route filtering and lease request; see the
[Pulse bug report and migration guide](GUIDE-PULSE-AVAILABLE-ROUTE-DISCOVERY.md).

Read-only review covered c2j revision `66010fb85af6` (pinned by the inspected Pulse
checkout) and HEAD `6a25847a90d0303f700bbade1bda7f67d858ce48` on 2026-10-16. The
relevant `pkg/joblist` files are unchanged between these revisions.

Evidence:

- `pkg/joblist/execution.go`, `ListExecutionJobs`: when the execution filter is
  nil, directly calls `lister.ListJobs(ctx, req)`. Pulse uses this branch.
- With an execution filter, the same function changes only `PageSize` and
  `PageToken` between fetches. It preserves all other request fields and filters
  by execution requirements, not by the stored route.
- `pkg/joblist/job.go`, `JobFromSummary`: copies `NextRoute` into the result. It
  does not use that route to reject jobs. Keeping the stored route is correct;
  scheduler availability may come from a due alternate.

The Pulse build must select the JobDB module containing `AvailableRoutes`, and
the JobDB server must implement the new filter. Pulse can update its own direct
JobDB dependency; this does not require a new c2j release solely to change the
wrapper. Updating c2j's own JobDB dependency is appropriate when adopting the new
API or adding coverage within c2j.

## Optional higher-level API extension

`pkg/joblist/query.go` defines a separate `Query` accepted by `Client.List`.
It currently has no `AvailableRoutes` field, and `BuildRequest` constructs a new
JobDB request from that type. Callers of this API cannot request available-route
discovery. **Pulse's inspected discovery path does not use it.**

If c2j wants to expose this capability through `Client.List`:

1. Update its JobDB dependency to a revision containing the new filter.
2. Add `AvailableRoutes []jobdb.Route` to `joblist.Query`.
3. Copy the slice into `jobdb.ListJobsRequest.AvailableRoutes` in `BuildRequest`.
   Existing `req.ValidateRoutes()` will validate it.
4. Preserve empty/omitted behavior and retain the existing `JobTasks` filter for
   inspecting stored pending-task routes. Add no local deadline or route logic.

## Suggested c2j regression tests

- `ListExecutionJobs` forwards `AvailableRoutes` with a nil execution filter.
- With an execution filter, every backend fetch retains the same routes, even
  when an empty filtered page causes another fetch.
- A returned job whose stored `NextRoute` differs from the requested available
  route survives the wrapper and projection.
- If the optional `Query` extension is added: `BuildRequest` forwards and copies
  routes, rejects invalid identifiers, and preserves ordinary listing when the
  field is omitted.

This review changed only documentation in JobDB. No c2j source or tests were
modified or executed.
