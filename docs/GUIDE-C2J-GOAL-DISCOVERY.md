# Goal discovery and complete SDK identities

Use goal records to enumerate goals. A page of candidate jobs is not an
authoritative goal list: a goal can have several candidates, including jobs
with other primary goals, archived jobs, and hidden jobs.

## Goal listing

```go
page, err := runtime.ListGoals(ctx, jobdb.ListGoalsRequest{
    TenantId: tenant,
    States: []jobdb.GoalState{jobdb.GoalOpen},
    PageSize: 100,
})
```

Each `GoalSummary` includes the committed `GoalInfo`: state, immutable winner,
failure snapshot, revision, and timestamps. Counts are computed at read time
over **all** primary and additional memberships, with duplicate jobs counted
once. The counts are `CandidateCount`, `NonTerminalCandidateCount`,
`TerminalCandidateCount`, and `UnavailableCandidateCount`. Nonterminal includes
ready, running, and blocked candidates; there is no candidate activation or
single active-candidate slot. Failed candidates do not close a goal.

Archived and soft-deleted/hidden jobs remain in counts and candidate filters.
A missing retained candidate identity is counted as unavailable, not nonterminal.
Built-in deletion hides jobs while retaining their identities and memberships.
A goal with no nonterminal candidates remains discoverable. Goal inspection is
tenant-wide lifecycle visibility; candidate-list visibility flags do not change
goal counts.

Filters include exact goal IDs, states, inclusive creation/update bounds, and
`Candidate` selectors for job IDs, types, and application metadata. For example:

```go
Candidate: &jobdb.GoalCandidateFilter{
    MetadataEquals: []jobdb.MetadataPredicate{{
        Path: []string{"repository"},
        Values: []any{"my-repository"},
    }},
},
```

Metadata paths are relative to application metadata. Values are non-null JSON
scalars. Predicates are ANDed; values within each predicate are ORed. All
candidate predicates must match the same candidate. A repository match selects
the goal without implying repository ownership, and counts still cover the
whole goal.

Goal `UpdatedAt` and `Revision` describe changes to the goal record itself.
Candidate attachment, progress, or failure can change counts without changing
either field. Do not use those fields as a version for candidate statistics.

## Dependency discovery

To inspect a consumer's current and retained historical waits:

```go
page, err := runtime.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{
    TenantId: tenant,
    ConsumerJobID: consumerJobID,
    // Omit States for all retained lifecycle states.
    States: []jobdb.DependencyState{jobdb.DependencyPending},
})
```

For reverse lookup, set `Target` to a goal or exact job:

```go
Target: &jobdb.DependencyTarget{GoalID: goalID},
```

Reverse lookup returns **only currently unresolved dependencies in pending
waits**. It has no historical reverse lookup. Once a target is satisfied, failed,
cancelled, or timed out for that consumer, that edge disappears from this query.
In a wait for G and J, resolving G removes the G edge while the J edge can
remain. The complete wait is still available by consumer or known wait ID.

Consumer and target filters can be combined. A target query accepts only an
omitted state filter or `PENDING`; requests for historical reverse states fail.
Every returned wait includes its ID, ordered target/condition pairs, deadline,
lifecycle state, blocked reasons, winning-job bindings, and failure snapshots.

These reads do not write history, renew leases, elect winners, or settle waits.
Elapsed deadlines are projected as `TIMED_OUT` on read, even if the durable
scheduler row has not yet been updated. Reverse lookup excludes those waits.
Already recorded bindings and failure snapshots remain unchanged.

`JobInfo`, `JobSummary`, `JobRunSummary`, and `workflow.JobRunOutcome` expose
`BlockingWait` when the job is currently blocked on a structured dependency
wait. Use its `WaitID` and `Dependencies` to explain `PENDING_JOBS`; do not
reconstruct SDK occurrence IDs. Legacy exact-job waits still use existing fields.

## Pagination and transport

Both listings return `NextPageToken`. Continue with the same tenant and filters.
Tokens are opaque and query-bound; changing tenant or filters rejects the token.
Page size may change. Default size is 100, capped at 200.

Goals sort by goal ID ascending. Waits sort by consumer job ID, then wait ID,
ascending. Each page is a live read; a traversal is not a frozen snapshot across
concurrent changes. Filtering occurs before returning the page. In particular,
expired entries do not cause matching later waits to be lost.

The HTTP operations are read-only queries with JSON bodies:

- `POST /v1/tenants/{tenantId}/goals/query`
- `POST /v1/tenants/{tenantId}/dependency-waits/query`

The path tenant is authoritative. Deployment tenant authorization applies to
every request; a pagination token is not an authorization credential. These
are tenant inspection APIs, not additional delegated-worker permissions.

CLI equivalents:

```sh
jobdb goals list --tenant acme --state OPEN --page-size 100
jobdb goals waits --tenant acme --consumer-job CONSUMER
jobdb goals waits --tenant acme --target-goal GOAL
```

Supply `--url` and `--page-token` as needed. Commands emit one JSON page.
These are JobDB commands; c2j CLI integration remains in c2j.

## SDK migration

Engine, execution-context, and task-context `SubmitJob` now return a complete
`jobdb.JobHandle`:

```go
child, err := task.SubmitJob(ctx, jobdb.SubmitJob{ /* ... */ })
// child.JobKey identifies the concrete job; child.GoalID identifies its goal.
```

Their `SubmitRestartJob` methods now accept the full
`jobdb.SubmitRestartJobRequest` and return `JobHandle`:

```go
child, err := task.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
    RequestID: stableRequestID,
    Job: jobdb.SubmitRestartJob{
        PriorJobKey: prior,
        ShareGoal: true,
        // Retain the existing valid restart boundary selection.
        ExecutionRestart: restartBoundary,
    },
})
```

The caller's request ID reaches the runtime unchanged. Returned identities come
directly from the submission response, without a follow-up lookup. Update
wrappers and callers to use `handle.JobKey` where an API expects a job key.

`ExecutionContext.Job()` and `TaskContext.Job()` expose the current execution's
tenant, primary goal, and job identity. Identity is available in live execution
and replay; manually constructed task contexts only know their supplied job key.
`JobRunSummary` also includes `GoalID` and `OtherGoalIDs`.

The expanded `GoalRuntime` interface requires wrappers and backend adapters to
forward both discovery methods. See the pgjobdb guide for indexes and
read-only query obligations.
