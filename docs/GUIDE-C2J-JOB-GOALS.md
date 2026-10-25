# Using job goals with c2j

Job goals let a consumer wait for a successful result across replacement jobs.
The consumer keeps the same goal ID while producers restart, run competing
attempts, or attach a separately evaluated result.

This guide covers the JobDB feature available to c2j integrators and operators.
SQLite, Toy, the JobDB HTTP client/server, and the Go workflow SDK implement it.
c2j CLI flags and recipe syntax for goals are separate integration work; this
change does not add a `c2j goals` command or a recipe-level goal-wait operation.
The `jobdb goals` commands below are implemented in this repository.

See the [full specification](SPEC-Job-Goals.md),
[REST schema](../openapi/jobdb-runtime.yaml), and
[pgjobdb implementation guide](GUIDE-PGJOBDB-JOB-GOALS.md).

## Choose the identity to wait on

Every submitted job has two opaque IDs:

- `jobId`: one concrete execution, with its own history, outcome, and artifacts.
- `goalId`: the result that execution is trying to produce. Multiple jobs can
  try to satisfy it, and the first committed success wins permanently.

A goal is created eagerly with its first job. Its ID uses the same format as a
generated job ID but is generated independently. Job inspection also returns
`otherGoalIds`, the additional goals to which that job has been attached.

| What the consumer needs | Dependency |
| --- | --- |
| Wait for this particular job to finish, regardless of outcome | `jobId` with `complete`; existing `AwaitJobs` behavior |
| Require this particular job to succeed | `jobId` with `success` |
| Require a successful result, including a replacement or another candidate | `goalId` with `success` |

A failed, cancelled, or exhausted candidate leaves its goal `OPEN`. Even if all
candidates have finished unsuccessfully, the goal remains open for another
attempt. An operator can explicitly fail it when no further attempt should be
accepted. Success and explicit failure are permanent.

Goal completion is stored on the goal itself. Each waiting consumer separately
stores its binding to the winning job or its failure snapshot. Consumers do not
elect winners, and later completions cannot change an earlier binding.

## Submit and capture identities

The existing local c2j workflow remains:

```bash
c2j submit --recipe-file ./recipes/my-recipe.yaml --embed --json
c2j run --job-id JOB_ID --embed
```

Use the same cell, tenant, and JobDB target for subsequent inspection. These
commands do not imply that the installed c2j version exposes `goalId` in its
output. During integration, preserve the new `JobHandle.GoalID` returned by
JobDB alongside the job ID in submission results and child-job references.
An existing job's primary goal is also available through `GetJob` or
`GET /v1/tenants/{tenantId}/jobs/{jobId}`.

For an HTTP deployment, the new JobDB handle has this shape; the abbreviated
IDs in this guide stand for real IDs:

```json
{
  "jobKey": {"tenantId": "acme", "jobId": "A"},
  "goalId": "G"
}
```

Do not replace job IDs with goal IDs in history, artifact, lease, parent-child,
or exact-job lookup APIs. Store both identities where a caller may want either.

## Wait for a successful result

The Go workflow SDK exposes `AwaitGoals` on execution and task contexts. Inside
a workflow or task body:

```go
bindings, err := ctx.AwaitGoals(goalID)
if err != nil {
    return nil, err
}
winnerJobID := bindings[0].SatisfiedByJobID
// Read the output and artifacts using winnerJobID in the same tenant.
```

The helper suspends the consumer and releases its lease while waiting. It
records the result durably before returning it to application code. Recovery
and replay retain the same binding. Use the returned job ID to read the actual
producer's output; the original job may have failed or produced different data.

`AwaitGoals(g1, g2)` requires both goals. For mixed dependency types, use:

```go
bindings, err := ctx.AwaitDependencies(
    jobdb.Dependency{
        Target: jobdb.DependencyTarget{GoalID: reportGoalID},
        Condition: jobdb.JobPrereqSuccess,
    },
    jobdb.Dependency{
        Target: jobdb.DependencyTarget{JobID: cleanupJobID},
        Condition: jobdb.JobPrereqComplete,
    },
)
```

Bindings follow declaration order. Each dependency must have exactly one target;
goal targets support only `success`. A failed dependency closes the group and
ends its remaining subscriptions. This does not cancel producers.

To gate a new job before its workflow starts, put structured dependencies in
the existing submission's `job.dependencies` field:

```json
{
  "job": {
    "jobType": "consume-report",
    "data": {"data": {}, "artifacts": []},
    "dependencies": [
      {"target": {"goalId": "G"}, "condition": "success"}
    ]
  }
}
```

This is a JobDB REST example, not c2j recipe YAML. Do not combine structured
dependencies with legacy prerequisites in the same submission. Admission waits
use the reserved wait ID `submission`.

## Restart an attempt while preserving its goal

An ordinary restart gets a new goal. To make the replacement eligible for the
source job's existing goal, set `job.shareGoal` on the existing restart API:

```http
POST /v1/tenants/acme/jobs/restarts
Content-Type: application/json
```

```json
{
  "requestId": "repair-report-1",
  "job": {
    "priorJobKey": {"tenantId": "acme", "jobId": "A"},
    "lastStepToKeep": 4,
    "shareGoal": true
  }
}
```

Use a valid restart boundary from the source's history; `4` is illustrative.
Keep c2j's existing logical execution restart selection when applicable rather
than substituting a guessed chapter offset. The returned job might be `A2`,
but its primary goal is still `G`.

In Go, these additions are `SubmitRestartJob.ShareGoal` and top-level
`SubmitRestartJobRequest.RequestID`. A generated-ID shared restart requires a
stable request ID. Retry an uncertain request with the same ID and same inputs;
it returns the same destination even after that destination progresses. A new
attempt needs a new request ID. Explicit destination IDs provide the alternative
idempotency key; in `PUT /jobs/{jobId}/restart`, the path names the destination.

The goal must be open when a new candidate publishes. Multiple candidates may
run concurrently, including the source if its history supports the restart.
Only the primary goal is inherited. Additional `otherGoalIds` associations are
not copied. Existing task, scope, and whole-job retry behavior is unchanged.

## Attach an independently explored solution

Suppose the original job's goal is still open. Submit ten independent attempts
with their own goals, evaluate their results, and add the chosen job to the
original goal. No separate adoption operation is needed:

```bash
jobdb goals add-candidate GOAL_ID CHOSEN_JOB_ID \
  --tenant acme --url http://127.0.0.1:9047
```

Equivalent REST call:

```http
PUT /v1/tenants/acme/goals/G/candidates/CHOSEN_JOB_ID
```

There is no request body. A successful chosen job resolves the open goal
immediately. A running job becomes eligible to resolve it later. Its primary
goal, history, and artifacts stay with that job; the extra association appears
in `otherGoalIds`.

Adding the same association again is safe, even after goal closure. Adding a
new association to a closed goal conflicts. A terminal unsuccessful candidate
can be attached for provenance but does not satisfy or fail the goal.

Choose when to attach based on the desired behavior:

- **Accept any success:** attach multiple running candidates. The first
  committed success wins; other candidates continue running.
- **Evaluate before accepting:** keep the exploratory jobs independent until
  selection, then attach only the chosen one.

The original primary job and any candidates already attached remain eligible
throughout. If one succeeds before selection, the goal is already resolved and
cannot be overridden. This API does not implement exclusive selection or a
"best result" contest among candidates already sharing a goal.

## Inspect progress and stop waiting for success

Against the HTTP server serving the relevant tenant:

```bash
jobdb goals get GOAL_ID --tenant acme --url http://127.0.0.1:9047
jobdb goals candidates GOAL_ID --tenant acme --url http://127.0.0.1:9047 \
  --page-size 100 --include-hidden
jobdb goals fail GOAL_ID --tenant acme --url http://127.0.0.1:9047 \
  --message "All evaluated approaches failed the acceptance checks."
```

These commands emit JSON. Candidate listing returns one page; continue with
`--page-token` using the returned token. `JOBDB_BEARER_TOKEN` supplies the token
when the deployment requires one. These commands connect to an HTTP server;
they do not select the c2j embedded database through `--embed`.

Failure is also available through:

```http
POST /v1/tenants/acme/goals/G/fail
Content-Type: application/json
```

```json
{"message": "All evaluated approaches failed the acceptance checks."}
```

The response contains the permanent human message, failure time, and sorted
`terminalJobIds` and `nonTerminalJobIds` snapshots. Repeating the same message
returns that original snapshot. A different message or an already successful
goal produces a conflict. Messages must be nonblank UTF-8 and at most 16 KiB.

Goal failure never cancels jobs. Running candidates may still succeed for other
goals. There is no goal-cancellation or cancel-candidates option in this version.
Candidate attachment and goal failure require tenant lifecycle authority; a
worker lease alone is insufficient.

Go callers can recognize explicit failure with
`errors.Is(err, jobdb.ErrGoalFailed)` and inspect a `*jobdb.GoalFailedError` with
`errors.As`. Preserve its `Failure` object when presenting an error. Returning
the error normally fails the waiting invocation without retrying that failure;
it does not automatically fail the consumer job's own goal.

## Diagnose a blocked consumer

Inspect `GET /v1/tenants/{tenantId}/jobs/{jobId}/dependency-waits/{waitId}`.
For submission dependencies, use `submission`; SDK-created joins use stable
generated wait IDs. The endpoint is read-only and returns `200` for failed or
timed-out groups too.

| State or diagnostic | Meaning / next action |
| --- | --- |
| `PENDING`, `waiting_for_execution` | At least one candidate is non-terminal; inspect candidate jobs |
| `PENDING`, `awaiting_candidate` | No active candidate remains; add an attempt or explicitly fail the goal |
| `SATISFIED` | Read each `satisfiedByJobId`; these bindings are permanent |
| `FAILED` | Show the recorded dependency failure and human message |
| `TIMED_OUT` | The consumer's deadline expired; producers and their goals continue |
| `CANCELLED` | The consumer stopped waiting; producers continue |

Deadlines are absolute. Adding a candidate or replacing a worker does not extend
them. Conservative cycle validation rejects a job waiting directly or
indirectly on a goal it could satisfy, even when another candidate might allow
progress. Restructure those dependencies instead of retrying the same cycle.

## c2j integration checklist

1. Update the JobDB dependency and all runtime wrappers. `WorkflowRuntime` now
   includes `GoalRuntime`. A pgjobdb deployment must implement the scheduler
   changes before c2j enables goal features against it.
2. Expose `goalId` with submission/child handles and `otherGoalIds` in inspection.
   Keep exact-job identifiers available. Add goal filtering through
   `ListJobsRequest.GoalIDs`, which includes primary and additional candidates.
3. Add an explicit user choice for goal success waits. Route it through
   `AwaitGoals`/`AwaitDependencies`, and load outputs from the returned bindings.
   Preserve current fail-fast, soft-result, and exact-job operations unless
   deliberately migrated.
4. Expose goal sharing on the existing restart path. Persist the restart request
   ID before sending the request so a process restart can retry it safely.
   Surface closed-goal conflicts without silently creating an independent goal.
5. Add candidate and failure operations to the administrative path using
   `AddGoalCandidate` and `FailGoal`. Do not issue an unrecorded mutation from
   replayed recipe evaluation.
6. Update execution-history compatibility handling for admission snapshots,
   dependency joins, and `dependency_binding` records. Older workers must not
   silently reinterpret histories using the new feature. Coordinate the SDK
   upgrade with c2j's supported worker and history versions.
7. Test embedded and remote operation: failed original then successful shared
   restart; attaching a running or already successful job; competing winners;
   explicit failure with continuing candidates; recovery before result history
   is written; replay using the original winner; timeout; and ordinary restart
   and `AwaitJobs` retaining their old behavior.

Treat missing goal fields or unsupported structured waits as an incompatible
server when using these features. Do not silently fall back to an exact-job
wait or a restart that loses the goal.

## Goal discovery and SDK identity

Tenant-wide goal enumeration and dependency discovery are implemented through
`ListGoals` and `ListDependencyWaits`. Reverse lookup intentionally covers only
currently unresolved dependencies in pending waits. Consumer lookup retains wait
history. Counts are read-time statistics; goal revision and update time continue
to describe goal-record changes only.

Engine, execution-context, and task-context submissions return `JobHandle`;
restart helpers accept `SubmitRestartJobRequest` to retain the caller's
`RequestID`. Live/replay contexts expose `Job()`, and job/run views expose
`BlockingWait`.

See [the discovery and SDK migration guide](GUIDE-C2J-GOAL-DISCOVERY.md) for
filters, pagination, visibility, timeout projection, CLI, and migration examples.
