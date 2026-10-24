# Job goals and success dependencies

Status: implemented for SQLite, Toy, the remote client/server, and workflow
execution. Core exposes the corresponding atomic scheduler contract for backend
implementers. MUST and MUST NOT define required behavior.

Related documents:

- [c2j user and integration guide](GUIDE-C2J-JOB-GOALS.md)
- [pgjobdb implementation guide](GUIDE-PGJOBDB-JOB-GOALS.md)
- [Implementation outline](../OUTLINE_JOB_DEPENDENCY_FOLLOWERS.md)
- [Original feature proposal](../PROPOSAL_JOB_DEPENDENCY_FOLLOWERS.md)
- [Current REST contract](../openapi/jobdb-runtime.yaml)
- [Workflow runtime guarantees](SPEC-WorkflowRuntime-Guarantees.md)

This specification uses generated goal IDs, multiple candidate jobs per goal,
`shareGoal` on restart, an idempotent add-candidate operation, and explicit goal
failure without job cancellation. A goal accepts the first successful resolution
committed to it. There is no exclusive adoption or selected-candidate mode.

## 1. Scope and terminology

A **job** is one concrete execution with its own history, outcome, and artifacts.
A **goal** identifies work that can be attempted by multiple jobs until one
succeeds or someone explicitly fails the goal.

Every job has an immutable, tenant-scoped primary `goalId`. Normal submission
generates a new goal. Restart normally generates a new goal too;
`shareGoal: true` carries forward the source job's primary goal. The job is
automatically a candidate for its primary goal through `jobs.goal_id`; no
`goal_candidates` row is created for that primary relationship. Every goal has
an explicit row from first-job publication.

An existing job may also be added as a candidate for other goals. A goal may
have multiple running candidates, and a job may satisfy several goals. Job
inspection exposes its additional associations as read-only `otherGoalIds`.
A consumer waits on either an exact job or the successful result of a goal.

This feature adds no replacement-chain traversal. It preserves existing job IDs,
artifact keys, chapter references, and lease identities. Migrating automatic
whole-job retries to separate jobs is a later feature; current job, task, and
scope retries keep their existing histories.

## 2. REST surface at a glance

All paths below are relative to `/v1/tenants/{tenantId}`. JSON uses the existing
camelCase convention. Successful mutations use the existing `200`/`204`
conventions. Existing authentication and tenant authorization apply.

### New endpoints

| Method | Path | Operation | Result |
| --- | --- | --- | --- |
| `GET` | `/goals/{goalId}` | `getGoal` | `200 GoalInfo` |
| `POST` | `/goals/{goalId}/fail` | `failGoal` | `200 GoalInfo` with durable failure; candidate jobs continue |
| `PUT` | `/goals/{goalId}/candidates/{jobId}` | `addGoalCandidate` | `200 GoalCandidateInfo` |
| `GET` | `/jobs/{jobId}/dependency-waits/{waitId}` | `getDependencyWait` | `200 DependencyWaitInfo` |

Goals are created by job submission, so there is no standalone create-goal API.
There is no reopen-goal or exclusive-adopt API. Goal failure has no candidate
cancellation option. Successful output and artifacts are retrieved using the
successful job's existing APIs.

### Changes to existing endpoints

| Existing operation | Additions |
| --- | --- |
| `POST /jobs`, `PUT /jobs/{jobId}` | Generate goal; return `goalId`; accept structured `dependencies` |
| `POST /jobs/restarts`, `PUT /jobs/{jobId}/restart` | Accept `job.shareGoal`, structured `dependencies`, and restart request idempotency described below; return `goalId` |
| Lease-authorized submission and restart variants | Same request/response additions and goal semantics |
| `GET /jobs/{jobId}` | Include primary `goalId` and derived `otherGoalIds` in `JobInfo` |
| `POST /jobs/query` | Match `goalIds` against all candidate associations; include `goalId` and `otherGoalIds` in `JobSummary` |
| Job handles in poll/lease responses and job summaries in schedule responses | Include `goalId` |
| `POST /jobs/{jobId}/leases/{leaseId}/reschedule` | Accept a durable `dependencyWait`; continue returning `204` |

The path ID in `PUT /jobs/{jobId}/restart` is the **destination job ID** under the
existing API. The source remains `job.priorJobKey`.

`failGoal` and `addGoalCandidate` require tenant lifecycle-mutation authority;
attachment also requires access to the referenced job. Holding an execution
lease alone does not grant these powers over other goals. Existing lease and
delegation checks remain in effect on worker mutation paths.

## 3. Goal identity and state

### Identity generation

- Generate a fresh goal ID using the same KSUID format as server-generated job
  IDs. Generate it independently of the job ID; it is not an alias for that job.
- Goal IDs are opaque to clients. Jobs and goals use separate typed references.
- Clients cannot set or change primary `goalId` on submission. A shared-goal
  restart derives it from the source. Additional associations are added only
  through the candidate endpoint.
- Persist generated identities before exposing a job. Equivalent retries of
  explicit-ID submission MUST return the original goal ID, including recovery
  when history exists but scheduler publication was interrupted.
- Create the goal row eagerly and atomically with its first scheduler job after
  history preparation. Shared-goal restarts reference an existing goal row.
  Missing rows never imply open goals; lazy creation is outside this design.
- Each scheduled occurrence creates its own goal through normal submission.
- Identifiers `A`, `A2`, `B`, and `G` in examples abbreviate real identifiers.

### States and invariants

| Goal state | Meaning | Can add new candidates? |
| --- | --- | --- |
| `OPEN` | No successful resolution and no explicit goal failure | Yes |
| `SUCCEEDED` | One candidate supplied the successful result | No |
| `FAILED` | Explicitly closed with a durable failure reason | No |

Multiple non-terminal jobs may be candidates for one goal. A job may be a
candidate for multiple goals. Primary goal identity is immutable, while
candidate associations are append-only. There is no removal or reassignment API.

Each succeeded goal has one immutable `successfulJobId`. Closed goals never
reopen. There is no authoritative `currentJobId` or `selectedJobId`.

A successful resolution transaction changes an open goal to `SUCCEEDED`.
Competing successful candidates use an atomic conditional transition; exactly
one becomes the winner for each goal. “First” means first committed goal
resolution, not earliest job start or wall-clock execution finish.

Unsuccessful completion, timeout, cancellation, and retry exhaustion do not
close associated goals. A job losing the race for one goal still completes
successfully and may satisfy other open goals. Closed goals are skipped during
job completion. Completing a job does not fail merely because one of its goals
has already closed.

Closing a goal never cancels candidate jobs, either on success or explicit
failure. Existing individual job-cancellation APIs remain available.

### GoalInfo

`GET /goals/{goalId}` returns a coherent snapshot:

```json
{
  "goalKey": {"tenantId": "acme", "goalId": "G"},
  "state": "OPEN",
  "revision": "1",
  "createdAt": "2026-10-24T10:00:00Z",
  "updatedAt": "2026-10-24T10:00:00Z"
}
```

Required fields are shown above. `revision` is a decimal string starting at
`"1"`. In v1, it advances to `"2"` when the goal transitions to `SUCCEEDED` or
`FAILED`. Candidate additions, shared-goal restarts, candidate progress,
unsuccessful completion, and duplicate requests do not update the goal row,
revision, or `updatedAt`. Times are server-assigned UTC timestamps.

The revision describes goal state only. It does not version candidate membership
or serve as a cache validator for candidate lists. Concurrent membership changes
still coordinate with goal closure even though they do not update the goal row.

Additional fields:

| Field | Presence | Meaning |
| --- | --- | --- |
| `successfulJobId` | Only `SUCCEEDED` | Concrete job supplying outputs/artifacts |
| `failure` | Only `FAILED` | Immutable `GoalFailure` defined below |
| `closedAt` | `SUCCEEDED` or `FAILED` | Timestamp of terminal goal transition |

Unknown goals return `404 goal_not_found`. Failed goals are existing resources
and return `200`. Candidate job details are obtained through the existing
paginated jobs query rather than embedded as an unbounded list in `GoalInfo`.

## 4. Creating jobs and inspecting membership

Normal submission remains on the existing endpoint:

```http
POST /v1/tenants/acme/jobs
Content-Type: application/json
```

```json
{
  "job": {
    "jobType": "build-report",
    "data": {"data": {"reportId": "r1"}, "artifacts": []}
  }
}
```

The extended `JobHandle` response is:

```json
{
  "jobKey": {"tenantId": "acme", "jobId": "A"},
  "goalId": "G"
}
```

`goalId` is also required in new-server `JobInfo` and `JobSummary` responses.
Those inspection responses additionally include `otherGoalIds`, a sorted,
duplicate-free array of other goals for which the job is a candidate. It is
empty when there are no additional associations, excludes primary `goalId`, and
includes closed goals for provenance. It is a derived read-only view, not a
writable job payload. `JobHandle` carries only primary `goalId`.

For example, the goal-related fields in an inspected job may be:

```json
{
  "goalId": "G0",
  "otherGoalIds": ["G1", "G2"]
}
```

This job is eligible to satisfy G0, G1, and G2 while each remains open.

Leave `JobKey` unchanged: chapters, artifacts, and exact-job reads still use it.
The handle addition applies to lease-authorized submissions and polling or
targeted lease acquisition. Job summaries in schedule responses include the
inspection fields too.

Add `goalIds: string[]` to `POST /jobs/query`. Match any candidate association,
including primary and additional goals. Return each matching job once even if
it matches several requested goals. The filter intersects existing filters
before pagination. Missing or empty means no goal filter. For example:

```json
{"goalIds": ["G"], "includeHidden": true, "pageSize": 100}
```

This uses the existing paginated `ListJobsResponse` and exposes every associated
job, including jobs hidden by deletion when `includeHidden` is requested.
Listing jobs does not define the failure snapshot; that snapshot is fixed by
the goal-failure transaction.

## 5. Publishing and attaching candidates

### Shared-goal restart

Add `shareGoal: boolean` to `SubmitRestartJob`, defaulting to `false`. No new
restart endpoint is required.

```http
POST /v1/tenants/acme/jobs/restarts
Content-Type: application/json
```

```json
{
  "requestId": "repair-report-2026-10-24-1",
  "job": {
    "priorJobKey": {"tenantId": "acme", "jobId": "A"},
    "lastStepToKeep": 4,
    "shareGoal": true
  }
}
```

```json
{
  "jobKey": {"tenantId": "acme", "jobId": "A2"},
  "goalId": "G"
}
```

All existing restart-boundary, schema, history, and artifact validations apply.
The source may be any job permitted by those rules, including a running source
with a valid immutable restart prefix. There is no current-job or source-terminal
restriction specific to goal sharing.

With `shareGoal: true`, the source's primary goal must be open when the new job
publishes. Publication creates the job with that same primary `goalId`, which
itself supplies primary membership, after dependency-cycle validation. It needs
no extra association row and does not update the goal row. Other
candidates may already be running. Different restart requests can publish
different candidates concurrently.

With `shareGoal: false` or omitted, generate a fresh primary goal. Additional
associations (`otherGoalIds`) are never copied by either restart mode. Clients
can add the destination to other goals explicitly after publication. The
add-candidate operation also handles destinations that already completed.

### Restart idempotency and races

Add top-level `requestId` to `SubmitRestartJobRequest`. It is required for
server-generated-ID restarts with `shareGoal: true`, including lease-authorized
POST requests, and optional otherwise. Missing required or blank request IDs
return `400 invalid_dependency`.

For explicit-destination PUT restarts, the destination job ID supplies durable
idempotency even without `requestId`. A supplied request ID must agree with that
destination. Request IDs are tenant-scoped restart-operation identities.

Persist the allocated destination ID, source ID, semantic restart parameters,
and sharing policy before preparing history. Reusing an operation identity with
different semantic inputs returns `409 existing_job_mismatch`. Transport lease
IDs and diagnostic worker/request times are not semantic inputs; current
authorization must still be validated on every request.

An equivalent retry of a committed operation returns its original handle even
after the destination progresses or its goal closes. Do not compare the entire
growing history with the original prefix. In-progress equivalent retries resume

There is no expected-current-job or goal-generation admission check. A new
shared-goal publication only requires the goal still to be open, besides
existing restart and cycle validations. Closure winning that race produces
`409 goal_conflict`; the destination must not become runnable without its
valid primary goal reference. Unpublished prepared history is recoverable or
eligible for cleanup.

### Add an existing candidate

```http
PUT /v1/tenants/acme/goals/G/candidates/A7
```

This operation takes no request body. The goal/job pair is its idempotency key.
The job must already be published and accessible in the same tenant; its primary
goal is unaffected.

For a new additional association, validate and insert the row in
`goal_candidates`, then evaluate the candidate's durable job outcome in the
same transaction:

| Job state | Effect |
| --- | --- |
| Non-terminal | Register it; it may satisfy the goal on future success |
| Succeeded | Register it and immediately resolve the open goal with this job |
| Terminal and unsuccessful | Register it for provenance; leave the goal open |

Primary membership already exists through `jobs.goal_id`. Adding that same pair
is an idempotent success and MUST NOT insert a duplicate `goal_candidates` row.
Adding any existing extra pair is also idempotent. Both cases return `200` even
after goal closure without altering history, failure snapshots, or the winner.
Adding a new extra pair to a closed goal returns `409 goal_conflict`. Check both
primary membership and existing extra association identity before rejecting a
closed goal. Unknown goals/jobs return their respective `404` errors.

A non-terminal or unsuccessful candidate addition does not update the goal row.
An already-successful candidate addition resolves the goal, updating its state
and revision. All additions still serialize with goal closure; merely reading
`OPEN` before an unrelated insert is insufficient.

Coordinate registration with job completion: if attachment wins, completion sees
the pair; if completion wins, attachment sees the terminal result. There must
be no state in which a successful registered candidate leaves the goal open.

The response is `GoalCandidateInfo`:

```json
{
  "goalKey": {"tenantId": "acme", "goalId": "G"},
  "jobId": "A7",
  "addedAt": "2026-10-24T10:07:00Z",
  "goalState": "SUCCEEDED",
  "successfulJobId": "A7"
}
```

The first four fields are required. `successfulJobId` is present when the goal
is succeeded and identifies its actual winner, which can be another candidate
on a later duplicate request. `addedAt` is fixed for an extra pair; for an
implicit primary pair it is the stored job creation timestamp. `goalState` and
the optional winner describe state at the response transaction. Idempotency
preserves the association, not a byte-identical state projection forever.

### Selection using the single candidate operation

There is no exclusive `adopt` API, priority, or selected-candidate state. Adding
a running job does not prefer it over other candidates. The first successful
goal resolution wins permanently.

To compare ten alternatives, submit them with independent goals and add the
chosen job to the original goal while it remains open. A successful chosen job
resolves it immediately; a running chosen job becomes eligible for later
resolution. Already registered candidates can still win, and a previously
succeeded goal cannot have its answer replaced.

## 6. Explicitly failing a goal

```http
POST /v1/tenants/acme/goals/G/fail
Content-Type: application/json
```

```json
{
  "message": "The upstream dataset was withdrawn; this report cannot be produced."
}
```

`message` is required, must contain a non-whitespace character, and is limited to
16 KiB of UTF-8. Preserve it exactly. Reject invalid input without mutation.
Clients do not supply the candidate lists or failure timestamp.

Failing a goal closes the goal and resolves its waits; it does not cancel,
reschedule, or revoke leases from any candidate job. There is no `cancelJobs`
option, bulk candidate-cancellation endpoint, or automatic loser cancellation
in v1. Requests containing an unsupported cancellation option are invalid.
Individual job cancellation remains available through the existing API.

Use `FAILED` for this explicit closure. A separate goal-cancel endpoint/state
is outside v1.

For an open goal, the operation MUST atomically:

1. Serialize against candidate attachment, shared-goal publication, and successful
   goal resolution.
2. Read a consistent snapshot of every candidate from both `jobs.goal_id` and
   extra `goal_candidates` rows, including hidden jobs, partitioned into terminal
   and non-terminal jobs.
3. Set the goal to `FAILED`, persisting the message, server timestamp, and both
   exact ID lists as one immutable failure snapshot.
4. Settle pending waits with that failure. Late registrations get the same
   snapshot.

Prepared jobs that never published are excluded. The operation mutates no
candidate job state. Success acknowledges durable goal failure; it does not
promise or request that any running process stop.

### Failure response

```json
{
  "goalKey": {"tenantId": "acme", "goalId": "G"},
  "state": "FAILED",
  "revision": "2",
  "createdAt": "2026-10-24T10:00:00Z",
  "updatedAt": "2026-10-24T10:10:00Z",
  "closedAt": "2026-10-24T10:10:00Z",
  "failure": {
    "code": "goal_failed",
    "goalId": "G",
    "message": "The upstream dataset was withdrawn; this report cannot be produced.",
    "terminalJobIds": ["A", "A2"],
    "nonTerminalJobIds": ["A7", "A8"],
    "failedAt": "2026-10-24T10:10:00Z"
  }
}
```

All fields in `failure` are required and define `GoalFailure`. Both lists are
sorted lexicographically by job ID, contain no duplicates, and are disjoint.
Together they contain all candidates at closure, including hidden jobs. Either
list can be empty. They are complete and never silently truncated.

The snapshot remains unchanged when A7 or A8 later finishes. Live job inspection
shows subsequent outcomes. A job may successfully complete after its primary
goal fails and may still satisfy any of its other open goals. Its completed
output remains available under its own job ID.

Failure is permanent. Retrying with the same message returns `200` and the
original snapshot, timestamp, and revision. A different message on a failed
goal, or any failure request on a succeeded goal, returns `409 goal_conflict`
without rewriting the outcome.

If a successful resolution wins the transaction race, failure conflicts. If
failure wins, later candidate successes leave this goal failed while still
completing normally. An attachment or shared restart that publishes first is
included in the snapshot; a new association losing to failure cannot publish.
Duplicate attachment of an existing pair remains a no-op success.

### Propagation to consumers

Goal failure produces a typed, non-retryable `GoalFailedError` containing the
same `GoalFailure` snapshot. In Go, callers can match `ErrGoalFailed` and extract
`*GoalFailedError` with `errors.Is`/`errors.As`.

- An execution or task waiting on G resumes with this error at its wait call.
  Existing catch handling can inspect the message and both job-ID lists.
- An uncaught error fails the consumer job through its normal completion path.
- A failed submission prerequisite prevents the consumer body from running and
  produces the same structured dependency failure through the worker's normal
  prerequisite-failure completion path.
- Already settled waits and completed consumers are unchanged.
- Failing G does not automatically close consumers' goals. A failed consumer
  execution may itself need repair.

Preserve error identity and all snapshot fields through history, remote codecs,
and job-result reads. Encode it in the existing application-error envelope with
a reserved `_jobdb_goal_failure` attribute containing the snapshot; the REST
`attrs` value uses existing typed Metadata encoding. Decoding must reconstruct
the typed, non-retryable error, including beneath `JobFailedError`. Do not flatten
the snapshot into a message or classify it as a transient system error.
Execution history containing this contract requires an explicitly supported
protocol version. Storage may share immutable snapshots internally, but public
errors must expose the complete snapshot.

## 7. Structured dependency declarations

Add a shared `Dependency` schema with required `target` and `condition`:

```json
{"target": {"goalId": "G"}, "condition": "success"}
```

The target contains exactly one nonempty `goalId` or `jobId`, resolved in the
consumer's tenant. No cross-tenant target is accepted.

| Target | Condition | Outcome |
| --- | --- | --- |
| Job | `complete` | Satisfied by terminal completion of that exact job |
| Job | `success` | Satisfied by exact-job success; otherwise dependency failure |
| Goal | `success` | Pending while open; satisfied by goal success; error on explicit goal failure |

Reject other combinations. No `followReplacements` field is needed. Missing
targets are explicit errors for this API; they never count as success. Reject
self-dependencies, cycles, and duplicate targets within a declaration.

Add `job.dependencies` to both submission and restart bodies. It is mutually
exclusive with a nonempty legacy `job.prerequisites`; new callers can express
mixed exact-job and goal prerequisites in `dependencies` alone. Existing
prerequisite calls retain their current behavior.

Persist submission dependencies under the reserved wait ID `submission`.
Expose their outcomes through the dependency-wait lookup and to the execution
context before the consumer body runs. A prerequisite that has already failed
still permits creation of the consumer, which becomes eligible only to record
and handle prerequisite failure, never to execute its body.

## 8. Registering and reading runtime waits

### Register using existing leased reschedule

Extend `RescheduleExecutionRequest` with `dependencyWait`:

```http
POST /v1/tenants/acme/jobs/B/leases/L/reschedule
Content-Type: application/json
```

```json
{
  "nextRoute": {"jobType": "publish-report"},
  "dependencyWait": {
    "waitId": "join-report-1",
    "dependencies": [
      {"target": {"goalId": "G"}, "condition": "success"}
    ],
    "deadline": "2026-10-25T10:00:00Z"
  }
}
```

The existing lease token/header is required as usual. `waitId` is a nonempty,
URL-safe opaque ID stable for this logical wait; `submission` is reserved.
`dependencies` is nonempty. `deadline` is optional and absolute. Existing
`taskWait`, client-payload, and routing rules still apply.

`dependencyWait` is mutually exclusive with nonempty `waitForJobIds`, `waitUntil`,
and alternate-route timing in v1. This prevents competing wakeup policies from
silently overriding the structured wait. Legacy reschedule requests are unchanged.

Registration, initial evaluation, and lease release happen in one scheduler
transaction. The response remains `204`. An already satisfied or failed group
makes its resume route eligible immediately; a pending group blocks it. A
deadline makes timeout processing eligible and does not count as goal success.

Re-registering the same wait ID with the same descriptor returns its durable
state without resetting the deadline or result. Changed dependencies, their
order, or deadline return `409 dependency_wait_conflict`. This does not bypass
lease validation: recovery after a lost response reads the wait under a newly
acquired lease rather than using the expired lease again.

### Read the durable result

```http
GET /v1/tenants/acme/jobs/B/dependency-waits/join-report-1
```

Successful response:

```json
{
  "jobKey": {"tenantId": "acme", "jobId": "B"},
  "waitId": "join-report-1",
  "state": "SATISFIED",
  "deadline": "2026-10-25T10:00:00Z",
  "dependencies": [
    {
      "target": {"goalId": "G"},
      "condition": "success",
      "state": "SATISFIED",
      "satisfiedByJobId": "A2"
    }
  ]
}
```

Required top-level fields are `jobKey`, `waitId`, `state`, and `dependencies`.
`deadline` is present only if supplied. Dependencies retain declaration order.
Group states are `PENDING`, `SATISFIED`, `FAILED`, `TIMED_OUT`, and `CANCELLED`.
Dependency states are `PENDING`, `SATISFIED`, `FAILED`, and `CANCELLED`.

Pending goal entries expose `blockedReason`: `waiting_for_execution` when any
candidate is non-terminal, otherwise `awaiting_candidate`. Candidate details
come from the goal-filtered jobs query; there is no single current producer.
Satisfied entries expose their immutable `satisfiedByJobId`. Failed entries
expose `failure`; for goal failure
this is exactly the `GoalFailure` object above. A failed group also exposes
`failedDependencyIndex` and `failure` for the dependency that settled the group.
Exact-job failure uses `code: job_dependency_failed`, `jobId`, and `message`.

All dependencies are required. Bind each satisfied dependency independently;
fail the group as soon as one fails. If multiple are already failed during one
evaluation, select the lowest declaration index. Once the group closes, cancel
remaining pending entries and stop their subscriptions, preserving earlier
bindings and failures. Later producer changes cannot modify a closed group.
Cancelling dependency entries ends subscriptions only; it never cancels the
referenced jobs or goals.

Lookup is read-only and returns `200` even for failed or timed-out waits.
Unknown consumers return `404 job_not_found`; unknown waits return
`404 dependency_wait_not_found`. The endpoint uses existing job-read authority;
it does not itself lease, resume, or mutate a job.

The SDK's new `AwaitGoals` helper returns bindings or `GoalFailedError`. It must
reuse the persisted wait identity and record the outcome in workflow history
before returning to application code. On recovery it reads an existing outcome
instead of repeatedly registering a completed wait. Task-level waits use the
same protocol with identity stable across suspension and worker replacement.

## 9. Ordering, history, and deadlines

The scheduler must provide one transaction ordering for registration, job
completion, candidate attachment/publication, explicit goal failure, and wait
settlement. For v1, settle affected pending waits in the transaction that
publishes the goal outcome. Notifications announce committed outcomes and must
not decide which outcome wins. Lease acquisition reads durable state and must
not depend on notification delivery.

Persist a successful job's result and artifacts before scheduler completion.
In the completion transaction, record its terminal outcome, combine its primary
`goal_id` with extra goals indexed by job ID, and conditionally resolve each
open goal with that job. Skip
closed goals; one closed goal must not invalidate completion or updates to other
open goals. Multiple goals may resolve to this job atomically.

Candidate attachment uses the same coordination as completion and goal closure.
When several goal rows need locking, use consistent ordering or equivalent
serializable coordination to avoid deadlocks and lost associations.

History preparation may span storage systems. Reserve stable identities before
preparation, publish only fully prepared jobs with their primary `goal_id`,
and recover or clean up unpublished preparation. For a fresh goal, create its
row in the same scheduler transaction as the first job. Store primary identity in
authoritative job metadata; cloned chapters must not accidentally assign an
independent restart to its source goal. Adding associations does not rewrite
chapters, client payload, or client-payload revisions.

Workflow history records each typed dependency declaration and exact outcome,
including successful bindings or the full goal failure snapshot. Replay reads
those records without live goal inspection. A crash after scheduler settlement
but before history recording recovers the same durable outcome. Retained history
on a consumer restart keeps its bindings; later waits register anew.

Adding candidates does not extend a consumer deadline. Once the deadline is
reached, unresolved groups time out on the next scheduler evaluation; already
settled outcomes remain unchanged. All settlement paths check the deadline
before assigning a new outcome. Consumer cancellation closes its waits. Neither
timeout nor cancellation changes producer goals or their candidates.

### Conservative cycle validation

Validate cycles on dependency registration, candidate attachment, and shared-goal
publication under coordination that also protects against concurrent cycle
creation. Model edges from jobs to unresolved dependency targets, and from
open goals to their non-terminal candidates.

For v1, reject any resulting directed cycle. A candidate cannot wait on its own
open goal, directly or indirectly. With multiple candidates, another independent
candidate might make some cyclic graphs live; v1 deliberately rejects those
graphs rather than implementing general alternative-path deadlock analysis.
This restriction must be documented as conservative validation, not proof that
every rejected graph would deadlock.

Evaluate a successful candidate attachment's prospective settled state: it
closes the target goal immediately and does not add a pending execution edge.
Terminal candidates create no non-terminal goal-to-job edge.

## 10. HTTP errors and compatibility

Keep the existing `ErrorResponse` shape, `{code, message}`, for request failures.
Business outcomes such as a failed goal live in `GoalInfo` and wait snapshots.

| HTTP | Code | Meaning |
| --- | --- | --- |
| `400` | `invalid_dependency` | Invalid message, target, policy, or field combination; missing required restart request ID |
| `404` | `goal_not_found` | Unknown goal in the request tenant |
| `404` | `job_not_found` | Unknown referenced job |
| `404` | `dependency_wait_not_found` | Unknown wait for an existing consumer |
| `409` | `goal_conflict` | New candidate/shared restart on a closed goal, or conflicting failure request |
| `409` | `existing_job_mismatch` | Destination or restart idempotency key reused with different inputs |
| `409` | `dependency_wait_conflict` | Wait identity reused with a different descriptor |
| `409` | `dependency_cycle` | Mutation introduces an unresolved cycle |

Existing authentication, authorization, lease-loss, schema-validation, and
storage-error behavior also applies. Conflict messages identify the current
goal/job where useful; callers can retrieve structured state through `getGoal`.

Backfill each preexisting job into a distinct generated, eagerly created primary
goal and persist its `goal_id`. Do not insert primary membership into
`goal_candidates`. Persist the mapping so interrupted migration cannot allocate
another goal on retry. Do not
group historical restarts by ancestry. Backfilled successful jobs yield succeeded
goals; unsuccessful or unfinished jobs yield open goals. Unknown historical
outcomes require explicit reconciliation and cannot be assumed successful.

Existing `AwaitJobs`, prerequisites, and persisted waits keep their semantics.
New clients MUST reject missing `goalId`, missing required association views, or
unsupported structured-wait responses when using the corresponding features;
an old server must never silently execute a `shareGoal` restart as an independent
restart. Server request schemas and decoders must reject unsupported new request
fields. Strict older response
decoders may require a coordinated client update for added `goalId` fields.

Soft deletion retains goals, candidate associations, failure snapshots, and
referenced history. Deleting a job does not implicitly fail associated goals.
Hard purge must preserve referenced results and failure snapshots or reject
deletion while they are needed for pending waits or replay.

## 11. Storage and implementation simplifications

Use `jobs.goal_id` for primary membership, `goal_candidates` for additional
late-bound links only, and `goals` for authoritative goal state:

```text
jobs:
  ... existing job fields ...
  goal_id                         # immutable primary goal
  INDEX (tenant_id, goal_id, job_id)

goals:
  tenant_id, goal_id               # primary key; eagerly created
  state
  successful_job_id
  revision                        # goal-state revision only
  failure                         # immutable structured snapshot
  created_at, updated_at, closed_at

goal_candidates:                   # additional associations only
  tenant_id, goal_id, job_id, added_at
  PRIMARY KEY (tenant_id, goal_id, job_id)
  INDEX (tenant_id, job_id, goal_id)
```

### Primary membership and extra links

A job's nonempty primary `goal_id` references an existing goal in the same
tenant. For a fresh goal, insert that goal and its first scheduler job in one
transaction after history preparation. A shared-goal restart inserts a job
referencing an existing open goal. Neither inserts into `goal_candidates`.

`addGoalCandidate` checks the job's immutable primary ID first. If it matches,
return the existing logical association without inserting a row. Otherwise,
insert the extra pair with uniqueness on `(tenant_id, goal_id, job_id)`.
Implementations must enforce that no extra row duplicates primary membership.

`otherGoalIds` comes directly from extra rows. There is no independently writable
membership array on jobs or goals.

To find candidates for a goal, combine:

1. Jobs selected by indexed `(tenant_id, goal_id)`.
2. Jobs referenced by extra rows selected by indexed `(tenant_id, goal_id)`.

Completion performs the reverse lookup: the job's own `goal_id` plus extra
rows selected by `(tenant_id, job_id)`. Failure snapshots, inspection, pending
wait diagnostics, and cycle validation MUST include both representations.
Omitting primary jobs from these paths is a correctness error.

Batch-load extra associations for paginated job inspection. Apply the union of
primary/extra goal filters before pagination and deduplicate jobs matching more
than one requested goal. Do not scan unrelated jobs to discover candidates.

### Eager goal creation and minimal updates

Every goal has a small explicit row from first-job publication. Recovery reuses
the goal ID reserved during preparation. Missing rows mean not found or a storage
integrity problem, never implicit `OPEN` state. An uncommitted preparation may
exist outside the scheduler, but it is not yet a publicly visible job or goal.

After creation, the goal row is updated only for success or explicit failure:

| Operation | Goal-row writes | Extra association writes |
| --- | --- | --- |
| Fresh job submission | Insert `OPEN` row | None |
| Shared-goal restart | None | None |
| Add running or unsuccessful extra candidate | None | Insert pair |
| Add already-successful extra candidate | Resolve open goal | Insert pair |
| Successful job completion | Resolve associated open goals | None |
| Unsuccessful completion or ordinary job progress | None | None |
| Explicit goal failure | Record failure | None |
| Duplicate candidate/failure operation | None | None |

“No write” does not mean no coordination. Candidate addition and shared-goal
publication must use goal-scoped locking or equivalent serializable coordination
with closure so they cannot insert membership based on stale `OPEN` state.
Locks need not update goal revision, timestamps, or other persisted fields.

The usual single-job lifecycle adds one goal insert and one terminal update,
with no candidate-table row. Failed jobs can leave the goal open indefinitely
without additional goal writes. Do not store or update candidate counts,
latest-attempt pointers, or membership generations on the goal row.

Winner selection uses a conditional open-to-terminal update. Successful
completion touches only the primary/extra goals for that job and their indexed
pending waits. Unsuccessful completion updates no goal rows and settles no
goal-success waits.

A full failure snapshot reads all candidates from both lookup paths. Store the
immutable snapshot once internally where practical; public errors still expose
complete lists.

V1 omits lazy goal creation, implicit-to-explicit goal promotion, exclusive
adoption, selected-candidate state, priorities, single-active-job enforcement,
candidate removal, automatic extra-goal inheritance, goal-driven cancellation,
automatic retries as new jobs, and goal reopening.

Durable restart-operation records remain necessary for idempotent preparation.
They hold identities and semantic requests without an expected-current-job
guard. Goal `revision` is a state-inspection field, not a membership token.

These are architectural choices, not measured performance claims. Validate
indexed lookup behavior, relevant fan-out costs, and the absence of extra rows
or goal updates on common operations.

## 12. Implementation and acceptance

Implementation touches shared runtime interfaces, the runtime core scheduler
port, SQLite and toy implementations, remote codecs, OpenAPI/generated bindings,
workflow history/error codecs, inspection, and CLI.

Expose `GetGoal`, `AddGoalCandidate`, `FailGoal`, and `GetDependencyWait` in Go.
Add `GoalID` to handles/inspection, derived `OtherGoalIDs` to job inspection,
`ShareGoal` to restart input, and structured dependencies to submission and
rescheduling. The CLI exposes goals get, goals candidates,
goals add-candidate, and goals fail --message, with --tenant and --url.
Responses are JSON; candidate listing supports pagination. Restart sharing
is available through the Go and REST restart inputs.

c2j explicitly adopts goal waits and reads the bound successful job's outputs
and artifacts. Fail-fast and soft-result operations retain their policies until
deliberately migrated.

Required acceptance across embedded, toy, core-backed, and remote runtimes:

1. Submission eagerly creates a distinct primary goal and job reference without
   an extra association row; explicit-ID retries and interrupted migration
   recover the same mapping.
2. Ordinary restart creates a new goal; `shareGoal` carries the primary goal
   only, permits concurrent candidates, and never copies extra associations.
3. Failed A keeps B blocked on G; concurrent successful A2/A3 produce exactly
   one permanent winner, while both jobs keep their real completion outcomes.
4. One job resolves several open goals; already failed or succeeded goals are
   skipped without breaking job completion or its other goals.
5. Adding running and successful jobs, adding terminal unsuccessful jobs,
   duplicate attachment after closure, and new attachment to closed goals match
   the candidate contract.
6. Attachment/completion, attachment/failure, restart/closure, competing success,
   and registration/outcome races have no missed resolution or orphan runnable
   publication.
7. Explicit failure preserves the human message and terminal/non-terminal
   candidate lists through REST, catches, job-result reads, and replay.
8. Running candidates continue after goal failure and can satisfy other goals;
   later finishes do not rewrite the frozen failure snapshot.
9. Independent forks can be evaluated and the chosen job added to the original
   open goal without changing any job's primary identity or artifacts.
10. Lost restart responses/preparation crashes recover the same destination even
    after its history advances or goal closes.
11. Worker crashes between settlement and history recording preserve outcomes;
    consumer restarts retain bindings in cloned history.
12. Deadlines survive added candidates, timeout/cancellation prevents rebinding,
    and consumer cancellation does not change producer jobs.
13. Mixed dependency groups, conservative cycle validation, tenant isolation,
    hidden candidates, deduplicated goal queries, and old API semantics conform.
14. Both primary and extra membership paths participate in completion, failure
    snapshots, query filtering, and cycle checks; lookup uses their indexes and
    job-list enrichment avoids one additional query per returned job.
15. Shared-goal restarts and primary-pair add requests create no extra association
    rows. Non-terminal/unsuccessful attachment and job progress do not update goal
    revision or timestamps. Goal creation and first-job publication are atomic.

## 13. Using the implementation

Go runtime clients use SubmitRestartJobRequest.RequestID for generated-ID
shared restarts, and SubmitRestartJob.ShareGoal for sharing. An explicit
destination JobID is also sufficient for idempotency.

Workflow execution contexts and task contexts expose:

    bindings, err := ctx.AwaitGoals(goalID)
    if err != nil {
        return nil, err // errors.Is(err, jobdb.ErrGoalFailed) recognizes explicit failure
    }
    winnerJobID := bindings[0].SatisfiedByJobID

AwaitDependencies accepts mixed jobdb.Dependency targets. Execution joins
record their full outcome in join_result history; task joins commit a
dependency_binding record before returning to task code. Restart admission
records the new submission wait at the continuation boundary. Replay uses these
snapshots and retains the human failure message and candidate lists.

SQLite migrates schema version 6 to 7 transactionally, eagerly backfilling
independent primary goals. Unknown archived completion statuses stop migration
for reconciliation. Ordinary candidate failures leave goals open. No command
or goal endpoint cancels candidates.

SQLite goal filtering uses the primary-goal and extra-candidate indexes before
pagination. Additional associations are fetched for the returned page.
Completion and lifecycle mutations acquire the SQLite writer reservation before
reading scheduler facts; competing connections serialize resolution. Goal rows
change only on initial publication and terminal resolution. Dependency reads
use a consistent read snapshot, including deadline projection.

External implementations of runtimecore.Scheduler must implement GoalScheduler
and honor the transaction requirements documented there. The core does not
maintain an independent in-memory fallback. Whole-job retries remain the
existing retry mechanism; c2j adoption is a separate integration.
