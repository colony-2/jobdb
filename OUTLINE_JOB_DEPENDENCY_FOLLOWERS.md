# Job goals and success dependencies: implementation outline

Status: agreed design direction; API names and schemas are illustrative.

Based on [PROPOSAL_JOB_DEPENDENCY_FOLLOWERS.md](PROPOSAL_JOB_DEPENDENCY_FOLLOWERS.md).
The concrete REST contract is specified in
[docs/SPEC-Job-Goals.md](docs/SPEC-Job-Goals.md).

A goal represents work whose successful result a consumer needs. Multiple jobs
can compete to satisfy a goal, and one job can satisfy multiple goals. The first
successful resolution committed to a goal becomes its permanent result.

## 1. Identity and invariants

| Identity | Meaning |
| --- | --- |
| `goal_id` | Work whose successful result a consumer needs |
| `job_id` | A concrete execution with its own history, outcome, and artifacts |

Every job has an immutable primary `goalId`. Normal submission generates a fresh
goal ID independently, using the same KSUID format as generated job IDs. A
restart with `shareGoal: true` carries forward the source's primary goal.
Create the goal row eagerly when publishing its first job; a shared-goal restart
uses that existing row. There is no implicit goal state or lazy creation path.

Additional associations make a job a candidate for other goals. Expose these as
read-only `otherGoalIds` on job inspection. Adding a candidate does not change its
primary goal, history, artifacts, or existing associations.

Core invariants:

- Goals and all their candidates belong to the same tenant.
- Multiple non-terminal jobs may be candidates for one goal.
- A job may be a candidate for multiple goals simultaneously.
- A goal is `OPEN`, `SUCCEEDED`, or `FAILED`.
- Each succeeded goal has one immutable `successfulJobId`.
- Closing a goal never cancels candidate jobs, including losing candidates.
- Candidate relationships are append-only; v1 has no removal or reassignment.
- Closed goals cannot accept new candidates or reopen. Retrying an association
  that already exists remains an idempotent success.

Preserve existing job IDs, chapter references, artifact keys, and leases.
Scheduling does not traverse restart ancestry or replacement chains.

## 2. Use one candidate operation; omit exclusive adoption

Add an idempotent operation:

```http
PUT /v1/tenants/{tenantId}/goals/{goalId}/candidates/{jobId}
```

The goal/job pair supplies idempotency, so this operation needs no separate
request ID. Registration evaluates the job's durable outcome in the same
transaction as insertion:

| Candidate state | Effect on an open goal |
| --- | --- |
| Non-terminal | Register it; wait for a future successful completion |
| Succeeded | Register it and immediately satisfy the goal |
| Terminal and unsuccessful | Register it for provenance; leave the goal open |

There is no separate `adopt` endpoint, selected-candidate pointer, ranking,
priority, or manual-resolution mode. All registered candidates are equally
eligible. An added running job can lose to any other candidate.

For “try ten approaches, choose the best,” run the alternatives in independent
goals and add only the chosen job to the original goal while it is still open.
An already-successful choice resolves it immediately. A running choice can
resolve it later, but receives no preference over candidates already registered.
A goal that has already succeeded cannot have its answer replaced.

## 3. Store primary membership on jobs and only extra links separately

Recommended SQLite schema:

```text
jobs:
  ...
  goal_id                         # immutable primary goal
  INDEX (tenant_id, goal_id, job_id)

goals:
  tenant_id, goal_id               # primary key; eagerly created
  state                           # OPEN | SUCCEEDED | FAILED
  successful_job_id               # immutable after success
  revision                        # goal-state revision, not membership revision
  failure                         # immutable failure snapshot
  created_at, updated_at

goal_candidates:                   # additional, late-bound associations only
  tenant_id, goal_id, job_id, added_at
  PRIMARY KEY (tenant_id, goal_id, job_id)
  INDEX (tenant_id, job_id, goal_id)
```

A job's `goal_id` is its primary candidate relationship. Do not duplicate that
pair in `goal_candidates`. Normal submission inserts a goal and a job; a
shared-goal restart inserts only a job referencing the existing goal. Neither
operation needs a `goal_candidates` insertion.

Adding a pair already represented by `jobs.goal_id` is a no-op success, even if
the goal has closed. Adding an extra pair inserts one unique association row.
Primary identity is immutable, so the two representations remain disjoint.
The public `otherGoalIds` list is derived directly from extra association rows.

Both lookup directions remain indexed:

- For one job, its associated goals are its primary `goal_id` plus the extra
  rows found by `(tenant_id, job_id)`.
- For one goal, its candidates are the jobs found by `(tenant_id, goal_id)` plus
  jobs referenced by extra rows found under that same goal.
- Goal-filtered job queries combine both sources, deduplicate jobs, then apply
  pagination. Failure snapshots and cycle checks also use both sources.

Keep the indexed extra relation rather than an array in a job or goal row:
completion needs job-to-goal lookup, while inspection, failure snapshots, and
graph checks need goal-to-job lookup. Do not maintain a second writable copy
of membership. Batch-load `otherGoalIds` during paginated inspection.

### Eager goal creation with few subsequent writes

Every goal has an explicit row from the first job's publication. Create that
row and publish the first scheduler job atomically, after history preparation.
A missing goal row means not found or inconsistent storage; it never means an
implicitly open goal. Recovery reuses the reserved goal ID.

The row starts `OPEN` and only changes on successful resolution or explicit
failure. Candidate additions, shared-goal restarts, job progress, unsuccessful
completion, and duplicate requests do not update the goal row, timestamps, or
revision. They still coordinate with closure transactionally; avoiding a row
update does not permit an unsafe check-then-insert race.

| Operation | Goal-row write | Extra association write |
| --- | --- | --- |
| Normal submission | Insert `OPEN` | None |
| Shared-goal restart | None | None |
| Add non-terminal or unsuccessful extra candidate | None | Insert pair |
| Add already-successful extra candidate | Resolve if open | Insert pair |
| Successful job completion | Resolve each associated open goal | None |
| Unsuccessful job completion | None | None |
| Explicit goal failure | Record failure | None |

A goal revision describes only goal state, not the entire candidate collection;
do not use it as a membership cache validator or candidate-admission guard.
No workflow chapter or client-payload revision changes on candidate attachment.

The normal lifecycle requires one initial goal insert and one terminal update,
with no separate membership row. Failed executions may leave that goal open
without further goal writes. No lazy creation or implicit-to-explicit promotion
protocol is part of v1.

Remove the old single-active-job check, `currentJobId`, selected-candidate
state, and expected-current-attempt generation checks.

## 4. Goal resolution and concurrency

```text
B waits on G
A and A2 are candidates for G and run concurrently
A fails                         -> G stays open; B stays blocked
A2 succeeds                     -> G succeeds with A2; B resumes
another candidate later succeeds -> G still resolves to A2
```

Persist job output and artifacts before exposing success. In the scheduler
completion transaction, record the job's terminal outcome and conditionally
resolve each associated goal that is still open. A job can satisfy several
goals in that transaction.

“First success” means the first successful resolution transaction for that goal,
not the earliest wall-clock execution finish. Adding an already-successful job
also competes at that same resolution point.

A candidate losing a goal race still completes successfully. Goals that have
already closed are skipped; they do not invalidate job completion or prevent
the job from satisfying other open goals.

Coordinate candidate registration with completion so neither can miss the
other. If registration wins, completion sees the association. If completion
wins, registration sees the terminal result. Use consistent transaction
coordination for multi-goal changes.

## 5. Shared-goal restarts are additional candidate publication

Existing ordinary restarts create a fresh primary goal. With `shareGoal: true`,
the new job uses the source's primary goal and becomes another candidate.

Allow any source accepted by the existing restart-history rules, including a
source that is still running when a valid immutable prefix is available. The
shared primary goal must still be open when the destination publishes. There
is no current-source restriction and no requirement for other candidates to
finish first.

Additional associations are not inherited automatically. To make the new job a
candidate for another goal, add that pair explicitly. This avoids accidentally
enrolling every fork into all goals associated with its source.

Retain durable restart-operation idempotency for generated destination IDs:

1. Reserve the new job ID and semantic request fingerprint.
2. Prepare cloned history and artifacts.
3. Atomically validate the open primary goal and dependency graph, publish the
   job with its primary `goal_id`, and record the operation result. No extra
   association row or goal-row update is required.

Different requests can publish different candidates concurrently. Equivalent
retries return the original job even after it progresses or its goal closes.
A new publication that loses to goal closure conflicts without publishing an
orphan runnable job. Prepared history is recoverable or eligible for cleanup.

## 6. Fail a goal without cancelling any jobs

```http
POST /v1/tenants/{tenantId}/goals/{goalId}/fail
```

```json
{
  "message": "The upstream dataset was withdrawn."
}
```

Failure closes only the goal and notifies its waiters. There is no `cancelJobs`
flag or bulk candidate-cancellation API in v1. Existing individual job
cancellation remains available.

Freeze this failure snapshot at closure:

```text
code = goal_failed
goal_id
message
failed_at
terminal_job_ids
non_terminal_job_ids
```

Both job-ID lists include all candidates at that transaction boundary, including
hidden jobs, sorted by job ID. They are disjoint and never change afterward.
If a running candidate later finishes, live inspection changes but the original
failure snapshot and replayed errors do not.

A job may succeed after its primary goal fails and may still satisfy its other
open goals. Explicit goal failure and candidate success serialize: the first
terminal goal transition wins. An unsuccessful candidate never automatically
fails a goal.

Use the existing `FAILED` state and fail endpoint for explicit goal closure in
v1; do not add a separate cancelled-goal state or endpoint.

## 7. Structured dependencies and durable wait results

Share one typed dependency model across submission prerequisites, workflow
waits, and task-level waits:

| Target | Condition | Behavior |
| --- | --- | --- |
| Job | `complete` | Observe that exact execution's terminal outcome |
| Job | `success` | Require exact-job success; fail on unsuccessful termination |
| Goal | `success` | Wait while open; return the winner or explicit goal failure |

Goal waits need no `followReplacements` field. Existing `AwaitJobs`, legacy
prerequisites, and persisted waits retain their semantics.

Persist wait groups and dependency outcomes with stable wait IDs, absolute
deadlines, typed targets, and immutable `satisfiedByJobId` bindings. Index pending
dependencies by target. Multiple dependencies retain all-required semantics.

A late waiter reads the same goal outcome as an existing waiter. Unsuccessful
candidates must not cause repeated consumer leases. Pending inspection uses
`waiting_for_execution` if any candidate is non-terminal, otherwise
`awaiting_candidate`; it does not identify one authoritative current job.

The workflow records the exact binding or structured goal failure in history
before returning it to application code. Replay uses that recorded outcome.
Recovery between scheduler settlement and history recording retrieves the same
durable result. Task-level waits and submission prerequisites expose the same
bindings.

c2j uses the winning job ID for outputs and artifacts. Goal failure propagates a
typed, non-retryable error with the human message and both job-ID snapshots.
Catch handlers may handle it; uncaught errors fail the consumer execution.
Consumer goals remain open for repair unless explicitly failed.

## 8. Deadlines, graph checks, and retention

Consumer deadlines include time awaiting manual repair and never reset when a
candidate is added. Closing a consumer wait on timeout or cancellation does not
change producer goals or candidate jobs. Closed waits never rebind.

Check unresolved dependencies during wait registration, candidate attachment,
and shared-goal restart publication. Multiple candidates introduce alternative
ways to satisfy a goal. For v1, conservatively reject any directed cycle using
edges from open goals to their non-terminal candidates and from jobs to their
unresolved dependencies. This may reject a cycle that another independent
candidate could break, but avoids implementing general alternative-path
deadlock analysis initially. Check the prospective settled state when adding
an already-successful candidate.

Preserve goal associations, successful job references, failure snapshots, and
required history/artifacts through retention. Existing job deletion must not
implicitly fail goals or erase candidate references. Replay records can refer
internally to shared immutable failure snapshots; public errors still expose
the complete lists.

## 9. Compatibility and delivery

Backfill each existing job with a distinct eagerly created primary goal and
persist its `goal_id` for migration recovery. Do not create extra association
rows for primary membership. Do not infer
sharing from historical restart ancestry. Successful jobs create succeeded
goals; unsuccessful and unfinished jobs create open goals.

Expose `goalId` in handles and job inspection, and derived `otherGoalIds` in
job inspection. A `goalIds` job-query filter matches any candidate association.
Implement the new candidate endpoint alongside goal lookup/failure and durable
wait lookup. Remote clients must not silently downgrade goal behavior.

Recommended sequence:

1. Goal records, association indexes, identity exposure, and migration.
2. Atomic completion-to-goal resolution and idempotent candidate registration.
3. Shared-goal restart publication and explicit goal failure without cancellation.
4. Structured dependency scheduling, conservative cycle checks, and durable waits.
5. Workflow/task history, error propagation, remote APIs, inspection, and c2j.
6. Optional later migration of whole-job automatic retries to new candidate jobs.

Current job, task, and scope retries remain inside their existing histories.

Acceptance coverage includes concurrent candidates, one job satisfying multiple
goals, adding a running or already-successful job, duplicate attachment after
closure, candidate/completion races, independent fork selection, restart
idempotency, failure snapshots with running candidates, jobs continuing after
goal failure, immutable winner bindings, timeout/replay recovery, cycle checks,
tenant isolation, hidden jobs, primary membership without duplicate association
rows, eager creation/recovery, goal revisions unchanged by membership additions,
and old API compatibility.

Candidate removal, exclusive adoption, reopening goals, candidate cancellation
as a goal operation, automatic loser cancellation, and ranking policies are
outside v1.


## Implementation status

SQLite, Toy, remote REST, workflow/task waits, failure replay, schema migration,
and goal CLI commands are implemented. See [the specification](docs/SPEC-Job-Goals.md)
for the exact endpoint and Go interfaces. Core-backed external adapters must
implement the expanded scheduler transaction contract. c2j integration and
moving automatic retries to separate candidate jobs remain separate work.
