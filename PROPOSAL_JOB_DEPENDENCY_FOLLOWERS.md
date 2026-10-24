# Success dependencies and restart with followers

Status: proposal; API names and syntax below are illustrative, not implemented.

## Problem

A job often needs the successful result of some work, rather than the outcome of
one particular execution of that work. Today, if producer A fails, consumer B can
fail as well. Restarting A creates a new job, A2, but does not automatically move
B's dependency to A2.

We should support dependencies that follow explicitly designated replacements:
“wait for this job, or its replacement, to succeed.” A restart operation with
`--with-followers` would designate the new execution as the replacement for
pending dependencies that opt into following.

The consumer should remain blocked while its producer is being repaired. It
should resume with the replacement's outputs and artifacts once that replacement
succeeds.

## Current behavior

The following observations are based on c2j's source and its pinned JobDB
dependency, `github.com/colony-2/jobdb v0.0.46`:

- JobDB submission prerequisites already support `complete` and `success`
  conditions through `JobPrerequisite`.
- Prerequisite normalization retains the conditions in execution metadata but
  passes a list of job IDs to the scheduler.
- SQLite's scheduler checks whether those jobs are archived; its wait list does
  not express success conditions. The worker subsequently checks success
  prerequisites and can fail the dependent job.
- Workflow `AwaitJobs` waits for terminal completion. In c2j,
  `waitAndGetRecipeOutput` then retrieves the result using the original job ID.
- `SubmitRestartJob` clones an execution-history prefix and creates a fresh
  scheduler job. It does not designate that job as the replacement for followers.

Relevant source locations:

- JobDB: `pkg/jobdb/jobs.go`, `pkg/jobdb/runtime/core/runtime_submit.go`,
  `pkg/jobdb/runtime/core/runtime_restart.go`,
  `pkg/jobdb/runtime/sqlite/scheduler.go`, and `pkg/workflow/worker_runner.go`.
- c2j: [recipe wait and result handling](pkg/ops/recipe/op.go),
  [soft result inspection](pkg/ops/recipe/await_result_soft.go), and
  [child-group handling](pkg/ops/recipe/child_group.go).

## Proposed dependency semantics

A dependency should express three independent choices:

| Field | Values | Meaning |
| --- | --- | --- |
| `condition` | `success`, `terminal` | What outcome satisfies the dependency? |
| `follow_replacements` | `true`, `false` | Can a designated replacement satisfy it? |
| `on_failure` | `wait`, `fail` | For a success dependency, does an unsuccessful terminal outcome leave the consumer blocked or fail the wait? |

Illustrative declaration:

```yaml
job_id: A
condition: success
follow_replacements: true
on_failure: wait
```

This means: wait for A or its designated replacement to succeed; leave the wait
pending if the current execution fails. For this policy, an unsuccessful terminal
outcome includes failure, cancellation, or timeout of the producer. Cancellation
or timeout of the consumer still terminates its own wait.

For a terminal dependency, any terminal outcome satisfies the condition, so
`on_failure` is inapplicable. Validation should reject contradictory declarations
instead of silently assigning them different semantics.

A success dependency pinned to one execution with `on_failure: wait` cannot
recover through a replacement. We should decide whether to reject that
combination or support it explicitly; it should not be a default.

## Example lifecycle

```text
B waits for A to succeed, following replacements
A fails                      -> B remains blocked
restart A --with-followers   -> creates A2; B now follows A2
A2 fails                     -> B remains blocked
restart A2 --with-followers  -> creates A3; B now follows A3
A3 succeeds                  -> B resumes with A3's outputs and artifacts
```

B does not fail or restart during this sequence. Its status should distinguish
“waiting for a running dependency” from “dependency failed; awaiting replacement.”

If C waits for B, C continues waiting for B. Recovering A does not require
recursively restarting the downstream graph.

## Restart with followers

A normal restart creates an independent execution. A restart with followers also
publishes a durable replacement relationship:

```text
A -> A2 -> A3
```

Restart ancestry and designated replacement are different concepts. Several jobs
may be experimental restarts of A, but only one can be its designated successor.

The `--with-followers` option means “redirect unresolved dependencies that follow
replacements.” It does not mean “restart every downstream job.” In particular:

- Pending following dependencies resolve through the replacement relationship.
- Dependencies pinned to an exact execution remain pinned.
- Already-satisfied dependencies retain their original result binding.
- Completed consumers remain completed.
- Already-failed consumers require separate recovery; this operation does not
  rewrite their history or resurrect them.

This proposal uses “restart” for an operation that creates a new execution. If
the user-facing command calls that operation “replay,” the option can live there.
Read-only history replay must not publish replacement relationships.

## Durable replacement resolution

Updating only the rows that happen to be waiting at restart time is insufficient:

- A consumer might register its wait after the replacement is published.
- A suspended workflow may reconstruct a wait whose history still names A.
- A second replacement may be published before any followers run.

All of these cases must resolve consistently. Two possible storage models are:

1. A durable successor relationship between execution IDs, with at most one
   designated successor per execution.
2. A stable logical-job identity whose current-execution pointer advances when
   a replacement is published.

Either can provide the required behavior. A successor relationship fits an API
that already accepts concrete job IDs; a logical identity makes the distinction
between work and execution explicit. The implementation choice remains open.

Resolution must stay within the tenant, reject replacement cycles, and provide
one authoritative current execution. For an initial version, requiring
`--with-followers` to target the current terminal, unsuccessful execution would
avoid ambiguity around replacing active or already-successful producers.

## Scheduler-level waits

Submission prerequisites and waits created during execution should share a
structured dependency representation. The scheduler needs the condition and
following policy, not just a list of job IDs.

For `success + follow_replacements + wait`:

- A running producer keeps the dependency pending.
- An unsuccessful terminal producer keeps the dependency pending and exposes a
  blocked reason.
- A replacement redirects the unresolved dependency.
- A successful current producer can satisfy the dependency.

A failed dependency must not cause repeated worker leases just to rediscover
that it is still failed. The scheduler should keep the consumer ineligible until
its dependencies are satisfied or another legitimate wakeup occurs.

An indexed dependency table is a useful implementation option. Conceptually, a
record would include:

```text
tenant_id
consumer_job_id
wait_id
requested_job_id
condition
follow_replacements
on_failure
state
satisfied_by_job_id       # populated once satisfied
```

The exact schema is open. The important requirements are durable wait identity,
efficient follower lookup, and agreement between scheduling and workflow history.
Multiple dependencies should retain the existing all-required behavior unless
an API explicitly requests another join policy.

## Bind the result to the execution that satisfied the wait

Scheduling changes alone are not enough. If B wakes after A2 succeeds but calls
`JobResult(A)`, it still retrieves A's failure.

The wait API should durably record and return a resolution such as:

```json
{
  "requested_job_id": "A",
  "satisfied_by_job_id": "A2"
}
```

c2j must use `satisfied_by_job_id` for outputs, artifacts, and inspection associated
with that wait. Exact lookup of `JobResult(A)` should continue to describe A.

Once a dependency is satisfied, its result binding is immutable. Later
replacements must not change data that a consumer has already accepted.
Workflow replay must reproduce the recorded resolution rather than resolve the
replacement chain again. A worker crash between dependency satisfaction and
result consumption must preserve the same binding.

This requires integration with both workflow history and task-level waits; merely
rewriting a scheduler row would leave cached job IDs and replayed waits incorrect.

## Concurrency and recovery requirements

The implementation must define a single ordering between wait registration,
replacement publication, and dependency satisfaction.

- **Concurrent replacements:** only one successor can be designated for a given
  current execution. Competing requests receive a conflict rather than creating
  ambiguous following behavior.
- **Retried restart requests:** a retried request must recover the same new job
  and replacement publication rather than create another successor.
- **Publication:** followers must never observe a replacement whose execution
  history and job record are not ready. If creation spans storage systems, use a
  recoverable preparation/publication protocol with an atomic publication point.
- **Concurrent registration:** a wait registered during publication must resolve
  consistently, including when it names an older execution.
- **Concurrent satisfaction:** once satisfaction is recorded, replacement cannot
  retarget that dependency. Before satisfaction, resolution follows the
  authoritative replacement state.
- **Dependency cycles:** reject replacements that introduce a cycle in unresolved
  dependencies, including a replacement that needs a consumer waiting on it.

## Timeouts, cancellation, and retention

Following replacements must not reset the consumer's configured wait deadline.
A deadline can wake the consumer for timeout handling; it does not make a success
dependency satisfied.

Consumers can still be cancelled while blocked. Producer cancellation follows
the dependency's declared terminal-outcome policy, just as producer failure does.

A missing or deleted target must never count as successful completion. The
implementation must define an explicit error or blocked state for this case.
Deletion and retention must preserve enough replacement and result-binding
information for pending waits and historical replay, or reject deletion while
that information is required.

## Defaults and compatibility

Recommended defaults for new success dependencies:

```yaml
condition: success
follow_replacements: true
on_failure: wait
```

Terminal waits remain necessary for cleanup, optional children, and workflows
that inspect failed results. Fail-fast success waits remain useful when the
consumer wants to handle dependency failure immediately.

Existing APIs must not silently change behavior. In particular, changing current
`await_result` calls to remain blocked on failure could prevent existing `catch`
handlers from running. Likewise, soft result inspection and child-group failure
collection need deliberate policies.

A migration should introduce explicit structured waits, preserve the semantics
of persisted waits and old API calls, then choose how new recipe declarations
adopt the recommended defaults. We should share the underlying condition model
with existing `JobPrerequisite` rather than maintain unrelated submission and
runtime dependency systems.

## Observability

Job inspection should show:

- The originally requested producer and the current effective producer.
- The dependency condition and whether it follows replacements.
- Whether it is waiting for execution, awaiting replacement, or satisfied.
- The execution that satisfied the dependency, once bound.
- The replacement chain and the restart operation that published each link.

Follower queries should distinguish pending followers from consumers that already
accepted a result. Restart output should identify the new job and the published
replacement relationship.

## Suggested implementation scope

1. Add structured scheduler dependencies and consistent evaluation across
   submission prerequisites and runtime waits.
2. Add durable replacement resolution and a restart option that publishes it.
3. Persist wait satisfaction with the actual producer ID and expose that binding
   through the workflow API.
4. Update c2j wait/result handling to consume the bound producer's outputs and
   artifacts.
5. Add inspection support and an explicit compatibility path for recipe defaults.

Acceptance coverage should include producer failure followed by successful
replacement, repeated replacement, late wait registration, pinned dependencies,
multiple followers, mixed dependency conditions, result/artifact provenance,
worker replacement, deterministic replay, concurrent restart requests, crash
recovery during publication, timeout/cancellation, and cycle rejection. The same
behavior should hold across supported embedded and remote runtimes.

## Open decisions

- Use successor links or a stable logical-job identity?
- Name the option `--with-followers`, `--replace`, or another term?
- Initially restrict following restarts to unsuccessful terminal producers?
- Reject exact-execution success waits with `on_failure: wait`, or allow them?
- Introduce a new recipe wait API or version the behavior of existing operations?
- How should required and optional child-group members select dependency policies?
- When should consumer deadlines include time spent awaiting manual repair?
- Should a later operation support adopting an already-created restart as the
  designated replacement?

The core requirement is the same under these choices: a pending consumer can
depend on successful work across explicit replacement executions, and it records
exactly which execution ultimately supplied its result.
