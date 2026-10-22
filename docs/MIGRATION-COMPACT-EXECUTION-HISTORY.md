# Compact execution history (execution protocol 3)

This is a hard history-format cutover. Delete jobs produced by execution
protocols 1 and 2 before deployment. The workflow runner rejects their histories
before invoking consumer code. It does not migrate or continue them.

Public Go interfaces, request/response structures, and REST endpoints are
unchanged. `ExecutionPatternVersion` is now 3. Deploy matching clients and servers.
The older `JobWorker` execution API is unaffected.

## Persistence and replay

An ordinary local task commits one outcome with its input snapshot, output/error,
artifacts, and invocation identity. Its declaration and attempt information travel
in chapter reconstruction metadata. Completed work bypasses input construction;
uncommitted work may run again. Retries reuse established input snapshots.

Scopes still manage cancellation, identities, retries, and catches. Organizational
scope entry/exit produces no record or metadata. Policies and restart declarations
that need validation are buffered and committed with the next material event.
Normal scope outputs, task ends, and successful “no retry” decisions are omitted.

Replay consumes reconstruction metadata before the associated event. Committed
catch selections survive changed predicates. Uncommitted choices may be evaluated
again. Durable waits, dispatches, retry wake times, and actual timeouts retain
records where required. Scheduler handoffs without an outcome checkpoint any
uncommitted path metadata before relinquishing the lease. A timeout propagated
through enclosing scopes does not
need another physical chapter for each ancestor.

A conflicting append never moves a stale event to another ordinal. A contender
may adopt a committed outcome for the same invocation/attempt and compatible
path; an incompatible winner stops that execution. Lease and external completion
generation checks remain in force.

## Timeout changes

Local invocation timers restart when uncommitted execution reruns. Timed blocks
instead use the preceding durable completion as their start anchor; the initial
block uses the execution header. Worker downtime counts against that budget.
Nested blocks retain their own anchors, and successful children do not extend an
enclosing deadline. Retries reset attempt budgets, not total budgets.

Live checks cover admission, scope completion, and job completion. Historical
replay consumes recorded outcomes without applying today's clock to them.

## Inspection and restart

`GetJobRun().Execution` expands reconstruction metadata into the existing record
structure. Multiple entries can therefore share a physical chapter ordinal.
Use logical IDs to distinguish them; count `ListChapters` results for physical
history volume. Exact scope entry/exit timing is no longer a durable guarantee.

The existing `ExecutionRestart.ScopeID` accepts a logical task ID from an
inspection entry of kind `task`, selecting the boundary before that invocation.
The restart retains the required intra-chapter prefix without retaining the task
outcome. For an after-task restart, use `LastStepToKeep` with its result ordinal.
Existing declared scope IDs and `OnRestart` callbacks remain supported through
metadata, with no separate scope chapter. Pure organizational scopes have no
independent persisted restart target.

Continuation budgets are committed with the next material event. If execution
stops before anything commits the continuation, recovery can establish it again.
A committed continuation retains its budgets. Source history remains immutable.

## Measured chapter count

The regression fixture matching story3's 235 scopes, 29 task results, and two
human-input waits uses **38 chapters** on toy, SQLite, and HTTP/SQLite runtimes:

- 29 task results and two pending waits;
- three worker activations;
- submission, execution header, job result, and terminal outcome.

The count is identical with no wrapper scopes, organizational scopes, or 235
restartable scopes. This is a synthetic fixture with local workers for the other
27 tasks, not a rerun of the original commands and Codex calls. Metadata bytes
can grow with policy declarations even when the chapter count is unchanged.
