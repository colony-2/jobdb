# Inspecting committed catch history

`workflow.ReadCatchHistory(ctx, runtime, jobdb.GetJobRunRequest{JobKey: key})`
reads the engine's committed catch/retry knowledge. Use the same function for
active runs and historical inspection. It requires only an existing
`jobdb.WorkflowRuntime`, with no engine, workers or application catch rules.
It executes no predicates, handlers, restart hooks or replay orchestration and
writes no chapters, artifacts or scheduler state.

```go
h, err := workflow.ReadCatchHistory(ctx, runtime, jobdb.GetJobRunRequest{JobKey: key})
if err != nil {
    return err
}
for _, a := range h.Attempts {
    if a.Selection != workflow.CatchSelectionSelected ||
        a.Handling != workflow.CatchHandlingAccepted || a.Accepted == nil ||
        a.Accepted.Disposition != workflow.CatchHandled || a.Accepted.Failure != nil {
        continue
    }
    fmt.Printf("boundary=%s attempt=%s rule=%s handled\n",
        a.Boundary.BoundaryID, a.Boundary.AttemptID, a.RuleID)
}
```

The helper obtains all needed evidence regardless of request inclusion flags.
The returned JSON, output snapshots, artifact descriptors and artifact keys
belong to the caller. Artifact contents remain accessible through the existing
runtime artifact API using the recorded keys.

## Identity and ordering

`CatchBoundaryRef` contains JobKey, JobAttemptID, BoundaryID, AttemptID and
AttemptNumber. IDs are opaque equality-only values; never parse them.
BoundaryID identifies an invocation/visit; AttemptID includes its enclosing
attempt ancestry. JobAttemptID distinguishes whole-job attempts. ParentID names
the enclosing boundary invocation, including for work executed inside a handler.
Site and BoundaryKind expose the recorded call site and `scope` or `task` kind.
The root job is a scope with an empty ParentID. Omitted organizational scopes
can still be referenced through their deterministic invocation identity.

Upsert polling results by JobKey and AttemptID, retaining earlier attempts.
Repeated visits and retries remain distinct, even when logical records share a
compact chapter. A copied restart prefix is scoped to the requested JobKey,
not merged with its source run. Attempts appear in first-evidence order.
Missing required identity or contradictory association produces a read error,
not a plausible substitute key.

## Selection and acceptance

| Field | States and meaning |
| --- | --- |
| Selection | `selected` names a recorded RuleID; `unmatched` requires recorded empty selection; `predicate_error` retains PredicateError; `unknown` lacks selection evidence. |
| Handling | `accepted` has a committed engine-validated Accepted result; `pending` has selection without acceptance; `not_applicable` is proven absence of applicable handling; `unknown` lacks sufficient evidence. |
| Accepted | Exact disposition, output and resulting failure, independently of Original. |
| Retry | Nil means no recorded decision; nonnil Retry=false means recorded false; WakeAt appears only with an applicable recorded retry. |
| Provenance | `handler_error`, `replacement`, `preserved` or `unknown`; current v4 handled records generally cannot distinguish these causes, so the projection reports unknown with a gap. |

Selection alone does not prove handler entry or completion. Compact writers
buffer catch and handled records until a durable boundary. Before that boundary,
even a running handler can have unknown selection in inspection. After nested
work commits selection, handling remains pending until accepted handling also
commits. A failed write does not expose buffered acceptance. Pending says nothing
about worker liveness and can remain in completed histories.

CatchHandled is successful only with no resulting Failure. CatchHandled with a
failure remains accepted handling, but is not success. CatchContinue continues
engine failure/retry processing; it does not mean a consumer's recipe continue
action. CatchPropagate records propagation. Ordinary retries without selected
catch rules remain failed attempts, not successful catches.

## Evidence gaps and diagnostics

Original preserves the structured boundary failure's Kind and JSON Data. It is
not reconstructed into an application Go error. PredicateError is separate from
Original. Accepted.Failure is the engine-accepted resulting failure.

There is no recorded causal-origin link. The helper reports
`original.origin/not_recorded`, never a guessed failed child. V4 handled records
can collapse a handler error, intentional replacement and preservation into the
same outcome. `accepted.provenance/not_recorded` explicitly retains that limit,
even during live inspection. Payload equality, text and chapter proximity do
not establish provenance. Invalid disposition without accepted handling remains
unaccepted, even when its rejection reason is absent.

Gaps identify a Field and Reason. Reasons include `not_recorded`, `frontier`,
`ambiguous` and `omitted`. A frontier gap marks absent control evidence in the
observed prefix, not instantaneous worker activity. Missing original or
selection evidence does not erase independently proven accepted handling or
retry. Unambiguous recorded rule declarations can retain nested handler parent
association when selection evidence is missing; that does not establish a selected
RuleID. Consumers must tolerate future string state values.

Attempts include relevant failed boundary attempts with catch/retry evidence or
missing control evidence. They do not catalog every successful scope or every
declared rule. Empty Attempts means no evidenced relevant failed attempts in
this prefix, not proof that application code never threw.

Supported-v4 full and compact histories retain logical reconstruction entries
sharing ordinals. Unsupported versions, invalid records, conflicting decisions,
missing required identities and inconsistent physical chapter prefixes return
explicit read errors. There is no conversion, backfill or history rewrite.
Runtime read errors and cancellation remain errors.

## Consistency and final outcomes

A read projects a validated chapter prefix obtained with one ListChapters call.
It does not combine separately fetched chapter lists into acceptance. The job
summary is loaded separately by existing run reads and never establishes catch
acceptance. Active reads may be stale immediately upon return; a frozen prefix
produces the same projection regardless of the current summary or application
code. Runtimes must provide coherent chapter lists; detectable inconsistencies
are rejected. This adds no transaction, revision, cursor, push subscription or
exactly-once notification protocol.

Catch rows describe historical handling episodes. A later failure, retry or
outer catch does not erase them or redefine overall status. Existing
JobRunOutcome and run output remain authoritative. OnJobEnd currently precedes
failure handling and must not be treated as a final post-catch event.

This helper does not change ReplayJobRun or guarantee that application replay
orchestration has no side effects. It avoids that orchestration entirely.
Recipe node joins, route interpretation and replay presentation remain consumer
responsibilities for a later integration stage.
