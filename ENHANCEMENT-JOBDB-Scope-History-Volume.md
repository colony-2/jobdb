# Proposal: task outcomes and exceptional events as durable execution history

Date: 2026-10-22

Status: implemented as execution protocol 3; hard history cutover

## Decision

Ordinary scopes should produce **zero durable chapters**. JobDB should reconstruct
scope traversal during deterministic replay and persist only information needed
to recover execution behavior that cannot be reconstructed.

A scope boundary alone is not a persistence trigger. Organizational scopes need
no durable information merely because they group work. Evaluate the requirements
a scope establishes, such as a timeout budget or retry policy, and preserve only
what recovery needs. That information can usually accompany another durable
event; establishing a requirement does not automatically require its own chapter.

The durable history should consist primarily of task outcomes, durable waits or
dispatches, and exceptional control-flow events. Scope entry, initial attempt,
normal result, successful “no retry” decision, and normal exit should not each
be persisted. Scope identity and attempt identity remain necessary, but do not
inherently require separate records.

This proposal replaces the earlier open-ended request to reduce scope lifecycle
overhead. Reducing five scope records to two is not the intended endpoint.

The expected first-attempt history for `story3.json` is **approximately 35–45
chapters under the local-execution assumptions below**, compared with a final
task-result ordinal of 1313 today. This is a design estimate, not a measured
implementation result or an exact count of the existing raw history.

## Accepted semantics

The purpose of the history is reliable replay and recovery:

- Reuse committed task outcomes when recovering the same logical attempt.
- Rerun a task if execution stopped before its outcome was committed.
- Reconstruct deterministic orchestration, including scope nesting, state
  transitions, expressions, and outputs derived from recorded task results.
- Manage local task invocation timeouts within each actual execution. A rerun
  after worker loss receives a fresh local invocation timer.
- Anchor block timeouts to preceding durable progress where possible. Preserve
  those block budgets across recovery; do not restart them for each task.
- Persist an actual timeout when it changes execution behavior.
- Offer explicit restart before or after tasks. Arbitrary structural scope
  locations do not need to remain selectable restart boundaries.

Task reruns can repeat external effects performed before a lost outcome commit;
this contract accepts that existing recovery model. It does not require durable
records of progress inside a local task.

A whole-block retry remains different from recovery or a task-boundary restart:
retrying a block can intentionally execute previously successful tasks again.

## Evidence from story3.json

The baseline is the first, failed job attempt in
`root.past_attempts[0]`, traversing its `children` recursively.

| Recipe scope kind | Occurrences |
| --- | ---: |
| Sequence | 119 |
| State visit | 72 |
| State machine | 18 |
| Op | 26 |
| **Total recipe scopes** | **235** |

The task-bearing nodes expose:

| Task category | Committed results |
| --- | ---: |
| Recipe-source resolution | 1 |
| Commands | 14 |
| Codex/extension execution | 9 |
| Input form generation | 2 |
| User-input collection | 2 |
| Squash/rebase/merge | 1 |
| **Total** | **29** |

There are 28 successful task results and one failed result. The failed merge
result has ordinal 1313. All exposed task attempts are numbered 1, and the
first-attempt tree exposes no populated `prior_attempts`. It also contains 62
transition-evaluation nodes; deterministic evaluations do not need chapters.

The inspected implementation writes `scope`, `attempt`, `result`, `decision`,
and `end` for an ordinary successful scope. Multiplying 235 scopes by five gives
approximately 1,175 scope records. This explains the scale, but is not an exact
accounting: some scopes are still open at the final task result, failures can
add records during unwinding, and a story projection is not the raw chapter log.
Ordinal 1313 is therefore a comparison point, not the exact completed-attempt
chapter count.

The complete export also includes a later, unfinished job attempt with nine
additional task results, reaching ordinal 1753. The estimates below apply to the
first attempt only; they do not describe the whole export.

## Execution model

### Read the next event before reconstructing its path

Replay should inspect the next durable event before choosing the path leading
to it:

```text
peek at next durable event
  -> read its reconstruction metadata
  -> reconstruct and validate the path to it
  -> consume the event
```

Task outcomes, pending waits/dispatches, timeout events, and terminal outcomes
can carry necessary reconstruction metadata: scope identities and policies,
retry attempts, catch selections, timeout anchors, and relevant transitions since
the previous event. Record or reference the information needed to reproduce the
committed path, including structural segments already exited before that event.
This is not a requirement to serialize every scope boundary or the full scope
stack: omit structural details that replay can derive and that establish no
additional recovery requirement.

A committed path is authoritative. If no event committed a choice, recovery may
choose again within the constraints of the retained prefix and its budgets.
There is no requirement to checkpoint each choice before running a local task;
its eventual outcome can commit the choice and result together. External
handoffs still require durable publication before transferring responsibility.

For example, a failure selects a repair handler, task A commits, and task B is
interrupted. A's record can carry `catch=repair`. Replay reads that selection
before entering the handler, reuses A, and resumes B even if the catch predicate
has changed. No separate catch chapter is required. If nothing committed the
selection, choosing a handler again is allowed.

Following a recorded choice and validating its declaration are separate duties.
Use attached descriptors or execution-definition references to reject incompatible
policies/contracts; do not rely on a task name or producer version string alone.

### Reconstruct scopes in memory

Keep `DoScope`, or its equivalent, for policy inheritance, identity, cancellation,
and retry/catch handling. Frames retain occurrence and attempt identities and
block-timeout anchors. Ordinary entry and exit write nothing.

```text
A commits at T -> exit two scopes -> enter four scopes -> B
```

Replay reconstructs those transitions and their context using the workflow and
next event's metadata. Derived scope outputs are recomputed. Any value or choice
needed to reproduce committed behavior that cannot be reconstructed belongs in
the next durable object's metadata. External effects remain task work.

### One outcome for an ordinary local task

An ordinary local outcome carries invocation/attempt identity, operation contract,
reconstruction metadata, required input snapshot, output/error, artifact references,
and completion time. No separate start, attempt, successful decision, or end
chapter is required.

Committed outcomes bypass input construction and execution. Task retries reuse
an established input snapshot; recovery may rebuild it only when none survived.
An execution without a committed outcome may rerun, unless a committed enclosing
timeout or other control event already settled its path. Read-only replay never
runs missing work.

External waits and dispatches retain explicit durable inputs and completion
associations. Metadata size may depend on structural complexity, but ordinary
scope traversal must not add appends or duplicate unchanged application outputs.

### Timeouts

Local invocation timers live within the actual execution. An interrupted,
uncommitted execution may rerun with a fresh timer, still bounded by enclosing
block deadlines. A committed timeout is replayed as an outcome.

A timed block uses the preceding durable completion as its start anchor; the
initial block uses durable job-attempt start metadata. Several structural scope
changes can share one anchor. It remains fixed for that block: later task
completions do not extend it. Inner blocks may use newer anchors. Attempt budgets
reset on intentional retries; total block budgets do not.

Worker downtime counts against anchored block budgets. After waits/backoff, use
specified recorded wake/resume information. Persist a separate anchor only when
the required start cannot be recovered from existing durable information.

Check live deadlines at each execution step, including scope exit and job
completion after the last task. During historical replay, consume committed
outcomes and timeout events without reevaluating them against today's clock.

An observed timeout is durable: associate it with a task outcome when appropriate,
or write a direct timeout event between tasks. Include the affected block/attempt
identity so replay unwinds correctly without executing interrupted work. Ordinary
propagation through ancestors needs no additional chapter.

### Retries and catches

Carry committed handler selections, retry progression, retryability, rewritten
failures, and dispositions in the next durable event when needed for recovery.
They remain authoritative on replay. A normal successful “no retry” decision
requires no record.

Use a separate exceptional event when there is no suitable task/outcome to carry
the information before a durable action, such as scheduling backoff. Absolute
wake times and persistent retry budgets must survive recovery, including retries
with no intervening task. No blanket pre-task catch/decision write is required.

Recovery reuses outcomes in the same enclosing attempt. A block retry creates
new child invocation identities and can intentionally repeat successful tasks.
These identities can be carried on events without scope-attempt chapters.

### Competing executions and write conflicts

Chapter slots are immutable and append order is monotonic. Competing valid
outcomes for the same logical invocation/attempt and slot may race; the loser
must read and adopt the committed winner. The candidate task outputs need not
be identical for subsequent replay to be deterministic.

Bind each append to its expected history position and invocation/attempt. After
a timeout settles attempt 1, its late success cannot move to the next free slot
or borrow attempt 2's writer position. A conflict requires reconciliation, not
incrementing the ordinal and retrying the stale event. Preserve cancellation,
lease/generation checks, and lost-acknowledgement handling; none requires ordinary
scope lifecycle chapters.

## Restart at task boundaries

Identify the target by logical task invocation, attempt, and boundary side:

- **Before B:** retain the prefix and reconstruction context leading to B, then
  execute B again. If that context was stored with B's outcome, retain it in
  restart metadata while discarding the outcome.
- **After B:** retain its committed outcome too, then reconstruct the path to
  the next task or terminal outcome. An unfinished task has no “after” boundary.

This reconstructs intervening scopes without making them selectable restart
targets. Distinguish repeated sites and attempts. Restart before B does not
implicitly retry its enclosing block. Record any explicitly supported input or
budget overrides once in restart metadata.

C2J can reconstruct the hierarchy for inspection while offering task boundaries
for restart. New histories no longer require every scope's `OnRestart` handler
or an ordinal identifying its entry.

## Estimated chapter count for the example

Count logical durable chapters, independently of database transactions or API
batching. The proposed accounting is:

```text
chapters = committed task outcomes
         + necessary wait/dispatch records
         + exceptional control/anchor records
         + job/attempt envelope records
```

No term is proportional to ordinary scope traversal.

### Expected local-execution case: approximately 35–45

Assume the two user-input collections require durable waits, and the remaining
27 task invocations execute locally. The story establishes the task categories
and results, but does not prove the local-versus-remote dispatch of every task.

| Contribution | Estimated chapters | Basis |
| --- | ---: | --- |
| Task outcomes | 29 | Includes the failed merge outcome |
| User-input wait registrations | 2 | One pending wait per collection; their results are already above |
| Ordinary recipe scope traversal | 0 | All 235 scopes are reconstructed |
| Deterministic transition evaluations | 0 | All 62 evaluations are reconstructed |
| Job/attempt envelope and activation bookkeeping | 4–8 | Budget for submission/header, attempt anchoring, terminal outcome, and resumes; exact packing remains open |
| Additional exceptional control records | 0–6 | Planning allowance, not observed raw events |
| **Total** | **35–45** | **Conditional design estimate** |

The final failure does not inherently need another record per enclosing scope.
If its propagation and handling are deterministic, the failed task outcome and
job-attempt outcome suffice. The exceptional allowance accommodates decisions
such as a whole-job retry/backoff at the attempt boundary; the story alone cannot
establish their exact record count.

Against approximately 1,313 chapter positions, 35–45 represents roughly a
**97% reduction**, or around **29–38 times fewer chapters**. Scope count is not
being used to inflate that target: additional ordinary nesting adds no records.

### Sensitivity and intermediate implementation

If all 27 other invocations also require one durable pending-intent record,
add 27: approximately **62–72 chapters**, before any additional routing,
activation, alternate-selection, or scheduling events. This is a sensitivity
calculation, not an upper bound on remote execution. More generally, add the
actual protocol records and activations for each handoff.

If an initial implementation removes scope records but retains today's roughly
five-record task lifecycle, the 29 tasks alone contribute about 145 chapters.
With input waits, envelopes, and some failure bookkeeping, **roughly 150–165
chapters** is a plausible intermediate estimate under the same dispatch
assumptions. It is not the endpoint of this proposal.

An exact forecast requires raw chapter classification by kind, owner, attempt,
and dispatch path. The exported story does not expose all activations, waits,
retry decisions, or low-level scheduling events. The chapter reductions above
are not claims of equivalent latency or byte reductions.

## Reliability and compatibility

The intended change is where recovery information lives, not whether JobDB owns
reliable replay. Preserve the behavioral guarantees established by `0345804`
and the subsequent timeout, artifact, wait, handoff, and replay fixes. Existing
[scenario coverage](docs/TESTING-EXECUTION-SCOPES.md) remains the regression basis;
update record-layout assertions without dropping their behavioral guarantees.

Introduce a new execution contract/version. The intentional semantic changes are
fresh local invocation timers after uncommitted interruption and inferred block
anchors. Public APIs remain unchanged; task boundaries and existing declared
scope restart IDs are supported through metadata. Pre-cutover jobs will be
deleted; versions 1 and 2 are rejected before consumer execution.

Implementation must preserve policy/contract validation, immutable retry inputs,
failed diagnostics, artifact references, pending-completion identity, and terminal
completion recovery. Audit task-visible ordinals and artifact allocation when
removing start/attempt records. Consumer version strings alone do not establish
compatibility with the interpreter that originally produced the history.

Exact historical scope entry/exit timing is no longer promised. Distinguish
inferred scope timing from recorded task timing. Optional telemetry must not
become required recovery history. JobDB owns these protocol rules; C2J should
not implement chapter-elision heuristics.

## Validation and acceptance

Use a fixture matching the first story attempt with local tasks and two input
waits; exercise remote dispatch separately. Measure chapters by kind, encoded
bytes, append requests/transactions, execution overhead, and replay time/memory.

Required checks:

1. Additional ordinary scope traversal adds zero chapters at fixed task work.
2. Event metadata reconstructs committed paths, including changed catch
   predicates and incompatible-policy rejection; uncommitted choices may be
   reevaluated. Terminal metadata covers the path after the last task.
3. Committed results bypass builders/workers; retries preserve input snapshots;
   lost acknowledgements are reconciled; read-only replay never executes work.
4. Local timers restart as specified, block budgets survive recovery, and timeout
   events unwind the correct boundary without executing interrupted work. Check
   live expiry at scope exit and job completion as well as task admission.
5. Competing writers adopt the committed winner. A late result from a settled
   attempt cannot append at another slot or write through a newer attempt.
6. Retry/catch handling, empty-block retries, and durable backoff survive crashes;
   whole-block retries use distinct child invocation identities.
7. Before/after restart preserves the required path metadata across loops,
   attempts, and the last task. External waits, artifacts, and scheduler handoffs
   retain their existing recovery guarantees.
8. Pre-cutover histories and unsupported contracts fail before new work.
   Fault-inject around each new persistence/scheduling boundary.

Every chapter above the 29-result baseline should identify a recovery or
coordination requirement. The estimate is not a cap on necessary exceptional
records. Ordinary successful scope traversal is not such a requirement.

## Implementation result

The synthetic story3-shaped fixture produces **38 physical chapters** on toy,
SQLite, and HTTP/SQLite, with the same count for flat, organizational, and
restartable scope variants. It uses 29 task results and two durable input waits,
with the other 27 tasks local. See the
[migration notes](docs/MIGRATION-COMPACT-EXECUTION-HISTORY.md) for cutover, API,
inspection, and timing details. This is not a rerun of the original story.
