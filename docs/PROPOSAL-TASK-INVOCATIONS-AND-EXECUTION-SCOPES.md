# Task invocations and durable execution scopes

Status: JobDB implementation available through `workflow.ExecutionWorker` during
consumer migration. API sketches below describe the design; see the
[migration guide](MIGRATION-EXECUTION-SCOPES.md) for concrete APIs and the
[scenario coverage](TESTING-EXECUTION-SCOPES.md) for validation. Consumer cutover
and removal of the legacy path follow successful migration.
Based on the [replay failure audit](REPLAY-FAILURE-PATTERNS-AUDIT.md).

Transition assumption: discard all pre-cutover jobs and histories. Keep the old
implementation only while developing and validating the replacement. Once c2j
switches, remove the old execution path; no legacy-job compatibility is required.

## Execution versions

Record three distinct version fields in execution metadata:

| Field | Meaning |
| --- | --- |
| `ExecutionPatternVersion` | JobDB-owned version of invocation matching, scope lifecycle, retry/catch, handoff, and restart semantics. Start the new contract at version 1. |
| `JobDBVersion` | Release/build identifier of the JobDB client executing the workflow. |
| `ConsumerVersions` | Named, opaque version strings supplied by higher layers, for example `c2j: X` and `pulse: Y`. |

The execution-pattern version is independent of the storage/wire schema version,
JobDB release number, and a task's `ContractVersion`. A compatible implementation
upgrade preserves the execution-pattern version. Changes to its specified
semantics require a new version and explicit support for reading and continuing
earlier supported histories.

JobDB selects and persists the execution-pattern version before the first workflow
callback runs. It remains pinned for that job across retries, suspension, and
recovery. A worker must support both replay and continued execution under that
version; otherwise fail with an unsupported-execution-version error before running
consumer code or advancing history. Missing versions in the new format are invalid.
Do not silently substitute the worker's current default.

### Consumer registration API

Extend the existing worker-registration options rather than adding a method to
every job worker. Higher layers compose the map when constructing the worker set:

```go
builder.PlusWorkersWithOptions(recipeWorker, WorkRegistrationOptions{
    ConsumerVersions: map[string]string{
        "c2j":   c2jVersion,
        "pulse": pulseVersion,
    },
}, taskWorkers...)
```

Copy and validate the map at registration; keys and values must be nonempty.
An omitted map means no consumer versions were declared. Provide the same option
for direct/supplied-lease execution paths so bypassing the worker loop does not
lose version information. JobDB fills its own version fields automatically.

Consumer versions describe the software executing orchestration. JobDB does not
infer compatibility from their strings or require equality after an upgrade.
Consumers retain responsibility for their versioned logical-command contracts;
recording release versions does not itself guarantee replay compatibility.

### Persistence and inspection

Persist the initial version metadata before the first workflow callback. On each
live worker activation, record its actual JobDB and consumer versions before it
can contribute new history, associating subsequent records with that activation.
Recovery under a newer binary therefore preserves the original producer versions
and records the newer versions instead of overwriting them. Worker activation is
provenance, not a new task/scope retry or a change to cache lifetimes. Read-only
replay reads this metadata without appending an activation.

Expose the pinned contract and producer versions through job/history inspection
and read-only execution context metadata. Compatibility errors include recorded
and current version information. Transport and storage must preserve arbitrary
consumer names without hardcoding c2j or pulse.

A restart retaining history inherits its execution-pattern version and prefix
producer metadata. Record the restarting worker's versions for the new suffix;
do not relabel inherited records. Unsupported source contracts are rejected.
Changing execution-pattern versions requires a separate, explicit migration
contract; a context patch cannot perform that migration.

## Input persistence clarification

Input construction belongs to the **logical task invocation**, not to each
execution attempt. One `DoTask` call builds its input once and JobDB reuses that
input across its internal retries. Those attempts remain hidden from consumer
orchestration.

JobDB retains an immutable input snapshot in memory and records it with the first
committed outcome, including a failure; later attempts may reference that snapshot.
Recovery consumes a recorded failure and reuses its input for the next attempt.
Workers receive fresh views/copies so mutation cannot change a later attempt.

If the process stops before any input-bearing outcome or handoff is durable,
recovery may derive fresh input. This is the limited exception agreed earlier:
there is no surviving input snapshot to reuse. No mandatory pre-execution input
write is needed for local tasks. If input construction itself fails, a retry may
run construction again because no usable input has been established.

Before repeating an attempt after an uncertain write, JobDB must establish
whether its outcome committed. A missing acknowledgement is not evidence that
the outcome is absent. External effects before outcome commit retain the usual
retry/idempotency requirements.

External handoff has a separate requirement: a waiting worker must have an
explicit input or durable input reference. It cannot depend on the previous
chapter happening to contain the right data. Keep that input fixed for the
active handoff. Retry or replacement of that handoff for the same invocation
reuses the established input. Replacing a handoff invalidates its old completion
token. Work requiring different input is a new logical invocation, not a hidden
task retry.

| State | Input behavior |
| --- | --- |
| Outcome committed | Consume outcome; do not rebuild input or run the worker. |
| Local retry with input retained in memory | Reuse input; do not run input construction again. |
| Recovery with no durable input snapshot or outcome | May rebuild input and execute. |
| Recorded failure followed by a live retry | Reuse that invocation's recorded input. |
| Active external handoff | Serve the input explicitly associated with that handoff. |
| Handoff replaced for the same invocation | Reuse input; reject completion of the old handoff. |
| Read-only replay reaches missing outcome | Return a replay miss; do not build input or execute. |

## 1. Task invocation

Separate the logical command from the execution input. JobDB matches recorded
work before invoking consumer code that constructs the input.

A call declares a stable call site, task type, operation contract version, and
logical command. The logical command describes the requested work independently
of the current Go request struct. For c2j this can identify the pinned recipe
node, its declared arguments, and the upstream outcome references it uses.
JobDB treats this as opaque versioned data; c2j owns its meaning and encoding.

JobDB allocates occurrences and attempts within the enclosing scope. A repeated
call site in a loop has distinct occurrences. Physical chapter ordinals are
storage coordinates, not invocation identities.

The runner:

1. Matches the next logical invocation, including task type, operation contract,
   and logical command. Rejects unrelated work even if execution inputs happen
   to have identical bytes. Declared task alternates require an explicit match
   rule.
2. Consumes recorded attempts and returns their outcomes or applies the recorded
   retry progression. Input construction and worker execution do not run for
   those attempts.
3. At unfinished live work, checks applicable deadlines and reuses the invocation's
   input snapshot. Constructs input only if none survives. Construction is inside
   the attempt's lifetime; its failure follows the same retry policy. Execution
   retries reuse successfully constructed input without re-entering orchestration.
4. Commits the outcome and only then advances the history cursor. Reconciles an
   uncertain commit before allowing repetition.

The execution-input hash remains useful for integrity and external completion
guards. Replay does not compare it with freshly constructed execution input.
Changes to the logical command still fail matching; reusing a call-site name is
not sufficient to claim old work.

Input construction returns ordinary `TaskData`. It does not schedule tasks or
decide recipe branches. Consumers retain their own input/configuration resolution
logic. Task outputs continue to drive application state updates and branching
on both live execution and replay.

Local execution needs no separate input-persistence write. Copy the input before
dispatch and commit that snapshot, including durable artifact references, with
the first outcome. This is required retry state rather than optional diagnostic
input capture. Resolved secret values and transient worker resources do not
belong in that snapshot. Existing job-attempt config/secrets caching semantics
remain unchanged; references in an input are distinct from resources resolved
inside worker execution.

### External tasks

Publish a waiting invocation with an explicit input snapshot or immutable
reference and a handoff generation. Make that input durable before the wait
becomes externally available. Completion validates the invocation, generation,
and input identity. `TaskHandle.Data()` reads the associated input directly.

Deadline markers, unrelated outcomes, and newly inserted framework records
cannot change which input the waiting worker sees. After interrupted publication,
JobDB reconciles the wait and its input before exposing or replacing it.

### API sketch

```go
type TaskCall struct {
    Site            string
    TaskType        string
    ContractVersion uint32
    Command         Command // Opaque, versioned logical command.
    Policy          RunPolicy
}

// Proposed final API. The existing API may coexist temporarily during development.
DoTask(call TaskCall,
    buildInput func(context.Context) (TaskData, error)) (TaskData, error)

out, err := scope.DoTask(TaskCall{
    Site:            "compile",
    TaskType:        "command_execution:command_execution",
    ContractVersion: 1,
    Command:         compileCommand, // Built from the stable application contract.
    Policy:          policy,
}, func(ctx context.Context) (TaskData, error) {
    return buildCompileInput(ctx) // Called only when no invocation input survives.
})
```

c2j switches to this orchestration API at cutover. The existing interface is
retained only during development and validation, then removed. Input construction
alone does not require changing `TaskWorker.Run`. Worker setup may still run on each execution attempt
to recreate transient resources; it receives the same task input. The separately
proposed preparation lifecycle is distinct from constructing that input.

## 2. Durable execution scope

A scope runs a block of orchestration under a JobDB-managed timeout and retry
policy. The block can contain tasks, nested scopes, and output-driven branches.
The same primitive represents a c2j operation, sequence, state, or recipe; JobDB
does not need to understand those recipe constructs.

c2j calls a scope once. JobDB owns its attempts, catch matching and handling, retry
eligibility, backoff, deadlines, cancellation, recovery, and final outcome. c2j
supplies the block and policy, not a retry or catch loop. Task execution and scope
execution use the same policy semantics and history lifecycle.

### Budgets and nested behavior

| Policy | Meaning |
| --- | --- |
| `TotalTimeout` | Bounds the whole scope occurrence, including attempts, backoff, and suspension. Never resets on a retry. |
| `AttemptTimeout` | Bounds one scope attempt, including its nested work and suspension. Resets only when JobDB starts a new attempt. |
| `Retry` | Controls attempts of this block, including maximum attempts and backoff. |
| Enclosing deadline | Always bounds nested work; a child cannot extend it. |

Recovery preserves the recorded deadlines and retry decisions. Lease renewal,
worker replacement, and suspension do not grant new budgets or consume retry
attempts by themselves. Expiry of an attempt may allow another attempt within
its remaining total budget; expiry of the scope total budget ends that scope.

Child policies govern child failures first. If a child ultimately fails, its
failure reaches the enclosing scope, whose catch/retry policy governs that block.
Retry policies are explicit per boundary; a parent's retry count is not silently
copied onto every child. Nested retry counts can multiply executions, so inspection
must expose which scope initiated each attempt.

### Recovery versus retry of the whole block

These are different operations:

```text
scope attempt 1: A succeeds, B is interrupted
    worker recovery -> consume A's outcome; resume B in attempt 1

scope attempt 1: A succeeds, B fails after its task retries are exhausted
    scope retry     -> start scope attempt 2; run A and B as new invocations
```

A whole-block retry intentionally repeats work in that block. It does not undo
previous external effects. The new enclosing attempt gives its child invocations
new identities. Their inputs may be newly derived, including from new upstream
outputs. Retries of one child invocation still reuse that child's established
input, as specified above.

JobDB allocates scope occurrences, scope attempts, and child identities. Consumers
do not use physical chapter ordinals or maintain retry counters. A loop visiting
the same site again is a new occurrence; recovery of that occurrence is not.

### Scope state and persistence

Record scope entry and policy, each attempt's start and deadline, and each attempt
outcome with its retry/terminal decision and backoff wake time. Reconcile uncertain
writes before advancing or executing again. Resume a recorded backoff using its
original wake time rather than sleeping for the full interval again.

The callback runs during replay to reconstruct application state from recorded
child results. Recorded tasks, attempt outcomes, and retry decisions are consumed
without executing those tasks again or applying today's clock to completed work.
Read-only replay stops at missing history without running live work.

Before admitting new work, check the earliest applicable deadline. That deadline
also cancels active work, input construction, and waits. A recorded scope timeout
can terminate an interrupted callback even when its next anticipated child has
no outcome; do not fabricate a child result. If scope completion never committed,
recovery may time out that scope even if its last child completed. Keep that
child's recorded result intact.

Each callback invocation uses fresh attempt-local application state. The callback
returns its result instead of publishing partially mutated state into the caller.
The consumer reconstructs that local state from its entry data and child results;
JobDB does not serialize arbitrary Go memory. Expired callbacks and their contexts
cannot publish results or mutate a later attempt. c2j must not share mutable recipe
state between old and new callback invocations.

Scope records are framework control records, distinct from task outcomes. They
do not become task inputs or require consumer registration of checkpoint workers.
Scope attempts do not introduce config/secrets cache lifetimes: caches retain the
existing live job-execution-attempt boundary.

### API sketch

```go
out, err := job.DoScope("develop", ScopeOptions{
    TotalTimeout:   10 * time.Minute,
    AttemptTimeout: 3 * time.Minute,
    Retry: RetryPolicy{
        MaximumAttempts: 3,
        InitialInterval: 2 * time.Second,
    },
    Catch: compiledCatchRules, // Ordered rules and handlers supplied by c2j.
}, func(scope JobContext) (JobData, error) {
    // Fresh local recipe state for this callback invocation.
    // May call DoTask or nest DoScope; no retry loop or timer here.
    return executeDevelopment(scope)
})
```

The worker receives a standard cancellable execution context, for example
`TaskContext.Context()`. This replaces c2j's `AwaitDuration`-based deadline polling.
Subprocess/tool adapters still need to honor that context.

### Scope failure handling

Catch belongs to the same JobDB primitive as timeout and retry. c2j compiles its
recipe catch clauses into ordered, versioned rules and handlers. JobDB owns
matching, handler invocation, recording the result, retrying, and propagation.
It does not interpret recipe YAML, CEL expressions, or state names. Where a rule
needs application-specific evaluation, the consumer supplies a pure predicate
against the recorded failure and declared application context.

At each boundary, the order is:

```text
body fails -> evaluate this boundary's catch rules in order
           -> run the selected handler, if any
           -> apply its disposition
           -> retry unhandled failure if policy and budget allow
           -> otherwise propagate to the enclosing boundary
```

A child exhausts its own handling/retry policy before its failure reaches its
parent. c2j must assign a recipe's catch and retry declarations to the correct
scope; do not accidentally add an inner retry loop ahead of a catch that used to
run first. Task-level catches use the same ordering before hidden task retries.

| Handler disposition | Engine behavior |
| --- | --- |
| Handled value | Complete this boundary with that value. Do not retry its body. |
| Continue failure | Apply this boundary's retry policy to the original or replacement failure. This is the default when no rule matches. |
| Propagate | Exit this boundary without a local retry and pass the failure outward. |

A recipe state transition can be carried in a handled application value. c2j
interprets that value in its surrounding state machine; JobDB need not know the
destination's meaning. Failure rewriting uses the continue-failure disposition.

Record catch selection before executing a handler, and record the resulting
disposition before retrying or propagating. Replay consumes those records rather
than re-evaluating current predicates. If a handler is interrupted, resume its
recorded task history under a stable handler identity. A handler may call normal
JobDB tasks/scopes; predicates cannot perform external effects. This keeps
recovery work inside the framework's normal durability rules.

Handlers receive a fresh execution context after the failed body is canceled.
They remain bounded by this scope's total deadline and all enclosing deadlines.
An attempt timeout may leave budget for a handler; an expired total budget does
not. In that case propagate the timeout outward, where an enclosing live scope
may handle it. A handler failure propagates outward without recursively invoking
the same catch rules. Handler retries require an explicit nested scope policy.

Lease loss, replay misses, and history incompatibility are engine control
conditions, not catchable application failures. Caller cancellation stops live
work. Recorded application timeouts can be matched as failures, but handling one
cannot authorize work through an expired scope or extend its deadline.

The exact rule, handler, and disposition types remain API specification work.
Migration tests must cover rule ordering, failure rewriting, handled outputs,
routed outputs, and catch-before-retry behavior.

## Context-patch restarts

Support intentional restarts at declared scope-entry boundaries. A restart creates
a new job with a retained history prefix, a logical target scope occurrence, and
an opaque, versioned restart payload. The source job remains unchanged. Target
identity includes enclosing scope occurrences; a site name alone is insufficient.
Physical chapter offsets are not the public restart contract.

The restart follows this sequence:

```text
replay retained outcomes
-> reach selected scope entry
-> deliver recorded restart payload and apply it to reconstructed context
-> execute fresh suffix
```

JobDB owns prefix selection, durable payload storage, boundary matching, and
delivery before the target scope body. c2j owns payload validation and deterministic
patch application to fresh, reconstructed recipe state. Apply the patch before
resolving affected variables, choosing branches, constructing logical commands,
or preparing task inputs. If a decision precedes the selected scope entry, the
caller must select an earlier boundary that encloses that decision.

Patching a visible prior output changes the restarted job's context projection,
not the recorded output. Patch application performs no external effects. Recovery
reconstructs the retained context and applies the recorded patch again at the same
boundary; it does not reapply the patch to already-mutated shared state. Read-only
replay applies this same reconstruction and stops when it reaches missing history.

The suffix contains new logical invocations and may derive new inputs. Automatic
task retries still reuse their established input; ordinary worker recovery does
not become an intentional restart. Retained completed work is consumed without
executing it again. External effects of discarded suffix work are not undone.

An intentional restart gives the target and its enclosing scopes fresh timeout
budgets and retry allowances for the new continuation. JobDB records the new scope
lifetimes when the continuation starts; subsequent recovery preserves them.
Retained history is replayed as history, without enforcing its old deadlines on
the new continuation. Nested work remains bounded by the new enclosing deadlines.

### API sketch

```go
RestartJob(RestartOptions{
    Source:  sourceJob,
    At:      scopeOccurrence, // A declared entry boundary from history inspection.
    Payload: versionedContextPatch,
})

// Additional DoScope option. Runs at the targeted entry before its body.
OnRestart: func(payload RestartData) error {
    return recipeState.ApplyContextPatch(payload) // Pure, validated application.
}
```

c2j enters the scope before making decisions affected by its patch. The hook
operates on that callback invocation's reconstructed state. Reject invalid targets,
unsupported payloads, or a payload targeting a scope with no restart handler.
Do not support cuts inside task execution, input construction, or retry chains.
Remove fake task outputs, intentional input mismatches, and mismatch recovery loops
from the restart path.

## What would change

| Area | Change |
| --- | --- |
| JobDB task API | Replace eager input with a logical call descriptor and deferred construction. Retain the old API only temporarily while validating the replacement. |
| Execution versions | Pin the JobDB execution-pattern version; register named consumer versions through worker options and persist producer versions per live activation. Expose them in inspection and diagnostics. |
| Replay matching | Validate logical invocation, task type, contract, and command before consuming history. Stop requiring reconstruction of execution bytes. |
| Local persistence | Record an immutable input snapshot with the first input-bearing outcome, including failure, and reuse it across task retries. No mandatory pre-execution input write. |
| External handoff | Associate explicit input and generation with the wait. Extend runtime/transport records and completion guards. Remove the preceding-chapter assumption. |
| Scope lifecycle | Add nested timeout/catch/retry execution, attempt and handling-decision records, durable backoff, and fencing of expired contexts. |
| Restart API/history | Add logical scope-entry targets, durable restart payloads, prefix provenance, and explicit continuation budgets. Expose eligible targets through history inspection. |
| History inspection | Expose scope relationships and invocation identity independently of physical ordinals. Preserve original input snapshots where captured. |
| c2j task dispatch | Supply stable logical commands; construct input once through the deferred callback. JobDB owns hidden task retries. Continue applying returned outputs to recipe state. |
| c2j waiting inputs | Remove the adapter that searches through timeout checkpoints to find a prepared form after the new handoff contract is in use. |
| c2j timeouts/retries | Replace timeout checkpoints, context wrappers, deadline polling, and operation/sequence/state retry loops with `DoScope`/`DoTask` policies. |
| c2j catch handling | Compile declarations into JobDB rules/handlers. Remove runtime catch loops; JobDB matches, executes, records, retries, and propagates. c2j interprets application routing values. |
| c2j attempt state | Initialize recipe state inside each scope callback and return its result; do not share mutable state across attempts. |
| c2j context-patch restart | Apply validated patches through the scope-entry hook before affected decisions. Remove synthetic task outputs and deliberate hash mismatches. |
| c2j compatibility code | Remove artifact-order hash recovery and other legacy history adapters at cutover. Old histories are discarded. |

## Transition and removal of the old path

This is a coordinated cutover with fresh jobs, not a mixed-version production
migration. There is no requirement to resume, inspect, copy, or convert old jobs.

1. Implement the new JobDB mechanisms alongside the current code while c2j still
   uses the existing API. Use a temporary separate entry point or registration
   surface so the current Go interface need not change mid-development.
2. Develop and test c2j's replacement path against fresh, isolated job histories.
   Reuse recipe scenarios to compare behavior, including catch ordering, timeout
   handling, retries, and context-patch restarts. Do not execute production side
   effects twice for a comparison.
3. Update server codecs, schema validators, external handoff, history inspection,
   restart submission/delivery, execution-version metadata, and client APIs
   together. The new protocol may be breaking; preserving the old record format
   or transport contract is not a requirement for this cutover.
4. Stop old submissions and workers, discard pre-cutover jobs, and deploy the new
   JobDB/c2j combination against fresh job state. Include scheduled submissions
   and external completion callers in the coordinated switch.
5. Once c2j has switched successfully, remove the old runner/API, c2j retry/catch
   and timeout machinery, mismatch-based restart handling, legacy history adapters,
   and temporary rollout switches.
   Keep one supported execution path rather than a permanent legacy/v2 split.

No job-format converter, old compiler interpreter, legacy reader, per-job legacy
routing, or old-history support matrix is needed. Shared storage/lease/artifact
utilities can remain where useful; the old workflow protocol need not constrain
new records.

Rollback before cutover simply leaves the existing deployment in use. After
cutover, any rollback to the old implementation requires another fresh-job reset;
new histories are not readable or resumable by the old engine. Document this in
the migration guide alongside the coordinated deployment steps and API changes.

The new protocol still needs explicit versions and a precise replay contract.
Discarding existing jobs removes this migration's compatibility burden, not the
requirement for future upgrades to resume jobs created under the new contract.
Start an immutable fixture corpus with the first new-format release and test
future supported versions against it.

## Acceptance tests

- Register c2j and pulse versions together and round-trip them through local and
  remote execution, persistence, and inspection. Cover direct/supplied-lease runs,
  omitted versions, invalid entries, and mutation of the caller's registration map.
- Recover with newer compatible JobDB/consumer releases: preserve the pinned
  execution-pattern version and original producer metadata, record the new live
  activation's versions, and associate new history with that activation.
- Reject a missing or unsupported execution-pattern version before consumer code
  runs or history advances. Read-only replay must not append version metadata.
- Restart with a retained prefix: inherit its execution-pattern version and
  producer metadata, record new suffix producers, and reject unsupported source
  contracts. A context patch cannot alter execution-version metadata.
- Replay a committed task after its request struct changes: input construction
  and execution do not run; the recorded outcome is returned.
- Change task type or logical command at the same site: fail before consuming
  history, even when execution input bytes would match.
- Stop the process before any input-bearing outcome or handoff commits, change
  the input source, and resume: fresh input is allowed. If an outcome committed
  but its acknowledgement was lost, consume it without rebuilding input.
- Retry after a recorded failure, both in-process and after recovery: retain the
  failure and reuse the same input even when the input source changes.
- Fail input construction, then retry: construction runs again. Once it succeeds,
  subsequent execution retries do not call it again.
- Insert a framework scope record before an external wait: the handler still
  receives the explicit task input. Reject a response to a replaced handoff.
- Resume after a scope deadline: consume its completed prefix; admit no new
  work inside it. Replay a completed inner scope and continue in its live parent.
- Recover midway through a scope: completed children do not execute again. Retry
  the whole scope after a committed failure: its children are new invocations.
- Interrupt during retry backoff: resume the recorded wake time and attempt count.
  An inner retry cannot reset outer deadlines or silently inherit outer retries.
- Preserve ordered catch matching, failure rewriting, and handled/routed values.
  Recorded catch selections and decisions survive predicate implementation changes.
- Interrupt a handler after a durable child task: recovery reuses that result.
  Handler failure cannot recursively select the same handler. A handler cannot
  outlive its enclosing total deadline or swallow engine control conditions.
- Time out an attempt whose worker ignores cancellation: its late calls/results
  cannot mutate history, application state published to the caller, or a later attempt.
- Restart at a declared scope entry using real c2j and JobDB runtimes: retained
  tasks do not execute, the source history remains unchanged, and the patch affects
  variables, branching, logical commands, and inputs only from that boundary onward.
- Interrupt a patched restart before and after its first new outcome, then recover
  and replay: reconstruct the same patched context, reuse committed suffix outcomes,
  and preserve the continuation's recorded timeout budgets and retry progression.
- Restart within nested scopes whose source deadlines have expired: use the new
  enclosing budgets and enforce them on fresh work. Reject ambiguous or invalid
  targets, unsupported patches, and cuts inside task attempts or retry chains.
- Run the same recovery cases through local and remote runtimes using the new
  format. Preserve fixtures from its first release for future upgrade tests.
- Verify the coordinated cutover starts only new jobs with new workers and updated
  completion clients. Unsupported formats fail explicitly rather than falling
  back to legacy execution.
- After migration, run c2j scenarios with the old API and adapters removed so no
  hidden dependency on the old implementation remains.
