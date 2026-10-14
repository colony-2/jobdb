# Migrating to versioned execution scopes

The new JobDB client engine is selected by `workflow.ExecutionWorker`. Register it
with the existing engine builder or supply it to `GetJobForRun` /
`GetJobForRunWithLease`. Existing workers remain available during consumer
migration; the two engines do not interpret each other's histories.

Deploy the server and client changes together. The REST chapter union and storage
protobuf now include execution control records. Use fresh jobs at the coordinated
cutover. Do not submit old histories to the new engine. Remove the legacy path
once c2j and other consumers have switched; this implementation does not delete
existing jobs or change a running deployment.

After cutover, rolling back to the old engine requires another fresh-job reset;
it cannot continue new execution histories. If a tenant has a chapter schema,
update it to admit the `execution` chapter body (`kind` and `record`) in addition
to job-start and job-outcome chapters before submitting new jobs.

## Registration and versions

```go
worker := workflow.ExecutionWorker{
    JobType: "recipe",
    Execute: func(ctx workflow.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
        return executeRecipe(ctx, in)
    },
}

engine, err := workflow.NewEngineBuilder().
    WithRuntime(runtime).
    WithWorkerTenantId(tenant).
    PlusWorkersWithOptions(worker, workflow.WorkRegistrationOptions{
        ConsumerVersions: map[string]string{
            "c2j": c2jVersion,
            "pulse": pulseVersion,
        },
    }, taskWorkers...).BuildEngine()
```

Direct and supplied-lease callers set `GetJobForRunRequest.ConsumerVersions`.
Registration copies the map and rejects empty names or values. JobDB fills its
own release/build identifier and execution-pattern version automatically.

Execution-pattern version 2 is pinned before application orchestration starts
for new jobs. Version 1 histories can still replay and continue under their
original version; timed task alternates require version 2.
A worker rejects missing/unsupported versions without invoking the workflow or
advancing its history. Consumer release strings are provenance, not compatibility
constraints. Keep logical call contracts stable across compatible releases.

`ExecutionContext.ExecutionVersions()` returns the original execution versions.
`GetJobRunResponse.Execution` exposes ordered control records, including the
initial versions and each contributing worker activation's versions. Resuming
with a newer worker adds provenance without relabeling retained history. Read-only
replay does not append activation records.

## Task calls

Replace eager `DoTask(policy, taskType, input)` calls with:

```go
out, err := ctx.DoTask(workflow.TaskCall{
    Site:            "compile",
    TaskType:        "command_execution:command_execution",
    ContractVersion: 1,
    Command:         json.RawMessage(`{"recipe_node":"compile","recipe_revision":"r1"}`),
    Policy:          policy,
}, func(ctx context.Context) (jobdb.TaskData, error) {
    return buildExecutionInput(ctx)
})
```

The command describes stable logical work. Include pinned source identity,
arguments, and upstream outcome references as appropriate for the consumer.
Do not copy the whole mutable execution-input struct into `Command`.

The engine matches site, occurrence, task type, contract, command, and policy
before input construction. Recorded outcomes bypass construction and execution.
Successful construction supplies an immutable input snapshot for internal task
retries. The first input-bearing outcome, including failure, persists that
snapshot and artifact references. A builder failure can retry construction. If
no snapshot survived an interruption before the first outcome, reconstruction is
allowed. Completed work never reconstructs inputs during read-only replay.

`TaskWorker.Run` keeps its existing signature. Use `TaskContext.Context()` for
cancellation in workers, subprocesses, and tool adapters. A timeout fences late
JobDB calls/results; it cannot roll back external effects or forcibly stop Go
code that ignores cancellation. Keep external operations idempotent.

For external waits that become eligible for another implementation after a
delay, set `TaskCall.Alternate`. See the [c2j timed-alternate guide](GUIDE-C2J-TIMED-TASK-ALTERNATES.md)
for registration, late-answer semantics, retries, and replay.

## Artifact references and task identity

Execution v2 preserves names of newly returned output artifacts. A task can
include its artifact reference directly in its JSON:

```go
func (task) Run(ctx workflow.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
    content := []byte("data")
    reference := jobdb.ArtifactKey{
        JobId:       ctx.JobKey.JobId,
        TaskOrdinal: ctx.Step,
        Name:        "result.txt",
        SizeBytes:   int64(len(content)),
    }
    return jobdb.NewTaskData(map[string]any{"reference": reference},
        jobdb.NewArtifactFromBytes("result.txt", content))
}
```

The reference becomes durable when the outcome commits. Input snapshot artifacts
use separate storage names, including when an input and output have the same
logical name. Consumers must not construct those internal input names; obtain
the persisted artifact's `ArtifactKey()` when forwarding an existing reference.

`TaskContext.Step` identifies the outcome for the current call to `Run`, including
in-process waits and child-job submissions. If a wait or lease action suspends
the task, return its error. Recovery calls `Run` with a new context whose `Step`
may differ; construct new output references from that context. Do not use `Step`
as an idempotency key or publish uncommitted artifact references as durable data.

Versioned workers also receive:

| Field | Contract |
| --- | --- |
| `InvocationID` | Opaque logical task ID within `JobKey`. Stable across task retries, suspension, and lease recovery; changes for new invocations in enclosing-scope or whole-job retries. |
| `AttemptNumber` | One-based task attempt. Increments on task retry; unchanged when unfinished work resumes. |

For child-job submission and external side effects, use tenant/job identity plus
`InvocationID` as the idempotency key. Include `AttemptNumber` only when the
effect should repeat for a new task attempt. A restart into a new job is
distinguished by its `JobKey`. Legacy `JobWorker.Run` contexts leave these new
fields empty/zero. Observer `InvocationID` retains its existing meaning as the
task **attempt** ID.

## Failed-task diagnostics

A versioned task may return both `TaskData` and an error. Execution v2 stores the
returned JSON and artifacts in `ExecutionRecord.Output` alongside the error;
the error still determines failure, catch selection, and retry behavior.
`DoTask` and replay observers expose both. Each failed attempt retains its own
diagnostics even if a later retry succeeds. Returning no data leaves the failure
output nil. Temporary output files are cleaned after snapshotting, including on
failure. Consumers remain responsible for sanitizing the diagnostics they return.

## Scopes, catches, and retries

Replace operation/sequence/state timeout wrappers, checkpoint tasks, retry loops,
and catch dispatch loops with scopes:

```go
out, err := ctx.DoScope("develop", workflow.ScopeOptions{
    TotalTimeout:   10 * time.Minute,
    AttemptTimeout: 3 * time.Minute,
    Retry: jobdb.RetryPolicy{MaximumAttempts: 3},
    Catch: []workflow.CatchRule{{
        ID: "recover-build",
        Match: func(err error) bool { return matchesBuildFailure(err) },
        Handle: func(ctx workflow.ExecutionContext, err error) (workflow.CatchResult, error) {
            result, err := runRecovery(ctx, err) // May call tasks or scopes.
            if err != nil { return workflow.CatchResult{}, err }
            return workflow.CatchResult{
                Disposition: workflow.CatchHandled,
                Output: result,
            }, nil
        },
    }},
}, func(scope workflow.ExecutionContext) (jobdb.JobData, error) {
    state := cloneEntryState(entry) // Fresh mutable state for this attempt.
    return executeDevelopment(scope, state)
})
```

JobDB processes the first matching catch before local retry policy. Rule IDs and
ordering are part of the recorded policy; predicates are pure and are not rerun
for recorded selections. Handlers use ordinary durable tasks/scopes. Return
`CatchHandled`, `CatchContinue`, or `CatchPropagate`. Continue may replace the
failure before retry; propagate skips local retry. Handler failures propagate
outward without recursively selecting the same handler.

Attempt deadlines survive suspension/recovery. A whole-scope retry grants a new
attempt deadline and reruns its children as new invocations. Total deadlines
include attempts, backoff, handlers, and suspension. Nested scopes cannot extend
parent deadlines. Timeout handlers get a fresh context bounded by remaining total
and enclosing budgets.

Calls on a scope are sequential. Use nested scopes for structured orchestration;
concurrent calls on one execution context are rejected. Keep mutable state local
to each callback invocation and return results to the caller. An abandoned callback
must not retain shared mutable state used by another attempt.

Use `ExecutionContext.AwaitDuration` and `AwaitJobs` for orchestration waits.
Child outcomes and retry wake times are recorded. Task-level waits can suspend an
unfinished worker; that worker may run again until its outcome is durable.
Configuration and secrets retain the live whole-job-attempt cache lifetime;
nested scope/task retries do not reset these caches. Whole-job retries do.

## External work and restart patches

An unregistered task publishes a durable explicit input snapshot before exposing
its external wait. `TaskHandle.Data()` reads that snapshot. Completion binds to
the exact wait generation and input identity. Expired and replaced waits reject
stale completions. Remove consumers' preceding-chapter searches.

Select restart targets from `GetJobRunResponse.Execution` records with kind
`scope`. Submit their logical `ID`:

```go
handle, err := runtime.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
    Job: jobdb.SubmitRestartJob{
        PriorJobKey: source,
        ExecutionRestart: &jobdb.ExecutionRestart{
            ScopeID: targetScopeID,
            Payload: json.RawMessage(`{"author":"updated"}`),
        },
    },
})
```

A patch target declares `ScopeOptions.OnRestart`. The hook validates and applies
the payload before the scope body makes affected decisions or constructs commands.
Rebuild patched entry data from immutable entry values; the body then takes its
own attempt-local copy. The hook runs during state reconstruction on recovery and
replay and must have no external effects.

Restarts retain the prefix and its provenance, preserve the source job, and create
fresh suffix work. Continuation budgets for enclosing scopes are recorded before
admitting fresh work; later recovery preserves them. Patches cannot change engine
versions. Remove fake task outputs and intentional hash-mismatch handling.

## Consumer cutover checklist

- Convert task dispatch to stable commands and deferred construction.
- Map each recipe policy boundary to a scope with ordered catch rules.
- Remove timeout checkpoints, deadline polling, retry/catch loops, ordinal repair,
  and artifact-order hash fallbacks.
- Apply patches at declared scope entries before resolving affected decisions.
- Update history readers to use execution records and logical invocation IDs.
  Replay task events also expose `InvocationID`; physical ordinals remain storage
  coordinates. Versioned task observer events describe committed outcomes.
- Run consumer recipe scenarios against fresh local and remote jobs, including
  interruption during task, scope, catch handler, backoff, and patched continuation.
- Coordinate workers, submissions, servers, schedules, and completion clients at
  cutover. After the consumer switch, remove the old API and temporary coexistence.
