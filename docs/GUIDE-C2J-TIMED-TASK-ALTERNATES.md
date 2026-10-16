# Timed task alternates for c2j

`workflow.ExecutionWorker` supports one logical task invocation that waits for
implementation A, then becomes eligible for implementation B. JobDB owns the
durable due time, scheduler handoff, leases, retries, and replay.

## Declare the alternate

Emit a single stable `TaskCall` from the recipe node:

```go
answer, err := exec.DoTask(workflow.TaskCall{
    Site:            "approval",
    TaskType:        "c2j:human_answer",
    ContractVersion: 1,
    Command:         json.RawMessage(`{"node":"approval","recipe_revision":"r1"}`),
    Alternate: &workflow.TaskAlternateAfter{
        TaskType: "c2j:automatic_answer",
        After:    5 * time.Minute,
    },
    Policy: jobdb.RunPolicy{
        TotalTimeout: jobdb.AsDuration(15 * time.Minute),
        Retry: jobdb.RetryPolicy{MaximumAttempts: 3},
    },
}, func(ctx context.Context) (jobdb.TaskData, error) {
    return buildAnswerInput(ctx)
})
```

Both implementations must accept the same input and return the same output
contract. `After` is a nonnegative duration; zero means eligible immediately
after entering the wait. The alternate type must be nonempty and differ from A.
Omit `Alternate` for ordinary task calls.

Keep `Site`, `TaskType`, `ContractVersion`, `Command`, policy, alternate type,
and delay stable during recovery and replay. Do not calculate a new delay from
the current clock or change `TaskType` to B in c2j. JobDB matches the declaration
before calling the input builder.

## Register workers and supply external answers

Register the recipe `ExecutionWorker` and an ordinary `TaskWorker` for B on the
fallback pool. Register other task workers that pool should execute as usual:

```go
engine, err := workflow.NewEngineBuilder().
    WithRuntime(runtime).
    WithWorkerTenantId(tenant).
    PlusWorkersWithOptions(recipeWorker, workflow.WorkRegistrationOptions{
        ConsumerVersions: map[string]string{
            "c2j": c2jVersion,
            "pulse": pulseVersion,
        },
    }, automaticAnswerWorker).BuildEngine()
```

`automaticAnswerWorker.Name()` returns `"c2j:automatic_answer"`; its existing
`Run(TaskContext, TaskData)` method returns the answer or an error. Use
`TaskContext.Context()` for cancellation. JobDB records its result and resumes
the recipe. Direct execution through `GetJobForRun` and supplied leases through
`GetJobForRunWithLease` use the same behavior.

For an external human answer, leave A unregistered on that pool. Use the existing
`GetWaitingTask` / `FindTasksWaitingForRoute` APIs to inspect the pending task,
then `TaskHandle.Finish(ctx, answer)` to answer it. A separately registered A
worker can also acquire and execute the wait. If A is registered in the process
when the invocation first runs, JobDB executes it locally immediately; no wait
or alternate timer is needed.

## Scheduling and retry rules

| Situation | Behavior |
| --- | --- |
| Enter external wait | Persist input and one absolute alternate due time; recovery reuses both. |
| Before due | B cannot acquire, even if registered. |
| Due reached | Existing alternate-route scheduling makes B eligible at acquisition. No application timer is required. |
| A external answer arrives after due, before B acquires | It may still complete the task. The due time is eligibility, not an answer cutoff. |
| A holds a valid lease when due passes | Its lease remains valid; B cannot preempt it. |
| A's lease expires | The original alternate due time still applies; B may acquire if due. |
| B acquires | The scheduler commits route B. An old A answer cannot overwrite it. |
| B fails or loses its lease | Retry/recovery stays on B and uses the same input. It does not restart the A wait. |
| Task retries | JobDB owns the attempts and reuses the recorded input. |
| Enclosing scope retries | A new invocation in the new scope attempt starts normally with A. |

An A failure before B selection follows normal task retry behavior. A locally
available A may run again. If it returns to external waiting, the original due
time is retained. Alternate eligibility itself does not consume a retry attempt.

Existing cancellation and hard task/scope deadlines still apply. An acquired B
must pass deadline admission before its callback executes. An external completion
may still win after the deadline if the waiting slot remains unclaimed. Once a
worker claims timeout/fallback processing, or the job is cancelled or finalized,
that answer is rejected. A recorded successful task result remains replayable
after those times pass; accepting it does not extend enclosing scope budgets.

Scheduling uses the existing single alternate-route slot: it schedules whichever
comes first, B eligibility or the hard deadline. If the deadline comes first,
the job route wakes to process the timeout. If B eligibility comes first, a
worker serving B must pick up the job, even if the deadline has since expired;
there is no second timer service that subsequently changes that pending route.
An eligible route requires a polling worker to process it. Serving job inspection
or accepting HTTP requests alone does not finalize a timeout. Pending-task listings
retain the stored waiting route until a claim; the scheduler evaluates alternate
eligibility when a worker polls or requests a lease.

## Recovery, replay, and rollout

The wait records the input and due time. Before a leased worker executes, a
`dispatch` history record captures the selected implementation. A crash before
that record is handled using the scheduler's committed lease route. A crash
after it reuses the recorded selection and input. As with other leased tasks,
uncommitted execution can repeat; external effects should be idempotent.

Read-only replay consumes recorded A or B outcomes without executing either
implementation, rebuilding inputs, or scheduling work. An unfinished invocation
returns a replay cache miss. Changing the alternate declaration produces a
determinism error, just like changing the task command.

New jobs use execution-pattern version **2** for the new wait fields and dispatch
records. This client can replay and continue version 1 histories under version 1
semantics, but cannot add alternates to them. Upgrade JobDB servers and all
workers serving these jobs before enabling alternates; old clients reject
version 2. Consumer version strings remain provenance, not routing or automatic
compatibility adapters.

## Changes in c2j

1. Compile the node's primary type, fallback type, and relative delay into one
   `TaskCall`. Pin the declaration with the recipe contract.
2. Keep input construction in the deferred builder and give both implementations
   the same input/output schema.
3. Register the fallback `TaskWorker` on the appropriate pool. Choose explicitly
   whether A should also be available locally.
4. Remove consumer timers, route-switch checkpoints, synthetic timeout failures,
   and fallback task/retry loops for this node. Keep normal output-driven recipe
   branching after `DoTask` returns.
5. Exercise human completion, fallback pickup, worker restart, and read-only
   recipe replay in c2j's compiler/runtime tests.

See [execution migration](MIGRATION-EXECUTION-SCOPES.md) for scopes and version
registration, and [scenario coverage](TESTING-EXECUTION-SCOPES.md) for JobDB tests.
