# Bug: parent workflow can fail to settle after child failure, with `unexpected chapter type "JobAttemptOutcome"` during replay/determinism handling

## Summary

On current `swf-go` HEAD as of 2026-03-17 (`c4ab8012193c373c3f10923d6976c43aed36d3a9`, module version `v0.0.0-20260317212331-c4ab8012193c`), a parent workflow that starts a child job and waits for its result can fail to settle correctly when the child fails.

The child failure itself is recorded, but the parent path can then:

- hang until `swf.WaitForJobToComplete(...)` times out, and/or
- emit:

```text
workflow was not deterministic: unexpected chapter type "JobAttemptOutcome" at ordinal 3
```

Instead of returning the child failure to the caller, the parent job remains non-terminal long enough to time out.

## Version

- `github.com/colony-2/swf-go v0.0.0-20260317212331-c4ab8012193c`
- `HEAD = c4ab8012193c373c3f10923d6976c43aed36d3a9`

## Runtime

- `runtime/toy`
- retested comparison: corresponding direct-runtime fixture passes on the same `swf-go` build

## Retest Result On Current HEAD

Retested on `v0.0.0-20260317212331-c4ab8012193c`:

- the separate toy-runtime artifact/idempotent-start bug is fixed on this build
- this parent/child failure path still reproduces on `runtime/toy`
- the analogous direct-runtime fixture passes on the same build

Observed failing toy cases:

- parent run-and-get child exit `1`
- state-machine child failure exit `127`
- state-machine child failure exit `1`

## Additional Data Captured On Current HEAD

I extracted `GetJobRun(...)`, replay errors, and stored chapter sequences for the failing toy-runtime cases.

The most important missing detail appears to be **retries**:

- the child is **not** started with an explicit `StartJob.JobID`
- after the first child failure, the parent job records a `JobAttemptOutcome`
- the parent is then retried and re-enters the child-start path, creating a **second child job with a new generated JobID**
- the failed child job is also retried under the default run policy, producing an interleaved chapter stream with repeated `TaskAttemptOutcome` / `JobAttemptOutcome` pairs

So the problematic shape is not just "parent awaits child, then resolves via `GetJobRun(...).GetOutput(...)`". It is:

1. parent starts child
2. child fails
3. parent `finish` step fails while surfacing that child failure
4. parent job attempt is recorded as failed
5. parent job retries
6. parent re-enters child start and launches a second child

That retry-driven shape is what still reproduces on current `runtime/toy`.

## Exact Shape

This is narrower than "parent waits on child, then parent calls `engine.GetJobResult(child)`".

The failing consumer shape is:

1. The child is started inside the parent workflow.
2. The parent waits via `AwaitJobs(...)`.
3. After the wait returns, the parent resolves the child result through:
   - `engine.GetJobRun(GetJobRunRequest{IncludeOutputs: true, IncludeArtifacts: true})`
   - `run.GetOutput(engine, tenantID)`
4. The parent then propagates that result or error upward.

So the key distinction is:

- wait path: `AwaitJobs(...)`
- result path: `GetJobRun(...).GetOutput(...)`
- not direct `engine.GetJobResult(childKey)`
- both parent and child are running with the default retry policy, so the failure occurs across multiple job attempts

Also confirmed from the captured start calls:

- child starts do **not** use an explicit `JobID`
- each parent retry launches a fresh child with a new generated job ID

## Scenario

The failing shape is:

1. Parent workflow starts a child job internally.
2. Parent waits for the child via `AwaitJobs(...)`.
3. Child fails with a normal runtime error.
4. Parent resolves the child result through `GetJobRun(...).GetOutput(...)`.
5. Parent should fail and surface the child error.

Observed instead:

- the child error is logged correctly
- the parent does not settle correctly
- replay/determinism handling reports `unexpected chapter type "JobAttemptOutcome"`
- waiting for the parent job times out

## Minimal Reproducer Shape

Any workflow with parent-child failure propagation should be enough. Pseudocode:

```go
parent := func(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
    childKey, err := startChildJob(...)
    if err != nil {
        return nil, err
    }

    // Parent waits for child completion.
    if err := ctx.AwaitJobs(childKey.JobId); err != nil {
        return nil, err
    }

    // Parent then resolves the child output via GetJobRun(...).GetOutput(...),
    // rather than calling engine.GetJobResult(childKey) directly.
    result, err := lookupChildOutputThroughGetJobRun(childKey)
    if err != nil {
        return nil, err
    }
    return result, nil
}
```

Where the child deterministically fails, for example with an application error or command failure.

From the caller side, the failure shows up as:

```go
jobKey, err := engine.StartJob(...)
if err != nil {
    panic(err)
}

err = swf.WaitForJobToComplete(ctx, 30*time.Second, jobKey, engine)
// Repro: this times out instead of returning the parent's terminal failure.
```

## Observed Behavior

The child failure is real and visible first, for example:

```text
error executing step: command execution failed: exit status 1
```

or:

```text
error executing step: command execution failed: exit status 127
```

Then the parent flow reports:

```text
workflow was not deterministic: unexpected chapter type "JobAttemptOutcome" at ordinal 3
```

and the caller eventually gets a timeout such as:

```text
job <tenant>/<job-id> did not complete within the specified timeout of 30s
```

Post-hoc replay on the captured stored runs also shows the chapter ordering issue directly:

- parent replay fails with `workflow was not deterministic: unexpected chapter type "TaskAttemptOutcome" at ordinal 4`
- each failed child replay fails with `workflow was not deterministic: unexpected chapter type "TaskAttemptOutcome" at ordinal 3`

Those replay errors appear after the runtime has already appended extra retry chapters beyond the first live execution failure, which is why the live worker logs can first show the earlier `unexpected chapter type "JobAttemptOutcome" at ordinal 3`.

## Captured Chapter Shape

Representative parent stored chapters from the minimal failing shape:

1. `ordinal 0`: `JobStart` for parent recipe
2. `ordinal 1`: `TaskAttemptOutcome` for `recipe.run_and_get_result:start` returning first child job id
3. `ordinal 2`: `TaskAttemptOutcome` for `recipe.run_and_get_result:finish` with `AppError`
4. `ordinal 3`: `JobAttemptOutcome` for the parent job with the same propagated child failure
5. `ordinal 4`: `TaskAttemptOutcome` for `recipe.run_and_get_result:start` again, now returning a **second** child job id

Representative child stored chapters from the same repro:

1. `ordinal 0`: `JobStart`
2. `ordinal 1`: `TaskAttemptOutcome` for the failing child task
3. `ordinal 2`: `JobAttemptOutcome` for child attempt 1
4. `ordinal 3`: `TaskAttemptOutcome` for the same failing child task again
5. `ordinal 4`: `JobAttemptOutcome` for child attempt 2
6. `ordinal 5`: `TaskAttemptOutcome` again
7. `ordinal 6`: `JobAttemptOutcome` for child attempt 3

That is the chapter layout currently seen on `runtime/toy` when this reproduces.

## Expected Behavior

When the child job fails:

- the parent job should reach a terminal failed state
- the parent should surface the child failure to the caller
- replay/determinism handling should not reject the chapter stream with `unexpected chapter type "JobAttemptOutcome"`

## Why This Looks Upstream

- The child failure itself is correct.
- The regression happens afterward, in parent completion / replay / chapter handling.
- The most specific new signal is the determinism mismatch around `JobAttemptOutcome`, which suggests an upstream change in expected chapter layout or replay semantics.
- The issue reproduces on current `swf-go` HEAD, not only on an older pinned commit.

This is not a deprecated-API migration issue; it reproduces while using the public engine/runtime APIs.

## Upstream Areas Likely Involved

- `pkg/swf/worker_runner.go`
- replay logic that consumes chapter streams
- handling of `JobAttemptOutcome` in parent/child result propagation
- retry / attempt handling after a child-surfaced `AppError`

## Impact

- Parent-child failure propagation becomes unreliable.
- Consumers can see timeouts where they should see a terminal parent failure.
- Replay/determinism handling appears inconsistent with the recorded child-failure path.
