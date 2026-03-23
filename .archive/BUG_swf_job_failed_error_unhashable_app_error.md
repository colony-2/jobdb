# Bug: `JobFailedError` / `GetJobRun(...).GetOutput(...)` path still panics with `hash of unhashable type swf.AppError`

## Summary

On current `swf-go` HEAD `96cd1785f854fa05c99b8ffe14c044bd2fe15cc4` (`v0.0.0-20260318201836-96cd1785f854`), a parent workflow that resolves a failed child through `GetJobRun(...).GetOutput(...)` still fails with:

```text
panic: runtime error: hash of unhashable type swf.AppError
```

instead of surfacing the child failure.

This reproduces on both:

- `runtime/toy`
- `runtime/direct`

So this appears to be a new cross-runtime regression in the shared error/output path, not a toy-only projection issue.

## Regression Window

- prior tested build: `c4ab8012193c373c3f10923d6976c43aed36d3a9`
  - direct runtime did **not** show this panic on the same parent/child failure shape
- regressing build: `7b7e2139c3a60990529ed6931e7f08743508e157`
  - both toy and direct now show the panic above
- current tested HEAD: `96cd1785f854fa05c99b8ffe14c044bd2fe15cc4`
  - both toy and direct still show the same panic above

## Version Retested

- `github.com/colony-2/swf-go v0.0.0-20260318201836-96cd1785f854`
- `HEAD = 96cd1785f854fa05c99b8ffe14c044bd2fe15cc4`

## Exact Shape

The failing shape is:

1. parent starts a child workflow internally
2. parent waits via `AwaitJobs(...)`
3. after the wait, parent resolves the child through:
   - `engine.GetJobRun(GetJobRunRequest{IncludeOutputs: true, IncludeArtifacts: true})`
   - `run.GetOutput(engine, tenantID)`
4. child fails with an ordinary application failure
5. parent returns or propagates that child error

This is the same public API shape that motivated the recent `JobFailedError` change.

## Observed Behavior

The child failure itself is real and logged first, for example:

```text
error executing step: command execution failed: exit status 1
```

or:

```text
error executing step: command execution failed: exit status 127
```

But the parent then fails with:

```text
panic: runtime error: hash of unhashable type swf.AppError
```

The caller sees a terminal error derived from that panic, for example:

```text
job failed: panic: runtime error: hash of unhashable type swf.AppError
```

instead of the original child failure.

## Expected Behavior

When the child fails:

- `GetJobRun(...).GetOutput(...)` should return a stable job-failed error shape
- the parent should surface the child failure
- no panic should occur
- runtime choice (`toy` vs `direct`) should not change this behavior

## Minimal Public-API Reproducer Shape

Pseudocode:

```go
child := func(ctx swf.JobContext, in swf.JobData) (swf.JobData, error) {
    return nil, swf.AppError{
        Payload: swf.AppErrorPayload{
            Message: "child failed",
            Level:   "error",
        },
    }
}

parent := func(ctx swf.JobContext, in swf.JobData) (swf.JobData, error) {
    childKey, err := engine.StartJob(ctx2, swf.StartJob{...})
    if err != nil {
        return nil, err
    }

    if err := ctx.AwaitJobs(childKey.JobId); err != nil {
        return nil, err
    }

    run, err := engine.GetJobRun(ctx2, swf.GetJobRunRequest{
        JobKey:           childKey,
        IncludeOutputs:   true,
        IncludeArtifacts: true,
    })
    if err != nil {
        return nil, err
    }

    _, err = run.GetOutput(engine, childKey.TenantId)
    if err != nil {
        return nil, err
    }
    return nil, nil
}
```

Then wait for the parent job to finish.

Observed on `96cd178...`: the parent path still panics with `hash of unhashable type swf.AppError`.

## Retest Notes

Retested on current HEAD with the same narrow shape on both built-in runtimes:

- `runtime/toy`
- `runtime/direct`

Both behave the same way:

- child failure is logged first
- parent enters the `GetJobRun(...).GetOutput(...)` path
- parent then fails with `panic: runtime error: hash of unhashable type swf.AppError`
- caller sees the panic-derived terminal error instead of the child failure

## Likely Upstream Area

This appears closely related to the new stable failed-job error path introduced in:

- `pkg/swf/job_failed_error.go`
- `pkg/swf/job_run_output.go`
- `pkg/swf/worker_envelope.go`
- `pkg/swf/worker_runtime_support.go`

Inference from the regression shape:

- the new `JobFailedError` wrapping changed the concrete error value that flows through the parent path
- that new value now reaches some code path that requires a hashable/comparable value
- the panic text specifically points at `swf.AppError`

I have not isolated the exact line yet, but this does not reproduce on the previous tested build and started when the new `JobFailedError` behavior was introduced. It still reproduces on current HEAD.

## Impact

- breaks parent/child failure propagation through the new recommended `GetJobRun(...).GetOutput(...)` path
- affects both built-in runtimes
- replaces real child failures with a panic-derived terminal error
