# Execution timeouts are opt-in

`jobdb.DefaultRunPolicy()` and `workflow.DefaultRunPolicy()` no longer add the
10-minute invocation timeout or 30-minute total timeout. Their retry defaults
are unchanged. An empty `RunPolicy` already had no execution time limit.

Consumers should pass through only the timeouts declared by their recipe,
operation, or other execution configuration:

```go
policy := jobdb.DefaultRunPolicy() // Retry defaults; no execution deadline.
if declaredAttemptTimeout != nil {
    policy.InvocationTimeout = jobdb.AsDuration(*declaredAttemptTimeout)
}
if declaredTotalTimeout != nil {
    policy.TotalTimeout = jobdb.AsDuration(*declaredTotalTimeout)
}
```

Explicit job, task, and scope limits remain enforced, including inherited parent
budgets, external waits, retries, and replay. Declaring one limit does not add the
other. Cancellation and lease loss still stop execution. Lease durations, polling
intervals, wait recycling, and transport/provider request limits are independent
of job execution deadlines.

Update the JobDB dependency in submitting applications and rebuild them to use
the new defaults. Already-submitted jobs retain their recorded policies and
deadlines: storage cannot distinguish a previous default from an intentionally
declared limit with the same value. This change does not rewrite existing jobs.
