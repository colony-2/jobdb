# Lease-bound secrets and fresh resolution

Providers receive `jobdb.SecretResolveRequest.Lease`, a verified
`jobdb.SecretLeaseContext` containing:

- `LeaseID` and `WorkerID`: identity of the exact lease attempt.
- `LeaseExpiresAt`: the authoritative backend lease expiry at validation.
- `AuthorizedNotAfter`: the earlier of backend expiry and the presenting
  capability's expiry. Embedded handles and remote tokens use the same rule.

The request contains no bearer token. Application metadata still comes from the
server's job record and may contain submitter-controlled data. Providers retain
responsibility for authorizing each name and for encoding the appropriate
identity and expiry into the credential itself.

For a dynamic credential, return `SecretValue{Value: credential, LeaseBound: true,
ExpiresAt: &expiry}`. Choose an expiry at or before `AuthorizedNotAfter`, including
any clock margin and verifier allowance your credential format requires. A zero
authorized deadline cannot authorize lease-bound issuance. `ctx.Deadline()` is
the lookup timeout, not the credential lifetime.

The broker rejects the entire result if a lease-bound value lacks an expiry,
has expired, or exceeds either the initial or revalidated authority deadline.
It revalidates after provider work without holding a transaction or renewal lock
across that work. Renewing the backend does not revive an expired older handle
or token. Use the handle returned by `Renew`; workflow runners do this for you.

`LeaseBound` defaults to false. Unmarked values preserve existing behavior:
`ExpiresAt` is optional but enforced when present. A static password can therefore
be returned alongside a lease-bound token without acquiring the token's lifetime.
Provider caches remain supported, provided returned credentials are valid and
authorized for the current request.

## Workflow reads

`JobContext`, `ExecutionContext`, and `TaskContext` expose both methods:

```go
cached, err := execution.ResolveSecrets(ctx, []string{"git_token"})
fresh, err := execution.ResolveSecretsFresh(ctx, []string{"git_token"})
```

Ordinary reads keep their existing cache behavior. An expired cached value fails;
it is not automatically fetched again. Fresh reads always consult the provider,
even for cached values or omissions. A provider may return the same still-valid
credential.

After validation, fresh reads replace only the requested entries in the current
job attempt's cache. Omitted names remove any old value and become cached
omissions. Failures do not change the cache. Values returned to callers are
independent copies. Tasks share the enclosing job attempt's cache; whole-job
retries and new executions receive new caches.

Fresh reads use the runner's latest capability, never acquire work or renew the
lease, and do not block heartbeats. Saved task contexts expire with their task;
all access stops on execution closure or lease loss. Neither method records
secret results in history. Do not put values into task outputs, artifacts, errors,
or logs. Offline replay rejects secret reads.

## Bootstrap access

Inside `GetJobForRunRequest.BeforeRun`, obtain a narrow resolver from its context:

```go
BeforeRun: func(ctx context.Context, _ jobdb.ExecutionLease) error {
    resolver, ok := workflow.SecretResolverFromContext(ctx)
    if !ok {
        return jobdb.ErrExecutionLeaseLost
    }
    secrets, err := resolver.ResolveSecretsFresh(ctx, []string{"git_token"})
    if err != nil {
        return err
    }
    // Pass secrets to application bootstrap without serializing or logging them.
    return bootstrap(ctx, secrets)
},
```

The hook uses the first live attempt's cache. A retained resolver follows the
current attempt after a whole-job retry, while each attempt retains a separate
cache. Calls already in flight stay bound to the attempt they started in. The
resolver exposes only cached and fresh secret reads; it cannot choose another
job, acquire work, or renew authority. It becomes unusable when its originating
context or the execution ends.

The HTTP endpoint is unchanged. Results add the optional `leaseBound` flag;
`no-store`, lease authentication, and sanitized errors remain in effect.
