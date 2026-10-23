# Using delegated leases in c2j and other clients

A delegated lease replaces the owner lease in the child process. It uses the same
Go `jobdb.ExecutionLease` interface, lease-token HTTP header and exported
`LeaseCapability` format. The owner continues refreshing its main lease; the
child obtains fresh tokens on use, directly from JobDB. Child refresh never
extends the main lease.

This guide describes the implemented JobDB API. c2j must update its JobDB
version and integrate these calls in its supplied-lease/operation path; this
repository does not define c2j command-line flags for delegation.

## Owner process: create and hand off

The owner already holds a remote execution lease. Create a child with exactly
the permissions its operation requires:

```go
func exportForOperation(ctx context.Context, runtime *remote.Runtime,
    owner jobdb.ExecutionLease, handoff io.Writer) error {
    child, err := runtime.CreateDelegation(ctx, owner, jobdb.CreateDelegationRequest{
        TTLSeconds: 30,
        Permissions: jobdb.DelegationPermissions{
            Secrets: []string{"model_api_key"},
            Read: &jobdb.DelegationChapters{Ordinals: []int64{0, 1}},
            Write: &jobdb.DelegationWritePermission{
                Ordinals: []int64{2},
                TaskTypes: jobdb.DelegationNames{Names: []string{"summarize"}},
                ArtifactNames: jobdb.DelegationNames{Names: []string{"summary.json"}},
            },
            Renew: &jobdb.DelegationRenewPermission{MaxTTLSeconds: 30},
        },
    })
    if err != nil { return err }
    capability, err := remote.ExportLease(child)
    if err != nil { return err }
    encoded, err := capability.Encode()
    if err != nil { return err }
    _, err = handoff.Write(encoded)
    return err
}
```

Use a private pipe or inherited file descriptor for `handoff`. Do not log the
encoded capability or put it in command-line arguments. `String`, `GoString` and
ordinary JSON marshaling redact `LeaseCapability`; `Encode` deliberately exports
its bearer token. Configure the server URL separately from the credential.

For a submission-only child, set `Submit` to a
`jobdb.DelegationSubmitPermission` containing exact `JobTypes` and `JobIDs`
selectors. `JobIDs.Any` allows generated IDs; a finite list requires explicit
IDs. Leave the other permissions absent. Add `Renew` only when required.

For a bounded nonrenewable child, set `NotAfter` and choose a suitable access
TTL (maximum 300 seconds). A hard deadline always disables renewal, even if
`Renew` is present. Omitting both `NotAfter` and `Renew` also creates a
nonrenewable child. Parent expiry may shorten every child's initial validity.

## Child process: use the existing lease path

Receive only the child capability and the configured service URL:

```go
func importForOperation(ctx context.Context, baseURL string,
    encoded []byte) (*remote.Runtime, jobdb.ExecutionLease, error) {
    runtime, err := remote.New(baseURL, &http.Client{Timeout: 30 * time.Second})
    if err != nil { return nil, nil, err }
    capability, err := remote.DecodeLeaseCapability(encoded)
    if err != nil { return nil, nil, err }
    lease, err := runtime.ImportLease(ctx, capability)
    if err != nil { return nil, nil, err }
    return runtime, lease, nil
}
```

Pass the returned lease into the operation's existing lease-dependent code:

```go
values, err := lease.ResolveSecrets(ctx, []string{"model_api_key"})
// Or lease.SubmitJob(ctx, request), when jobs.submit was granted.
```

An unexpired child imports without renewal. An expired renewable child refreshes
on import using its own token. `SubmitJob`, `ResolveSecrets` and the runtime's
lease-authorized chapter operations use the same lazy refresh mechanism. There
is no child heartbeat to start and no owner callback to configure.

`KeepAlive`/`Renew` are still supported methods, but fail unless the child has
renewal permission and no deadline. A generic worker's periodic lease-renewal
loop should therefore be disabled for delegated operations; lazy refresh handles
the required token maintenance, including nonrenewable leases. The optional
`interface { Delegation() *jobdb.DelegationInfo }` method identifies a restricted
remote lease and exposes a copy of its policy without its bearer token.

The delegated snapshot includes identity, route, expiry and permissions. Its
`ClientPayload` and `ExecutionState` are empty: importing a lease must not leak
ungranted job state. Pass the operation's intended input through its normal input
channel, or read explicitly authorized chapters. A restricted operation is not
an unrestricted workflow dispatcher; acquisition, rescheduling and completion
remain with the owner.

## Chapter reads and writes

Use the lease-scoped read path by setting both `LeaseID` and `LeaseToken` on
`ChapterRef` or `ListChaptersRequest`. Ordinary unscoped read methods are not an
authority fallback for delegated work.

```go
token := lease.(interface{ LeaseToken() string }).LeaseToken()
chapter, err := runtime.GetChapter(ctx, jobdb.ChapterRef{
    JobKey: lease.Job().JobKey,
    Ordinal: 1,
    LeaseID: lease.LeaseID(),
    LeaseToken: token,
})
chapters, err := runtime.ListChapters(ctx, jobdb.ListChaptersRequest{
    JobKey: lease.Job().JobKey,
    StartOrdinal: 0,
    LeaseID: lease.LeaseID(),
    LeaseToken: token,
})
```

Lists omit disallowed ordinals. Single reads outside the allowlist return a
permission error, even if the chapter does not exist. Bodies, metadata and
artifact descriptors are included; downloading artifact contents is not granted.

For writes use the existing `runtime.PutChapter` with the child `LeaseID` and
`LeaseToken`. The chapter ordinal and task type must match the policy. Only task
outcome bodies are accepted. Upload new artifact content through
`ArtifactUploads`, with allowed names; copying existing artifact references is
not supported. Existing schema and conflict checks still apply.

The remote client buffers upload bodies as in the existing REST implementation.
Delegated write/submission authorization buffers at most 64 MiB of request data.

## Implementing another HTTP client

Use the delegated token in `X-JobDB-Lease-Token` on the existing lease-scoped
operations. On **exactly** `401` with `code: delegation_access_token_expired`:

1. Serialize renewal against concurrent users of that cached token.
2. `POST` to the same lease's `/keepalive`, using that child token, even though its
   access expiry has passed. There is no renewal request body or parent token.
3. Read `leaseToken` and `lease.expiresAt` from the response; replace the cache.
4. Rebuild the original request body and retry once with the new token.

The server guarantees that this specific expiry response precedes dispatch.
Do not use arbitrary 401/403 responses, timeouts, connection failures or mutation
conflicts as reasons to replay an operation. Retain normal idempotency and
ambiguous-result handling. Renewal failure must not trigger acquisition of a new
job lease, a lookup of the owner's token, or a retry with broader credentials.

A renewable token remains renewal authority after access expiry. Refresh does
not revoke old token copies. Protect both current and old serialized copies
until the parent lease is lost. Do not follow redirects with the token.

Errors are described in the [spec](PROPOSAL-Restricted-Lease-Capabilities.md).
Permission failures are `403 lease_permission_denied`; loss of the parent is
`409 execution_lease_lost`. A hard-deadline failure is terminal. The Go client
provides `jobdb.ErrLeasePermissionDenied` and `jobdb.ErrExecutionLeaseLost` for
these cases.

## Isolation and deployment

Run the operation with only the delegated credential. Do not inherit the owner's
lease environment, credential files, unrestricted service credentials, signing
key, or access to an owner credential broker. Keep the service URL in trusted
configuration and use TLS outside local development.

Expose `remote.NewDelegationServerWithOptions` on the ingress available to
isolated workers. This handler requires a delegated token on every request;
omitting the header, supplying an owner token, or invoking an unscoped/admin
route cannot bypass its checks. The owner creates delegations through the regular
handler, which must remain behind a separate access boundary.

```go
options := remote.ServerOptions{LeaseTokenSigningKey: signingKey}
ownerHandler, err := remote.NewServerWithOptions(backend, options)
workerHandler, err := remote.NewDelegationServerWithOptions(backend, options)
// Serve ownerHandler on the trusted owner/admin ingress.
// Serve workerHandler on the isolated workers' ingress.
```

Use the same backend and signing key for both. Give the child the worker ingress
URL in trusted configuration; capabilities contain no destination URL. JobDB's
regular unscoped/admin APIs rely on deployment authorization, so the child must
not also be able to reach them anonymously or with inherited service credentials.
An equivalent authenticated gateway enforcing only lease-scoped calls can replace
the delegation-only handler.

Deploy replicas with the same `ServerOptions.LeaseTokenSigningKey`, unique to
that JobDB trust domain. The signed policy is stateless and portable among those
replicas; every replica consults the authoritative backend lease. Third-party
backends must implement or forward read-only `ValidateExecutionLeaseByID`.

## Integration acceptance checks

- The delegated capability enters c2j through its supplied-lease path and remains
  an `ExecutionLease`; it does not become an owner token during import or renewal.
- A separate operation process can use an expired renewable token with only its
  own capability. It reaches JobDB directly and never contacts the owner.
- Permission failures are terminal for that operation and never broaden its
  credentials or cause unrestricted runtime calls.
- Child operations and renewal leave the parent expiry unchanged. The owner
  continues its ordinary parent lease renewal independently.
- Every hard-deadline/renewal-permission combination follows the spec's truth
  table. In particular, valid nonrenewable children can still be imported.
- Losing or replacing the parent stops both child operations and renewal.
- Chapter reads/writes and secret resolution remain within their allowlists.

`TestDelegationIsolatedProcess` exercises the separate-process handoff, expired
child import, direct refresh and submission against SQLite. The remaining
`TestDelegation*` tests cover permissions, concurrency and parent invalidation.
