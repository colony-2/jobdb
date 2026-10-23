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
    deadline := time.Now().Add(15 * time.Minute)
    child, err := runtime.CreateDelegation(ctx, owner, jobdb.CreateDelegationRequest{
        NotAfter: &deadline,
        Permissions: jobdb.DelegationPermissions{
            Secrets: []string{"model_api_key"},
            Read: &jobdb.DelegationChapters{Ordinals: []int64{0, 1}},
            Write: &jobdb.DelegationWritePermission{
                Ordinals: []int64{2},
                TaskTypes: jobdb.DelegationNames{Names: []string{"summarize"}},
                ArtifactNames: jobdb.DelegationNames{Names: []string{"summary.json"}},
            },
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
IDs. Leave the other permissions absent.

Set `NotAfter` to the absolute time when the child's authority must end. Omit it
if the child should remain authorized while the exact parent lease is live.
There is no renewal permission or access TTL to configure. The SDK maintains
credentials internally, without extending this deadline or the parent lease.
At or after the deadline, both resource operations and credential refresh fail.

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

A child with a current credential imports without refresh. A stale credential
refreshes on import using only the child's token, as long as its parent is live
and its deadline has not been reached. `SubmitJob`, `ResolveSecrets` and the
runtime's lease-authorized chapter operations use the same automatic mechanism.
There is no child heartbeat to start and no owner callback to configure.

`KeepAlive`/`Renew` remain supported for existing lease consumers. They maintain
only the child credential and never extend its authority deadline or the parent.
A periodic loop is unnecessary; the SDK maintains credentials on use. The
optional `interface { Delegation() *jobdb.DelegationInfo }` method exposes the
permissions and `NotAfter` without its bearer token. `LeaseExpiry()` is a current
credential snapshot, not a second deadline on the grant; do not terminate a
child solely because that snapshot has passed before allowing the SDK to refresh.

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

A token retains its grant authority after internal access expiry. Refresh does
not revoke old token copies. Protect both current and old serialized copies
until the parent lease is lost or the hard deadline is reached. Do not follow
redirects with the token.

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
- A separate operation process can use a stale access token with only its own
  capability. It reaches JobDB directly and never contacts the owner.
- Permission failures are terminal for that operation and never broaden its
  credentials or cause unrestricted runtime calls.
- Child operations and renewal leave the parent expiry unchanged. The owner
  continues its ordinary parent lease renewal independently.
- A child stays usable until its hard deadline while its parent is live, without
  a refresh permission or timer. Omitting the deadline ties its lifetime solely
  to the parent. At the deadline, operations, import and refresh all fail.
- Losing or replacing the parent stops both child operations and renewal.
- Chapter reads/writes and secret resolution remain within their allowlists.

`TestDelegationIsolatedProcess` exercises the separate-process handoff, expired
child import, direct refresh and submission against SQLite. The remaining
`TestDelegation*` tests cover permissions, concurrency and parent invalidation.
