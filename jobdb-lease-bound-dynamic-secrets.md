# Feature request: lease context and fresh resolution for dynamic secrets

Target: JobDB. Status: implemented, 2026-10-22. Baseline: `v0.0.46`.

Implemented APIs: `jobdb.SecretResolveRequest.Lease`, `jobdb.SecretValue.LeaseBound`,
workflow `ResolveSecretsFresh`, and `workflow.SecretResolverFromContext` for bootstrap.
See the [usage guide](docs/GUIDE-LEASE-BOUND-SECRETS.md).

## Problem and requested behavior

A secret provider should be able to mint an external credential that expires no later than the current execution lease authority. A running worker must be able to request a replacement using its latest renewed capability, without creating another refresh-token system or renewing the lease as a side effect.

Please add two generic capabilities: **verified lease context in provider requests**, and **fresh secret resolution available through the active execution**. Gitvend JWTs are the first consumer; the contract should also support other dynamic credential providers.

## Existing behavior

`pkg/jobdb/secrets.go` defines `SecretProvider`, `SecretResolveRequest`, and `SecretValue`, which already includes `ExpiresAt`. Providers receive tenant/job identity, optional verified principal, server-loaded application metadata, and requested names, but no lease identity or expiry.

`pkg/jobdb/internal/secretsbroker/broker.go` validates lease authority before and after provider execution. Runtime backends supply authoritative validation. The existing HTTP API accepts a lease token at:

```text
POST /v1/tenants/{tenantId}/jobs/{jobId}/leases/{leaseId}/secrets/resolve
```

Core does not cache results. `pkg/workflow/secrets.go` caches values and omissions above the provider, within a job execution attempt. These caches are not shared across jobs, executions, or attempts. An expired cached name fails instead of being returned or automatically fetched again. Providers may independently cache, but must only return values valid for the current request. `pkg/workflow/lease_session.go` already resolves against a snapshot of the current renewed capability without holding the renewal lock during provider I/O.

## 1. Verified provider lease context

Extend the public provider request with a structured context, for example:

```go
type SecretLeaseContext struct {
    LeaseID            string
    WorkerID           string
    LeaseExpiresAt     time.Time
    AuthorizedNotAfter time.Time
}
```

Exact field names are flexible. Required semantics:

- Populate fields from authoritative backend validation and verified transport claims, never from caller-provided metadata or expiry overrides.
- `AuthorizedNotAfter` is the minimum of the current backend lease expiry and the presenting capability's expiry: the verified token expiry for remote callers, or the issued handle's expiry for embedded callers. Embedded and remote callers follow the same authority rules. Renewal returns a new capability; retaining an older handle does not give it the renewed capability's lifetime.
- Identify the exact lease attempt; renewal preserves identity, while a new attempt must not inherit its authority.
- Do not forward raw lease tokens to providers. They need validated context, not authority to mutate or renew the job.
- Preserve server-loaded metadata and current provider authorization responsibility. Metadata may still contain submitter-controlled fields.
- The lookup context deadline remains an execution/time-limit mechanism; it must not be overloaded as the credential expiry contract.

Change the backend validation callback to return the required validated snapshot along with application metadata. Apply equivalent semantics across supported core, SQLite, toy, and remote paths.

Revalidate after provider execution and before exposing results. Reject results if the lease is cancelled, released, expired, archived, or superseded. If the deadline can shrink during issuance, do not return credentials whose declared validity exceeds the new safe deadline. If renewal extends the deadline, accepting a token bounded by the earlier snapshot is safe. Do not hold a database transaction or the heartbeat lock across slow external provider work.

Add an explicit per-value flag, for example `SecretValue.LeaseBound`, defaulting to false. When true, require a nonzero safe deadline and `ExpiresAt`, and require the value to be unexpired and bounded by both the initial and revalidated authorized deadlines. The provider must encode the same or an earlier deadline inside the actual credential: changing response metadata cannot shorten an already-signed JWT. When false or omitted, preserve existing semantics: no lease-expiry bound applies, an omitted `ExpiresAt` means no declared expiry, and a supplied `ExpiresAt` is still enforced. A result may mix static and lease-bound values; invalid values reject the whole result.

## 2. Fresh resolution through the active execution

Expose an additive fresh-resolution method or options-based API, conceptually `ResolveSecretsFresh(ctx, names)`. Make it reachable through a narrow public execution resolver hook, including before credential-dependent application bootstrap.

Required semantics:

- Bypass the execution cache's values **and omissions** for the requested names. A fresh call must cross the broker's live-lease validation and invoke the provider. Providers may return their own cached credentials if still valid and authorized for the current request, including its lease identity and deadline where applicable. Fresh means consulting the provider again, not necessarily minting a different credential; no provider-level freshness mode is required.
- After successful validation, atomically replace the requested names in that execution attempt's cache. Returned values replace old values or omissions; omitted names become cached omissions and remove any old value. Leave unrelated names unchanged. Failed or cancelled calls do not update the cache. Serialize cache access using the existing cache lock, independently of lease renewal. Do not share these execution caches across jobs, executions, or attempts.
- Use the current capability owned by the workflow lease session, not the original supplied capability.
- Do not acquire work, import a lease, renew it, or start another heartbeat. The execution runner remains the only renewal owner.
- Stop access on execution completion, rescheduling/handoff, cancellation, or authority loss, including requests already in flight whose result has not been returned.
- Propagate cancellation and bounded lookup timeouts without blocking lease renewal.
- Keep values out of workflow history, task outputs, serialized runner state, and diagnostics.

Ordinary `ResolveSecrets` reads use the updated cache and continue to reject expired values. Preserve existing cache ownership and attempt-reset boundaries; automatic refetch on expiry is outside this feature. A supported resolver callback or capability interface is preferable to exposing the private `leaseSession` type. The existing `BeforeRun` callback already runs under the renewing session and is a natural integration point for bootstrap access. Callers must not be able to select an unrelated job or lease through the hook.

The existing remote secret-resolution endpoint already performs uncached broker resolution; a new HTTP endpoint is not required unless implementation constraints justify one. Preserve its `no-store`, error sanitization, and lease-token authentication behavior.

## Credential lifetime example

If the backend lease expires at 12:05 and the presented capability expires at 12:04:50, pass 12:04:50 as the safe deadline. A JWT provider subtracts any verifier expiry allowance and clock margin, then applies its maximum token-lifetime ceiling. It must not issue until 12:10 on the assumption that the lease will be renewed.

After the runner renews, the next fresh request may use the new deadline. Previously issued tokens do not change. Cancellation rejects subsequent lookups and causes in-flight results to be discarded on revalidation. An already-running provider may still issue an external credential; cancellation cannot immediately revoke credentials verified offline.

## Acceptance criteria

1. Providers receive correct identity/deadline for embedded and remote leases; a caller cannot spoof these fields.
2. A provider result is discarded when cancellation, handoff, expiry, or ownership change occurs during lookup.
3. Lease-bound results require a safe deadline and a valid `ExpiresAt` within the initial and revalidated bounds. The default unmarked result preserves existing behavior, including enforcement of an optional `ExpiresAt`. Mixed results are supported.
4. A slow provider does not block normal heartbeat renewal.
5. Two fresh requests for the same name invoke resolution twice, including after an earlier omission or expired value. A valid provider-cached credential may be returned again.
6. Fresh resolution after renewal uses the latest capability. An old expired token or embedded handle cannot resolve secrets or renew itself merely because the backend lease was renewed elsewhere. Calls that outlive their presenting capability discard their results, even if a newer capability remains valid. Cover both transports and every supported backend.
7. No fresh-resolution call extends the lease, acquires replacement work, or survives execution closure.
8. Existing cached-secret tests continue to pass, and new tests cover the explicit fresh path.
9. A successful fresh read updates subsequent ordinary reads for the requested names only, including transitions between values and omissions. Failed refreshes leave the cache unchanged, expired values are never returned, and caches remain isolated across executions and attempts.

Related requests: [plugin host](jobdb-secret-provider-go-plugin.md), [c2j helper](c2j-secret-git-credential-helper.md), and [Gitvend provider](gitvend-jobdb-secret-provider-plugin.md).
