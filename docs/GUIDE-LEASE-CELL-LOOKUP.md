# Cell lookup through an execution lease

Use `ExecutionLease.LookupCell(ctx, name)` when an operation needs another cell's
registered Git workspace. A live owner or delegated lease can look up any active
cell in its tenant, even with an empty permission grant or a submission filter
excluding that cell. No extra lookup grant is needed.

```go
target, err := lease.LookupCell(ctx, "payments")
if err != nil {
    return err
}
// target.Cell is the current registered name, including when queried by alias.
// target.CellID is immutable; target.CellRepo is the canonical repository.
```

The HTTP equivalent is
`POST /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/cells/lookup`,
with `X-JobDB-Lease-Token` and JSON `{"cell":"payments"}`. Route identifiers must
match the authenticated lease. Responses carry `Cache-Control: no-store`.
The response contains only `cell`, `cellId`, and `cellRepo`.

Lookup validates the current lease and delegation on each request. Cancellation,
completion, expiry, or lease replacement invalidates authority; delegated
deadlines and signature verification still apply. Names use the existing
case-sensitive rules and permanent aliases. A successful response represents
one coherent current registry mapping. Later calls observe edits; an earlier
returned value remains unchanged. Consumers can record the value for replay.

Use `errors.Is` for `ErrInvalidCell`, `ErrCellNotFound`, `ErrCellRetired`, and
`ErrExecutionLeaseLost`. Remote delegation deadline violations retain the
existing `ErrLeasePermissionDenied` mapping. A cell excluded from submission
scope remains available to lookup.

Lookup performs no registry mutation, job submission, or Git access. Git
credentials and the Git server authorize repository access. `ResolveCell`,
ordinary submission, and restart submission retain their existing authorization
checks. Returning an identity or repository never grants submission authority.

Toy, SQLite, PostgreSQL, and HTTP runtimes share this contract. Custom
`ExecutionLease` implementations and wrappers must implement or forward
`LookupCell`. Runtime core schedulers must implement `LookupCellWithLeaseByID`,
revalidating the exact live lease and reading its tenant registry under one lock
or transaction. The public `cellstate.State.Lookup` helper assumes those checks
have already been performed; it is not an authentication boundary.

Deploy JobDB, pgjobdb, and c2j together. No compatibility negotiation or fallback
is provided. c2j workspace lookup should use this API and retain repository
assertions, independent child-submission authorization, and existing secret
grants for obtaining Git credentials.
