# Cell creation hooks and repository provisioning

Status: proposal for review. This document describes a possible extension; it is not implemented.

## Purpose

Allow cell creation to trigger integrations, initially creating a GitHub or other Git repository. The design should work with an external Git provider today and a combined Git/JobDB server later.

Cells retain server-generated IDs, unique mutable names, mutable repository assignments, and arbitrary metadata. Creation remains explicit. `root` remains an ordinary cell.

## Recommendation

Introduce a durable `cell.created` event and a server-configured handler interface. Persist the event in the same database transaction as the cell, then invoke handlers after commit. Start with a repository provisioner rather than a general plugin framework.

Treat delivery and provisioning as separate concerns:

- Delivery ensures an integration receives an event and can retry after failure.
- Provisioning determines whether required setup is complete and whether the cell can accept work.

A Go handler and an external webhook adapter can use the same delivery mechanism. Repository provisioning should be explicitly requested; attaching an existing repository should retain today's behavior.

## Alternatives

| Approach | Benefits | Costs and failure behavior |
| --- | --- | --- |
| Caller creates the repository, then the cell | Smallest JobDB change; straightforward in Cortex | Each creation path must coordinate both operations. A repository may exist without a cell after a partial failure. |
| Synchronous callback during creation | Simple interface; caller can wait for the result | The database and Git provider cannot commit atomically. Network calls inside a transaction hold locks; calls outside it leave partial failures. |
| Durable event with callback or webhook | Survives restarts; supports retries and independent integrations | Provisioning completes asynchronously. Handlers must tolerate repeated execution. |
| Durable event plus provisioning lifecycle | Adds an explicit readiness guarantee before jobs use the cell | Requires API-visible provisioning status and consistent enforcement across job admission paths. |

The proposed foundation is durable delivery. Add readiness enforcement if repository creation must complete before the cell can accept work.

## Creation flow

1. Authorize and validate the cell creation request, including any provisioning request.
2. In one transaction, create the cell, preserve request-id replay information, and record the event and its configured deliveries.
3. Commit and return the cell. Successful cell creation does not imply successful repository creation.
4. A dispatcher claims a delivery and invokes its handler outside the transaction.
5. Record success, a retryable failure, or a terminal failure. Expired delivery claims become eligible for retry after a worker crashes.

The SQLite runtime already creates cells in a database transaction. Event insertion belongs at that storage boundary, with equivalent behavior in pgjobdb. A remote-server-only wrapper would miss embedded callers and cannot guarantee atomic event recording.

An idempotent replay of `CreateCell` must return the existing creation result without emitting another creation event. A failed or rolled-back creation must not emit an event.

## Handler contract

An illustrative Go interface is:

```go
type CellCreatedHandler interface {
    HandleCellCreated(context.Context, CellCreatedEvent) error
}
```

`CellCreatedEvent` should contain:

- A stable event ID and event schema version.
- Tenant ID and immutable cell ID.
- The creation revision and creation timestamp.
- A snapshot of the cell's creation-time name, repository, description, and metadata.
- Any typed provisioning intent accepted during creation.

The exact Go types and error classification are an implementation decision. The delivery system needs to distinguish retryable failures from terminal failures and cancellation.

Handlers are registered by trusted server configuration. Requests do not supply executable code, arbitrary callback URLs, or provider credentials. A configured webhook adapter can translate the event into an authenticated HTTP request.

Handler registration should have stable identifiers. Record the intended recipients at creation time so changing server configuration does not silently add recipients to historical events. Backfilling a new integration would be a separate, explicit operation.

## Delivery guarantees

Delivery is **at least once**, not exactly once. A process can create the repository and crash before recording completion.

Each handler must use the event ID and immutable cell identity to reconcile repeated execution. Delivery attempts have independent IDs, but retries retain the original event ID. With multiple handlers, maintain progress per handler so a failure in one does not rerun every successful handler.

Use bounded retry backoff, persisted errors, and an explicit retry operation for terminal or exhausted failures. Retain enough history to diagnose failures and distinguish never-attempted, running, completed, and failed deliveries.

A repository already existing under the requested name is not sufficient evidence that a retry succeeded. The provisioner must verify ownership or a durable association with the provisioning operation. If the provider cannot supply that evidence after an ambiguous timeout, surface a conflict for reconciliation rather than silently adopting the repository.

## Repository intent and identity

Make these two operations explicit:

- **Attach:** register a cell pointing at an existing repository; no repository creation is requested.
- **Create:** register a cell and request repository provisioning through a configured provider.

Today, `repo` is required and normalized, but cell creation does not verify that the repository exists. The initial proposal keeps that field required for both operations. For creation, it identifies the intended locator, such as `github.com/acme/example`.

An illustrative request extension is:

```json
{
  "name": "example",
  "repo": "github.com/acme/example",
  "repositoryProvisioning": {
    "mode": "create",
    "provider": "github-acme"
  },
  "requestId": "create-example-001"
}
```

Omitting `repositoryProvisioning` preserves existing registration behavior. `github-acme` identifies trusted server configuration containing the provider credentials and allowed repository destination. Provisioning fields participate in request-id conflict detection.

The GitHub adapter would use the [organization repository creation API](https://docs.github.com/en/rest/repos/repos#create-an-organization-repository). Other adapters implement the same provisioning contract. Provider-specific settings, such as visibility or a repository template, need an explicit configuration policy; they should not be inferred from arbitrary cell metadata.

Associate provisioning with tenant ID and cell ID. Renaming a cell does not rename its repository. Repository rename, reassignment, and retirement policies are separate from the creation hook.

## Optional readiness enforcement

If the repository must exist before work begins, expose a separate `provisioningStatus` with values such as `PENDING`, `READY`, and `FAILED`. Keep cell lifecycle state as `ACTIVE` or `RETIRED`; an active cell can still be awaiting setup.

For a cell requiring provisioning:

- Creation returns the cell with status `PENDING`.
- Jobs cannot be admitted until provisioning is `READY`.
- A terminal provisioning failure leaves a visible cell with status `FAILED` and an actionable error.
- An authorized retry resumes the provisioning operation.

Readiness checks must cover every path that can introduce work, including direct submissions, lease submissions, restarts, and schedules. They belong in runtime behavior shared by remote and embedded callers.

If repository creation is only a convenience, omit job gating and expose delivery status separately. This is the main product decision to resolve before implementation.

## Concurrent changes and partial failures

A handler operates on creation-time intent, but must check current state before applying results. A metadata or display-name change should not invalidate an otherwise unchanged repository operation. A repository reassignment or retirement must prevent a stale result from completing the wrong operation.

Use an operation identity or repository-assignment generation for conditional completion. Checking current state before making the provider call is useful, but cannot eliminate a race while that call is in flight.

If a repository is created just as the cell is retired or reassigned, record the result for reconciliation. Do not automatically delete a repository as rollback: it may already contain user work. Do not overwrite a newer repository assignment with a stale handler result.

For the first implementation, consider rejecting repository reassignment while required provisioning is pending. This simplifies coordination, but the restriction needs explicit agreement because repository `PATCH` is currently supported.

## Persistence and implementation boundaries

Use ordinary columns for event identity, tenant/cell identity, event type, snapshot properties, handler identity, status, attempts, claim expiry, retry time, and error details. Arbitrary metadata can remain JSON. A JSON event body on the wire does not require a JSON envelope in storage.

Responsibilities would be:

- **JobDB:** public event/provisioning contracts, SQLite persistence, dispatcher contracts, remote status/retry API, toy behavior, and conformance tests.
- **pgjobdb:** equivalent transactional persistence and delivery claims, verified against the same conformance suite.
- **Provider integration:** GitHub or other Git API calls, credentials, retry reconciliation, and repository ownership checks.
- **Cortex/c2j:** optional provisioning request and status display; readiness handling if enabled.

Existing callers that register cells without provisioning keep their current behavior. Callers requesting provisioning must understand asynchronous completion. The storage changes still require an explicit database-format decision.

## Verification

Tests should establish that:

- Cell creation and event recording commit or roll back together.
- Request-id replay creates neither a second cell nor a second event.
- Delivery resumes after restart and expired claims can be recovered.
- A crash after provider success can be reconciled without creating another repository.
- Concurrent dispatchers do not ordinarily claim the same delivery, while handlers still tolerate duplicate execution after claim expiry.
- Successful handlers stay completed when another handler fails.
- Repository reassignment and retirement prevent stale completion.
- Existing-repository registration does not invoke provisioning.
- Required readiness is enforced consistently across all work-admission paths.

## Decisions for review

1. Is repository provisioning required before a cell can accept work, or an optional side effect?
2. Is the first integration an in-process Go handler, an external webhook, or both? Recommendation: Go handler first, with an adapter boundary for webhooks.
3. Should the caller supply the intended repository locator? Recommendation: retain the required `repo` field initially.
4. Should repository reassignment be blocked while provisioning is pending?
5. Which provider options are request-controlled, and which are fixed by tenant/server configuration?
6. How should operators inspect failures and request retries: API and CLI, or API only initially?

The first implementation should cover creation events and repository provisioning. Update/retirement events, automatic repository rename or deletion, and historical backfills can be proposed separately.
