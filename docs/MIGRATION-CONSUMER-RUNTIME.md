# Runtime inputs without goal attachment

The worker SDK now accepts `jobdb.ConsumerRuntime`. It contains the retained
runtime operations, including goal discovery, waits and explicit message failure,
without `AddGoalCandidate`. `workflow.WorkflowRuntime` aliases this consumer
contract. Existing backend values continue to work as SDK inputs.

During this preparation, `jobdb.WorkflowRuntime` still composes
`ConsumerRuntime` and the temporary `GoalCandidateMutator`. `GoalRuntime` likewise
composes `ConsumerGoalRuntime` and that mutator. Existing backends and legacy
HTTP constructors keep their current behavior. This prepares consumers to remove
attachment; it does not change stored membership or goal lifecycle semantics.

Use `remote.NewConsumerServer` or `NewConsumerServerWithOptions` for a server
without attachment. Its generated strict interface and registered routes omit
candidate PUT. The legacy `NewServer` constructors still register it.
`NewDelegationServerWithOptions` also accepts the consumer contract and uses the
consumer handler, with its existing credential and permission checks.

`jobdb.GetJobRun`, `workflow.GetJobRun` and `workflow.ReadCatchHistory` accept
`jobdb.JobRunReader`: exact job detail, job listing and chapter listing. The
validating `jobdb.ListLeaseEvents` helper accepts `LeaseEventReader`. Selected
primary-goal conformance and `celltest.WithCells` accept `ConsumerRuntime`.

## Decorators and optional capabilities

Pass the original runtime value when no override is needed. Its dynamic type
continues to expose its optional capabilities. A decorator embedding only an
interface promotes only that interface's methods. It must explicitly forward
optional capabilities it supports, while preserving its authorization, tenant,
visibility and exact-identity overrides. Do not replace a consumer decorator with
its raw backend to satisfy a signature.

For example, a consumer client decorator supporting delegation can compose the
retained interface and `jobdb.LeaseDelegator`. Supply both fields with real
implementations. Do not embed the legacy full runtime or a concrete backend:
either would promote attachment again. Do not add an error-returning mutation
stub to satisfy the old contract.

Server decorators need `ConfigurationAdmin` in addition to the consumer methods.
Forward `JobSchemaRegistry`, `PrimaryGoalSelectionCapability`,
`JobDeletionRuntime`, configuration/secret limit readers and development secret
administration when supported. Lease transport needs the backend's existing
`*WithLeaseByID` operations (including `StartOrAppendJobWithLeaseByID`), expiry-aware keepalive, `RenewExecutionLeaseByID`,
`ValidateExecutionLeaseByID` and cell lease operations. These ports remain
optional on the primary interface. Delegation relies on the real validation and
renewal implementations; declaring unsupported methods would misrepresent
capabilities. The retained-only server fixture in
[`consumer_server_test.go`](../pkg/jobdb/runtime/remote/consumer_server_test.go)
shows explicit forwarding tested against toy and SQLite.

`celltest.WithCells` promotes mandatory consumer methods only, as its former
full-runtime wrapper promoted only mandatory methods. Fixtures needing optional
capabilities should use `celltest.Submit` on their original decorated runtime,
or explicitly compose those capabilities around the fixture wrapper.

## Source compatibility and verification

Ordinary calls supplying existing runtimes remain valid. Exact function types,
callbacks and method expressions using the former input signatures must be
updated, including the delegation-server constructor and narrow readers. This
is an intentional input-contract change; legacy backend contracts remain.

The acceptance tests run a wrapper with no attachment method through SDK worker
execution, durable replay and inspection. Server tests compare actual registered
routes, reject direct candidate requests without changing membership, verify
both legacy constructors, and exercise retained selection, receipts, waits,
message failure, configuration, secrets, leases, delegation, cells, chapters,
artifacts, schema administration, cancellation and crash-clear forwarding.

Regenerate both server inventories with:

```sh
go generate ./pkg/jobdb/internal/runtimeapi
```

The retained generator derives its interfaces, wrappers and registrar from the
same complete generated operation inventory, sharing wire types and middleware.
It checks the expected retired operation and generated shape before writing the
consumer bindings. The REST specification retains the legacy operation until
backend retirement.
