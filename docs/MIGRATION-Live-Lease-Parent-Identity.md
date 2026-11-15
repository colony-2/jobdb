# Parent identity at the server boundary

The shared HTTP server and runtime core validate the submitting execution lease.
An omitted parent claim under a lease derives that lease's job ID. A non-empty
compatibility claim must agree with it. Without lease context, only an empty
root claim is accepted. A worker label or cell administration permission cannot
substitute for the submitting lease.

HTTP child submit and restart operations bind tenant, parent job and lease ID to
the signed credential. All four POST/PUT handlers also check the exact current
stored lease and worker before dispatching submission to the provider. Expired,
cancelled, completed, replaced or missing authority fails, including on retries.
A provider without the existing `ValidateExecutionLeaseByID` capability receives
HTTP 501 for these operations; the server does not dispatch unchecked work.
Existing delegation permissions and restart restrictions still apply.

Runtime core normalizes parent identity before constructing its stable request
descriptor, validates current stored authority before preparation, and rechecks
after history staging before publication or successful recovery. A saved
preparation supplies retry identity, not lease authority. When core loses lease
authority after preparation, it retains the reservation so a later lease of the
same parent can recover its stable identity. Staged chapters/artifacts may also
remain for reconciliation. This does not publish a child job.

Omission and an explicit matching parent describe the same request. A later live
lease of the same job, including another worker, can retry identical input.
Changing the parent or immutable input fails. Root restarts remain roots;
lease-backed restarts record the submitting job independently of the source's
ancestry.

Compatibility fields, metadata helpers, storage layouts and SQL signatures
remain available. Read `JobSummary.ParentJobID` (HTTP `parentJobId`) for recorded
parent identity, including an empty root value. Application metadata is arbitrary
user data: `ExtractParentJobID(summary.Metadata)` cannot establish ancestry.
Historical parent values are preserved without certifying their provenance.

Rebuild serving applications with this JobDB version. Enforcement uses the
provider's existing lease validation port and requires no separate provider
repair, schema migration or SQL reinstall. Older serving binaries retain their
previous behavior.

The enforcement boundary is the shared server/runtime API. Trusted native
scheduler/storage entry points and direct SQL still accept compatibility fields;
callers bypassing the server/runtime are responsible for their authority. In
particular, this change does not repair native pgjobdb bare-parent calls. There
is no new transaction-local or commit-time lease guarantee: invalidation racing
with a provider mutation remains governed by that provider's existing storage
semantics.
