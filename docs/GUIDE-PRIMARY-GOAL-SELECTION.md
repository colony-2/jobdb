# Submit fresh work under an existing goal

Set `SubmitJob.GoalID` to submit new instructions under an existing primary goal.
Supply an explicit destination `JobID` and retain it across retries. The returned
handle contains the requested goal, tenant/job key, cell name, stable cell ID and
repository. Input, metadata, policy and chapter zero belong to the new job; no
source history is copied and no additional goal or membership is created.

Omitting `GoalID` preserves ordinary submission. `SubmitRestartJob.ShareGoal`
and `AddGoalCandidate` retain their existing separate meanings. Schedules do not
select an existing primary goal.

The selected goal must be open in the authenticated tenant and must have the
same stable cell ID and canonical repository as the existing active destination.
Cell aliases resolve on the server; selection cannot register cells. A running
candidate does not prevent another submission. Any successful candidate can
satisfy the goal's existing dependents.

## Authorize embedded and HTTP callers

Trusted embedding code installs `WithPrimaryGoalSelectionAuthorizer` in addition
to `WithCellAuthority` destination `jobs.submit` permission. The selection hook
must check current goal access and explicit authority to select that goal. A
submit-only grant or worker lease never supplies this authority. Return
`ErrPrimaryGoalSelectionForbidden` when denying selection. The hook runs during
preparation, atomic publication and receipt replay; it must not call the runtime
or block on its storage locks. It must be safe for concurrent calls and must
consult current grants rather than an old cached approval.

For HTTP, upstream authentication middleware installs these trusted context
values. Neither request bodies nor job metadata can grant authority. The default
reusable server provides no selection authorizer. The bundled toy and SQLite
servers adapt their existing trusted local-administrator policy to this hook,
with no additional permission or configuration. Capability discovery reports
backend support, not caller authorization. This hook does not introduce general
RBAC or replace an application's end-user authorization. Native and HTTP
execution-lease/delegated submission routes currently reject selection with
`ErrPrimaryGoalSelectionUnsupported`, including for otherwise authorized callers.

## Retry the same destination

Publication shares the goal closure lock/transaction. If successful closure or
`FailGoal` wins, publication fails with `ErrGoalConflict`. If publication wins,
closure retains acceptance, does not reopen the goal and does not automatically
cancel the job. Explicit failure freezes a snapshot including the new candidate.
Prospective dependency cycles reject publication atomically.

The runtime retains the immutable semantic descriptor and complete accepted
handle without automatic expiry. After current authority checks, an identical
published retry returns that original handle before testing openness or public
visibility. This works after closure, terminal completion, archive, soft deletion
and cell rename, without unhiding work. A different selector, operation, input,
metadata, policy, dependencies, schema, payload, availability or resolved
ownership conflicts with the reserved destination. Equivalent JSON key order,
artifact/dependency order, selected-policy defaults and timestamp offsets are
canonicalized. Ordinary persisted descriptors retain their previous encoding.

A response loss is ambiguous: retry using the original tenant and destination
ID. Never generate a replacement ID or switch to ordinary submission.
`ErrSubmissionReceiptUnavailable` means the runtime cannot establish the captured
identity; it must not republish. Rejected/interrupted unpublished preparation is
not public work and must revalidate eligibility on retry. Cleanup must preserve
committed receipts. Toy retains receipts for its engine lifetime; file-backed
SQLite retains them across reopen. Neither exposes a public hard-purge API;
backend reclamation must retain receipts or permanently prohibit ID reuse.

## HTTP and backend compatibility

The client first calls `GET /v1/capabilities/primary-goal-selection` and requires
`{"supported":true}`. It then sends only
`PUT /v1/tenants/{tenantId}/jobs/{jobId}/primary-goal`, with a protected envelope
containing `goalId` and `submission`. The latter uses the ordinary submission
shape. The handler independently checks backend capability and trusted authority.
The client never sends selected work to ordinary submission, follows redirects
for these operations, or falls back when discovery or routing changes fail.
Unknown support and missing protected routes are unsupported. Typed goal,
authority, request mismatch, cycle, cell and receipt errors survive HTTP.

Backend implementers may advertise `PrimaryGoalSelectionCapability` only after
implementing the complete contract. `runtimecore.JobPreparation` carries
`SelectedGoalID`; `PreparedJob.Receipt` carries the captured handle independently
of `GetJob`; `CreateJobRequest.SelectedGoalID` distinguishes selected publication
from Restart sharing. Scheduler preparation must reauthorize replay and compare
immutable identity before fresh eligibility checks. Publication must reauthorize
and validate ownership, openness and cycles within the same transaction domain
as both closure paths, committing the receipt with visibility. Receipt storage
must survive job/preparation cleanup and deletion. Core rejects schedulers that
do not positively advertise support and requires a complete selected receipt.

Run `runtimetest.RunPrimaryGoalSelectionConformance` directly and through HTTP,
with explicit test-only authority installed on both sides. Run
`RunPrimaryGoalSelectionBoundaries` on native runtimes for controlled post-reservation
closure and interruption checks. SQLite also tests file reopen and retained
identity after simulated reclamation. Production Postgres support belongs to
pgjobdb; c2j propagation follows the shared revision. Those deployments are
separate from these source changes.
