# JobDB production security: requirements and decisions

Status: Proposed. Based on the current checkout, including the cell and lease
delegation work in progress. This defines **what must be true before production**;
permission names are provisional. Storage schemas, middleware interfaces, token
formats, and implementation sequencing within each workstream come later.

The recommended direction is to establish explicit tenant admission, cover every
base API with resource authorization, and make execution authority an explicit
grant issued at lease acquisition. Put root credential ownership and access
administration in the deployment/control plane above JobDB. JobDB should enforce
resource and live-lease constraints without needing to own root signing keys.

## 1. What exists and what is missing

| Area | Current foundation | Remaining production requirement |
| --- | --- | --- |
| Tenant isolation | Most resources and requests carry a tenant ID. | An authoritative set of tenants, explicit permission to create them, and authenticated access to specific tenants. A tenant ID in a URL or request is a selector, not authority. |
| Cell permissions | `CellAuthority` carries a principal, tenant, permission map, and cell allowlist. Cell administration, submission, listing, and acquisition have checks. | One authorization contract across all resources. The type explicitly leaves history, goals, and other mutations to deployment authorization. |
| Base HTTP API | OpenAPI declares bearer authentication; embeddings can supply authenticated context. | Actual credential verification and action/resource checks on every exposed operation. An OpenAPI security declaration does not enforce them. |
| Execution leases | Owner credentials bind tenant/job/lease/worker and expiry; protected operations validate backend ownership. | Define which execution powers an acquisition may issue. Acquisition must not silently expand a worker's authority. |
| Delegation | Macaroons support offline narrowing of selected cell creation, job submission, secret, and chapter operations, plus optional deadlines and live-parent validation. | Integrate these grants with tenant access and execution ceilings; preserve restrictions across every API path. |
| Key ownership | The remote server generates or accepts an HMAC key and derives its macaroon key from it. | Explicit production ownership, rotation, revocation, replica behavior, and a supported boundary that keeps root keys above core. |
| Development server | The CLI installs a local administrator with wildcard tenant access. Development secret administration can be enabled. | A production mode that cannot accidentally expose these defaults. |

Evidence: [authority types](../pkg/jobdb/cells.go),
[runtime HTTP handler](../pkg/jobdb/runtime/remote/server.go),
[owner credentials](../pkg/jobdb/runtime/remote/lease_token.go),
[macaroon implementation](../pkg/jobdb/runtime/remote/attenuation.go),
[delegated ingress](../pkg/jobdb/runtime/remote/delegation_server.go), and
[local server wiring](../pkg/jobdb/cli/root.go).

## 2. The authorization contract

Every production operation must establish:

1. **Trusted authority:** an authenticated principal or a verified bearer
   capability, with its issuer, audience, purpose, and validity established.
2. **Tenant admission:** the tenant exists, is enabled, and the authority permits
   access to that exact tenant.
3. **Action and resource scope:** the requested action is allowed on the actual
   resource, including any cell, job, route, name, or ordinal restrictions.
4. **Runtime conditions:** required lease ownership, generation, deadlines,
   lifecycle state, and provider policy remain valid.

For lease calls, authority is the intersection of the issued execution ceiling,
all attenuation restrictions, current tenant/access policy, and live backend
state. A valid signature alone is insufficient.

Required scope semantics:

- Grants bind actions to their own resource scopes. Read access to tenant A plus
  write access to tenant B must never become write access to both tenants.
- Ordinary grants identify one tenant or an explicit tenant set. All-tenant
  authority is an explicit platform privilege; omission never means wildcard.
- Cell visibility, workload access, and cell administration remain independent.
  `cells.get` does not imply `jobs.get`; `cells.create` does not imply submission.
- Existing resources are authorized using stored tenant and immutable cell IDs.
  Names and aliases resolve to those identities; renaming cannot widen a grant.
  Grants for future cell creation need an explicit name scope.
- Worker IDs, job metadata, repository locators, request bodies, and caller
  supplied headers cannot assert a privileged identity. A worker ID is an
  execution label bound to authenticated authority, not an authentication method.
- Missing authority, unknown actions/caveats, unavailable verification, and
  ambiguous credentials fail closed. A failed lease check must never fall back
  to a less restricted base API authorization path.
- Each ingress declares whether it requires base identity, lease authority, or
  both. When both are required, base authentication cannot override lease
  restrictions. Delegation-only ingress must support a child bearer without
  requiring the owner's credential.

These rules apply to HTTP and supported embedded entry points. Trusted internal
services may have explicit service authority; absence of caller context cannot
mean administrator. Direct database access remains a privileged infrastructure
boundary rather than a public JobDB API.

## 3. Tenant lifecycle and access administration

Tenant existence and tenant access need explicit semantics even if their source
of truth lives in the surrounding product. JobDB need not become an identity
provider or host a tenant-management UI.

| Proposed permission | Scope and required behavior |
| --- | --- |
| `tenants.create` | Platform/provisioning scope. Register a new tenant and establish its initial administrator through a controlled bootstrap operation. Ordinary writes must not create arbitrary tenants implicitly. |
| `tenants.access` | Specific tenant or explicit tenant set. Required admission for tenant operations; grants no data or mutation permissions by itself. Lease grants carry tenant admission within their ceiling. |
| `tenants.get`, `tenants.list` | Read tenant metadata or discover accessible tenants. Lists contain only authorized tenants; listing every tenant requires explicit platform scope. |
| `tenants.update`, `tenants.suspend`, `tenants.resume` | Explicit tenant management authority. Suspension stops ordinary reads, writes, acquisition, renewal, and use of outstanding capabilities, including descendants and scheduled execution. Recovery uses separately authorized administration. |
| `access.read`, `access.grant`, `access.revoke` | Access-management scope for a tenant and bounded grantable actions/resources. Grant administration must not let a tenant administrator create platform privileges or grant beyond their administrative ceiling. |

Tenant creation and routine tenant access are independent privileges. Provisioning
may assign an initial administrator without giving the provisioner ongoing access
to workload data. Tenant IDs must not be reused in a way that revives old grants.

For an initial deployment, provisioning and grants may be managed entirely by
the control plane. The production requirement is that JobDB honors those records
and their changes. A new public JobDB tenant CRUD service is not a prerequisite.
Tenant deletion and physical data erasure can be a later, separately authorized
lifecycle; they must not be implied by suspension or job deletion.

## 4. Base API permission coverage

Every row below additionally requires tenant admission and matching resource
scope. Existing names are retained where practical. This is the target contract,
not a claim that these checks already exist.

| Current operation IDs / surface | Required permissions | Scope and additional requirements |
| --- | --- | --- |
| `createCell`, `getCell`, `listCells`, `updateCell`, `retireCell` | Existing `cells.create`, `cells.get`, `cells.list`, `cells.update`, `cells.retire` | Preserve item visibility on lists and existing get requirements for update/retire. Creation may be name-scoped; it grants no workload access. |
| `submitJob`, `putJob` | `jobs.submit` | Target cell, job type, and optional job ID constraints. Both generated-ID and caller-chosen-ID submission obey the same policy. |
| `submitRestartJob`, `putRestartJob` | `jobs.restart` on source; `jobs.submit` on destination | Also require access to retained source history and artifacts. Restarts cannot copy inaccessible data, move it across tenant boundaries, or bypass goal/schema authorization. |
| `getJob`, `listJobs` | `jobs.get`, `jobs.list` | Direct reads and every returned list item must be authorized. Define the data each permission reveals, including payloads, completion details, execution state, and related IDs. |
| `cancelJob` | `jobs.cancel` | Exact job scope. Cancellation is separate from submission, acquisition, and reads. |
| `deleteJobs` | `jobs.delete` | Every selected job, including dry-run counts. Require tenant-wide delete scope for `all`; authorize explicit IDs before mutation, and constrain filtered selection to authorized resources. Preserve existing logical deletion semantics. |
| `pollWork`, `getJobLease` | Existing `jobs.acquire` plus an approved execution profile | Tenant, cell, route/job type/task type, and any job restrictions. Apply authorization before claiming work or disclosing its payload. Caller filters only narrow the authorized work set. |
| `getChapter`, `listChapters` | `chapters.read` | Job plus permitted ordinals/ranges. List results and embedded data follow the same constraints as direct reads. |
| `openArtifact` | `artifacts.read` | Owning job, chapter, and artifact. A digest, filename, stored reference, or blob URL is not authority. |
| `commitChapterIfWaiting` | `tasks.completeExternal` | Exact waiting task generation, route, ordinals, and input binding. This mutates workflow state without an owner lease and needs an explicit grant; knowing an input hash is insufficient. |
| `upsertSchedule` | `schedules.create` or `schedules.update`, plus `jobs.submit` | Authorize the actual create/update outcome and the scheduled target cell/type. Persist an approved execution authority for future runs. |
| `getSchedule`, `listSchedules`, `listScheduleRuns` | `schedules.get`, `schedules.list`, `schedules.runs.read` | Scope the schedule and each disclosed run/job. Run lists must not expose inaccessible job data through an alternate API. |
| `pauseSchedule`, `resumeSchedule`, `archiveSchedule`, `triggerSchedule` | `schedules.pause`, `schedules.resume`, `schedules.archive`, `schedules.trigger` | Resume and trigger also require valid submission authority for the configured target. Background firing must honor current tenant and schedule-grant status. |
| `registerJobSchema`, `getJobSchema`, `listJobSchemas`, `archiveJobSchema` | `schemas.register`, `schemas.get`, `schemas.list`, `schemas.archive` | Tenant-local schema scope. A schema hash does not grant access. Inline schema registration during submission needs registration authority too. |
| `getGoal`, `listGoals` | `goals.get`, `goals.list` | Explicit goal scope and authorized candidate/result visibility. A goal may link multiple jobs; one visible job cannot grant access to all candidates. |
| `addGoalCandidate`, `failGoal` | `goals.candidates.add`, `goals.fail` | Authorize the goal and the candidate job association. Automatic goal attachment during restart/submission obeys equivalent rules. Creating a job's own default goal may be included in `jobs.submit`; modifying an existing goal is separate. |
| `getDependencyWait`, `listDependencyWaits` | `dependencyWaits.get`, `dependencyWaits.list` | Owning job plus authorized views of referenced jobs/goals. Dependencies must not disclose hidden target IDs, failures, or outcomes. |
| `getTenantConfiguration`, `setTenantConfigurationValue`, `removeTenantConfigurationValue` | `configuration.get`, `configuration.set`, `configuration.remove` | Tenant administration, independent of leases and secret administration. Current reads return the whole tenant snapshot; granting them acknowledges that scope. |
| `setDevSecret`, `removeDevSecret` | Disabled in production | If production secret administration is needed, define `secrets.set`/`secrets.remove` over tenant and secret name in the appropriate control plane. A development endpoint must not become an implicit production administration API. |

Additional rules that cross API families:

- Authorization precedes conflict details, idempotency replay results, counts,
  payload retrieval, and mutation. Continuation tokens bind the query and caller
  scope; every page rechecks current authority. Cursors are not access grants.
- Establish a consistent response contract: invalid/missing credentials yield
  401; a known disallowed action yields 403; inaccessible object lookups are
  indistinguishable from missing objects where existence must be concealed.
- Access to a reference is separate from access to its target. This covers
  dependencies, goal candidates, restart prefixes, copied artifacts, and lazy
  artifact reads obtained through `GetJob`. Authorize actual disclosure, including
  subsequent downloads, rather than only the first object lookup.
- For the first release, registering a dependency requires read authority on its
  target job/goal. A narrower permission to observe completion can be added later.
- A schedule is durable execution authority. Revoking its authority or suspending
  its tenant stops future runs; expiry of the creator's login session need not.
  The schedule's grant and revocation behavior must be explicit.
- Job deletion currently hides jobs from lists and revokes leases while retaining
  direct reads/history/artifacts. Those retained reads still need authorization;
  deletion is not erasure. See [the existing contract](../pkg/jobdb/delete_jobs.go).

The [runtime OpenAPI](../openapi/jobdb-runtime.yaml), optional runtime interfaces,
CLI, and development routes must all be included in the final coverage inventory.
New operations cannot ship without a declared authorization requirement.

## 5. Lease authority must have an explicit ceiling

Separate the right to **acquire work** from the powers issued for **executing that
work**. An execution profile is the approved set of actions, resource selectors,
delegation limits, and lifetime constraints that acquisition may grant. It can be
assigned to a trusted scheduler/worker identity or workload policy without giving
that worker equivalent unrestricted base API access.

Today owner leases can create cells and submit to registered active cells across
their tenant. That must become an explicit profile choice. A worker confined to
cell A must not acquire a lease in A and thereby obtain implicit submission or
creation rights across the tenant. Source execution cell and permitted target
cells are separate scopes.

| Lease operation IDs | Required authority in the issued profile |
| --- | --- |
| `inspectLease` | Inspect this exact lease. A delegated response contains only its permitted snapshot, never owner credentials or execution payload. |
| `keepAliveLease` | Owner renewal authority for this lease. A delegated keepalive retains today's validation/snapshot behavior and cannot extend the parent. |
| `completeJobWithLease`, `rescheduleJobWithLease` | Explicit owner completion/rescheduling authority. Bind route changes, waits, and payload updates to the execution profile and runtime rules. |
| `getChapterWithLease`, `listChaptersWithLease`, `addChapterWithLease` | Selected chapter reads/writes. Preserve delegated task-outcome-only writes and fresh-upload restrictions. Base chapter APIs must not provide an escape from these selectors. |
| `submitJobWithLease`, `putJobWithLease` | `jobs.submit` ceiling over target cells, job types, and IDs, further narrowed by delegation. Child submission does not inherit platform or tenant administration. |
| `submitRestartJobWithLease`, `putRestartJobWithLease` | Explicit restart/submission authority and authorized source history; remain unavailable to restricted delegations unless deliberately added later. |
| `createCellWithLease`, `resolveCellWithLease` | Explicit cell-creation names or permitted submission targets, respectively. Creation does not expand target-cell scope. |
| `resolveSecretsWithLease` | `secrets.resolve` over allowed names, intersected with provider authorization and exact live-lease checks. Trusted lease facts stay separate from submitter-controlled metadata. |
| `getConfigurationWithLease` | Explicit whole-tenant configuration read authority. Restricted delegations currently exclude this operation; preserve that exclusion unless the profile is deliberately extended. |
| `createLeaseDelegation` and offline derivation | A child is bounded by the lease's delegation ceiling and every ancestor restriction. The HTTP endpoint remains owner-only; a child may narrow itself offline. |

Preserve the current restricted delegation surface in
[DelegationPermissions](../pkg/jobdb/delegation.go): cell creation, submission,
selected secrets, and selected chapter reads/writes. Configuration, owner
completion/rescheduling, restart, schema administration, and artifact downloads
remain excluded. A uniform credential family does not require equal authority.

Required lifecycle behavior:

- Bind grants to the deployment/audience, tenant, exact job and lease generation,
  worker authority, immutable cell identity, and relevant execution/schema facts.
  Knowing another worker's lease ID cannot authorize it.
- Check live backend ownership on use. Completion, cancellation, rescheduling,
  expiry, replacement, and tenant/access revocation invalidate the affected
  authority and descendants. Reacquiring a job never revives an old grant.
- A child may reduce actions, resources, or lifetime; it cannot widen them or
  obtain owner powers. Missing actions deny access. No operation can discard
  ancestor restrictions, including snapshot refresh and idempotent retry.
- Preserve child independence from owner access-token rotation for the same live
  lease. A child's hard deadline and the live parent are separate constraints;
  an old expiry snapshot is not a new grant of time.
- Revalidate sensitive disclosure and mutation against relevant state changes.
  Secret credentials marked lease-bound must expire no later than the verified
  safe lease deadline. Static secrets do not become revocable merely because
  their retrieval required a lease.
- Bearer grants are transferable. Offline attenuation cannot prevent copying,
  guarantee the identity of the recipient, or provide a complete issuance log.

Example acceptance case: worker W may acquire `build` tasks in tenant T/cell A.
Its profile permits selected chapter writes and secret `build_token`, but no cell
creation or child submission. Its owner lease and every child must retain those
limits even though other jobs in T use broader profiles.

## 6. Can root signing keys live above JobDB?

**Yes, as a target architecture.** Keep the tenant/access authority, root key
inventory, issuance policy, and key lifecycle in the embedding service or control
plane. JobDB core owns resource checks and authoritative lease state. Supporting
that boundary is a production requirement; a particular crypto protocol is a
subsequent design decision.

There are two distinct boundaries to choose between:

| Goal | Required trust arrangement |
| --- | --- |
| No root keys in the JobDB core package | The embedding authentication/credential layer owns issuance and verification. Core receives trusted, bounded authority and enforces it. The outer layer may share the same process. |
| No root keys anywhere in the JobDB process | Root issuance and secret-key verification live outside that process, or the chosen credential family supports verification using public material. Core requires authenticated, request-bound verification results and cannot accept client assertions of verified claims. |

The current storage core already differs from the remote credential layer: keys
live in `runtime/remote`, while `runtime/core` owns workflow semantics. Production
work should make the trust boundary explicit and support external ownership;
renaming or moving the current key field is not sufficient.

### Macaroon-based leases are a plausible fit

A macaroon holder can append restrictions without the root key. Conventional
macaroon verification still uses a root secret; publishing a list of key IDs does
not enable public-key verification. These are properties of the construction and
the current library's verification API, not an implementation obstacle unique to
JobDB. See the [original macaroon paper](https://research.google/pubs/macaroons-cookies-with-contextual-caveats-for-decentralized-authorization-in-the-cloud/)
and [Go verification API](https://pkg.go.dev/gopkg.in/macaroon.v2#Macaroon.Verify).

The proposed direction to evaluate is:

1. The higher authority establishes root trust and grants bounded authority for
   this deployment, tenant scope, and approved execution profiles.
2. Successful acquisition produces a capability bound to the backend-confirmed
   lease and its approved profile. A suitably bounded parent capability could be
   attenuated for this purpose without giving core a root signing key.
3. Owners and isolated workers can further attenuate permitted authority. The
   verification boundary validates the full chain; JobDB also checks live lease
   state and the operation's actual resources.

This is a design proposal, not something the current owner-token implementation
already supports. The follow-up design must satisfy these requirements:

- **No manufactured lease authority.** Adding a caveat naming a live lease is not
  proof of assignment. The trusted acquisition/issuance result must bind that
  lease to the same authorized grantee and execution ceiling. A caller cannot
  select someone else's live lease and attenuate a generic API token to claim it.
- **No authority amplification.** Attenuation cannot turn an acquire-only grant
  into completion or secret access. Any such powers must already be explicitly
  authorized in a parent execution ceiling, or be issued by the higher authority
  following a successful acquisition.
- **No expiry removal.** A short-lived login/access credential cannot become a
  longer-lived lease through attenuation. Use a separately authorized execution
  lineage or require reauthorization; every true ancestor deadline still applies.
  Preserve the current distinction between owner access-token expiry and the
  separate delegation root's lifetime.
- **Limited parent material.** A broad service macaroon is itself powerful bearer
  authority even without a root key. Its compromise scope must be explicit and
  bounded; do not hand platform-wide derivation authority to each worker.
- **Verification remains trusted.** If keys stay outside the process, verification
  results must bind the exact credential/request and effective restrictions.
  Verification failure or an unavailable authority cannot become allow-all.

The recommended first boundary is **root keys owned one level above core**, with
the same authorization contract usable by an external verifier. Decide whether
process separation is required before choosing the implementation. Third-party
caveats/discharges, a new policy language, and replacement of every owner token
are not prerequisites for defining or closing base API authorization.

### Root key administration belongs above core

The higher layer must support a trusted inventory of issuers/root IDs, purposes,
audiences, activation/retirement state, and bounded issuance authority. A “list
root keys” operation returns metadata or public verification material where
applicable, never root secrets. Inventory and rotation/revocation administration
require explicit platform permissions, separate from tenant data access.
Provisional higher-layer actions are `keys.list`, `keys.rotate`, `keys.revoke`,
and `credentials.issue`; issuance is constrained by its own grantable ceiling.
Bootstrap of the initial root and administrator must be an explicit deployment
operation, with no unauthenticated bootstrap path left enabled in production.

Define overlapping rotation, replica consistency, restart behavior, and emergency
revocation. Removing a root invalidates its descendants; routine owner-token
renewal must not. The current remote implementation accepts one configured key,
so an overlapping key lifecycle is additional work. A production deployment
cannot depend on a new random root key on each server start.

## 7. Remaining production requirements

| Requirement | Completion condition |
| --- | --- |
| Complete ingress coverage | Every exposed base, lease, optional, and administrative operation enforces its declared authority. Delegated workers cannot reach a more privileged alternate ingress. Minimal health checks are explicitly public; metrics and diagnostics have a deliberate policy. |
| Explicit production configuration | Startup rejects missing authentication/authorization configuration and local administrator defaults. Development secret routes are disabled. Credentials travel over authenticated encrypted channels and never appear in URLs, logs, traces, or ordinary object formatting. |
| Revocation contract | Tenant suspension, principal/grant revocation, root retirement, and parent lease invalidation have documented effects and a maximum propagation delay. Cached decisions cannot outlive that contract; renewals and scheduled runs recheck applicable authority. |
| Auditability | Record verified actor/grant lineage, tenant/resource, action, result, and correlation ID for privileged mutations, lease issuance/use, and access changes. Audit secret names only as permitted by the deployment; never values or bearer credentials. Offline children are observable on use, not necessarily on creation. |
| Availability boundaries | Tenant/principal limits cover submissions, concurrent leases, polling, schedules, bulk operations, secret resolution, payload sizes, and attenuation depth. One admitted tenant must not have unbounded authority to exhaust the service. |
| Backend and client consistency | Supported backends, embedded use, HTTP, SDKs, and CLI preserve the same permission decisions. Internal schedulers and providers operate under explicit bounded service authority. |

Individual child-grant revocation can remain deferred if the first production
contract explicitly offers hard deadlines and parent/grant/root revocation at
documented scopes. Do not promise selective child revocation or a complete child
inventory while issuance is offline and unrecorded.

## 8. Plan and release gates

| Workstream | Reviewable result | Exit condition |
| --- | --- | --- |
| 1. Agree on authority | Final permission catalog, tenant lifecycle, resource selectors, execution profiles, and the decisions below. | Every current API and implicit side effect has an owner and an authorization rule. |
| 2. Close base API access | Production authentication boundary and complete action/resource enforcement, including reads and alternate paths. | Missing credentials, wrong-tenant access, restricted-cell access, and permission mismatches fail before disclosure or mutation. |
| 3. Bound execution and delegation | Acquisition ceilings, owner/delegate separation, schedule authority, lifecycle and revocation contract. | Acquiring, renewing, delegating, restarting, or scheduling cannot expand effective authority. |
| 4. Establish key and operational ownership | Higher-layer root inventory/lifecycle, selected core/process boundary, production defaults, audit and limits. | Replica/restart/rotation behavior and emergency revocation are demonstrated without embedding root ownership in core. |
| 5. Demonstrate the contract | Backend/transport conformance evidence and deployment acceptance results. | All mandatory scenarios below pass in the production configuration. |

Mandatory acceptance scenarios:

- Anonymous callers cannot use any data or mutation API. Supplying tenant B in a
  URL/body or forging principal/worker metadata does not expand tenant A access.
- A read-only user cannot mutate jobs, goals, waits, schemas, schedules,
  configuration, cells, secrets, or access grants. Tenant creation requires its
  separate provisioning permission.
- A cell-restricted caller cannot retrieve hidden data through direct IDs,
  list counts/pages, goal links, run lists, dependency results, restart copies,
  artifacts, or idempotency conflicts.
- A limited worker cannot convert acquisition into tenant-wide submission, cell
  creation, administration, broader secrets, or owner powers for a child.
- Narrowing a grant repeatedly preserves every ancestor restriction. Malformed,
  cross-audience, wrong-tenant, wrong-lease, expired, and unknown-root credentials
  fail without falling back to another authorization path.
- Parent loss stops its descendants. Tenant suspension, access revocation, and
  root retirement stop the capabilities and future scheduled work covered by
  that authority within the declared revocation bound. Owner access-token
  renewal preserves legitimate child use as specified.
- Authorization changes during paging, polling, sensitive reads, or mutations
  cannot disclose or commit outside the documented validity window. Explicit
  batch IDs cannot cause partial unauthorized side effects.
- Production startup cannot use local administrator wiring or enable development
  secret administration accidentally. Root rotation works across replicas, and
  failed verification or unavailable policy state fails closed.

## 9. Decisions to settle before the implementation design

| Decision | Recommended starting point |
| --- | --- |
| Who owns tenants and grants? | The surrounding control plane; JobDB consumes authoritative tenant admission and bounded grants. |
| What does acquiring a lease grant? | An explicit execution profile. Tenant-wide child submission and cell creation require an intentional profile grant. |
| What does “keys outside core” mean? | Require separation from the core package now; decide whether separation from the whole server process is also mandatory. |
| Must owners also use macaroons? | Make externally rooted, attenuable lease authority a design candidate. First agree on ceilings and lifetimes; do not make a wire-format rewrite a substitute for API coverage. |
| What are the revocation bounds? | Parent invalidation follows existing live-lease checks. Set a concrete maximum propagation delay for control-plane changes before launch; renewals cannot bypass it. |
| How are cross-cell goals exposed? | Require explicit goal and candidate visibility; hide unauthorized links/results. Tenant-wide goal visibility is a deliberate broader grant. |
| Can every worker read tenant configuration? | Only profiles explicitly allowed the current whole-tenant snapshot. Add per-key reads later if required; keep sensitive values in the secret system. |
| Which administration must be possible at launch? | Provision tenants, assign/revoke access, suspend tenants, rotate/revoke roots, and inspect audit events through the higher layer. A JobDB IAM UI and selective offline-child revocation may follow later. |

The next design should implement these agreed requirements and resolve the
remaining choices without weakening tenant isolation or existing attenuation.
