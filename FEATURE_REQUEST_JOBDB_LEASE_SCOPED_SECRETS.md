# JobDB: resolve job secrets through an execution lease

Starting a job should require permission to submit work, without requiring
access to its credentials. Jobs persist logical secret references; executing
runners retrieve values only when needed. Make JobDB the broker so runners and
orchestrators such as Pulse need no separate secret-manager integration.

## Proposed API

Add a lease-authenticated endpoint and corresponding client method. Illustrative
HTTP shape, to be aligned with the existing lease API:

```http
POST /.../leases/{lease_id}/secrets:resolve
X-JobDB-Lease-Token: <current lease token>
Content-Type: application/json

{"names": ["github_token"]}
```

Return the requested values, with optional provider version and expiry metadata.
Use job-local names rather than caller-supplied external store paths. Fail the
request if any requested binding is unavailable or unauthorized; do not return
partial values or silently substitute empty strings.

The method must also work through the embedded/local runtime, using equivalent
lease and authorization checks.

## Authorization and lifecycle

- Validate the token against the tenant, job, and currently active lease on every
  request. Reject expired, released, superseded, or completed leases, and jobs
  whose cancellation has been requested. Recheck lease validity before returning
  values if an external lookup outlives the lease.
- Resolve only bindings allowed by trusted server policy for that job. Job
  declarations and submission metadata are requests, not authority to select
  arbitrary secrets. Policy may use authenticated submitter identity and trusted
  job provenance; JobDB need not interpret c2j recipes.
- Bindings identify authorized secrets, but grant no listing, writing, or
  administrative access. Child jobs require their own authorization.
- Lease renewal should continue working with the existing token lifecycle.
  Denying further reads cannot revoke values already delivered; preserve expiry
  metadata for providers that issue short-lived credentials.

Pulse already claims and hands off a lease. The runner can use that lease to
resolve secrets immediately before an uncached invocation. Cached tasks and
story replay perform no secret reads. JobDB should reject secret resolution in
its read-only replay runtime as well.

Because Pulse handles the bearer lease credential, it technically shares this
authority. This proposal assumes Pulse is trusted. Cryptographically restricting
access to the eventual runner would require a separate runner-bound identity.

## Storage and persistence

Provide a server-side secret-provider interface with interchangeable backends:

- **Local:** dedicated encrypted secret storage, with its encryption key managed
  outside the database and its backups.
- **External:** configured bindings to an external secret manager, accessed using
  JobDB's service identity. Backend addresses and credentials stay server-side.

Configure bindings and manage local secrets through a separate administrative
surface. A minimal first implementation can use operator configuration plus one
backend; provider-specific credential issuance can follow later.

Secret responses must bypass workflow chapters, task inputs/results, client
payloads, artifacts, and ordinary job inspection APIs. Set `Cache-Control:
no-store`; exclude request credentials and response bodies from logs, traces,
and error messages. Audit only names/identifiers, job and lease identity, time,
and outcome. Keep provider errors sanitized and distinguish denied, unavailable,
and transient failures without exposing unrelated secret existence.

This protects the resolution path. c2j remains responsible for transient
injection, reference-only durable inputs, and sanitizing op logs/errors before
storage. Once an op receives plaintext, arbitrary output or artifact leakage
cannot be prevented by the secret endpoint alone.

## Acceptance criteria

- A submitter without secret-read access can start an authorized job; its runner
  can retrieve only the job's permitted bindings through its lease.
- Cross-tenant/job requests, unauthorized names, and invalidated leases fail.
- Renewed leases work; slow backend lookups do not return secrets after lease
  loss. Backend failures never expose partial values or credentials.
- Local and external providers obey the same contract; new reads reflect
  rotation unless a binding explicitly selects a version.
- History, replay, job exports, and telemetry contain no resolved values from
  this API. Replaying a job requires neither a live lease nor secret-store access.

Related c2j design: [Runner-resolved secrets](PROPOSAL_RUNNER_RESOLVED_SECRETS.md).
