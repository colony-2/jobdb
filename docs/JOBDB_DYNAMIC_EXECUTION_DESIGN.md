# Dynamic execution requirements: proposed JobDB boundary

Status: discussion draft. This note revises the environment matching model in
the earlier dynamic execution requirements draft.

## Model

A job has a current execution requirement describing the environment in which
its next automated work should run. JobDB stores it and exposes it in `GetJob`
and paginated `ListJobs`, alongside `NextNeed` and readiness. The base and
per-job override layers described in the requirements document can produce
that current requirement; their source and precedence remain visible.

`NextNeed` is a lease predicate: a lease requester advertises the work
capabilities it supports. The execution requirement is information for the
system that places work. JobDB does not ask a lease requester to report CPU,
memory, storage, image, platform, or an allocation, and does not compare those
properties while granting a lease.

## Expected production flow

1. An external controller lists jobs and reads each job's current requirement
   and `NextNeed`.
2. The controller chooses a job and provisions an appropriate executor. How it
   selects or verifies that environment is outside JobDB.
3. The executor requests a lease for that job ID and advertises its supported
   `NextNeed` capabilities. JobDB applies its existing capability, readiness,
   cancellation, and lease ownership rules. A requester can acquire the lease
   without supplying any environment information.
4. The trusted executor runs the work in the provisioned environment.

`PollWork` has the same rule: it can return a job with an execution requirement
without an environment claim from the poller. A production poller is responsible
for using it appropriately. To make a list-to-lease race visible, an acquired
lease should also return the requirement and revision observed at acquisition;
the caller checks that snapshot before running work. JobDB does not perform an
environment match.

## Changing the requirement

The current lease owner can submit a requirement patch and, when appropriate,
a new `NextNeed`. JobDB atomically stores the change and relinquishes that
lease. The same job and durable work history continue under a later lease.
The transition yields even if the current executor could satisfy the new
requirement. A stable request ID makes retries and workflow replay safe.
Cancellation and expired or superseded leases retain their existing authority.

This design requires no capacity reservation, allocation attestation, or
record of actual allocated resources in JobDB. It revises the environment
matching rules and incompatible-executor denial scenarios in the earlier
requirements document. Numeric and image fields still need validation and a
stable representation so external controllers can use them consistently.
