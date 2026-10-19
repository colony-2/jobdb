# pgjobdb requirements for job deletion

pgjobdb needs a scheduler storage change and a new optional scheduler capability.
Update its JobDB dependency to include this change. The shared runtime core and
HTTP server already provide the public API; no pgjobdb source changes are included
in this commit.

Deletion hides jobs, revokes leases, and cancels unfinished work. It retains job
identity, history, and artifacts so thin clones continue to work. It does not
reclaim storage or cascade to children, clones, or schedules. This change targets
fresh databases; migration design is outside its scope.

## Scheduler API

Implement [`runtimecore.JobDeletionScheduler`](../pkg/jobdb/runtime/core/delete_jobs.go)
on the scheduler supplied to runtime core:

```go
// Replace Scheduler with the adapter's scheduler type.
var _ runtimecore.JobDeletionScheduler = (*Scheduler)(nil)

func (s *Scheduler) DeleteJobs(
    ctx context.Context,
    req runtimecore.DeleteJobsRequest,
) (jobdb.DeleteJobsResponse, error)
```

The request contains `TenantID`, `JobIDs`, `CreatedBefore`, `MetadataEquals`,
`All`, `DryRun`, and `Now`. Runtime core validates the public request, translates
metadata predicates, and supplies its current time. The mandatory `Scheduler`
and `WorkflowRuntime` interfaces are unchanged. Until the adapter implements this
capability, runtime core returns `jobdb.ErrJobDeletionUnsupported` and HTTP returns
501 for deletion requests.

## Storage and transaction behavior

Add a persistent visibility marker to scheduler job records, such as a nullable
hidden timestamp. SQLite uses `hidden_at_ns` and fresh database format 6;
PostgreSQL can use its own appropriate representation.

Selection and mutation must be atomic in one transaction:

1. Select only visible jobs in the requested tenant. Selectors intersect: job IDs
   are ORed, creation time must be strictly before `CreatedBefore`, and metadata
   uses the existing listing semantics (predicates ANDed, values within a
   predicate ORed). `All` selects all visible jobs in that tenant and cannot be
   combined with selectors. Metadata paths address application metadata.
2. For a dry run, return the match count with zero deletions and no writes. A dry
   run does not reserve matches for a later request.
3. Hide every selected job and revoke all its leases. Cancel and archive
   unfinished jobs, using completion status `cancelled` and detail `job deleted`.
   Preserve already-terminal outcomes. Use `Now` for mutation timestamps.
4. Return `MatchedCount` and `DeletedCount`. Successful non-dry-run counts are
   equal. Already-hidden jobs, missing IDs, and duplicate IDs do not add matches.
   Any failure rolls back both visibility changes and lease revocation.

Serialize deletion with lease acquisition, renewal, completion, and other
lease-authorized mutations through the adapter's transaction/locking rules.
After deletion commits, existing tokens must fail validation and cannot complete,
reschedule, or submit children through the deleted job's authority. Deleted jobs
must remain ineligible for polling and direct acquisition. A racing operation
that commits before deletion may succeed; deletion must revoke the resulting
lease and preserve any terminal outcome that already committed.

Do not implement bulk deletion as paginated listing followed by independent
per-job requests. It must cover the full selection and roll back as a unit.

## Listing and retained history

- Default `ListJobs` excludes hidden jobs **before pagination**, including queries
  by exact job ID and queries that select archived jobs.
- Honor the new `runtimecore.ListJobsRequest.IncludeHidden`. When true, include
  both hidden and visible jobs while applying all other filters. This does not
  restore execution eligibility. Shared `GetJobRun` uses this flag internally
  when looking up history.
- `ListScheduleRuns` excludes hidden jobs before pagination. Deleting a run does
  not delete its schedule.
- Keep scheduler `GetJob`, chapter reads, artifact reads, and clone-source access
  available. Preserve dependency facts and original job identities. Idempotent
  submission must not recreate or unhide a deleted job.
- Retain all chapter and artifact data, including shared prefixes. Existing
  clones must replay after their source is hidden, and creating a new restart
  from a hidden source must work. No chapter-log or blob-store API changes are
  required, and deletion must not purge those stores.

## Verification

Adapt the scenarios in
[`pkg/workflow/job_deletion_test.go`](../pkg/workflow/job_deletion_test.go) to the
PostgreSQL test harness. Cover tenant isolation, exclusive cutoff boundaries,
metadata conjunctions, dry runs, duplicate/missing IDs, repeated deletion, and
selections larger than a listing page. Verify hidden filtering and
`IncludeHidden` with pagination.

Also test transaction rollback and persistence after reopening, revoked lease
operations, acquisition/completion races, preserved terminal outcomes, schedule
run visibility, and no cascade. Replay a clone after hiding its source, then
restart from that hidden clone and read shared artifact bytes. Direct history
inspection must still work throughout.

The SQLite rollback test in
[`delete_jobs_test.go`](../pkg/jobdb/runtime/sqlite/delete_jobs_test.go) and the
runtime-core delegation tests in
[`delete_jobs_test.go`](../pkg/jobdb/runtime/core/delete_jobs_test.go) provide
additional reference cases. JobDB's integration and race suites pass for its
built-in backends; PostgreSQL needs its own adapter verification.

See the [user guide](GUIDE-JOB-DELETION.md) for CLI usage and
[`openapi/jobdb-runtime.yaml`](../openapi/jobdb-runtime.yaml) for
`POST /v1/tenants/{tenantId}/jobs/delete` and its typed metadata request format.
