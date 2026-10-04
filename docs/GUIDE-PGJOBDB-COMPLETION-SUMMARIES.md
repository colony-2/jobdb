# pgjobdb adoption of completion summaries

Update the JobDB dependency to the release containing `JobSummary.CompletionStatus`
and `CompletionDetail`, then run the runtime conformance tests. No pgjobdb files
were changed as part of this implementation.

In the inspected pgjobdb checkout (`d957cc6`), `storedJobFromDetail` already copies
the native completion category and detail into `runtimecore.StoredJob.Completion`.
The shared JobDB core now projects that snapshot into job and schedule-run
summaries. JobDB also updates the remote codec and `GetJobRun` summary. No new
scheduler method, query, storage column, or chapter transaction is required.

Verify the following against PostgreSQL and its remote endpoint:

- Job and schedule-run listings expose the stored final category and exact detail;
  unknown categories survive, and missing information remains empty.
- A failed attempt awaiting retry has no terminal completion. Final success does.
- A cancellation request alone has no completion snapshot; finalized cancellation
  exposes the scheduler's stored category and reason.
- Cancelling an already-finalized job preserves its original completion category
  and detail (rejecting the late request or treating it as a no-op is acceptable).
- Existing archived records work without a backfill, and listing does not read
  chapters/artifacts or replay workflows. Status filters and pagination are unchanged.

JobDB's `RunWorkflowRuntimeConformance` includes a `completion_summaries` case for
these projections, retry and chapter/finalization boundaries, and pagination.
The core and remote unit tests additionally cover absent and unfamiliar fields.
If a pgjobdb-specific cancellation or mapping path violates these semantics,
correct it in pgjobdb; the dependency update alone cannot repair stored details
that were previously overwritten.
