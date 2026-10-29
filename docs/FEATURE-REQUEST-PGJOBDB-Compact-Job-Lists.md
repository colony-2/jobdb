# pgjobdb: implement compact job listing without per-job enrichment

## Requested behavior and upstream contract

JobDB's ordinary `ListJobs` / `POST /jobs/query` now defaults to compact summaries.
Only `BlockingWait` and `OtherGoalIDs` remain deprecated and unloaded. Restore
stored `CompletionDetail`, full opaque `ClientPayload`, `ExecutionState`, and
`WaitFor` on ordinary lists without flags or detail reads. This supersedes the
initial compact migration's removal of those four fields. See the companion
[migration guide](MIGRATION-Compact-Job-Lists.md) and
[removal plan](REMOVAL-Deprecated-Job-List-Details.md).

Keep full application metadata, identity/type/parent/schema/primary goal,
status/completion status and detail, stored route and task coordinates, run policy,
stored prerequisites, timestamps, cancellation flag, and full client payload with
its revision. Populate the new `BlockingWaitSummary` with only `WaitID` and
optional `Deadline`, from the current persisted pending dependency wait. An
elapsed deadline is no longer blocking. Do not evaluate the dependency graph.

The upstream scheduler port now has
`runtimecore.StoredJob.BlockingWaitSummary *jobdb.DependencyWaitSummary`.
It has no new interface method or request flag. Populate this field for lists;
keep full `BlockingWait` and additional memberships in `GetJob`. JobDB's `JobInfo` now
also exposes `CompletionStatus`, `CompletionDetail`, `WaitFor`, and `NextRoute`.
The core maps them from the existing scheduler detail row. Upgrade the JobDB
dependency to a release containing these additions; no unreleased version is
prescribed here.

## Original expensive path

The original pre-compact pgjobdb implementation used:

- `pkg/pgjobdb/runtime/internal/runtimeadapter/jobs.go`: `Scheduler.ListJobs`
  calls `BlockingDependencyWait` once per result, including archived jobs and
  jobs with no wait. A 100-row page incurs 101 client/database queries before
  counting work inside SQL functions.
- `pkg/pgjobdb/goal_discovery.go`: `BlockingDependencyWait` calls
  `dependency_wait_info`, which invokes `_evaluate_wait` and can inspect target
  goals/jobs, goal memberships, and candidate terminal states.
- `pkg/pgjobdb/pgjobdb.sql`: `list_native_jobs` projects `native_jobs`, including
  correlated `other_goal_ids`, and joins `job_client_state` to retrieve payload
  content. `to_jsonb(n)` also serializes fields irrelevant to the compact list.
- `pkg/pgjobdb/list_jobs.go`: the shared `JobDetail` decoding path loads detail
  data that the upstream list response will discard.

These observations describe source structure, not a production query-plan or
latency measurement. The retained status/availability contract still requires
lightweight dependency-state/prerequisite-completion checks.

## Implementation request

1. Keep the compact list SQL projection and page selection over `jobs` /
   `jobs_archive` with existing joins to `job_facts` and `job_client_state`.
   Carry completion detail, task input/output ordinals/hash/resume job type, run
   policy, and stored prerequisite IDs through both branches. Archived task state
   must use stored `final_*` snapshots. Return full `client_payload` alongside
   revision from the existing join as raw JSON, without converting it to JSONB.
   Preserve arbitrary application keys, arrays, JSON null, and large integers;
   do not prune or truncate. Extend `JobListSummary` and the adapter's `StoredJob`
   mapping. No new columns, joins, queries, or task/history reads are required.
2. Join the current `dependency_waits` row to obtain wait ID, stored state, and
   deadline. Emit `BlockingWaitSummary` only for a live pending wait whose
   deadline has not elapsed; omit it for cancelled/archived jobs. Use the same
   observation time for status and this deadline projection. Do not call
   `dependency_wait_info`, `_evaluate_wait`, or candidate enumeration to populate
   list output.
3. Remove the per-result `BlockingDependencyWait` loop. Do not hide it behind the
   compact JobDB mapper: discarded enrichment still costs database work.
4. Avoid the `other_goal_ids` projection. Keep the `GoalIDs` filter matching
   primary and additional memberships through indexed `EXISTS`/joins without
   duplicates and before pagination. It must not require returning memberships.
5. Preserve all other filters, hidden-job behavior, ordering/cursors, metadata,
   completion status, and available-route semantics. The stored `nextRoute`
   must remain distinct from the effective due-alternate route used for matching.
6. Keep detail, dependency-discovery, task execution, and schedule-run inspection
   APIs intact. Prefer a dedicated internal list projection/decoder over making
   their shared `JobDetail` or `native_jobs` representations incomplete.

Target one set-based query per page, or a small fixed number of batched queries.
Do not issue one query per returned job. Filtering and pagination belong in SQL;
materialize only the selected page plus a lookahead row. Metadata is intentionally
unrestricted in this change, so large metadata and low-selectivity filters can
still cost more than the page size alone suggests.

## Acceptance tests

- Add `runtimetest.RunCompactListConformance` to the adapter's conformance suites.
  Run existing completion, available-route, task-discovery/completion, and
  job-run inspection tests against the updated JobDB.
- Verify the two empty enriched fields and all restored stored fields over direct
  and HTTP listing, including active/archived/child/keyed rows, exact failure text,
  Unicode/whitespace, unknown status, empty detail, and retries without completion.
- Cover arbitrary payload namespaces, arrays, JSON null, large integers, and the
  maximum payload size; payload and revision must come from the same query.
- Cover complete task coordinates, run policy, archived final state, and stored
  prerequisites without resolving targets.
- Verify detail reads still return payload, policy/task coordinates, memberships,
  prerequisites, completion text, and evaluated blocking explanations.
- Test page sizes 1/100/200 and selective filters with pending goal waits; assert
  query counts do not grow with the number of returned jobs. Confirm list queries
  do not call `_evaluate_wait`, candidate enumeration for diagnostics, chapter
  storage, or artifact storage.
- Exercise wait deadlines, persisted terminal waits, legacy prerequisite lists,
  active leases, cancellation, hidden jobs, metadata equality, parent/root filters,
  same-timestamp cursor ties, and due alternates. Compare eligibility with leasing.
- Capture representative `EXPLAIN (ANALYZE, BUFFERS)` output to check filter and
  limit placement. Update indexes only where the measured plans justify them.

Coordinate the JobDB dependency release, pgjobdb server deployment, and downstream
c2j dependency upgrade. Older servers may still omit the restored fields; do not
add per-row detail loops in c2j, Cortex, or Pulse to compensate.
