# Protected terminal job read contract

The shared API reads protected goal-failure records without execution chapters.
Toy and SQLite atomically publish these records, first closure and waiter
settlement in their scheduler transaction. Origins and closure facts live on
retained jobs. Both advertise their enforced guarantees through
[job-goal capabilities](GUIDE-JOB-GOAL-CAPABILITIES.md). Initial Cortex/c2j cleanup is complete and the JobDB attachment surface is
removed. Later consumer integration and deployment remain separately owned.

`JobInfo`, `JobSummary`, `JobRunSummary` and the core `StoredJob` port carry
`RecordKind` and `TerminalFailure`. HTTP detail and summaries expose
`recordKind` and `terminalFailure`.

| Kind | Meaning |
| --- | --- |
| `EXECUTION` or an absent legacy discriminator | An ordinary execution with its existing history and output semantics. |
| `GOAL_FAILURE` | A protected scheduler record representing explicit permanent goal failure. |
| `MIGRATED_GOAL_FAILURE` | A protected record retaining a historical explicit failure during conversion. |

Job type, metadata, an encoded error and missing chapters never confer
protection. Unknown kinds or malformed protected facts return
`ErrInvalidJobRecord`. Protected records retain their cell name, cell ID,
repository, goal and saved failure. They have completed lifecycle status,
`CompletionStatus == "failed"`, exact failure message as completion detail,
an archive timestamp and summary/run job type `jobdb.goal_failure`. They have
no runnable route, lease or dependency wait. Shared validation requires a
`goal_failed` snapshot matching the goal and message, with a nonzero failure
time and the same valid nonblank UTF-8 message limit as `FailGoal` (16 KiB).
Validation preserves saved member arrays, including their nil/empty shape;
it does not recompute historical membership.

`GoalInfo.FailureJobID` (`failureJobId` on HTTP) names the exact retained
protected job. It is separate from `GoalFailure.JobID`: that older snapshot
field must retain its historical meaning and cannot be overwritten with a
newly allocated record ID. The readers preserve both independently. Toy sets
`FailureJobID` on publication and returns the same ID on equal-message replay;
SQLite also returns its committed protected record ID.

Core and toy detail/list readers validate explicitly marked protected rows.
Shared `GetJobRun` and toy's direct run reader return the saved outcome and
ownership with zero start, no execution entries and no attempts. Core detail
and shared run inspection skip chapter reads only after validating the
protected discriminator and facts. Ordinary archived core jobs still require
their actual final chapter; a missing log remains an integrity error.

`JobInfo.Data` is an error-bearing accessor: `GetData` returns nil bytes and
a `JobFailedError` caused by the saved `GoalFailedError`. Shared
`GetJobRunResponse.GetOutput` returns nil data and the same typed error.
`errors.Is` matches both `ErrJobFailed` and `ErrGoalFailed`; `errors.As` can
recover the exact saved goal failure. No successful empty output, task data
or artifacts are fabricated. HTTP detail carries null data, and the remote
client reconstructs the typed error from the validated saved failure. The
client rejects protected wire detail containing task data.

Fresh restart of a protected source and workflow execution replay return
`ErrJobRecordOperationUnsupported` before history cloning or workflow
callbacks. Existing accepted restart receipts are checked first so adding the
reader guard does not invalidate exact accepted-request replay. The HTTP error
code for unsupported protected operations is `job_record_operation_unsupported`
(501); corrupt record errors use `invalid_job_record` (500).

The reader fixtures exercise core ports, shared helpers and workflow replay.
`runtimetest.RunTerminalFailureConformance` requires actual protected publication
through native toy/SQLite and both HTTP adapters. It covers saved inspection,
empty history, typed output, nonleaseability, waiter settlement, late success
and hidden replay. Public SQLite checks also cover rollback/reopen, lost committed
HTTP responses, converted records and controlled independent-writer commit
orders. Toy stages failure insertion, closure and waiter settlement under its
engine mutex. These are local source guarantees; consumer cleanup and deployment
writer exclusion remain separately coordinated.

## Why membership alone is insufficient

An immutable goal field can locate attempts, but cannot distinguish an open
goal after ordinary failure from an explicitly permanently failed goal. It
also cannot preserve a historical successful producer whose primary membership
belongs to a different goal. The approved design removes independent goal
tables by retaining origin and protected closure facts with jobs, including
exact historical producer references and failure snapshots. Toy uses retained job
facts with a separate wait-only store; fresh and converted SQLite format 13 omit
both legacy lifecycle tables. Public attachment declarations and mutation routes are removed after completed
initial consumer cleanup.
