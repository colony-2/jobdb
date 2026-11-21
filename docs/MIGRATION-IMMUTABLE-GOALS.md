# Immutable-goal SQLite manual migration

Check an existing database without changing it:

```sh
jobdb sqlite check-migrate --db /absolute/path/jobs.db
```

Supply `--db` explicitly, before or after the subcommand. The command refuses
`--sqlite-dsn` and `JOBDB_SQLITE_DSN` overrides rather than selecting another
database. A missing file is an error; checking never creates it. An existing
empty file returns `EMPTY` without initialization. Runtime serving through
`jobdb sqlite` remains available with its existing arguments.

Checking opens the selected literal path with SQLite `mode=ro` and query-only
access, in one consistent transaction. It does not start the runtime, worker,
HTTP server, chapter/blob/configuration stores or secret providers. It does not
normalize the format marker or write schema/data. Live WAL content participates
in the snapshot; the checker does not use SQLite's immutable-file shortcut.

## Report and exit status

Standard output contains a `GoalMigrationReport` JSON object. The exported
`sqlite.CheckGoalMigration(ctx, path)` function returns the same report.

| Verdict | Meaning | Exit |
| --- | --- | --- |
| `CURRENT` | Valid retained-job format 13; no conversion needed. | 0 |
| `MIGRATED` | Manual conversion committed, with failure-job IDs in closure plans. | 0 |
| `EMPTY` | Existing empty storage; no conversion or initialization occurred. | 0 |
| `CONVERTIBLE` | Supported legacy input passed the implemented preflight without notices. | 0 |
| `CONVERTIBLE_WITH_NOTICES` | Supported input passed with accepted losses or operational notices. | 0 |
| `BLOCKED` | Inspection completed and found unsupported schema/data or integrity failures. | Nonzero; `sqlite.ErrGoalMigrationBlocked`. |
| `ERROR` | Inspection could not complete, for example a missing/unreadable/corrupt file or cancellation. | Nonzero; original execution error preserved. |

The report identifies the source and intended target format 13, all tenants,
hidden and visible row counts, deterministic primary origins, saved success
producers/closure relocation, exact removed secondary relationships and their
timestamps, affected pending waits/preparations, and active-member IDs for
overlap drain. Creation/update discovery-time derivations are explicit. Error
issues identify affected goals/jobs/waits or inconsistent schema objects.

Failure plans set `createFailureRecord` without inventing a failure job ID.
Only the converter allocates and commits a migrated terminal record.
Preflight neither recomputes winners nor re-evaluates frozen settled bindings.
An unpublished preparation does not create a discoverable goal. Archived failed
attempts with no active successor remain valid open recovery gaps.

The validator checks the supported frozen base schema, optional format-11
lease-event extension, required format-12 events, additive append columns/events, schedule-free inputs
and preserved dormant historical schedule data, optional selected-submission
receipts, SQLite integrity/foreign keys, captured ownership and primary origins,
closure witnesses/snapshots, dependency subscriptions and settled producer
references, and preparation/receipt identity and descriptors. Unknown dependent
objects referencing the removed goal tables block conversion planning. Unrelated
application/chapter/configuration tables remain untouched. Execution chapters
and external artifact bytes are not reconstructed or inspected by this checker.
Stored UTF-8 is checked before JSON decoding so corrupt byte sequences cannot
silently become replacement characters in a saved message or identity.

## Accepted conversion direction and current limits

Primary membership wins. Conversion drops secondary associations and their
future completion effects, preserving each job's actual primary identity.
Pending waits and preparations retain their original targets even when a
secondary producer is lost. Historical cross-primary successful producers remain
the exact result producers, referenced from the target's retained origin.
Original failure snapshots retain historical IDs, including dropped members.

Live overlaps may convert. Every fresh admission must remain blocked while any
retained member is nonterminal; conversion alone cannot certify that overlaps
drained or old writers stopped. Consumer retirement order is Cortex → c2j → PostgreSQL, then JobDB contract
removal. These consumer retirements are published, including independent
PostgreSQL v0.0.27 acceptance. This revision removes JobDB attachment APIs.
PostgreSQL backend conversion and pin update follow shared contract publication.
Input/artifact attachments are independent.

Fresh initialization uses format 13. Both runtime constructors reject formats 11
and 12 with `*sqlite.MigrationRequiredError`, matching `sqlite.ErrMigrationRequired`
through `errors.Is` and including `SourceFormat`, the
available database `Path`, and the manual commands. Refusal happens before
chapter/blob/configuration/secret initialization. Constructors never normalize,
reset or convert legacy storage; incomplete format-13 definitions are rejected.

Stop all serving processes and writers, including embedded CLI hosts, then take
a consistent SQLite backup containing committed WAL data. Keep external blob
stores and separate secret files with their normal backups. Run the check and
review its per-tenant losses, affected pending references and remaining overlaps.
Then run:

```sh
jobdb sqlite migrate --db /absolute/path/jobs.db
```

`sqlite.MigrateGoals(ctx, path)` is the corresponding Go operation. The command
requires the explicit existing filesystem path and rejects DSN overrides, just
like checking. It does not initialize any ancillary stores. It takes SQLite
exclusive conversion control, reruns the full inspector on locked inputs, copies
every original job field and rowid, marks deterministic origins, preserves exact
saved producers and snapshots, and creates marked migrated failure jobs. It
verifies retained facts, schema and integrity, removes both old tables, and
advances `user_version` last in the same transaction. Existing chapters, input
artifacts, receipts, preparation descriptors, waits/subscriptions, lease events,
unrelated tables and application job indexes/triggers remain retained.

Any failure or cancellation rolls back data, definitions and format marker.
Rerunning on a valid format 13 returns `CURRENT` without duplicate failure jobs.
An empty file is not converted; ordinary empty initialization remains the runtime
constructor's responsibility. A saved check report never bypasses locked checks.
Exclusive acquisition may fail while serving handles remain open, including WAL
handles. Close them and rerun; do not force reset or delete database files.

After conversion, start compatible services and retain the `MIGRATED` report.
Hidden origins and closure jobs still govern identity, replay and admission.
Protected failures can be read without execution chapters and replay their exact
message without allocation or unhiding. They are never worker executions.
Actual overlap drain and exclusion of obsolete writers require separate cutover
evidence. Toy and SQLite advertise their enforced local guarantees. Those flags
do not establish consumer cleanup, exclusion of old writers or overlap drain.
Backup restoration may discard writes made after the backup. Local tests do not
establish a production preservation percentage or deployment result.
