# Recurring scheduling removed

JobDB no longer exposes recurring cron/interval schedules in its Go APIs,
workflow facade, HTTP API, toy runtime, SQLite runtime, or PostgreSQL backend.
The scheduling CRUD/trigger/run-list methods, types, metadata, and lease
preflight logic are removed. Normal job availability, execution rescheduling,
retries, deadlines, dependencies, leases, and task alternates remain supported.
General job metadata helpers now live in `pkg/jobdb/metadata.go`.

## SQLite

Existing databases and their jobs, chapters, artifacts, configuration, goals,
and leases are retained. The job-table definition and format marker are
unchanged. Startup neither requires nor creates `jobdb_schedules`; an old
unused table is simply ignored. No runtime compatibility layer or automatic
cleanup is installed.

For installations that never used schedules, optional offline cleanup is:

```sh
# Stop processes using this database, then run from the JobDB checkout.
python3 scripts/drop-unused-sqlite-schedules.py /path/to/jobs.db
```

The script opens an existing database, verifies the schedule table is empty
and no jobs have schedule metadata, and drops only `jobdb_schedules` and its
indexes in a transaction. It leaves job data and the format marker untouched.
Rerunning it is safe. The new runtime opens databases before and after cleanup.
No production database is modified by building or testing this change.

## PostgreSQL

PostgreSQL format 11 requires a fresh database. There is no migration from
formats 10 or earlier. New installations have no `schedules` table, no schedule
columns in `job_facts` or `native_jobs`, and no schedule functions. Native
submission no longer accepts a schedule argument. Installation and verification
use the new format; ordinary job execution remains available.

## Consumers and validation

Cortex has no direct recurring-schedule API usage. c2j needs its three unused
local-runtime forwarding methods removed when adopting the new JobDB version;
[C2J-Remove-Schedule-Wrappers.patch](C2J-Remove-Schedule-Wrappers.patch) contains
that change. Remove schedule methods/types from downstream mocks as needed.

Publish the revised JobDB dependency before updating pgjobdb's pinned version.
To validate both local checkouts before publication, use a temporary Go workspace:

```sh
mkdir /tmp/jobdb-removal-validation
cd /tmp/jobdb-removal-validation
go work init /path/to/jobdb /path/to/pgjobdb
cd /path/to/pgjobdb
GOWORK=/tmp/jobdb-removal-validation/go.work go test ./...
```
