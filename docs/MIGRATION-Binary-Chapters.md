# Migrating SQLite databases to binary, reference-only chapters

This release makes a hard storage and Go API break. Existing SQLite databases
must be converted offline before the new runtime opens them. There is no live
mixed-format reader or automatic startup rewrite.

The versions are SQLite schema 9, chapter format 2, and execution protocol 4.
`ExecutionArtifact.Data` is removed. Persisted artifacts carry a logical name and
a durable key with a known size; artifact bytes remain in artifact storage.
Chapters are raw protobuf, including typed execution and reconstruction records.
Public HTTP chapter responses still use JSON and artifact references.

## Before running the converter

Stop every server, worker, CLI, and other writer using the database. Take a
consistent backup of the database and its artifact directory. Include committed
WAL data using SQLite's backup API or a verified offline checkpoint/copy; copying
only a live main database file is insufficient. Keep the backup for rollback.

The converter supports schema **8** and local artifact files. Versions 6/7 and
remote blob providers are rejected; upgrade an older database using the prior
release first, and make remote artifacts available as a verified local copy
before using this tool. Do not point old binaries at a converted database.

Allow disk space for the existing database, its backup, and a rewrite transaction
journal/WAL that can approach the original chapter volume. Conversion processes
one chapter at a time, but source JSON/base64 decoding still needs buffers
proportional to the largest old chapter. It does not load a whole job history.

## Build, inspect, rewrite, verify

From this JobDB checkout:

```sh
go build -o jobdb-migrate-chapter-artifacts ./cmd/jobdb-migrate-chapter-artifacts

./jobdb-migrate-chapter-artifacts check \
  --db /path/jobdb.db --blob-dir /path/jobdb.db.blobs > check.json

./jobdb-migrate-chapter-artifacts rewrite \
  --db /path/jobdb.db --blob-dir /path/jobdb.db.blobs > rewrite.json

./jobdb-migrate-chapter-artifacts verify \
  --db /path/jobdb.db --blob-dir /path/jobdb.db.blobs > verify.json
```

`--blob-dir` defaults to `<db>.blobs`. `check` and `verify` open SQLite read-only;
`rewrite` uses an exclusive write transaction. Offline ownership is still an
operator requirement: an exclusive transaction does not exclude every WAL reader.

Reports go to stdout even on failure; errors go to stderr and the exit status is
nonzero. Inspect the exit status as well as the JSON. The report includes source
and target versions, chapter/body sizes, artifact reference counts, materialized
artifacts, repaired wait/reference counts, and payload-limit violations. At most
100 violation details are retained; `payloadViolationCount` gives the total.

The tool verifies referenced artifact size and SHA-256, including linked story
history, and compares an inline copy when present. Keyless inline artifacts get
new durable keys. Missing or corrupt keyed artifacts block conversion even when
an inline copy exists. Unknown owned fields/versions also block conversion;
application fields named `data` or `artifacts` are preserved.

Rewrite commits chapter bodies, known derived input references, active wait
hashes, and the new format marker together. On failure, the database transaction
rolls back. New blobs are journaled before publication; rerunning `rewrite`
cleans migration-owned orphan files after checking committed references. It
preserves committed artifacts after an interrupted successful commit. Keep the
manifest files with the blob directory until recovery completes. A repeat run
against schema 9 validates without rewriting chapters.

The database file will generally **not shrink** after a successful rewrite;
SQLite retains free pages. Disk reclamation is a separate offline `VACUUM` or
compact-copy operation, with its own temporary disk requirement. Conversion does
not compact automatically.

## Application-payload limit

The default is **262,144 bytes (256 KiB) per chapter**, summed across its application
input/output and nested reconstruction payloads. The reserved chapter input
field also counts. Metadata, error envelopes, lifecycle descriptors, protobuf
framing, artifact references, and artifact contents are excluded. This is not a
limit on the encoded chapter or the HTTP response.

Zero selects the default, positive values set a limit, and negative values are
invalid. SQLite/core expose `Config.MaxChapterApplicationPayloadBytes`; toy and
SQLite have `WithMaxChapterApplicationPayloadBytes`. The JobDB CLI and converter
accept `--max-chapter-application-payload-bytes`.

If `check` reports legitimate historical application payloads over the default,
choose an explicit higher value and use it consistently for check/rewrite/verify
and the deployed runtime. For example:

```sh
./jobdb-migrate-chapter-artifacts rewrite --db /path/jobdb.db \
  --max-chapter-application-payload-bytes 2097152
```

A running workflow that attempts an oversized chapter permanently fails with
`chapter_application_payload_too_large`. The rejected payload is not recorded;
JobDB records a compact terminal error and suppresses workflow retries/catches.
External completions follow the same lifecycle transition. Stale leases/waits
cannot fail a newer execution. Submissions rejected before creation return the
size error without publishing a job or reserving its identity. Direct low-level
append errors leave the current lease/history usable.

HTTP returns 413 with typed size details. Errors include exact actual/allowed/
excess bytes and bounded JSON diagnostics: contributing fields, largest paths,
array sizes, counts, and complete/partial/non-JSON status. JSON scalar values and
artifact contents are not included. Root member sizes plus remainder partition
complete object payloads; nested subtree sizes overlap and must not be summed.

## Client cutover and rollback

Apply the [c2j source patch and guide](MIGRATION-C2J-Reference-Only-Chapter-Artifacts.md),
rebuild compatible clients, and deploy them with the converted database. Discard
cached pending-task handles and rediscover them: the wait identity encoding has
changed. No Cortex-specific source change is required for this field.

Rollback requires restoring the consistent pre-conversion database/artifact
backup and old binaries together. There is no downgrade converter. Avoid new
writes until verification and client smoke tests pass.

See [validation results](VALIDATION-Binary-Chapters.md) for the incident-copy
rehearsal, test coverage, and downstream baseline failures.
