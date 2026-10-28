# Binary chapter cutover: validation

Tested on Linux ARM64 on 2026-10-28. This report records local validation; it does not imply a deployment or an
in-place migration of user data.

## Incident database rehearsal

Created consistent SQLite backups of `/src/problem3/jobdb.db` under `/tmp` and
converted the copies. The final converted copy is
`/tmp/jobdb-incident-final.db`. The original remains schema 8 and was not rewritten.
Existing artifact files were read for hash verification; the converter created
zero artifacts for this database.

| Measurement | Result |
| --- | ---: |
| All chapters converted | 4,346 |
| Total old chapter bodies | 22,136,444,510 bytes |
| Total new chapter bodies | 155,354,228 bytes |
| Largest new chapter body | 1,262,333 bytes |
| Artifact references verified | 12,192 |
| Active wait hashes repaired | 4 |
| Chapters with derived input references repaired | 48 |
| Rewrite elapsed time, excluding backup | 120.4 seconds |
| Rewrite peak process RSS | 685,012 KiB (about 669 MiB) |
| Independent verification elapsed time | 17.6 seconds |

The 256 KiB default detects **53** historical oversized application payloads.
The largest measured application payload is **1,260,816 bytes**. Rewrite and
verification used an explicit **2,097,152-byte** limit. A subsequent read-only
check at the default limit still reported the same 53 violations. The default
was not changed to accommodate this fixture.

For tenant `c2`, job `3LIsi58lrjR29QBCS9dMcUbaUBK`, the converted database contains
1,231 chapters totaling **46,296,175 stored bytes**, down from
19,272,006,285 bytes. The actual HTTP endpoint was exercised with `startOrdinal=0`
and no end ordinal, using the new SQLite runtime and HTTP server in an isolated
process. It returned all **1,231 chapters**, HTTP 200, **52,889,608 response bytes**,
in approximately **1.5 seconds**, with **356,860 KiB peak process RSS** (about
349 MiB). Response bytes were streamed to a temporary file for counting.

These are single local rehearsals with warm filesystem caches, not a throughput
benchmark or a fixed memory bound. Listing an unbounded history still builds a
response proportional to application data and metadata. SQLite file size also
remains large until a separate compaction reclaims its free pages.

## Regression coverage

The tests cover:

- Raw protobuf chapters and typed reconstruction/restart records; rejection of
  old wrappers, protocol versions, inline artifact fields, and unknown protobuf
  fields; exact large-number handling and a pinned deterministic wait identity.
- Artifact keys, logical versus stored names, zero-byte artifacts, lazy readers,
  and publishing assigned keys to live consumers as well as replay.
- Exact/default/custom payload limits, combined input/output and reserved chapter
  input, excluded metadata/artifacts, JSON statistics, redaction, scan/depth
  budgets, cancellation, and compact diagnostics.
- SQLite, toy, and HTTP workflow terminal failures for task output, final output,
  swallowed errors, and external completions. Retrying/catching does not swallow
  the failure; replay does not execute the rejected task again.
- Low-level append rejection without publication or upload reads; corrected
  submissions/restarts using the same requested identity; oversized schedules
  rejected before publication.
- Generic core external-completion fencing and injected claim, validation,
  append, and scheduler-completion failures; permanent nonretryable status.
- Offline check/rewrite/verify, idempotence, corruption and payload-limit rollback,
  keyless artifact materialization, empty artifacts without backing bytes,
  pending-task completion after conversion, stale handle rejection, derived
  reference repair, nested reconstruction, unknown legacy fields, blob cleanup
  after rollback, and journal recovery before/after a simulated commit boundary.

The full integration suite passed, including PostgreSQL adapters and runtime
conformance. Race coverage passed across workflow, remote, core, SQLite, toy,
migration, codecs, payload diagnostics, and CLI packages. Backend suites were
rerun after the final preflight fix; the affected workflow cases were rerun
under the race detector as well. HTTP tests passed after OpenAPI regeneration.

Commands used for the complete suite and race coverage:

```sh
go test -tags=integration ./... -count=1 -timeout=240s

go test -race ./pkg/workflow ./pkg/jobdb/runtime/sqlite \
  ./pkg/jobdb/runtime/core ./pkg/jobdb/runtime/remote \
  ./pkg/jobdb/runtime/toy/... ./pkg/jobdb/migration \
  ./pkg/internal/runtimecodec ./pkg/jobdb ./pkg/jobdb/cli \
  -count=1 -timeout=240s

GOTOOLCHAIN=go1.26.1 go run ./cmd/jobdb-api-snapshot \
  -check api/jobdb.public.txt
```

The normal Go environment is 1.27.1. Snapshot generation/check used the CI
Go 1.26.1 toolchain because the repository's existing `x/tools` package loader
cannot load Go 1.27 export data. Targeted codec, migration, core, and workflow
cutover tests were also run under Go 1.26.1.

## c2j compatibility

Applied the local [c2j source/test patch](C2J-Reference-Only-Artifacts.patch) in an
isolated worktree at `f72955c2f206d93419c59f0594cab93db2e589f7`, with a local JobDB
module replacement. `pkg/swfutil`, `pkg/worker/workflow`, and `pkg/story/...`
passed. Reference validation tests cover missing keys, invalid keys, unknown
sizes, and zero-size artifacts with distinct logical/stored names.

The `pkg/input/...` run still has these existing failures:

- `TestPendingInputSQLitePaginationBeyondBackendPageLimit`
- `TestReviewSQLiteAttachmentsReplayAndStaleSubmission`
- `TestTimedInputPublicAPIs`

The same three tests fail against the pre-cutover JobDB commit `1077c32`
(`Make job lists compact by default and deprecate detail projections`), using
the same c2j worktree and an alternate module file. This comparison establishes
that they are not new chapter-cutover regressions; it does not establish that
they predate the compact-list change. The paginated c2j discovery path still
reads `ExecutionState.TaskWait` from list results and skips jobs when it is nil,
which is incompatible with compact lists. It needs to obtain wait details from
`GetWaitingTask` or job detail. The c2j input suite is therefore
**not fully green**. The cutover did expose one separate missing-live-artifact-key
regression; publishing the assigned key during staging fixed it, and the affected
review flow then passed.

No changes were made to the main c2j checkout. Cortex source was not changed;
its clients still need compatible JobDB/c2j dependencies when deployed.
