# Plan: binary protobuf chapters, reference-only artifacts, and bounded size

Status: implemented in the working tree. Existing databases require an offline
cutover; the incident database was tested using copies, not rewritten in place.
See the [operator migration guide](MIGRATION-Binary-Chapters.md),
[c2j migration guide and local patch](MIGRATION-C2J-Reference-Only-Chapter-Artifacts.md),
and [validation results](VALIDATION-Binary-Chapters.md).

The implementation uses SQLite schema **9**, chapter format **2**, and execution
protocol **4**. The design below records the scope and acceptance criteria;
these implementation details qualify it:

- Execution identities, snapshots, artifacts, versions, timestamps, and retained
  records have explicit protobuf fields. Extensible descriptors, dependencies,
  and lifecycle values use recursive typed protobuf
  `ExecutionValue` messages. Number lexemes preserve precision; these are not
  embedded JSON strings or `google.protobuf.Struct` floating-point numbers.
  Execution errors use the existing typed protobuf task-outcome messages.
- Public execution APIs still present JSON. The existing public `RawMessage`
  marshaling boundary compacts JSON; the size validator counts the application
  bytes it receives and never normalizes them to fit. Storage conversion keeps
  application payload fields opaque. Wait identity normalizes the public
  presentation before hashing deterministic protobuf.
- SQLite and toy publish compact terminal size failures atomically. Generic
  runtime core uses its existing independent chapter/scheduler completion
  protocol with lease fencing and recovery; it does not invent a transaction
  spanning independently provided stores. Core marks these completions explicitly
  nonretryable, including failures of external task completions.
- The converter supports source schema 8 with local blob files, and validates
  schema 9 on repeat runs. It materializes genuinely keyless inline artifacts.
  A broken existing key is a blocker even if inline bytes remain; repair that
  inconsistency before conversion rather than guessing artifact ownership.
- Reports include chapter/body byte totals, artifact reference/materialization
  counts, identity repairs, and bounded application-size violations. They do not
  promise separate counts for every embedded execution-record kind. Memory is
  bounded by individual source records, decoder buffers, and the wait identity
  map, not by the target payload limit.


## Decision

Make a hard cutover to execution histories whose artifact entries contain
references and no inline artifact bytes. Remove `ExecutionArtifact.Data` from
the Go API and the corresponding `data` property from persisted execution
artifacts. Existing databases must be converted with a separate offline SQLite
migration executable before the new runtime opens them. Clients do not support
mixed or intermediate representations.

Use the same cutover to store chapter bodies as **raw binary protobuf**, with
typed protobuf fields for all JobDB-owned chapter/execution structures. Remove
both the outer JSON/base64 envelope and the inner execution-record JSON and
reconstruction JSON strings. Application-owned payloads remain opaque bytes;
JSON application content is not converted into a JobDB-defined object schema.

Add a configurable maximum application-payload size per chapter, defaulting to
**256 KiB (262,144 bytes)**. Measure only the application's payload bytes, excluding
JobDB structure, metadata, and artifact references. This is independent of
artifact size and of SQLite's existing `MaxInlineArtifactBytes` setting.

The work belongs primarily in JobDB: recording, artifact reconstruction, format
validation, migration, and size enforcement. c2j needs a JobDB dependency update
and a small source change described in the companion guide. No separate Cortex
migration request is planned: it does not interpret execution-artifact `Data`,
and its raw exporter has no downstream consumers that need compatibility.
Cortex must nevertheless be rebuilt with the updated JobDB dependency when the
runtime deployment cuts over.

## Evidence and scope

The read-only investigation of `/src/problem3/jobdb.db`, tenant `c2`, job
`3LIsi58lrjR29QBCS9dMcUbaUBK`, found:

| Measurement | Bytes/count |
| --- | ---: |
| Chapters | 1,231 |
| Stored chapter bodies | 19,272,006,285 bytes |
| Execution-record JSON | 14,436,481,586 bytes |
| Base64 artifact contents inside execution JSON | 14,403,756,092 bytes |
| Execution JSON excluding those base64 strings | 32,725,494 bytes |
| Artifact contents also stored separately | 10,802,813,604 bytes |
| Largest encoded chapter body | 59,725,523 bytes |

Embedded artifact contents are 99.77% of execution JSON. Most belong to 608
successive session tar snapshots. The same contents are stored separately as
artifacts. JSON encoding expands artifact bytes into base64, then JobDB wraps
its protobuf chapter in another base64 JSON envelope.

Remove artifact content from every JobDB-defined execution-snapshot location:

- Input and output snapshots in execution chapter bodies.
- Deferred reconstruction records in `__execution_prefix_v3` metadata, including
  metadata on final outcome chapters.
- Reconstruction records carried in restart descriptors and their supported
  nested record containers.
- External task completion results and records generated by scheduler/runtime
  completion paths, not just ordinary workflow task results.

This is a typed transformation. Do not recursively delete arbitrary fields named
`data`. `ExecutionSnapshot.Data`, application inputs/outputs, error payloads, and
user metadata remain intact. Application payloads may contain their own fields
named `artifacts` or `data`; those are not execution-artifact fields.

Artifact-table `inline_data` is also distinct: small artifacts can remain stored
there under the existing storage threshold. The change eliminates bytes inside
chapter records, not the artifact store's small-object optimization. Large
artifacts remain permitted and are retrieved through the artifact API.

Whole-response buffering remains a separate follow-up opportunity. This plan
removes redundant storage encodings but does not bound total encoded chapter
size or the size of an unlimited history read.

## Binary protobuf storage

The current storage path is:

```text
execution record JSON (including base64 artifact data)
  -> protobuf ExecutionChapter.record_json
  -> base64 protobuf bytes in a JSON envelope
  -> chapter body column
```

SQLite already declares the chapter body as `BLOB`; the in-repository PostgreSQL
chapter store declares it as `bytea`. The remaining JSON requirement comes from
JobDB's chapter-store abstraction: `storage.ChapterRecord.Body` is
`json.RawMessage`, and chapter construction/append validate `json.Valid`. These
are implementation constraints to remove, not database requirements.

Target storage:

```text
typed ChapterRecord protobuf
  - typed execution record, lifecycle descriptors, errors, and artifact keys
  - typed reconstruction/restart records
  - application payload bytes and typed metadata values
  -> raw protobuf bytes in chapter body column
```

Implement this as part of the same new database format:

1. Make the private chapter-store body contract opaque `[]byte`. Remove JSON
   validation and JSON-specific copying/marshalling at that boundary. The JobDB
   codec validates protobuf structure and execution semantics; the storage
   adapter stores bytes. Update SQLite, PostgreSQL chapter-store, toy, core,
   mocks, and conformance tests consistently.
2. Have the chapter encoder marshal protobuf directly and the decoder unmarshal
   it directly. Remove the chapter path through `encodeProtobufJSONWrapper` and
   `decodeProtobufJSONWrapper`. Do not serialize binary chapter bytes through
   JSON on the way to storage. Scheduler payloads in SQLite already use binary
   protobuf; audit shared wrapper helpers before removing unused ones.
3. Replace `ExecutionChapter.record_json` with typed execution messages. Model
   JobDB-owned identities, versions, timestamps, policies, waits, dependencies,
   error envelopes, snapshot references, and per-kind descriptors/payloads with
   explicit protobuf fields/messages and appropriate variants. Reserve retired
   field numbers/names; do not reinterpret the old `record_json` field number as
   a new message. Retain the strict execution-kind and version validation.
4. Move `__execution_prefix_v3` reconstruction records out of persisted metadata
   strings into a dedicated repeated execution-record field on the chapter.
   Model restart-retained records as typed repeated messages as well. Convert
   all supported nested reconstruction containers; do not leave a JSON execution
   record hidden in a protobuf `bytes` or `string` field. Ordinary user metadata
   continues to use typed metadata values and retains its contents.
5. Keep application-owned snapshot data, inputs, outputs, continuation/restart
   application data, and other opaque extension payloads in `bytes` fields,
   preserving the original bytes where their contract is opaque. Keep JobDB's
   error structure typed while preserving user attributes. Do not route opaque
   JSON through `google.protobuf.Struct` or a generic floating-point map: doing
   so can alter numbers, null/presence distinctions, and byte-based identities.
6. Use direct typed conversions in the persistence/replay path. Remove avoidable
   metadata/record JSON marshal-unmarshal round trips. JSON belongs at explicit
   public API or application-payload boundaries, not between internal storage
   representations.

This describes JobDB-owned structures **within chapter storage**. It does not
require replacing relational job columns/indexes with protobuf blobs or changing
the artifact store's content format.

### Public API and identity boundaries

Keep the HTTP chapter API as its existing JSON presentation, with the separately
planned removal of inline artifact `data`. Keep the public Go execution-record
JSON view usable by existing decoding callers after their dependency update.
Translate typed storage records at these boundaries; do not expose a base64
protobuf blob as the replacement HTTP chapter response. If the public view still
needs the private reconstruction metadata shape, synthesize it there from typed
fields; never persist a second JSON copy. This avoids requiring c2j or Cortex to
understand the internal storage protobuf schema.

Audit every comparison/hash derived from execution serialization. Today replay
compares descriptor JSON bytes, and waiting tasks hash JSON records. Define a
single versioned encoding for JobDB-owned descriptor comparisons and record
identities, shared by live writers, replay, retry reconciliation, and migration.
Use deterministic protobuf serialization with specified field presence and
normalization rules for this protocol version. Deterministic marshaling alone
is not a permanent canonical-encoding guarantee across schema/library changes;
pin the identity contract with fixtures and explicit versioning. Preserve
application-byte hashes where the application bytes are unchanged. Migrating
hashes without changing the corresponding runtime comparisons is insufficient.

Validate unknown versions, record variants, required fields, and unsupported
lifecycle fields explicitly; successful `proto.Unmarshal` alone does not imply
a supported execution record. Define unknown-field handling before freezing the
format, and reject unsupported lifecycle semantics rather than silently dropping
them. Source data the migrator cannot preserve safely must block conversion.

## Target representation and format boundary

Retain the logical artifact name and require a valid durable artifact key:

```go
type ExecutionArtifact struct {
    Key  *ArtifactKey `json:"key"` // Required and non-nil in persisted records.
    Name string       `json:"name"`
}
```

Keep the pointer type to minimize unrelated source changes, but validate it as
mandatory. The key supplies job ID, chapter ordinal, stored name, and byte size;
tenant identity comes from the owning job/runtime context. Preserve the logical
name separately: a prefix/input artifact's stored name can differ from the name
exposed to a task. Do not substitute the current job or ordinal for an explicit
key targeting an ancestor or another permitted job.

Persist the actual, nonnegative byte size in every key. General-purpose
`ArtifactKey.Validate()` currently permits `SizeBytes == -1` for unknown size;
persisted execution references need a stricter check. Writers and migration must
resolve the size before publishing the record, so inspection never needs to open
artifact content just to determine its size.

Use a new execution protocol version, proposed as **4** after the current 3,
and a new database format version, allocated at implementation time after the
current SQLite schema version 8. Use an explicit binary-protobuf/reference-only
format marker as part of that database contract. Fresh databases start in the new
format. The ordinary runtime must reject old or incomplete formats before
serving reads, scheduling work, or applying normal schema migrations; its error
must identify the offline migration command.

The storage decoder accepts only raw protobuf with the new typed records; it
does not autodetect or fall back to JSON-wrapped or `record_json` chapters. The
public JSON decoder rejects an execution artifact with a `data` property,
including null/empty values, or with an absent/invalid key. Old binaries must
reject the new database format/protocol; verify that behavior, rather than relying
on a documentation-only warning.
Legacy decoders belong only in the migration executable/internal migration
package, not in live clients. There is no compatibility flag or mixed-format
serving mode.

The old `__execution_prefix_v3` metadata container is a migration input and,
if required by the public API, a synthesized presentation only. It is absent
from new persisted metadata; typed reconstruction fields hold its records.

The converter must update record versions and embedded header/activation version
values consistently. Keep original producer/consumer provenance separately from
the migration version; do not claim the original records were produced by the
new binary. Preserve all execution semantics. The initial supported migration
source is the current schema and execution protocol v3, including the incident
database. Recognize older known formats explicitly and either provide a tested,
semantics-preserving conversion or reject them with an actionable unsupported-
format error. Never make v1/v2 histories runnable merely by changing their
version numbers.

## Recording and reconstruction changes

1. Separate transient task values/artifact handles from serialized execution
   records. `executionSnapshot` currently materializes artifact bytes in a
   serializable field; replace that arrangement with private staging state.
   Uncommitted local task input and replay-safe reconstruction may use transient
   artifact handles without appending new history or writing during replay.
2. Before persisting a record, assign/reference durable artifact keys and prepare
   uploads for newly created artifacts. Existing references must remain lazy;
   do not download and re-upload them just to build a snapshot.
3. Commit artifact descriptors/content and the referencing chapter using the
   existing publication guarantees. A visible chapter must never reference an
   uncommitted artifact. Preserve retries, conflict detection, and uncertain
   append reconciliation; generated keys/names must be deterministic on retry.
4. Update `appendLocked`, `attachPrefix`, external task completion, restart
   construction, final completion, and all SQLite/toy/core equivalents. Audit
   every writer of `ExecutionChapter` and reconstruction metadata. Removing the
   field from the shared public struct is necessary but not sufficient.
5. Replace `snapshotData`'s `NewArtifactFromBytes(a.Data)` with artifact handles
   backed by the runtime and stored key. Opening/reading an artifact fetches its
   bytes; listing chapters, constructing task values, inspecting names/sizes, and
   rendering task diagnostics must not fetch them. Pass the required runtime and
   tenant context to every reconstruction call site.
6. Cover ordinary replay, external task input, unfinished-task observers, scope
   catch results, final job output, and continuation/restart reconstruction.
   Preserve `TaskData` and artifact reader behavior for application callers.
7. Preserve retention dependencies for referenced artifacts. Hidden/deleted
   ancestor jobs and thin-clone histories must remain readable according to the
   existing lifecycle contract; physical reclamation cannot discard a target
   still needed by a retained history. Validate this before removing the fallback
   inline copy.
8. Update API snapshots, execution-format documentation, generated schemas where
   applicable, and tests. `ListChapters` must return reference-only bodies, not
   rehydrate bytes to simulate the old representation.

## Application-payload size limit

Proposed configuration/API names:

- `DefaultMaxChapterApplicationPayloadBytes = 256 * 1024`.
- `MaxChapterApplicationPayloadBytes` on core, SQLite, and toy runtime
  configuration, with matching constructor options and runtime-server CLI
  configuration.
- Zero/unset selects the default; negative values are invalid; positive values
  set an explicit limit. No unlimited sentinel in this change.
- A typed `ErrChapterApplicationPayloadTooLarge` with structured actual/maximum
  application-payload byte counts and job key/ordinal where known; a stable remote
  error code `chapter_application_payload_too_large` and HTTP 413. Do not include
  chapter contents in the error.

**Measurement:** sum the raw byte lengths of application-payload fields carried
by one chapter. This remains a per-chapter budget: input and output payloads in
the same chapter share it. Count application input/output bytes, snapshot `Data`,
and application-owned continuation/restart/extension payloads. Include those
fields when nested in typed reconstruction records; count each persisted
occurrence once, without counting additional API presentations of the same data.

Use an explicit schema-aware field traversal, shared by writers, migration, and
validation. Do not count every protobuf `bytes` field indiscriminately or recurse
into application JSON looking for fields named `data`. For JSON application
payloads, count their original UTF-8 bytes, including JSON syntax and whitespace,
before any transport escaping/base64 or protobuf framing. Preserve those bytes;
do not normalize application JSON to make it fit.

Exclude protobuf tags/length prefixes, JobDB lifecycle records and descriptors,
error envelopes, timestamps, policies, ordinary metadata, artifact keys/names,
artifact-table rows, and artifact contents. Reconstruction structure itself
does not count; only its application-payload fields do. The final protobuf body
and HTTP JSON response can exceed 256 KiB without violating this limit. There is
no separate total-chapter byte cap in this change.

The rule is the same across core, SQLite, toy, and remote access; a configured
limit is an operator/runtime setting, not a per-request client override.

Exactly the configured number of bytes is accepted; one byte more is rejected.
Apply the check once all application-payload fields, including reconstruction
records, are assembled, before artifact publication or scheduler/chapter
mutations become visible. Check known oversized application payloads before
consuming upload streams where possible. Every path that
can create a chapter must enforce the rule: submission/start, normal append,
external task completion, final completion, restart/clone-added chapters,
scheduled-job creation, and internal checkpoint/prefix writes.

Rejection of the attempted write must leave no partial chapter, completed job
without its final record, advanced ordinal, or changed task wait. For a running
workflow, JobDB then records the separate compact terminal failure described
below. That intentional failure transition must be atomic and fenced; it is not
a partially successful version of the rejected write.

Use the same configured limit and field traversal when validating migrated or
persisted chapters. Do not reject a read based on `length(body)` or overall
protobuf size; those include excluded fields. Do not assume removing artifacts
makes every chapter's application payload fit. Operators can select a higher
limit deliberately; migration never truncates or splits application data to make
it fit. Report application-payload bytes separately from total encoded bytes.

## Workflow failure and payload diagnostics

An oversized application payload is a permanent failure of the running job.
Returning an append error to the worker loop is insufficient: it can leave the
job eligible to run again and repeat the same rejected write. JobDB owns both
the terminal transition and diagnostic generation; c2j only displays the result.

### Specialized terminal path

1. Preserve `ErrChapterApplicationPayloadTooLarge` and its structured details
   through wrapped errors and HTTP clients. Recognize it with typed/code-based
   handling, never a message substring. The authoritative runtime check returns
   the configured limit and actual application-payload count. A worker preflight
   can improve feedback but cannot replace that check or assume the default.
2. Latch this error as fatal in the execution run, cancel active execution
   contexts, and stop producing history. It bypasses task/scope/job retries,
   catch/rewrite rules, continuations, and application code that swallows an
   error. Already committed work remains committed; cancellation does not undo
   external task side effects.
3. Add explicit handling around `executionRun` append/control paths and
   `workerRunner.runExecution`, including final-result preparation and
   `lease.Complete` failures that occur after the normal fatal-error check.
   Adapt `completeExecutionFailure` or a dedicated helper to produce a compact
   application failure (`failed_app`) with stable code
   `chapter_application_payload_too_large`, marked non-retryable. Preserve this
   classification through the public error view and job-run outcome.
4. Persist only the error envelope and bounded typed diagnostics. Do not attach
   the rejected output, its artifacts, pending oversized reconstruction payloads,
   or an execution prefix that reintroduces the failure. The terminal record has
   zero application-payload bytes, so it works even with a tiny configured
   application budget. Diagnostics have their own small fixed size bound and are
   represented as JobDB-owned protobuf fields, not opaque application output.
5. Complete the job and append its terminal record atomically at the next
   committed ordinal under the current lease/generation. Cancel or close pending
   work through existing lifecycle machinery. Preserve ownership checks, conflict
   handling, and uncertain-completion reconciliation; a stale worker cannot fail
   a newer run. An infrastructure error writing the compact failure is still an
   infrastructure error, not proof that the job was durably failed.
6. Give the terminal failure an explicit recorded reason so replay/detail reads
   can return it without requiring the rejected event or omitted transient
   prefix. Never synthesize a successful task result or re-execute the task to
   explain the failure. Inspection and read-only replay do not write or change
   job state, including when a configured limit is lowered later.
7. Cover external task completion too. After validating the current task lease
   and wait generation, convert an oversized completion into the same compact
   job failure through the authorized JobDB lifecycle path. Commit that terminal
   result atomically, without publishing the rejected result, and return its
   structured non-retryable outcome to the producer. A stale/invalid completion
   must never fail the job. This cannot depend on a c2j worker catching HTTP 413
   and inventing its own lifecycle action.

Submission rejected before job creation returns the structured size error and
creates no failed job. A low-level append without execution ownership remains a
validation error; it does not grant the caller permission to terminate a job.
Migration check/verify reports oversized data without changing job status.

### Diagnostic contents

Use a shared analyzer for runtime rejection and offline migration reports.
Always provide these exact counts, even when JSON analysis cannot finish:

- Configured limit, total application-payload bytes, and excess bytes.
- Chapter kind and attempted ordinal; logical task/scope identity, attempt, and
  write phase when available (input, result, final output, or reconstruction).
- Per-application-payload contribution: its structural location, byte count, and
  share of the chapter total. Rank contributors and retain an omitted-fields
  count/byte total when there are too many to display. This explains aggregate
  failures where no individual payload exceeds the limit.

For each analyzed JSON payload, add:

- Root kind and raw byte count; object-member, array-element, and scalar counts,
  maximum observed depth, and the largest string token's encoded byte length.
- The largest object fields/subtrees by original encoded byte span, with escaped
  JSON Pointer paths, value kinds, byte counts, and shares of that payload.
- For large arrays, element count, total encoded bytes, and largest element
  sizes/indexes, so many moderate entries are distinguishable from one huge one.
- A non-overlapping root-member breakdown (or root-array summary), including an
  explicit remainder for omitted members and structural/whitespace bytes. Nested
  ranked subtrees are inclusive and may overlap; label them as such rather than
  adding parent and child sizes into a false total.

For example, the job failure message can say:

```text
Application payload limit exceeded while recording task "summarize" output:
524288 bytes; limit 262144; excess 262144.
output.data contributes 524288 bytes (JSON object).
Largest JSON subtrees: /history 400000 bytes (array, 1200 elements),
/debugText 100000 bytes (string). Store large content in artifacts.
```

Keep structured diagnostics alongside that concise message. Expose them through
the existing job-detail/final-error surface and the returned typed error;
project them into the public application-error attributes with the stable code.
Lists retain a concise failure summary, not the full breakdown. Existing clients
that only show the message must still give users an actionable explanation.

### Bounded analysis and representation

Analyze only counted application-payload fields after detecting a size violation.
Never read artifact contents. Use a token/span scanner over the original bytes;
do not unmarshal a giant JSON document into maps or decode/copy huge string
values just to compute statistics. Count original UTF-8/escaped token bytes, not
character counts or re-marshaled JSON. Preserve duplicate-key occurrences in the
analysis and disambiguate their paths; do not collapse them through a map.

Start with fixed implementation bounds: at most 8 MiB scanned across a rejection,
100,000 tokens, depth 64, 16 payload contributors, and 10 ranked JSON paths in
the emitted report. Cap displayed paths at 160 UTF-8 bytes and the complete
persisted diagnostic at 8 KiB including its message. Rank payload contributors
using exact lengths first, then spend the JSON scan budget on the largest ones.
Check cancellation during scanning; use bounded memory and work even for deeply
nested input, enormous strings, wide objects, and long property names.

Reports carry explicit analysis status: complete JSON, non-JSON/invalid JSON,
partial because a budget was reached, or unavailable. A partial scan must not
claim whole-document JSON validity, exact unvisited counts, or a globally largest
subtree. Distinguish observed statistics from exact totals. For non-JSON data,
return byte counts and structural location without content samples. Analyzer
failure or truncation must never suppress the original size error or prevent
the compact terminal failure; fall back to the exact byte-count summary.

Include field paths and sizes, never scalar values, string prefixes, or raw JSON
snippets. Treat paths as application-controlled text: escape control characters
and cap their length. The report records shape and size, not copies of payload
content.

## Separate offline SQLite migration executable

Add `cmd/jobdb-migrate-chapter-artifacts`, separate from the server and ordinary
runtime startup. Proposed usage:

```sh
# All JobDB processes using this database are stopped; a database/blob backup exists.
jobdb-migrate-chapter-artifacts check \
  --db /path/jobdb.db --blob-dir /path/jobdb.db.blobs

jobdb-migrate-chapter-artifacts rewrite \
  --db /path/jobdb.db --blob-dir /path/jobdb.db.blobs \
  --max-chapter-application-payload-bytes 262144

jobdb-migrate-chapter-artifacts verify \
  --db /path/jobdb.db --blob-dir /path/jobdb.db.blobs \
  --max-chapter-application-payload-bytes 262144
```

Support explicit blob-store configuration rather than assuming every database
uses `<db>.blobs`. Require access to the actual backing store; a database file
alone is insufficient to validate durable artifact references. Initially support
local SQLite plus local blob storage; reject unsupported blob providers clearly
rather than attempting a partial migration. The executable must open SQLite
without invoking the normal runtime constructor or automatic migrations.

### Inspection and conversion algorithm

1. Validate source schema/format and obtain exclusive migration ownership. Require
   an offline database; do not revoke live leases or change job statuses as a
   shortcut. Inspect physical chapter rows for all tenants/jobs, including
   archived, hidden, and retained deleted stories. Resolve story links without
   duplicating inherited chapters into clones.
2. `check` is read-only and traverses the same conversion logic. Report source
   versions, chapter/embedded-record counts, inline bytes, existing and missing
   artifact references, target sizes, and blockers. Include the shared payload
   diagnostics for application-size violations. It must inspect actual
   artifact content when validating hashes, not just trust a table descriptor.
3. Decode the storage JSON wrapper, protobuf chapter, and only the typed
   execution-record containers. Traverse their input/output artifacts and any
   nested supported reconstruction/restart records. Preserve unrelated protobuf
   fields, application JSON precision, metadata, ordinals, timestamps, record
   identities, outcomes, routes, deadlines, and story links.
4. For an artifact with an existing key, resolve the referenced stored artifact,
   including linked/retained histories. Verify size and content digest against
   the inline copy before deleting the copy. Report inconsistent references as
   errors; do not silently redirect them or drop contents.
5. For genuinely inline-only artifacts, create a durable artifact at an explicit
   owning chapter with a collision-free deterministic stored name, assign a key,
   and preserve the original logical name. Reuse a verified matching artifact
   when possible. If a keyed target is missing, fail and require a separate repair
   before conversion, even when an inline copy is available.
   Missing bytes and no recoverable stored target are a hard error. Handle empty
   artifacts explicitly; zero bytes are valid, a missing target is not.
6. Remove the artifact `data` property entirely, map every supported JobDB-owned
   execution structure to typed protobuf fields, and move reconstruction
   metadata/restart records into their typed containers. Write the new format
   versions and encode raw protobuf directly, with no JSON/base64 envelope or
   embedded execution-record JSON. Convert all chapter kinds, including those
   without artifacts; every old chapter has the outer wrapper. Validate every
   target chapter against `MaxChapterApplicationPayloadBytes` using the shared
   application-payload field traversal, not its final binary length.
   Leave the original unchanged if a record cannot be safely converted. A larger
   explicit limit can unblock legitimate larger application payloads; log that
   choice.
7. **Repair identities derived from rewritten records.** External task waiting
   currently hashes the serialized wait record. Recompute those hashes using
   the target runtime's canonical serialization and update scheduler TaskWait
   state and all identified persisted references atomically. Audit input hashes,
   input references, descriptors, restart/preparation fingerprints, and
   idempotency records; update only values actually derived from changed bytes.
   Do not recompute semantic application input hashes indiscriminately. Preserve
   pending routes, ordinals, deadlines, alternates, and retry state. Pre-cutover
   task handles must be discarded and rediscovered after migration.
8. Validate every transformed row and referenced artifact, then mark the database
   as the completed new format. The marker is never advanced on a partial scan
   or conversion. `verify` independently rejects any old execution-artifact
   field/version, JSON wrapper or embedded execution-record JSON, invalid
   reference, size violation, or stale derived wait hash. This is structural
   validation of JobDB fields, not a byte search through opaque user content.

### Atomicity, scale, and operator behavior

Use a database-wide atomic SQLite transaction for the chapter rewrites, dependent
scheduler/fingerprint updates, and final format marker. Traverse by primary key
and release each chapter after conversion; never load an entire job or database
into memory. SQLite's transaction journal can grow on disk; bound page-cache
usage and report required working-space considerations separately from RAM.
Stream/hash large artifact content rather than collecting the artifact corpus.
Document that initial memory bounds are per largest source chapter plus decoder
buffers, not the 256 KiB application-payload limit; the incident's ~60 MB source
chapters must be supported without whole-history buffering.

Stage newly materialized blobs under migration-owned identifiers, make them
available durably before committing references, and retain a manifest for cleanup
on rollback/crash. Never overwrite or delete existing blobs during conversion.
A failed run leaves the database in the old format and recoverable from its
journal; an interrupted run is not a client-supported intermediate state. A
re-run after success validates and exits without rewriting already converted
records. Publish a completed migration report with counts, old/new body bytes,
largest target chapters and application-payload totals, created/reused artifacts,
verification results, and format/limit settings. Do not log artifact or
application contents.

Require a consistent backup of the SQLite database (including any WAL state) and
its backing blobs before rewrite. Use SQLite backup/checkpoint facilities or a
verified offline copy, not a live copy of only the main file. Rollback restores
the database and corresponding artifact state together. Old binaries must not
be used on a successfully migrated database.

Shrinking row values usually makes SQLite pages reusable without shrinking the
file. Make disk reclamation a separate, explicit offline `VACUUM`/compact-copy
step with sufficient disk space; it is not required for format correctness or
for fixing the chapter response size.

## Rollout sequence

1. Implement binary typed chapter storage, reference-only artifacts, lazy
   readers, size enforcement, converter, and regression coverage together.
   Allocate/freeze the actual format versions and identity encoding.
2. Update c2j according to the companion guide. Rebuild c2j, Cortex, workers,
   servers, and any backend adapters with mutually compatible JobDB versions.
   A PostgreSQL/pgjobdb deployment needs its own data-conversion path before this
   hard cutover; the SQLite tool must not be presented as covering PostgreSQL.
3. Rehearse on a copy of `problem3` and representative databases, including
   pending external tasks, restarts/clones, and deleted ancestors. Do not use the
   only copy of a user's database as a test fixture.
4. Stop clients, servers, workers, and background schedulers; take verified
   backups; run check, rewrite, and independent verification offline.
5. Start only the new binaries with the chosen application-payload limit.
   Rediscover waiting task handles. Resume pending work and verify artifact
   downloads and replay.
6. Optionally reclaim SQLite file space after verification. Keep the pre-cutover
   backup until the deployment has been validated.

No online dual-reader phase, no feature flag to emit old inline records, and no
separate Cortex consumer migration document are part of this rollout.

## Required tests and acceptance criteria

### Representation and runtime behavior

- Inspect stored rows: raw bytes decode directly as the new `ChapterRecord`,
  every execution record/prefix/restart container uses typed messages, and no
  retired wrapper/record-JSON field is emitted. Cover every chapter variant,
  including non-execution chapters without artifacts.
- Run chapter-store conformance tests with binary bodies containing zero bytes
  and invalid UTF-8/JSON. Test malformed/truncated protobuf, unsupported versions
  and variants, old JSON-wrapped input, and missing required semantic fields at
  the JobDB codec boundary. Remove obsolete tests requiring valid storage JSON.
- Round-trip every execution kind and descriptor, field presence, timestamps,
  duration units, policies, errors, dependencies, reconstruction order, and
  opaque application bytes. Include large integers, explicit nulls/empty values,
  application strings that resemble retired containers, and metadata precision.
- Add fixed identity-encoding fixtures and migration-to-live-runtime comparisons
  for waits, descriptors, retries, and uncertain append reconciliation. Prove
  replay of converted histories does not report false descriptor drift.
- Verify the HTTP/public Go view remains consumable by updated c2j and Cortex:
  execution JSON is a presentation of typed storage and artifact keys; binary
  storage does not appear as a base64 protobuf response field.
- New execution artifacts have no `Data` field or serialized `data` property.
  Reject old fields even when empty/null; reject missing/invalid keys. Preserve
  application snapshot `data` fields and arbitrary user payloads unchanged.
- Exercise task inputs/results, scopes/catch outputs, external answers,
  unfinished-task observation, terminal results, deferred prefixes, restart
  descriptors, and subsequent writes after migration. Inspect persisted bytes,
  not just the public summary.
- Replay and continue with artifact bytes equal to the originals across toy,
  SQLite, core adapters, and HTTP. Verify artifact name/key/size identity and
  existing error behavior when content cannot be opened.
- Instrument artifact access: listing/decoding chapters, task-handle discovery,
  names/sizes, and diagnostics perform zero artifact-content opens. Opening a
  selected artifact retrieves its contents lazily. Replay may open artifacts
  actually required by replay-safe orchestration; it must not eagerly hydrate
  every historical snapshot.
- Preserve deterministic retries, uncertain append handling, task completion
  generation checks, restart prefix semantics, deleted-ancestor access, and
  artifact retention. Include concurrent/stale completion regression tests.

### Limit enforcement

- Test default, explicit higher/lower limits, zero/default, negative rejection,
  exactly-at-limit, and one-byte-over-limit cases using application-payload bytes.
  Cover combined input/output and nested reconstruction payloads, UTF-8 byte
  counts, preserved JSON whitespace, empty/absent fields, and transport expansion.
- Accept a chapter with exactly 256 KiB of application payload even though
  protobuf framing and HTTP JSON make its representations larger. Increasing
  metadata, lifecycle structure, or artifact references must not consume the
  application budget. Test chapters with no application payload and more than
  256 KiB of excluded metadata.
- Test all append/submit/complete/restart/schedule/internal paths and backend
  parity. Large artifact content is accepted independently of this limit;
  oversized application payloads, including those in reconstruction records,
  are rejected. Metadata and reconstruction structure alone do not trigger it.
- Low-level rejections produce the typed/remote error and leave scheduler/history
  state unchanged. The owning workflow then commits only the intentional compact
  terminal failure. No artifacts are published solely for a rejected chapter and
  no retry loop repeatedly attempts the same oversized history write.

### Terminal failure and diagnostic regressions

- Across toy, SQLite, and remote execution, reject oversized task input, local
  result, external completion, final output, and nested reconstruction payloads.
  Verify one compact durable failure, `failed_app`, stable error code and stats,
  closed pending work, and no rejected data/artifact publication.
- Enable retries at task, scope, and job levels; add catch/rewrite handlers and
  deliberately swallowed callback errors. The job still fails without rerunning
  the offending task or allowing later workflow steps to commit.
- Cover failures after the ordinary fatal-error check, concurrent writers,
  stale external completions, lease loss, and uncertain terminal commits. A
  duplicate failure attempt reconciles; a stale attempt cannot overwrite a newer
  result. Test a transient failure while saving diagnostics and the minimal
  fallback path without falsely reporting a durable completion.
- After a worker process restart, verify detail/read-only replay of the recorded
  failure returns its reason without task execution, artifact reads,
  missing-prefix errors, or writes. An explicit user-requested job restart still
  follows normal restart policy. Test a one-byte application budget: diagnostic
  error envelopes still fit.
- Check exact field totals and encoded JSON spans for objects, arrays, root
  scalars, many small members, a giant string, whitespace, escaping, Unicode,
  duplicate keys, and aggregate input/output overflow. Verify inclusive ranked
  subtrees are not summed and remainder accounting balances when complete.
- Exercise malformed/non-JSON, oversized tokens/keys, excessive nesting, scan
  cancellation, each analysis bound, and diagnostic failures. Totals remain
  exact, partial observations are labeled, and the terminal failure still works.
  Assert the diagnostic cap, bounded allocations, and absence of scalar values
  or payload snippets in the error, report, logs, and API responses.
- Verify error code/structured statistics survive HTTP and persisted error
  round-trips, appear in job details, and are not expanded into compact lists.

### Migration

- Test existing matching references, inline-only materialization, key/name
  differences, zero-byte artifacts, missing blobs, digest mismatches, references
  across retained/cloned stories, and all embedded record containers.
- Test repeated rewrite/verify, rollback on a late failure, disk-full/blob-write
  failures, process interruption, and format-marker ordering. Neither old nor new
  clients may serve a partially converted database.
- Test pending external tasks: completion succeeds after rediscovery, stale
  pre-migration handles fail safely, and alternates/deadlines/retries retain their
  original meanings. Include active-job records after workers are stopped.
- Preserve integer precision, unrelated JSON named `data`/`artifacts`, chapter
  ordering, job identities/statuses, and reference reachability. Validate
  unsupported source versions rather than silently upgrading their semantics.
- On a copied incident database, convert all 1,231 target chapters, verify all
  4,880 artifact references and content integrity, and enumerate any remaining
  chapters whose application payloads exceed 256 KiB. Finalize only if all fit
  the selected application-payload limit; report total encoded sizes separately.
  Measure resulting database-body bytes, HTTP response size, peak memory, and
  artifact-content opens. The target response should be tens of MB rather than
  ~14 GB; establish measured values rather than assuming an exact total from the
  base64-only subtraction.

Run the full integration suite and appropriate workflow/runtime race suites,
plus c2j artifact/replay/input/story regressions against the new dependency.
Cortex's existing story, artifact-view/download, and raw chapter-export tests
remain useful deployment validation without a bespoke migration change there.
