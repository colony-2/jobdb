# c2j migration: reference-only JobDB execution artifacts

Status: JobDB implementation and migration executable are available in this
working tree. This is a required c2j source/dependency migration for the hard
cutover. Companion: [operator guide](MIGRATION-Binary-Chapters.md).

Apply the local [C2J-Reference-Only-Artifacts.patch](C2J-Reference-Only-Artifacts.patch)
to c2j, then update its JobDB dependency to this change. The patch is based on
c2j `f72955c2f206d93419c59f0594cab93db2e589f7` and includes reference-validation
tests. It was applied and tested in an isolated c2j worktree; the main c2j
checkout has not been changed.

```sh
# From the c2j checkout; adjust the path to this JobDB checkout.
git apply ../jobdb/docs/C2J-Reference-Only-Artifacts.patch
```


The same offline migration also replaces JSON/base64-wrapped chapter storage
with raw protobuf and typed execution/reconstruction messages. JobDB handles
this internally: c2j does not decode the storage protobuf directly. The public
chapter/execution JSON view remains available, with artifact references instead
of inline artifact data.

## Required source change

JobDB removes `ExecutionArtifact.Data`. Execution-artifact entries carry
`Name` and a required durable `Key`; bytes are available through the artifact
reader API. `ExecutionSnapshot.Data` remains application input/output JSON and
must not be removed or confused with the artifact field.

In `pkg/swfutil/execution_tasks.go`, `executionTaskIO` currently does:

```go
size := int64(len(a.Data))
if a.Key != nil {
    size = a.Key.SizeBytes
}
```

Remove the inline-data fallback. Use the required key's size and report an
invalid reference rather than silently rendering size zero for a missing key.
A concrete implementation is to have `executionTaskIO` return `(*jobdb.TaskIO,
error)` and propagate that error from `ExecutionTasks` for both input and output:

```go
for _, a := range s.Artifacts {
    if a.Key == nil {
        return nil, fmt.Errorf("execution artifact %q has no stored key", a.Name)
    }
    if err := a.Key.Validate(); err != nil {
        return nil, fmt.Errorf("execution artifact %q: %w", a.Name, err)
    }
    if a.Key.SizeBytes < 0 {
        return nil, fmt.Errorf("execution artifact %q has unknown stored size", a.Name)
    }
    out.Artifacts = append(out.Artifacts, jobdb.ArtifactInfo{
        Name: a.Name, SizeBytes: a.Key.SizeBytes, Key: a.Key,
    })
}
```

Retain the logical artifact name. It can differ from `Key.Name`, which addresses
the stored artifact. Do not fetch artifact contents to calculate a display size.
The new persisted representation requires a known size; the additional check is
needed because general-purpose `ArtifactKey.Validate()` currently accepts `-1`.

This was the only production read of execution-artifact `.Data` found in the
source audit at c2j `f72955c2f206d93419c59f0594cab93db2e589f7`. The same fallback
exists at Cortex's pinned c2j revision
`6ecd59dcfa7f02fccfa8c25e95e4b03a3251b293`. The implementation validation recompiled the affected c2j packages with this
change; see [validation results and baseline failures](VALIDATION-Binary-Chapters.md).

## What stays the same for c2j

- Application input/output remains in `ExecutionSnapshot.Data` / `TaskIO.Data`.
- Task workers continue using `TaskData.GetArtifacts()` and artifact readers.
- Inspection and cached-output paths can continue constructing lazy artifacts
  from `ArtifactKey`. Existing code in `pkg/worker/workflow/control.go` and
  `task_history.go` already follows that pattern.
- Artifact viewing/download continues through stored keys and the artifact API.
- c2j must not reconstruct inline byte fields, prefetch every artifact during
  chapter inspection, or create its own parallel history representation.

JobDB's updated client performs lazy artifact reconstruction for replay,
continuation, waiting-task input, and observer callbacks. That client update is
mandatory: the old `snapshotData` implementation uses inline bytes and cannot
correctly read reference-only histories.

## Upgrade procedure

1. Update the JobDB dependency to the release containing binary typed chapter
   storage, reference-only artifacts, lazy reconstruction, format checks, and
   chapter-size configuration. Remove the fallback above and update any
   fixtures/composite literals that set
   `ExecutionArtifact.Data`.
2. Rebuild c2j workers/CLI and applications embedding c2j with the compatible
   dependency. Do not mix old and new clients against a converted database.
3. Stop all users of an existing SQLite database, back up its database/blob
   storage, then run the separate JobDB migration executable's check, rewrite,
   and verify commands. See the parent plan for failure/rollback behavior.
4. Start the new runtime/clients. Discard cached waiting-task handles and
   rediscover them: rewriting wait records and their identity encoding can change
   their generation hashes. JobDB owns this conversion and its replay comparisons.
5. Use the default 256 KiB application-payload budget per chapter unless the
   migration report identifies legitimate larger application payloads requiring
   an explicit higher limit. Input/output and nested reconstruction application
   payloads share that budget. JobDB structure, metadata, protobuf framing,
   artifact references, and artifact contents are excluded. Move large
   application content to artifacts; total encoded chapter size can exceed the
   budget without violating it.

There is no intermediate client state or dual-format fallback. The offline tool
understands the supported old representation; running c2j understands only the
new one after cutover. Ordinary startup must not attempt the data rewrite.

## Oversized application-payload failures

JobDB turns a running workflow's payload-limit violation into a permanent
job failure with code `chapter_application_payload_too_large`. It owns retry
suppression, terminal persistence, and JSON size analysis. c2j must not implement
a competing failure transition or retry this as a transient transport error.

The error message includes actual/allowed bytes, the contributing task/payload,
and a short JSON breakdown when available. Structured error attributes add
payload contributions, largest JSON paths/arrays, and analysis completeness.
Retain those attributes in existing error/detail projections and display the
message through existing failure reporting. Do not fetch artifacts or payloads
to recompute the statistics; no specialized Cortex change is required for the
message to remain useful.

## Validation

- Extend `TestExecutionTasksKeepsBlockInvocationsAndTaskAttemptsDistinct`, which
  already uses a keyed artifact without inline bytes. Verify the displayed size
  comes from the key and logical/stored names may differ.
- Cover missing/invalid keys and valid zero-byte artifacts. Missing references
  must be errors, not empty-artifact fallbacks.
- Run recipe execution, stored/cached output, replay, restart/continuation,
  unfinished-task story, pending-input/review, artifact viewing, and download
  regressions with the new JobDB dependency and a converted fixture database.
- Verify that story/diagnostic artifact enumeration does not open artifact
  contents; readers still produce the exact bytes when requested.
- Verify c2j's chapter decoding and replay against converted binary storage
  through the public JobDB API; no caller should depend on the old database JSON
  wrapper or interpret protobuf storage bytes as a JSON chapter.
- Exercise an oversized task result and pending external completion: JobDB
  produces a terminal failure, c2j shows the byte-count/JSON summary, and worker
  retries do not repeat the rejected operation. Preserve structured error
  attributes through existing story/detail projections.
- Remove obsolete inline-artifact fixture fields, while preserving tests of
  application payload `Data` and separate submission/upload payloads.

## Cortex

No separate Cortex source migration is expected for this field. At audited
revision `8a4c3515efcb44064f53554d24cb4a50de3a1e60`, the exporter passes execution
JSON through, the browser downloads it as an opaque blob, and artifact viewers
use a separate keyed endpoint. The exporter has no downstream consumers requiring
its previous embedded byte representation. Rebuild Cortex with compatible
JobDB/c2j dependencies and run its existing tests as part of deployment.
