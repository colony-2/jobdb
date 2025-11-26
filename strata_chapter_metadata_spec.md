# Strata Task Chapter Metadata & Determinism Spec

Problem: Strata chapters currently persist only raw `TaskData` bodies. When `DoTask` sees an existing chapter for an ordinal, it blindly returns that chapter instead of re-running the task, even if the input data for this invocation differs. We need workflow-level metadata that allows us to prove a cached chapter corresponds to the same inputs, or fail with a deterministic error.

## Goals
- Persist task payloads alongside workflow metadata (step, capability, worker) in a single chapter body format.
- Attach an input hash to every `TaskData` so cache hits are validated before reuse.
- On `DoTask`, reject cached chapters whose stored input hash does not match the current input and surface a `workflow was not deterministic` error.
- Keep artifacts stored as Strata artifacts, not inlined into the new envelope.

## Data Model (Chapter Envelope)
- Chapter body becomes a JSON envelope instead of raw payload bytes:
  ```json
  {
    "meta": {
      "version": 1,
      "job_id": "<story id>",
      "ordinal": <int>,
      "task_type": "<capability name>",
      "worker_id": "<swf worker id>",
      "created_at": "<RFC3339>",
      "input_hash": "<hex sha256>",
      "source": "swf"
    },
    "payload": <legacy task payload bytes, base64-encoded>
  }
  ```
- Artifacts stay on the chapter (as today); they are not embedded in the envelope. The input hash must incorporate artifact references (see hashing).
- `version` allows future format changes; `source` tags who wrote it.

## Hashing
- Compute `input_hash` as `sha256` over:
  - Serialized task data bytes (`TaskData.GetData().ToBytes()`).
  - Deterministic encoding of artifacts: concatenate each artifact’s URI/digest/filename in sorted order by URI. If no artifacts, the artifact segment is empty.
- Represent the hash as lowercase hex. The same hashing helper must be used both when writing and when validating cache hits.
- The hash is computed on the task input to this ordinal (the data handed into `DoTask`), not on the task output.

## Write Path
- When `DoTask` (or a `TaskHandle.Finish`) is about to persist a chapter:
  - Compute `input_hash` from the input `TaskData`.
  - Wrap the output `TaskData` bytes in the envelope above, filling metadata fields from the current runner context (`job_id`, `ordinal`, `task_type`, `worker_id`, timestamp).
  - Save the chapter with `WithBytes(envelope)` and attach artifacts as today.
- Initial job creation (`StartJob`) and restarts (`RestartJob`) should also envelope their initial chapter so every ordinal participates in deterministic validation.

## Read Path / Cache Validation
- At the start of `DoTask`, compute `input_hash` for the provided input.
- Fetch the chapter for the target ordinal:
  - On cache hit: decode the envelope. If `meta.input_hash` is absent or does not match the computed hash, return an error (`workflow was not deterministic`), do not return the cached payload.
  - On cache hit with matching hash: unwrap the payload bytes back into `TaskData` and return without executing the task worker.
  - On miss: run the task worker and persist output as above.
- If envelope decoding fails, treat it as a non-deterministic error (same error path) to avoid silently accepting malformed legacy data.

## Errors
- Add a well-known error (e.g., `ErrWorkflowNotDeterministic`) whose message includes `workflow was not deterministic` and mentions the ordinal/task type. `DoTask` should propagate this error directly.

## Backward Compatibility / Migration
- Legacy chapters without the envelope/hash should be treated as non-deterministic when encountered (force a rerun or restart), rather than silently reused. This prevents false cache hits with unknown provenance. Migration tooling can re-wrap old chapters if needed.

## Testing
- Cache hit with same input: `DoTask` returns without executing and does not error.
- Cache hit with different input: `DoTask` errors with `workflow was not deterministic`.
- Cache hit with same data but different artifacts: hash mismatch triggers the deterministic error.
- Legacy chapter (no envelope or missing hash): deterministic error is raised.
- Envelope round-trip: writing then reading preserves payload bytes and artifacts unchanged aside from added metadata.
