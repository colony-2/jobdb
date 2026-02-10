# Chapter Type Migration Guide (API Users)

This guide describes how to migrate to the new **required** top-level `chapter_type` field in Strata chapter envelopes. The change is **breaking** and **not backward compatible**.

## Summary of Breaking Changes
- All chapter envelopes must include a top-level `chapter_type` string.
- The server rejects envelopes that omit `chapter_type` or use an unexpected value.
- `chapter_type` is now the authoritative semantic role of a chapter, instead of inferring from `meta.task_type` and ordinal.

## New Required Field
Each chapter body must be JSON with a top-level `chapter_type`:

```json
{
  "chapter_type": "TaskAttemptOutcome",
  "meta": {
    "version": 1,
    "ordinal": 5,
    "task_type": "echo",
    "worker_id": "worker-1",
    "created_at": "2026-02-10T00:00:00Z",
    "input_hash": "..."
  },
  "payload_kind": "App",
  "payload": { "ok": true }
}
```

## Allowed `chapter_type` Values
- `JobStart`
- `JobAttemptOutcome`
- `TaskAttemptOutcome`
- `RestartExtra`

## How to Set `chapter_type`
Use these rules when emitting chapters:
- Job start chapter (ordinal 0): `JobStart`
- Job attempt outcome chapters (job worker completion): `JobAttemptOutcome`
- Task attempt outcome chapters: `TaskAttemptOutcome`
- Restart extra chapters appended via `RestartJob`: `RestartExtra`

## API User Actions
1. **Update all chapter writers** to include `chapter_type`.
1. **Remove any reliance on implicit classification** based on `meta.task_type` or ordinal.
1. **Add validation** on the client side to ensure `chapter_type` is present and correct before writing.
1. **Update any envelope decoding logic** to parse the new field.

## Restart Behavior
`RestartExtra` chapters are now explicitly labeled. If you pass `ExtraTaskInput` / `ExtraTaskOutput`, the extra chapter will be stored as:
- `chapter_type`: `RestartExtra`
- `meta.task_type`: `__restart_extra__`

If your workflow expects to interpret `RestartExtra` as a task, treat it as a task chapter in your client code.

## Determinism Errors
If a cached chapter’s input hash mismatches:
- The engine returns `ErrWorkflowNotDeterministic`.
- For task chapters, the enriched error type `TaskInputMismatchError` is returned and includes cached data.

If you depend on this, use `swf.UnexpectedChapter(err)` to access the cached payload and metadata.

## Example Migration Checklist
- Update any custom workers that write chapters manually.
- Update any tests that deserialize envelope JSON (add `chapter_type`).
- Update any mocks/fixtures to include `chapter_type`.

## Compatibility Notes
- There is **no backward compatibility**. Envelopes without `chapter_type` will be rejected as non-deterministic.
- Replays against legacy chapters will fail until data is migrated.

## Suggested Data Migration (If You Have Stored Legacy Chapters)
If you have pre-existing chapters without `chapter_type`, you must rewrite them with the new field or archive them and re-run jobs. There is no automatic upgrade path.

