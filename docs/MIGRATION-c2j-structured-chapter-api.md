# c2j Migration: Structured SWF Chapter API

This migration applies after the SWF protobuf storage change that removes custom
chapter storage and adds `swf.StructuredWorkflowRuntime`.

## Current c2j Usage

The current c2j checkout uses SWF direct chapter APIs only in
`cmd/c2j/internal/swfruntime`:

- `chapter_visibility_runtime.go` wraps `PutChapter`, then polls `GetChapter`
  until the written chapter is visible.
- `runtime_test.go` uses `swf.StoredChapter`, `swf.PutChapterRequest`, and a
  fake `delayedChapterRuntime` only to test that visibility polling behavior.
- Other `ChapterType` and `PayloadKind` references in c2j are c2j story-service
  fields, not `swf-go` stored chapter APIs.

Normal job submission, worker execution, runtime opening, and engine use do not
need to change.

## What Changes In swf-go

Legacy direct chapter access remains available but is deprecated:

- `swf.StoredChapter`
- `swf.PutChapterRequest`
- `WorkflowRuntime.GetChapter`
- `WorkflowRuntime.ListChapters`
- `WorkflowRuntime.PutChapter`

New direct chapter code should use:

- `swf.StructuredWorkflowRuntime`
- `swf.NewStructuredWorkflowRuntime(runtime)`
- `swf.StructuredChapterRecord`
- `swf.PutStructuredChapterRequest`
- one of `JobStartChapter`, `TaskAttemptOutcomeChapter`,
  `JobAttemptOutcomeChapter`, or `RestartExtraChapter`
- one of `ApplicationOutputOutcome`, `AppErrorOutcome`,
  `SystemErrorOutcome`, or `TimeoutOutcome` for task/job outcomes

The legacy `WorkflowRuntime` interface still exists because the SWF engine uses
it internally. c2j should keep `Handle.Runtime swf.WorkflowRuntime` unless and
until the engine itself moves to the structured interface.

## Recommended c2j Migration

1. Keep `Handle.Runtime swf.WorkflowRuntime` unchanged for engine wiring.
2. Optionally add a structured handle for callers that need direct chapter
   access:

```go
type Handle struct {
	Runtime           swf.WorkflowRuntime
	StructuredRuntime swf.StructuredWorkflowRuntime
	Engine            swf.SWFEngine
	cleanup           func() error
}
```

Populate it after wrapping visibility:

```go
runtime := withChapterVisibility(baseRuntime)
structuredRuntime := swf.NewStructuredWorkflowRuntime(runtime)

return &Handle{
	Runtime:           runtime,
	StructuredRuntime: structuredRuntime,
	Engine:            engine,
	cleanup:           cleanup,
}, nil
```

3. Keep `chapterVisibilityRuntime.PutChapter` for the SWF engine path, but add
   structured methods so direct structured writes also get the visibility poll:

```go
func (r *chapterVisibilityRuntime) PutStructuredChapter(ctx context.Context, req swf.PutStructuredChapterRequest) error {
	if err := swf.NewStructuredWorkflowRuntime(r.WorkflowRuntime).PutStructuredChapter(ctx, req); err != nil {
		return err
	}
	return r.awaitChapterVisible(ctx, req.Ref)
}

func (r *chapterVisibilityRuntime) GetStructuredChapter(ctx context.Context, ref swf.ChapterRef) (swf.StructuredChapterRecord, error) {
	return swf.NewStructuredWorkflowRuntime(r.WorkflowRuntime).GetStructuredChapter(ctx, ref)
}

func (r *chapterVisibilityRuntime) ListStructuredChapters(ctx context.Context, req swf.ListChaptersRequest) ([]swf.StructuredChapterRecord, error) {
	return swf.NewStructuredWorkflowRuntime(r.WorkflowRuntime).ListStructuredChapters(ctx, req)
}
```

4. Update tests that directly construct chapters to use a concrete body:

```go
chapter := swf.StructuredChapterRecord{
	Ordinal:  ref.Ordinal,
	TaskType: "task",
	Body: swf.TaskAttemptOutcomeChapter{
		Outcome: swf.ApplicationOutputOutcome{
			Output: swf.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		},
	},
}

err := runtime.PutStructuredChapter(ctx, swf.PutStructuredChapterRequest{
	Ref:     ref,
	Chapter: chapter,
})
```

5. Do not write custom/manual SWF chapters. If a test previously used
   `ChapterType: "Manual"`, use a supported body such as
   `TaskAttemptOutcomeChapter` with `ApplicationOutputOutcome`.

## Mapping From Legacy Shapes

| Legacy `StoredChapter` shape | Structured shape |
| --- | --- |
| `ChapterType: "JobStart"`, `PayloadKind: "App"` | `JobStartChapter{Input: ApplicationInputBytes{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "App"` | `TaskAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "AppError"` | `TaskAttemptOutcomeChapter{Outcome: AppErrorOutcome{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "SystemError"` | `TaskAttemptOutcomeChapter{Outcome: SystemErrorOutcome{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "Timeout"` | `TaskAttemptOutcomeChapter{Outcome: TimeoutOutcome{...}}` |
| `ChapterType: "JobAttemptOutcome"`, same payload kinds as task outcomes | `JobAttemptOutcomeChapter{Outcome: ...}` |
| `ChapterType: "RestartExtra"`, `PayloadKind: "App"` | `RestartExtraChapter{Output: ApplicationOutputBytes{...}}` |

There is no structured replacement for custom chapter types. That storage shape
has been removed.

## Validation

After updating c2j, run a compile-only pass against the local SWF checkout:

```sh
cp go.mod local.swfgo.mod
cp go.sum local.swfgo.sum
go mod edit -modfile=local.swfgo.mod -replace=github.com/colony-2/swf-go=/path/to/swf-go
go test -modfile=local.swfgo.mod ./... -run '^$'
```

Then run the swfruntime package tests:

```sh
go test -modfile=local.swfgo.mod ./cmd/c2j/internal/swfruntime
```
