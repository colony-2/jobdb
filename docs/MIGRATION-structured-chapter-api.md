# Migration: Structured Chapter API

This migration is for consumers that use direct SWF chapter APIs. Generic
chapter storage is deprecated and will be removed. Update to the latest
`swf-go` release, then move any direct chapter reads or writes to the structured
chapter API.

High-level job, worker, artifact, and engine APIs are not part of this
migration unless they manually construct or inspect stored chapters.

## What Is Going Away

Stop using the generic chapter representation for new code:

- `swf.StoredChapter`
- `swf.PutChapterRequest`
- `WorkflowRuntime.GetChapter`
- `WorkflowRuntime.ListChapters`
- `WorkflowRuntime.PutChapter`
- arbitrary `ChapterType` or `PayloadKind` strings

These APIs are still present as a compatibility bridge, but they are deprecated.
They should not be used for new direct chapter code.

## What To Use Instead

Use `swf.StructuredWorkflowRuntime` for direct chapter access:

```go
structured := swf.NewStructuredWorkflowRuntime(runtime)

chapter, err := structured.GetStructuredChapter(ctx, ref)
chapters, err := structured.ListStructuredChapters(ctx, req)
err := structured.PutStructuredChapter(ctx, swf.PutStructuredChapterRequest{
	Ref:     ref,
	Chapter: chapter,
})
```

Use `swf.StructuredChapterRecord` and one concrete body type:

- `swf.JobStartChapter`
- `swf.TaskAttemptOutcomeChapter`
- `swf.JobAttemptOutcomeChapter`
- `swf.RestartExtraChapter`

For task or job outcomes, use one concrete outcome type:

- `swf.ApplicationOutputOutcome`
- `swf.AppErrorOutcome`
- `swf.SystemErrorOutcome`
- `swf.TimeoutOutcome`

Application bytes are explicit:

- `swf.ApplicationInputBytes`
- `swf.ApplicationOutputBytes`

Chapter metadata is explicit through `swf.ChapterMetadata` and
`swf.ChapterMetadataValue`.

## Write Migration

Replace generic chapter construction:

```go
legacy := swf.StoredChapter{
	Ordinal:     ref.Ordinal,
	TaskType:    "task",
	ChapterType: "TaskAttemptOutcome",
	PayloadKind: "App",
	InputHash:   "input-hash",
	CreatedAt:   now,
	Data:        json.RawMessage(`{"ok":true}`),
}

err := runtime.PutChapter(ctx, swf.PutChapterRequest{
	Ref:     ref,
	Chapter: legacy,
})
```

with a concrete structured chapter:

```go
chapter := swf.StructuredChapterRecord{
	Ordinal:   ref.Ordinal,
	TaskType:  "task",
	InputHash: "input-hash",
	CreatedAt: now,
	Body: swf.TaskAttemptOutcomeChapter{
		Outcome: swf.ApplicationOutputOutcome{
			Output: swf.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		},
	},
}

err := swf.NewStructuredWorkflowRuntime(runtime).PutStructuredChapter(ctx, swf.PutStructuredChapterRequest{
	Ref:     ref,
	Chapter: chapter,
})
```

Do not write custom chapter types. If old code used an arbitrary value such as
`ChapterType: "Manual"`, replace it with the specific SWF chapter type that
matches the data being stored.

## Read Migration

Replace generic discriminator switches:

```go
chapter, err := runtime.GetChapter(ctx, ref)
if err != nil {
	return err
}

switch chapter.ChapterType {
case "TaskAttemptOutcome":
	switch chapter.PayloadKind {
	case "App":
		// chapter.Data is application output
	}
}
```

with concrete body and outcome switches:

```go
chapter, err := swf.NewStructuredWorkflowRuntime(runtime).GetStructuredChapter(ctx, ref)
if err != nil {
	return err
}

switch body := chapter.Body.(type) {
case swf.TaskAttemptOutcomeChapter:
	switch outcome := body.Outcome.(type) {
	case swf.ApplicationOutputOutcome:
		_ = outcome.Output.Data
	case swf.AppErrorOutcome:
		_ = outcome.Error
	case swf.SystemErrorOutcome:
		_ = outcome.Error
	case swf.TimeoutOutcome:
		_ = outcome.Timeout
	}
}
```

Pointer variants are also accepted by the structured conversion helpers, but
value variants are the simplest default.

## Shape Mapping

| Generic shape | Structured shape |
| --- | --- |
| `ChapterType: "JobStart"`, `PayloadKind: "App"` | `JobStartChapter{Input: ApplicationInputBytes{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "App"` | `TaskAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "AppError"` | `TaskAttemptOutcomeChapter{Outcome: AppErrorOutcome{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "SystemError"` | `TaskAttemptOutcomeChapter{Outcome: SystemErrorOutcome{...}}` |
| `ChapterType: "TaskAttemptOutcome"`, `PayloadKind: "Timeout"` | `TaskAttemptOutcomeChapter{Outcome: TimeoutOutcome{...}}` |
| `ChapterType: "JobAttemptOutcome"`, `PayloadKind: "App"` | `JobAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{...}}` |
| `ChapterType: "JobAttemptOutcome"`, `PayloadKind: "AppError"` | `JobAttemptOutcomeChapter{Outcome: AppErrorOutcome{...}}` |
| `ChapterType: "JobAttemptOutcome"`, `PayloadKind: "SystemError"` | `JobAttemptOutcomeChapter{Outcome: SystemErrorOutcome{...}}` |
| `ChapterType: "JobAttemptOutcome"`, `PayloadKind: "Timeout"` | `JobAttemptOutcomeChapter{Outcome: TimeoutOutcome{...}}` |
| `ChapterType: "RestartExtra"`, `PayloadKind: "App"` | `RestartExtraChapter{Output: ApplicationOutputBytes{...}}` |

There is no structured replacement for custom chapter types.

## Compatibility Helpers

During migration, use these helpers at boundaries where legacy values still
exist:

```go
structured, err := swf.StructuredChapterFromStored(legacy)
legacy, err := swf.StoredChapterFromStructured(structured)
```

These helpers are intended for migration and adapter boundaries. Application
code should move toward passing structured chapter records directly.

## Validation

After updating to the latest `swf-go` release and migrating direct chapter
usage, run the consumer test suite normally:

```sh
go get github.com/colony-2/swf-go@latest
go test ./...
```
