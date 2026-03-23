# GetJobRunResponse Migration Guide (SWF Users)

This guide helps SWF users migrate from the previous `GetJobRunResponse` shape to the new API.

## What Changed

Old response (deprecated):
- `Tasks []TaskRun` at top level
- `JobAttempts []JobAttempt`
- `Result *JobAttempt`

New response:
- `Attempts []JobAttempt`
- Each `JobAttempt` now contains `Tasks []TaskRun`
- `Result` and top-level `Tasks` are removed

## Why It Changed

The job timeline is now grouped by **job attempt**:
- All task runs are nested under the job attempt in which they executed.
- The “current attempt” exists even before completion.
- Job attempt results are carried on the attempt itself (no separate `Result` field).

## Migration Steps

### 1) Replace `Result` with the latest attempt
**Old**
```go
attempt := resp.Result
```

**New**
```go
attempt := lastAttempt(resp.Attempts)
```

Helper:
```go
func lastAttempt(attempts []swf.JobAttempt) *swf.JobAttempt {
	if len(attempts) == 0 {
		return nil
	}
	return &attempts[len(attempts)-1]
}
```

### 2) Replace top-level `Tasks` with attempt-scoped tasks
**Old**
```go
for _, task := range resp.Tasks {
    // ...
}
```

**New**
```go
for _, attempt := range resp.Attempts {
	for _, task := range attempt.Tasks {
		// ...
	}
}
```

### 3) Replace `JobAttempts` with `Attempts`
**Old**
```go
for _, attempt := range resp.JobAttempts {
    // ...
}
```

**New**
```go
for _, attempt := range resp.Attempts {
    // ...
}
```

### 4) Update output access
If you used `Result` or `JobAttempts` for output, use the last attempt:
```go
latest := lastAttempt(resp.Attempts)
if latest == nil || latest.Output == nil {
	// not complete
}
```

The helper `GetOutput` continues to work with the new shape.

## Behavioral Notes

- The **current attempt** exists even if the job is not complete.
- The runtime task (next_need) is appended under the **current attempt**.
- Job attempts are no longer derived by scanning for `meta.task_type == jobType` alone; they are explicit in the response.

## Quick Mapping Table

| Old Field | New Field |
| --- | --- |
| `Tasks` | `Attempts[i].Tasks` |
| `JobAttempts` | `Attempts` |
| `Result` | `lastAttempt(Attempts)` |
