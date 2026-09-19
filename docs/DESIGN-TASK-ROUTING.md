# Typed task routing

Resolution to `JOBDB_TASK_ROUTE_GUIDANCE_REQUEST.md`.
Consumer changes: [typed route migration](MIGRATION-TYPED-ROUTES.md).

Represent job and task types as separate fields throughout the APIs and storage.
Remove combined capability/need strings and their parsers. No compatibility API
or database migration is required; document the consumer source changes.

## Contract

```go
type Route struct {
    JobType  string `json:"jobType"`
    TaskType string `json:"taskType,omitempty"`
}
```

`JobType` is required. An empty/omitted `TaskType` means job work; a nonempty
`TaskType` means task work. Task-specific APIs require a task type. Identifiers
are case-sensitive UTF-8 strings without U+0000; preserve them exactly. Colons
are ordinary characters in either identifier, with no routing significance.

The consumer's handoff becomes:

```json
{"jobType":"recipe","taskType":"input:collect_user_input"}
```

Use the same route type for selection, handoff, inspection, and completion.
Keep task ordinals, input hash, and resume job type in the separate `TaskWait`.
A task handoff requires those coordinates; a job handoff clears them.

| Current API | Replacement |
| --- | --- |
| Poll/lease `Capabilities []string` | `Routes []Route` |
| Lease `Capability() string` | `Route() Route` |
| Reschedule `NextNeed` / `AlternateNeed` | `NextRoute Route` / `AlternateRoute *Route` |
| External completion `Capability` | `Route Route`, requiring task work |
| `TaskWait.ResumeNeed` and completion `ResumeNeed` | `ResumeJobType string` |
| Summary/current-need string fields | Typed route and task-wait fields |

Existing job/task filters and discovery arguments already have separate fields.
Registration, invocation, task handles, chapters, and replay retain the original
task identifier. No route formatting or encoding is part of task identity.
Validate identifiers at registration/invocation before writing chapters, and
at runtime API boundaries. Remove the workflow's delimiter-era name restriction.

## Implementation and rollout

- **JobDB:** update workflow dispatch, runtime interfaces, toy/SQLite storage,
  remote schemas and generated bindings, filters, discovery, and completion.
  Replace string-based route matching with field comparisons. Remove route
  join/split helpers and redundant summary fields in favor of typed state.
- **pgjobdb:** retain its separate job/task columns and typed selectors. Remove
  combined need columns and update their SQL, indexes, notifications, and Go
  callers to use structured routing. Update its JobDB dependency.
- **Tests:** cover registration, handoff, polling, alternate routes, filtered
  discovery, external completion, resume, and replay on every runtime and remote
  transport. Assert exact identifiers, including colons in both fields and
  distinct pairs that previously produced the same concatenated string.
- **Consumer guide:** keep existing identifiers; replace combined strings with
  route objects and rename resume fields. Deploy matching module versions with
  fresh databases. Do not provide a legacy parser or dual-format API.

Client-payload semantics are unchanged. Ordinary chapter completion still does
not update scheduler rows. Use matching JobDB and pgjobdb revisions; both use database format 3.
