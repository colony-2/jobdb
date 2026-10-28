# Remove deprecated job-list detail fields

## Current transition

Ordinary `ListJobs` responses are compact by default. The following `JobSummary`
fields remain in Go and OpenAPI for compatibility, are deprecated, and are no
longer populated by job listing:

- `BlockingWait` (`blockingWait`)
- `OtherGoalIDs` (`otherGoalIds`)
- `WaitFor` (`waitFor`)
- `CompletionDetail` (`completionDetail`)
- `ClientPayload` (`clientPayload`)
- `ExecutionState` (`executionState`)

This transition preserves field names and types, not previous values. Consumers
must follow the [migration guide](MIGRATION-Compact-Job-Lists.md). No removal
version or date is assigned by this document.

`BlockingWaitSummary` is the retained compact wait identity/deadline. Primary
goal ID, status/completion status, payload revision, routes, timestamps, parent,
schema, and **all application metadata** remain list fields.

## Removal gates

Before deleting fields or changing the required OpenAPI properties:

1. Update and release all shipped backends, including pgjobdb, and run the shared
   compact-list, availability, task-discovery, and job-run regression suites.
2. Migrate c2j and other clients: audit list tables, task completion/discovery,
   payload viewers, dependency explanations, goal membership, and error displays.
   Confirm they use explicit detail reads and do not recreate a per-row refresh
   loop in clients.
3. Inventory consumers using generated clients or serialized `JobSummary` data.
   Their compilation and strict schema validation must be included in migration.
4. Separate list and detail wire models where shared types currently interfere.
   In particular, schedule-run responses embed `JobSummary` and still carry
   details; removing fields from that shared type would otherwise alter them.
5. Choose and announce a breaking API/version boundary. Update the public API
   snapshot, generated OpenAPI client/server, documentation, fixtures, and any
   supported legacy adapters together.

## Eventual removal

At that boundary, remove the six deprecated fields from the list-specific Go
and HTTP schemas, including `otherGoalIds`, `waitFor`, and `executionState` from
the list schema's required properties. Delete compatibility encoding and
parsing for that version. Do not remove the corresponding `GetJob`, job-run,
schedule-inspection, or dependency-detail data.

Keep tests proving that listing never loads execution history/artifacts,
client-payload data, additional memberships for output, or evaluated dependency
explanations. Retain filter/pagination and concurrent task-completion regression
coverage. Measure query counts for pages of several sizes and representative
pending waits; row-count growth must not create one database round trip per job.
