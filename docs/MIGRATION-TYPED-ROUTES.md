# Migrating to typed routes

Job and task types now travel as separate fields. Keep identifiers such as
`two-step-op:second` and `input:collect_user_input` unchanged. Never concatenate,
split, or escape identifiers to construct a route.

```go
route := jobdb.Route{
    JobType:  "recipe",
    TaskType: "input:collect_user_input",
}
jobRoute := jobdb.Route{JobType: "recipe"}
```

An omitted/empty task type selects job work. Task-specific operations require a
nonempty task type. Both identifiers are case-sensitive, nonempty UTF-8 strings
without U+0000; colons are allowed in either field. There is no normalization.
Normal JSON/HTTP encoding still applies.

## API changes

| Previous API | Replacement |
| --- | --- |
| Poll/targeted lease `Capabilities []string` | `Routes []jobdb.Route` |
| `lease.Capability()` | `lease.Route()` |
| Reschedule `NextNeed string` | `NextRoute jobdb.Route` |
| Reschedule `AlternateNeed string` | `AlternateRoute *jobdb.Route` |
| External completion `Capability string` | Required `Route jobdb.Route` |
| `TaskWait.ResumeNeed`, completion `ResumeNeed` | `ResumeJobType string` |
| Summary/task-runtime `NextNeed *string` | `NextRoute *jobdb.Route` |
| Summary `TaskWaitInput`, `TaskWaitOutput`, `TaskWaitInputHash`, `TaskWaitNext` | `ExecutionState.TaskWait` fields `InputOrdinal`, `OutputOrdinal`, `InputHash`, `ResumeJobType` |
| `JobRunOutcome.NextNeed`, `MissingCapability` | `NextRoute`, `MissingRoute` |
| `FindTasksWaitingForCapability(...)` | `FindTasksWaitingForRoute(...)`, with the same separate job/task arguments |
| `JobTypeFromNextNeed(...)` | Read `route.JobType` directly |

Worker names, `DoTask` arguments, `JobTaskFilter`, discovery arguments,
`TaskHandle.TaskType()`, chapter task types, and replay use the original
application identifiers. Invalid names fail at registration/invocation or the
runtime boundary. Do not rename chapters or encode task names.

Each reschedule supplies its next route and task coordinates. A task route
requires `TaskWait`; a job route omits it. Omit `AlternateRoute` and
`AlternateAfter` to clear the alternate. When supplying an alternate, also
supply a nonnegative delay; zero selects an immediate fallback. An alternate
task route requires the main handoff's task coordinates.

The REST equivalents use `routes`, `route`, `nextRoute`, `alternateRoute`, and
`resumeJobType`. Regenerate clients from `openapi/jobdb-runtime.yaml`. For example:

```json
{
  "nextRoute": {"jobType":"recipe","taskType":"input:collect_user_input"},
  "taskWait": {
    "inputOrdinal":1,
    "outputOrdinal":2,
    "inputHash":"your-input-hash",
    "resumeJobType":"recipe"
  }
}
```

For external completion, prefer the discovered task handle's `Finish` or
`FinishWithClientPayload`. Direct callers supply the exact route and coordinates
from the current summary; a different route conflicts. `ResumeJobType`, when
supplied on completion, guards the stored resume job type. It does not override
it.

Client-payload updates retain their existing patch/reset and persistence
semantics. Ordinary chapter completion does not write scheduler rows. See the
[client-payload guide](MIGRATION-CLIENT-PAYLOAD.md) when upgrading from the earlier
combined-payload API.

## Deploy and verify

1. Update Go callers and REST clients, including test fakes and worker adapters.
2. Deploy matching JobDB and pgjobdb revisions. Stop the old processes and create
   fresh databases and artifact namespaces as described in the client-payload
   guide. SQLite and PostgreSQL now require format **3**; there is no in-place
   migration or compatibility API.
3. Re-register schemas, recreate schedules, and submit new jobs. Old task
   handles, lease tokens, and restart history are not reusable.
4. Rerun the consumer's compiler and input integration tests from the request.
   Verify discovery, completion, and replay retain each complete task identifier.

Native pgjobdb already uses separate job/task fields; keep those values intact.
Its removed `next_need`/`alternate_next_need` SQL columns have no replacement
string column. Use `route_job_type`, `work_kind`, `task_type`, the corresponding
alternate fields, and `final_*` archive fields. Dependency wake-up notifications
use the fixed `pgjobdb.work` channel with a JSON object containing `tenantId`,
`jobId`, and `route`; update custom listeners that used `pgjobdb.need.<name>`.
