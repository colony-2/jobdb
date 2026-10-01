# Delayed alternate task implementations

Use `workflow.WithTaskOptions(data, workflow.TaskOptions{Alternate: &workflow.TaskAlternate{TaskType: "input-alternate", At: due}})` when calling `JobContext.DoTask(policy, "input", data)`. Store `due` and fallback values in an earlier durable task outcome and reconstruct these options on replay. Options do not alter the data bytes or input hash. JobContext decorators that replace TaskData must preserve options using `TaskOptionsFor` and `WithTaskOptions`.

The normal logical task remains `input`. The runner hands missing work to that route with a delayed alternate, then dispatches `input-alternate` only when the acquired lease matches the declared route, current task coordinates, resume job, input hash, and due time. Registration alone does not invoke fallback. Results, schema validation and history retain the logical task name. The alternate returns its result through ordinary task execution; it must not externally complete its own leased task.

The due time is eligibility, not a response cutoff. An external response can still complete the original pending task until an alternate worker acquires it. After acquisition, lease ownership excludes external completion. Once either result is stored, replay consumes that result. A crashed alternate worker can resume the same pending occurrence after lease expiry. Supplied leases retain the same route and coordinates through import and renewal.

Task and job hard deadlines take precedence for unfinished work. External handoff uses the earliest applicable absolute timeout or alternate time. Completed outcomes remain replayable after their deadline. Terminal job timeout recording skips occupied task chapters rather than colliding with them.

The primary job worker must always be available to classify timeout wakeups. A declared alternate also requires a worker for its route. There is one scheduled alternate: an earlier task alternate may be claimed after a later hard deadline, in which case the runner records the timeout instead of invoking the handler. Job invocation timeouts bound each worker run and reset on resume; use the job total timeout to bound time across handoffs. External task invocation timeouts remain anchored to durable history across handoffs.

SQLite job listings report the committed pending route; alternate eligibility changes the route only on successful acquisition. Toy runtime follows the same rule. Scheduler adapters must preserve that distinction so listing an overdue input cannot prevent its external completion. Dependency waits and future availability still govern lease eligibility; this feature does not bypass those waits.

Production adapter requirements are in [the pgjobdb guide](docs/GUIDE-PGJOBDB-TASK-ALTERNATES.md). No database migration or additional timer service is required by this change.

Tests cover toy, persistent SQLite, and remote SQLite, including human/fallback races, late answers, expiry recovery, supplied leases, mismatched dispatch identities, and external hard timeouts.
