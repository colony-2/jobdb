# Discover job-goal guarantees

Clients that need immutable membership, one active attempt per goal, or an
inspectable permanent failure must discover those guarantees separately from
creation-time primary-goal selection. `SupportsPrimaryGoalSelection` describes
selection and accepted-receipt replay; it does not establish admission or
protected failure behavior.

Embedded backends can implement the optional `jobdb.JobGoalCapability` interface:

```go
JobGoalCapabilities() jobdb.JobGoalCapabilities
```

Runtime core forwards only the scheduler's explicitly advertised values. A
backend without the interface reports all fields false. Toy and SQLite advertise
all three guarantees, enforced in their scheduler transaction. Their HTTP
adapters preserve these values. This describes compatible writer behavior;
Cortex, c2j and PostgreSQL consumer retirement preceded JobDB attachment
removal; PostgreSQL backend conversion and deployment cutover remain separate
release steps.

HTTP callers use `GET /v1/capabilities/job-goals`, or the remote client's
`GetJobGoalCapabilities(ctx)` method. The response always includes these booleans:

```json
{"immutableMembership":true,"atomicAdmission":true,"terminalFailureRecords":true}
```

`immutableMembership` means one primary goal selected only at creation.
`atomicAdmission` means fresh publication rejects another nonterminal member
while any member remains nonterminal, preserving accepted-request replay.
`terminalFailureRecords` means protected failed-job publication, goal closure
and durable waiter settlement commit together without execution chapters.

Unknown, missing and false values provide no guarantee. An absent or redirected
discovery route returns `ErrJobGoalSemanticsUnsupported`; denied, malformed and
unavailable responses retain their errors. Discovery performs no submission or
goal-state checks, follows no redirects and returns `Cache-Control: no-store`.
The typed unsupported error uses HTTP 501 code `job_goal_semantics_unsupported`.
Consumers must not turn an unsupported result into ordinary submission, strip a
selected goal, or emulate permanent failure with submit followed by completion.

Atomic admission only describes fresh publication by compatible writers. It
does not certify that historical overlaps have drained or that obsolete writers
have stopped. Those conditions require separate migration and cutover evidence.
Capability discovery cannot replace publication's transaction checks or prevent
replay of an already accepted request based on today's active members.

Core and HTTP same-goal submission and shared restart require both immutable
membership and atomic admission. `FailGoal` requires terminal-failure records.
The remote client checks discovery before sending these mutations; the server
also enforces capabilities when clients bypass discovery. Unsupported operations
return the typed error before backend preparation or publication. Ordinary
submission and independent restart keep their existing contracts.

The [protected reader contract](GUIDE-TERMINAL-JOB-RECORDS.md) describes chapter-free
inspection. SQLite format 13 and manual conversion are implemented; toy retains
origins and first closures on jobs with a separate wait-only store. Post-creation
attachment declarations and mutation routes are removed after Cortex, c2j and PostgreSQL consumer retirement. Member reads and empty deprecated `OtherGoalIDs` remain.
