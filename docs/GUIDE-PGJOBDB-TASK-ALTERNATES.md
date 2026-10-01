# pgjobdb requirements for task alternates

Update the JobDB dependency to include `workflow.TaskOptions` and delayed task
alternates. The shared runner implements dispatch and timeout classification;
there is no new scheduler API or chapter/scheduler transaction to implement.
No pgjobdb source changes are included in this JobDB change.

Verify these scheduler semantics, correcting pgjobdb where necessary:

- Job and schedule-run listings return the stored pending route. An alternate
  becoming due must not hide the original waiting task from external completion.
- Worker acquisition matches the effective route, promoting a due alternate only
  after a successful claim. A poll with no matching worker must not promote it.
- Claiming the primary route before the alternate is due preserves that alternate
  so a crashed primary can recover after lease expiry. A successful alternate
  claim persists that route and consumes the alternate.
- Acquisition, validation, renewal, and supplied-lease import agree on the claimed
  route and task-wait coordinates. See the existing
  [supplied-lease guide](GUIDE-PGJOBDB-SUPPLIED-LEASE.md) for the route persistence
  issue identified in the previously reviewed pgjobdb implementation.
- External completion and alternate acquisition compete through conditional
  scheduler ownership. Passing the alternate time alone does not reject an answer;
  an active competing lease does. Chapter insertion remains an independent atomic
  append with ordinal uniqueness and sequence guarantees.

The primary job worker is required for timeout wakeups, and a declared alternate
requires a worker serving that route. Keep the single alternate mechanism and
normal availability/dependency restrictions. No additional timer is requested.

Run `TestTaskAlternateLifecycle`, `TestTaskAlternateHardTimeout`,
`TestTaskAlternateClaimAfterHardDeadline`, and `TestTaskAlternateSuppliedLease`
from JobDB's workflow tests against PostgreSQL. Also exercise both polling and
direct acquisition: claim the primary before the alternate is due, let its lease
expire, and claim the now-due alternate. Check listings and conditional completion
both before eligibility and after eligibility but before acquisition.
