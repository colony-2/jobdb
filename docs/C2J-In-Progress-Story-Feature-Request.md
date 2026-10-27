# c2j: expose unfinished task input and state in job stories

## Requested outcome

`GetJobRunStory` should expose an unfinished task as a named operation leaf with
its resolved input, logical invocation identity, current attempt, and JobDB state.
For example, while an operation is blocked waiting for its worker to finish, a
Cortex user should be able to select it and inspect its input. After completion,
the same logical leaf should show the result.

The accompanying implementation and regression tests are in
[c2j-in-progress-story.patch](c2j-in-progress-story.patch), in the same directory
as this document. The patch was prepared against c2j
`v0.0.105-0.20261027024848-fbcd5cd24a8a` and tested against the updated JobDB source.
It has not been applied to a maintained c2j checkout or released.

## Ownership and upstream dependency

JobDB owns lifecycle interpretation, invocation/attempt identity, scope handling,
retries, and replay. c2j owns the recipe story projection: operation names, paths,
input-envelope decoding, visible leaves, and prior-attempt presentation.

The updated JobDB replay contract provides:

- `TaskStartEvent.Unfinished` and `TaskStartEvent.State`, alongside the existing
  identity, attempt, task type, input, and timestamp fields.
- `ReplayCacheMissError.InvocationID`, `Input`, and `State`, alongside its existing
  task type, attempt, ordinal, and reason fields.
- An unfinished start observation without a fabricated task or job end.
- Replay through scope structure, respecting recorded outcomes and control
  decisions, until an unfinished task or another unresolved boundary is reached.

No additional lifecycle recording is needed. JobDB reuses stored input where
available and otherwise reconstructs input through the existing replay-safe
input builder. Builders must be deterministic and side-effect-free; workers and
runtime preparation must not execute during inspection. This supersedes the
earlier request's blanket prohibition on invoking input builders.

An unfinished task in an active job is logically running. This is an inference
owned by JobDB, not a guarantee of instantaneous worker liveness. Unknown start
times remain unset. Physical ordinals are storage coordinates; use the supplied
logical identities to associate task attempts across inspection and completion.

Update c2j's JobDB dependency to a version containing this contract before
integrating the patch. The patch intentionally does not name an unreleased
version or include a local module replacement. Do not add c2j lifecycle storage,
private-history decoding, or a second task-state inference mechanism.

## Current projection gaps

In `pkg/story/live/recorder.go`:

1. `OnTaskStart` fills a separate attempt node, but the visible step receives
   those fields only in `OnTaskEnd`. A start-only observation therefore leaves
   its input and attempt details hidden.
2. A new attempt can leave the previous attempt's output, artifacts, error, and
   finish time on the visible step until the new attempt ends.
3. Successful single-step operations collapse into their parent operation;
   unfinished operations take an early return that preserves the child instead.
   Completion therefore changes the visible tree shape.
4. Cache-miss handling assumes running state and can overwrite JobDB's waiting
   state or attach an execution error to an ordinary unfinished boundary.

## Requested changes

- On `OnTaskStart`, copy the current attempt's available input and details onto
  the visible leaf immediately. Apply this to operation steps and recipe-source
  resolution nodes.
- Replace the visible attempt's fields when a retry starts. Keep previous
  attempts and their diagnostic outputs/artifacts in `PriorAttempts`; leave the
  active attempt's output, output artifacts, error, and finish time absent until
  provided by its own outcome.
- Project JobDB's unfinished state using the existing story statuses:

  | JobDB task state | Story node status |
  | --- | --- |
  | `RUNNING` | `running` |
  | `READY`, `WAITING` | `pending` |
  | `CANCELLED` | `canceled` |
  | `UNKNOWN` or unrecognized | `unknown` |

- Preserve completed-event handling. Only unfinished observations use the new
  state field; recorded outcomes continue to determine completed attempt status.
- Recognize an already-observed unfinished invocation by its supplied identity.
  Do not duplicate it or treat its cache miss as an output/error event.
- Use the same single-step collapse rule before and after completion. Preserve
  the operation label, invocation identity, path, and visible leaf selection
  across repeated reads and completion.
- Preserve pending/unknown child states when deriving container status. Retain
  real execution failures rather than letting projection changes mask them.

## Acceptance tests

Tests must exercise `GetJobRunStory` with real JobDB, not only a fake replay
engine. The patch supplies eight integration scenarios: SQLite and remote
runtimes, each with local execution, external dispatch, local task retry, and
externally dispatched task retry.

Each scenario holds the task behind an explicit barrier and inspects it through
a separate engine. Assert:

- The named leaf and resolved input are visible before completion.
- Queued external work is pending; the active task is running.
- Repeated reads preserve identity and do not execute workers or change history.
- The unfinished attempt has no output, output artifacts, finish time, or error.
- A retry preserves the previous attempt's diagnostic output and artifacts
  separately from the active attempt.
- Releasing the barrier produces a completed leaf with the same node ID,
  invocation identity, and path, now containing its output.

Recorder tests additionally verify that `OnTaskStart` alone exposes input and
clears previous-attempt fields for running, ready, waiting, and cancelled states.
Existing fake replay tests are adjusted to expect the consistent collapsed leaf.

## Patch integration and validation

Apply the linked patch from the c2j repository root, after checking that it applies
to the target revision. With the updated JobDB dependency, run:

```sh
go test ./pkg/story/...
go test -race ./pkg/story/...
go test ./pkg/worker/compiler ./pkg/executionruntime ./pkg/template
```

These suites passed in the prepared working copy using a temporary modfile that
replaced JobDB with the updated local source. Patch applicability was checked
against the cited c2j version. The new local story integration test also fails
when the original recorder is restored, confirming that it detects the regression.

JobDB's companion changes separately passed its full integration suite, workflow
race checks, and unfinished-task tests across toy, SQLite, and remote runtimes.
The broader design and verification record is in
[PLAN-In-Progress-Replay.md](PLAN-In-Progress-Replay.md).
