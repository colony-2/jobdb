# Scope policy changes can block historical replay

Investigation of `/src/problem7`, tenant `c2`, job
`MDho0an4ijWSdPaTlGeo6Ls0Edw`, on 2026-10-19.

## Findings

The job's history is intact. The reported mismatch is at the first
`op:command_execution` scope inside `build/setup/cell-setup/check`, before its
completed task result is read. Chapter 18 records:

```json
{
  "total_timeout": 300000000000,
  "attempt_timeout": 0,
  "retry": {
    "InitialInterval": 0,
    "BackoffCoefficient": 1,
    "MaximumInterval": 0,
    "MaximumAttempts": 1,
    "NonRetryableErrorTypes": null
  },
  "restart": true
}
```

This is a five-minute operation limit, supplied through `ScopeOptions` by the
consumer. JobDB does not supply that scope default. The job separately records
the former ten-minute job-attempt and thirty-minute total limits. It ultimately
records a total timeout at 2026-10-17T21:28:53Z, while waiting for user input on
its third job attempt. The failing replay location is earlier completed work.

Every recorded activation identifies JobDB
`v0.0.41-0.20261014050424-623a8276fedc` and consumer version `c2j: dev`. The reader
may be newer; these are the recorded execution versions, not the reader's build.

## Reproduction

The original database was opened read-only and backed up to an isolated SQLite
database. A diagnostic driver on current JobDB recreated the scope/task
declarations and nesting from the recorded descriptors. It did not run c2j,
task workers, or input builders.

| Declarations supplied to current JobDB | Result |
| --- | --- |
| Recorded declarations | Three root attempts, 171 nested scopes, 54 task calls; recorded total timeout returned; zero input builders |
| First `check/command_execution` scope changed from five minutes to unlimited | `logical descriptor changed` at the exact path in the report |

This establishes that the captured history can be consumed and that removing
the operation timeout reproduces the failure. The user subsequently confirmed
that all implicit timeouts were removed from the consumers. Reading a job
created with an implicit operation timeout using a consumer without that
default is consistent with this reproduction. The original error does not
include the reader's computed descriptor, so the exact field difference was
not independently observed in the deployed reader.

The appended “missing events” message follows from stopping before the task
call. It does not establish that the stored task result is missing.

## Remaining responsibility boundary

JobDB owns task input capture, attempts, deadlines, retries, catch selection,
and suspension. It still requires the consumer to re-declare matching scope
and task descriptors during replay. `DoScope` compares the newly supplied
policy before entering the scope body; `DoTask` similarly checks its descriptor
before reading a recorded result. A changed consumer default can therefore
break replay even when task inputs and results are already durable.

Removing JobDB's default job timeouts did not rewrite stored policies. The root
job policy is loaded from history, but nested scopes still receive options from
the consumer. `c2j: dev` also provides no useful revision distinction when
investigating a consumer change.

## Changes in this investigation

Mismatch errors now identify changed descriptor fields. Scope timeout changes
include recorded and current durations, for example:

```text
logical descriptor changed for ...; changed fields: total_timeout (recorded=5m0s, current=0s)
```

Command values and other consumer payloads are not included. Regression coverage
on toy, SQLite, and HTTP runtimes checks matching replay after a recorded
timeout, removed/added limits, changed retries/catch/restart declarations,
changed task commands/policies, secret-safe diagnostics, and no extra task work
or history writes. The capture and c2j code were not changed.

These changes improve diagnosis; they do not make changed declarations
compatible.

## Resolution

Keep strict descriptor matching. Do not add compatibility handling or change
policy binding to accommodate the removal of implicit timeouts. The user will
delete old jobs rather than preserve replay compatibility for them.

New jobs use only explicitly declared timeouts. Execution and replay must use
the same recorded recipe and interpret its declarations consistently. This
avoids the implicit-timeout transition failure while preserving validation of
task identity, commands, policies, catch rules, and restart-hook shape.

## Policy binding update (2026-10-29)

The earlier decision to reject changed timeout defaults is superseded by the
recorded-policy behavior described in
[Recorded lifecycle policies](REPLAY-Recorded-Lifecycle-Policies.md).
JobDB now binds existing scope/task timeout and retry settings to history,
including recovery and retained restart ancestors. Structural validation remains.
This addresses the same class of mismatch observed in the migrated job
`3LIsi58lrjR29QBCS9dMcUbaUBK`; its task contract-version mismatch in current c2j
is a separate consumer compatibility issue.
