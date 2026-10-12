# Workflow configuration and secrets caching

`JobContext.GetConfiguration`, `TaskContext.GetConfiguration`, and both
`ResolveSecrets` methods now cache reads within a **live job attempt**. Signatures
are unchanged. This change is independent of task preparation.

The scope is one invocation of the job worker for a job attempt. JobDB already
creates an attempt-local runner and cancellation context. There is no separate
public ID for this scope: neither a job key nor a durable attempt number identifies
it uniquely across suspension/resumption, and a lease can span whole-job retries.

| Event | Cache behavior |
| --- | --- |
| Another task, task retry, lease renewal, or in-process wait | Reuse |
| Whole-job retry, even within the same worker run | Start empty |
| Suspension/resumption, separate leased execution, or process restart | Start empty |
| Completed, canceled, timed-out, or lease-lost context | Reject further reads |
| Offline replay | Reject live reads; recorded tasks make no reads |

Configuration caches the first successful **complete tenant snapshot**, including
an empty snapshot and absent keys. Secrets cache each name's first successful
resolution, including omissions for denied/unknown names. Only unresolved names
are sent to the provider, so a later request can fetch new names without changing
previously resolved values. Secret names first requested at different times do not
form an atomic provider snapshot.

Concurrent callers share successful fetches. Failed or canceled fetches publish
no values and can be retried. Callers can cancel while waiting for another read.
Returned maps, JSON bytes, and secret expiry timestamps are copied; mutating a
result cannot change the cache. Reads retain caller and job/task cancellation
checks even on cache hits. Provider I/O does not block lease renewal.

Expired cached secrets return `ErrSecretsUnavailable` without partial values or
refreshing. A new job attempt can obtain a replacement. Rotations and permission
changes take effect on the next attempt for already resolved names.

Caches are in memory only. Values are not automatically stored in chapters,
inputs, outputs, artifacts, or replay data. Direct runtime, lease, HTTP, and
administration APIs continue to perform fresh reads; the runtime/server does not
cache provider results.

## Consumer migration

- Remove redundant per-attempt caches around the workflow context methods.
- Expect repeated context reads to retain the first values despite configuration
  edits, secret rotation, or a newly granted secret. Use a whole-job retry or new
  invocation when the workflow needs to observe those changes.
- Handle secret expiration as a resolution error. A task retry shares the same
  cache; it cannot refresh an expired secret.
- Keep replay-dependent observations in ordinary task outcomes. Never put secret
  values in those outcomes or error messages. Providers must resolve the supplied
  names independently of earlier requests and return only requested names.

No database or wire migration is required.
