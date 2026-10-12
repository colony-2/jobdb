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
an empty snapshot and absent keys. Secrets distinguish names never retrieved from
names omitted by a successful provider response (denied or unknown).

When a request includes a name never retrieved, fetch that name and **all cached
available names together** in one provider call. Replace their cached values or
omissions atomically on success. Previously omitted names are not retried. For
example, `resolve(foo)` followed by `resolve(bar)` calls the provider with `foo`,
then with `foo` and `bar`; subsequent reads reuse the second response. Return only
the names requested by the caller. Errors do not mark new names as unavailable or
replace earlier values.

Earlier caller-owned results cannot be changed retroactively. Consumers needing
matching values together should request them together. Consistency within a
single provider response is the provider's responsibility.

Concurrent callers share successful fetches. Failed or canceled fetches publish
no values and can be retried. Callers can cancel while waiting for another read.
Returned maps, JSON bytes, and secret expiry timestamps are copied; mutating a
result cannot change the cache. Reads retain caller and job/task cancellation
checks even on cache hits. Provider I/O does not block lease renewal.

A cache-only read of an expired secret returns `ErrSecretsUnavailable` without
partial values. Expiration alone does not trigger a refresh. Adding a new name
refreshes all available names, including expired ones; a new job attempt also
starts fresh. Any expired value in a refresh response rejects the entire refresh.

Caches are in memory only. Values are not automatically stored in chapters,
inputs, outputs, artifacts, or replay data. Direct runtime, lease, HTTP, and
administration APIs continue to perform fresh reads; the runtime/server does not
cache provider results.

## Consumer migration

- Remove redundant per-attempt caches around the workflow context methods.
- Expect repeated context reads to retain the first values despite configuration
  edits, secret rotation, or a newly granted secret. Requesting new secret names
  refreshes available secrets together; known omissions remain cached. Whole-job
  retries or new invocations reset both caches.
- Handle secret expiration as a resolution error. A task retry shares the same
  cache; retrying the same names does not refresh an expired secret.
- Keep replay-dependent observations in ordinary task outcomes. Never put secret
  values in those outcomes or error messages. Providers must resolve the supplied
  names independently of earlier requests and return only requested names.

No database or wire migration is required.
