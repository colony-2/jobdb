# Starting or appending to a shared job

`WorkflowRuntime.StartOrAppendJob` and `Engine.StartOrAppendJob` accept a `StartOrAppendJob` containing an `AppendKey`, a `SubmitJob` template, an `Append` JSON value, and optional `AppendArtifacts` references. `ExecutionContext`, `TaskContext`, and `ExecutionLease` expose the same operation using the caller's current lease. Shared jobs have no parent job identity.

```go
handle, err := engine.StartOrAppendJob(ctx, workflow.StartOrAppendJob{
    AppendKey: "acme/api#main",
    MaxInputs: 50,
    Job: workflow.SubmitJob{
        TenantId: "acme", Cell: "api", JobType: "merge-batch",
        Data: jobdb.NewTaskDataOrPanic(map[string]string{"branch": "main"}),
    },
    Append: json.RawMessage(`{"pr":1234}`),
    AppendArtifacts: []jobdb.ArtifactKey{{
        JobId: "build-1234", TaskOrdinal: 2,
        Name: "report.txt", SizeBytes: 42,
    }},
})
```

Wait for the returned handle through the existing job wait mechanisms. Delivery is **at least once**: retrying a request can add another copy. Every caller targeting one generation receives the same job result.

Chains are scoped to `(tenant, immutable cell ID, job type, append key)`. The creator chooses the start data, artifacts, metadata, policy, schema, client payload, and availability for a generation. Later appends do not compare or replace that template. `jobId`, `goalId`, `dependencies`, and `prerequisites` are server assigned and cannot be supplied.

A generation accepts appends until a worker writes its first chapter, subject to lease ownership, cancellation, and the calling request's cap. Requests arriving while a worker holds the lease start or append to a successor. Each successor waits for its predecessor to terminate, regardless of outcome. A worker that loses the race for its first chapter releases its lease for a fresh attempt; the conflict does not consume an application retry.

`MaxInputs` is per call, including the caller's first append. Zero uses **50**; the ceiling is **1000**. The value is not stored on the job. Keys must contain 1–1024 UTF-8 bytes, cannot contain NUL, and cannot be all whitespace. Appender claims last ten seconds. Contention retries are bounded and context cancellation interrupts backoff.

## Worker input and artifacts

Workers use the ordinary `JobWorker` API. Input data is:

```json
{"start":{"branch":"main"},"appends":[{"pr":1234},{"pr":1240}]}
```

Append order is durable and identical on replay. Every JSON value, including `null`, is valid. Artifacts are available through the input's ordinary `GetArtifacts()`:

- Start artifacts are named `start/<original name>`.
- Artifacts for the first append are named `appends/0/<original name>`.
- Artifacts for the next append are named `appends/1/<original name>`, and so on.

`AppendArtifacts` references already persisted artifacts in the same tenant; it does not upload new bytes. References retain their original storage identity while their worker-visible names are namespaced. Names within each append must be unique relative paths without traversal. Referenced data remains subject to the existing artifact storage and retention rules.

The first chapter's start JSON plus append JSON share the chapter application byte budget. Each later append has its own chapter budget. The assembled input can be larger than one chapter; use `MaxInputs` to bound batch size.

## Schemas

The optional `appendShape` is a JSON Schema applied directly to each append's JSON value. Existing `firstChapterShape`/`chapterShape` validation of the start chapter is unchanged. Omission of `appendShape` permits any JSON append.

```json
{
  "chapterShape": true,
  "appendShape": {
    "type": "object",
    "required": ["pr"],
    "properties": {"pr": {"type": "integer"}}
  }
}
```

Append artifact references are separate from the JSON value validated by `appendShape`.

## HTTP and inspection

- `POST /v1/tenants/{tenantId}/jobs/start-or-append`
- `POST /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs/start-or-append`

The lease route uses the existing lease-token header. Both return the existing `JobHandle`. The request is `{appendKey, maxInputs?, job, append, appendArtifacts?, workerId?, requestTime?}`. `job` uses the existing submission template and `appendArtifacts` uses `{jobId, taskOrdinal, name, sizeBytes}` references.

Invalid append requests return `400 invalid_append_request`; bounded contention returns `409 append_busy`; chapter size violations retain the existing `413` payload error. Authorization uses ordinary `jobs.submit` authority on the target cell. There is no capability endpoint.

`JobInfo.AppendKey` and `JobSummary.AppendKey` identify chain members. `ListJobsRequest.AppendKeys` (HTTP `appendKeys`) filters before pagination. Restarts retain the selected input prefix but have an empty append key and do not participate in sequencing.

## Storage and upgrades

The shared runtime algorithm writes a start chapter containing the first append and recoverable publication facts, followed by `AdditionalInput` prefix chapters. Backends store opaque chapter bytes. SQLite adds two indexed columns to `jobdb_jobs`; PostgreSQL adds them to its existing immutable `job_facts` row so chain identity survives archival. No new tables are required. Lease events use acquisition source `append`; expired append claims preserve crash counters, while expired worker leases still count.

Install the updated PostgreSQL DDL before running the updated pgjobdb runtime. SQLite applies its additive migration on open. Update workers and servers together: older codecs cannot decode the new chapter kind. Ordinary job encoding remains unchanged.

## Compact grouped job IDs

New grouped generations use deterministic 27-character KSUID base62 strings. The
20 encoded bytes are the first 16 bytes of SHA-256 over the existing canonical
`fmt.Sprintf("%q/%q/%q", cellID, jobType, appendKey)` encoding, followed by the
generation as four unsigned big-endian bytes. Tenant remains part of the storage
key. These IDs are opaque: their decoded timestamp has no chronological meaning.
Use `append_gen` for queue order and `created_at` for chronological order. String
order increases within one compact queue, but does not order different queues or
mixed legacy/compact chains.

Generations start at one and may reach **4,294,967,295**. When a new successor would
exceed that limit, the operation returns `ErrInvalidAppendRequest` with
`append generation exhausted (maximum 4294967295)` (HTTP `400 invalid_append_request`).
Existing open jobs at or above the limit can still accept ordinary appends.

Existing `ak-…` jobs keep their IDs and remain addressable for reads, artifacts,
appends, and waits. A newly created successor uses the compact ID unless a legacy
chapter zero for that generation already exists. The shared runtime checks for
that chapter first and recovers its saved publication facts, including the actual
predecessor ID. No data rewrite, DDL change, or new storage interface is required.

Pause grouped submissions on old writers, drain in-flight grouped calls, update
all writers sharing the storage, then resume. Simultaneous old and new writers
are unsupported: they could create different IDs for the same generation.

Before rollback, keep the new writer available to recover and publish any compact
chapter zero left by an interrupted creation. Establish that no compact creations
remain unpublished before enabling old writers. Pausing and draining alone does
not establish this: old writers cannot discover unpublished compact chapters.
If completeness cannot be established, retain the new writer. Automatic global
auditing and rollback recovery orchestration are outside this change.
