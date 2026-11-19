# Preserving jobs after execution infrastructure failures

A determinism mismatch, unreadable durable history or runtime I/O failure stops the
runner without initiating later completion or rescheduling. The invocation latches
SDK-observed failures before catches, retries and result serialization. It cancels
and joins renewal that it owns before returning. An operation admitted before the
failure was observed may already have committed; this contract provides no rollback.
The next eligible invocation can resume the same stored history after repair.
Repeated expired invocations still use the existing crash-concern policy and its
guarded clear operation.

`jobdb.ExecutionInfrastructureError` exposes `Reason` (`determinism`,
`durable_decode`, `runtime_io`), `Operation`, optional `JobKey`, optional physical
`Ordinal`, and the original local `Err`. Zero is a known ordinal; nil is unknown.
`errors.Is` and `errors.As` preserve local causes, including joined authority
errors and `ErrWorkflowNotDeterministic`. Classification never replaces an existing
origin, including a prerequisite's identity. Pure input parsing does not establish
persisted origin. Application errors, task-owned I/O, configured timeouts,
cancellation, semantic conflicts and lease-authority failures retain their handling.

```go
if jobdb.IsExecutionInfrastructureError(runErr) {
    var failure *jobdb.ExecutionInfrastructureError
    if errors.As(runErr, &failure) {
        // Inspect failure.Reason, Operation, JobKey and Ordinal.
    }
    // Keep runErr even if the optional diagnostic fails.
}
```

HTTP 500 responses add `execution_infrastructure_error` and a typed descriptor.
The client restores classification and the determinism sentinel. It preserves
local transport causes, classifies legacy 5xx and malformed runtime responses,
and introduces no retry. Remote descriptors use safe static messages; arbitrary
backend Go types and source text are available only locally. Envelopes are at most
16 KiB, operation context at most 128 UTF-8 bytes; truncation is explicit. Existing
authorization and semantic statuses retain their meaning.

## Read retained writer provenance

`ExecutionProvenanceReader` is optional; it does not extend `WorkflowRuntime`.
`jobdb.ReadExecutionProvenance` forwards optional support or returns
`ErrExecutionProvenanceUnsupported`. SQLite and toy support it. Runtime core
supports it when its chapter port implements optional `ProvenanceChapterSource`;
the shared PostgreSQL chapter store supplies that port. Custom runtime owners
remain responsible for native authorization and forwarding optional capabilities.

```go
ctx, cancel := context.WithTimeout(callerCtx, 2*time.Second)
defer cancel()
evidence, err := jobdb.ReadExecutionProvenance(ctx, runtime,
    jobdb.ReadExecutionProvenanceRequest{JobKey: key, Consumer: "c2j"})
```

The reader records the committed physical ceiling, then scans body-only storage
pages through it, including linked restart prefixes, reconstruction and retained
execution records. It extracts one whole consumer/version/optional-commit/JobDB
triple per recognized versions record. It never creates Cartesian combinations,
adds the current worker or fetches artifact contents. Strict replay validation is
unchanged. Unknown unrelated fields and corrupt application payloads do not prevent
independent provenance extraction; malformed framing and unsupported layouts create
structured coverage issues.

`complete` means gap-free examination of retained execution provenance through the
ceiling, with meaningful metadata. It does not identify every process that ever
wrote the job. `partial` retains observed pairs or inspected coverage with gaps;
`unavailable` means usable coverage could not be established. Missing metadata,
missing records, unknown ceilings and exhausted bounds never become empty complete
evidence. Nullable `scannedThroughOrdinal` describes contiguous examined physical
positions, not a claim that their contents were valid.

Hard local and HTTP maxima/defaults are 64 pages, 10,000 physical/nested records,
16 MiB cumulative raw/expanded bytes, 512 distinct pairs, 128 issues, 1,500 ms,
256 KiB response, 256 physical entries/1 MiB per page, depth 64 and 4,096 UTF-8
bytes per identity. Normal page exhaustion continues; oversized bodies are skipped
before loading. Limits, usage, specific issues and detected omission counts are
returned explicitly. Unvisited counts remain null when nested records are unknown.
A shorter caller deadline lowers the reported time budget. Storage implementations
must honor context cancellation; the budget cannot forcibly interrupt foreign code
that ignores its context.

HTTP uses `GET /v1/tenants/{tenantId}/jobs/{jobId}/execution-provenance?consumer=c2j`,
`Cache-Control: no-store` and positive `execution_provenance_v1` signaling.
Delegated readers require unrestricted chapter read authority and a live parent;
ordinal subsets do not authorize an all-history summary. Hidden, unpublished,
denied and cross-tenant reads disclose no versions. Authorization projection
corruption yields unavailable evidence without reading bodies. Unsupported new
servers return HTTP 501 with `execution_provenance_unsupported`; old 404/405 routes
remain errors, with no fallback or retry. See the [optional HTTP contract](../openapi/jobdb-execution-provenance.yaml).

## Identify the linked library

`jobdb.CurrentBuildInfo()` returns JobDB's linked module path/version, nullable
revision/modified state and optional effective replacement identity. It retains
pseudo versions and versioned replacements, reports `(devel)` for local replacements,
and never borrows a host consumer's VCS revision. New execution header and activation
labels use this helper, without changing stored fields or reconstructing historical
labels from current builds. Consumer identity remains the consumer's responsibility.

The managed publication check resolves the exact published commit in a fresh module
cache, compiles the public examples and runs a separate consumer asserting its linked
version. Repository-local tests cannot establish a published version. c2j integration
and pgjobdb adoption remain with their owners.
