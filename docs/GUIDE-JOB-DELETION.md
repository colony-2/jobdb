# Deleting jobs

Delete jobs from listings by tenant, creation age, job ID, or application
metadata:

```sh
# Preview the selection.
jobdb jobs delete -t c2 --older-than 30d -m recipe=build --dry-run

# Hide matching jobs and revoke their leases.
jobdb jobs delete -t c2 --older-than 30d -m recipe=build

# Multiple predicates are ANDed; values are strings.
jobdb jobs delete -t c2 -o 7d -m recipe=build -m environment=dev

# Select IDs, a fixed cutoff, or the entire tenant.
jobdb jobs delete -t c2 JOB_ID ANOTHER_JOB_ID
jobdb jobs delete -t c2 -b 2026-10-01T00:00:00Z
jobdb jobs delete -t c2 --all
```

`--tenant/-t` is required. Supply at least one selector, or explicitly use
`--all/-a`. `--all` cannot be combined with selectors. Age, IDs, and metadata
selectors intersect; any listed ID can match. Metadata names refer to top-level
application fields, such as `recipe`, without an `app.` prefix.

`--older-than/-o` accepts days (`30d`, each day is 24 hours) or Go durations such
as `12h`. It becomes one fixed UTC cutoff for the request. `--created-before/-b`
accepts RFC3339 timestamps. These two options are mutually exclusive. The cutoff
is **exclusive** and applies to creation time, not completion time.

Use `--url/-u` to select a server (default `http://127.0.0.1:9047`),
`--dry-run/-n` to count without mutation, and `--json/-j` for JSON output.
`JOBDB_BEARER_TOKEN` supplies the bearer token when the deployment requires one.
A dry run does not reserve its matches; the deletion request evaluates current
state. There is no interactive confirmation prompt.

## Semantics

- Matching jobs disappear from normal job and schedule-run listings, before
  pagination. Repeating deletion does not count already-hidden jobs again.
- Leases are revoked. Unfinished jobs become cancelled and cannot be picked up
  again. Completed outcomes are preserved.
- Direct job and history reads remain available. Chapters, artifact bytes, and
  dependency facts remain stored, so thin clones and restarted jobs retain
  their shared history. Restarting from a hidden source is supported.
- There is no implicit cascade to child jobs, clones, or schedules. IDs continue
  to identify the same jobs; idempotent submission does not unhide them.
- This does not reclaim disk space. `ListJobs` can explicitly opt into
  `includeHidden` for inspection; direct history lookup does this internally.

Selection and mutation are atomic per request in the built-in runtimes. A failed
transaction leaves visibility and leases unchanged.

## REST API

`POST /v1/tenants/{tenantId}/jobs/delete` is included in the main OpenAPI spec.

```json
{
  "createdBefore": "2026-10-01T00:00:00Z",
  "metadataPredicates": [
    {"path": ["recipe"], "values": [{"kind": "string", "stringValue": "build"}]}
  ],
  "dryRun": false
}
```

Optional selectors are `jobIds`, `createdBefore`, and `metadataPredicates`.
Use `{"all": true}` to select every visible job in the path tenant. Metadata
uses the same typed equality format as job listing: predicates are ANDed, and
values within one predicate are ORed. Invalid or unknown request fields are
rejected rather than ignored.

The response reports counts without returning an unbounded list of job IDs:

```json
{"matchedCount": 42, "deletedCount": 42}
```

Dry runs return `deletedCount: 0`. Invalid selectors return HTTP 400; unsupported
backends return HTTP 501.

## Go and backend support

```go
filter, err := jobdb.Metadata().EqualFilter("recipe", "build")
if err != nil { return err }
admin, ok := runtime.(jobdb.JobDeletionRuntime)
if !ok { return jobdb.ErrJobDeletionUnsupported }
result, err := admin.DeleteJobs(ctx, jobdb.DeleteJobsRequest{
    TenantID: "c2",
    CreatedBefore: &cutoff,
    MetadataFilter: filter,
    DryRun: true,
})
```

SQLite, toy, and the HTTP client implement `JobDeletionRuntime`. Runtime core
delegates to the optional `runtimecore.JobDeletionScheduler` port; other storage
adapters must implement its atomic selection, hiding, and lease-revocation
contract and honor `ListJobsRequest.IncludeHidden`. Until then, deletion returns
`ErrJobDeletionUnsupported`. No changes to chapter/blob stores are required.

This change targets fresh SQLite databases (format 6). No migration from prior
database formats is provided.
