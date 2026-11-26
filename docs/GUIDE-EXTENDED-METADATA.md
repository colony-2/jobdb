# Edit job metadata independently of execution

Toy and SQLite support `JobMetadataRuntime.EditJobMetadata` and `JobMetadataLease.EditJobMetadata`. These are optional Go interfaces. Use them for independently maintained application annotations on real jobs, not for task input, task output, or goal data. These annotations are separate from the ordinary metadata supplied when creating a job. Editing a completed or retained job does not reopen it. If an edit commits and the task later fails, the edit remains.

```go
version := jobdb.MetadataVersion(0)
entry, err := writer.EditJobMetadata(ctx, jobKey, jobdb.JobMetadataEdit{
    Key:             "annotation",
    ExpectedVersion: &version,
    Set:             json.RawMessage(`{"score":9007199254740993,"comment":null}`),
})
```

Version `0` means the key has never been written. Every change advances that key's version by one. A version mismatch returns `jobdb.ErrConflict`; read the current entry before deciding whether to retry. Other keys have independent versions. For removal, set `Remove: true` and omit `Set`. Setting the JSON value `null` instead leaves the key present. A removal leaves the last version visible with `Present: false` and no value. An edit that leaves the key unchanged does not advance its version. `JobInfo.ExtendedMetadata` and `JobSummary.ExtendedMetadata` return the current entries; they do not expose the internal change log.

Lease-free calls use the runtime's existing project admission and the deployment's existing access boundary. An ordinary live execution lease can instead edit its own job using `JobMetadataLease`. Restricted, expired, or replaced leases cannot edit and never fall back to lease-free admission. Deleted jobs and scheduler placeholders cannot be edited. Extended metadata on detail reads follows the same tenant, cell, and `jobs.list` scope as list reads. Outside that scope, a detail read that otherwise succeeds still returns its legacy fields, but omits extended metadata and its versions.

On HTTP, send `PATCH /v1/tenants/{tenantId}/jobs/{jobId}/extended-metadata` or `PATCH /v1/projects/{projectId}/jobs/{jobId}/extended-metadata`. For a lease edit, insert `/leases/{leaseId}` after the job ID and send `X-JobDB-Lease-Token`. Use only the path to select the job. A request body is `{"key":"annotation","expectedVersion":"0","set":null}` or `{"key":"annotation","expectedVersion":"1","remove":true}`. Versions are decimal strings to avoid precision loss. The response and existing detail/list endpoints use the same entry shape, for example `{"version":"1","present":true,"value":null}`.

SQLite opens existing format-15 databases and installs the job metadata tables together, without rewriting jobs, goals, or chapters. Prior formats still use the existing explicit migration process.
