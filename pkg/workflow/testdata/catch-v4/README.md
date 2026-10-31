# Supported v4 catch fixtures

`compact.json` retains execution chapters and their public chapter metadata from
a real public-API engine run: scope `caught` fails with `original`, rule
`recover` handles it with output `recovered`, and retry is recorded false.
All input, output, worker names and failures are synthetic. There are no secrets,
artifacts or generated job-key references. Activation IDs and recording times
are retained as evidence, never used to derive expected associations.

`full.json` expands that run through public GetJobRun into one logical record per
entry. Tests place these records in separate physical ExecutionChapters to
exercise the supported v4 full representation. It is an expanded fixture, not
a claim that current compact writers naturally produce full histories.

Regenerate from the repository root using:

```sh
go run pkg/workflow/testdata/catch-v4/generate.go
```

The generator uses only public SDK/runtime APIs. It contains no private metadata
parser. Public-contract tests independently expect one accepted scope catch,
original failure, selected rule, failure-free handled output, false retry and
explicit origin/provenance gaps. Prefix and corruption variants are intentional
test-controlled history reads; they are not claims about naturally committed
engine frontiers.

`storage-sqlite.json` freezes physical execution records and complete backend
metadata from pre-feature revision `0a075fb5d39af7dfec3377c8d917c7b3d4cc248b`.
It was captured in an isolated `git archive` of that revision using the same
public-API scenario as the generator, through SQLite `ListChapters`. It includes
SQLite's chapter envelope metadata, absent from the toy runtime fixture above.
All payloads are synthetic; no artifacts, secrets or generated job keys occur.

`TestCatchHistoryStorageWriterCompatibility` compares new physical writes against
that pre-feature fixture through both SQLite and HTTP clients. It uses generic
JSON to retain unknown fields and checks chapter count, order, all record fields
and the complete metadata wrapper, including the packed prefix string. Only
per-run record `at`, activation IDs, chapter `created_at` and build-version values
are normalized; field presence, execution version, zero deadlines, policies,
errors, outputs, identities, dispositions and packed-record order remain exact.
The unchanged test also passes in the isolated pre-feature tree, without the new
reader API. Existing full/compact tests separately verify reader compatibility.

Do not regenerate these fixtures to accommodate format changes without explicitly
reviewing the storage compatibility requirement. The existing generator updates
only the toy compact/full fixtures; it does not overwrite this pre-feature
storage reference.
