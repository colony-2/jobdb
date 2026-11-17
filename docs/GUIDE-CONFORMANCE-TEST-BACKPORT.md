# Run corrected conformance tests without changing the runtime pin

The correction changes two shared test sources. Keep the runtime at
`github.com/colony-2/jobdb v0.0.87-0.20261116191523-78a27e5f3db6`.
No runtime API, dependency replacement or production pin update is needed.

Use `scripts/verify-conformance-backport.py` and its adjacent
`conformance-backport/backport_test.go.txt` fixture from the correction checkout.
The script creates a new disposable consumer, uses an isolated module download
cache, vendors the exact graph, verifies every included JobDB file against the
pinned module, then executes a Go overlay containing exactly these two paths:

- `pkg/jobdb/runtimetest/submission_availability.go`
- `pkg/jobdb/runtimetest/restart_submission.go`

It never writes replacement bytes into the module cache or vendor files. Both
on-disk originals remain byte-identical. After execution it checks the complete
vendor inventory, every vendor file, replacement hashes, overlay mappings,
`go.mod`, `go.sum` and `vendor/modules.txt`. Unexpected additions, deletions or
changes fail verification. Go intentionally omits dependency test files and
module files from vendor; every included JobDB file must match the exact module.

## Immutable source identities

The original full revision is
`78a27e5f3db6663062a85d16f0a27042d7501e3d`. Original sources:

- [Availability](https://github.com/colony-2/jobdb/blob/78a27e5f3db6663062a85d16f0a27042d7501e3d/pkg/jobdb/runtimetest/submission_availability.go)
- [Restart](https://github.com/colony-2/jobdb/blob/78a27e5f3db6663062a85d16f0a27042d7501e3d/pkg/jobdb/runtimetest/restart_submission.go)

| Source | Original SHA-256 | Correction SHA-256 |
| --- | --- | --- |
| `submission_availability.go` | `36eb77e8bbb0ccd9473b98e2d8142e742e9d6bac046d04449ef774c58124c9e7` | `32ada631aff7c870142e1d35fb66fc3edfe86c92a2a2307ef5b87f1d007cd0c0` |
| `restart_submission.go` | `bc07fe4f837681515950a243ade67d50c7bfd3c7f393b70fb3956a9d4130e400` | `9932df1800035cbe6926007ee5a72c21e7247445c6b262b72e7e5fd3ac9c0e8a` |

The upstream correction and exact-pin backport use identical replacement bytes.
The correction's full published commit must be recorded after managed publication;
a local saved candidate is not evidence of fetchability. The published-mode
command below independently fetches that full commit and records complete immutable
source URLs and hashes in `result.json`. It rejects abbreviated refs and branches.
Do not replace this step with the local compatibility command.

## Execute against published sources

Run from a checkout containing the verifier and its adjacent fixture. Set
`CORRECTION_REVISION` to the full normally published correction commit, and use a
new scratch directory. The default repository is the normal GitHub URL; an
existing Git transport mirror may be passed with `--repository`.

```sh
export GOWORK=off
CORRECTION_REVISION='<full published 40-hex commit>'
python3 scripts/verify-conformance-backport.py \
  --revision "$CORRECTION_REVISION" \
  --availability-sha256 32ada631aff7c870142e1d35fb66fc3edfe86c92a2a2307ef5b87f1d007cd0c0 \
  --restart-sha256 9932df1800035cbe6926007ee5a72c21e7247445c6b262b72e7e5fd3ac9c0e8a \
  --work-dir /tmp/jobdb-backport-published
```

To validate a real consumer's unchanged graph, add `--consumer /path/to/consumer`.
The script copies that consumer without `.git` or existing vendor output, adds its
fixture only in the copy, and vendors without changing its module metadata. It
requires the exact JobDB version and rejects a replacement module. If that graph
cannot compile the fixture with its existing metadata, verification fails; do not
use `go mod tidy` or update the pin to make that run pass. Any original consumer
workspace is disabled by `GOWORK=off`.

The standalone fixture resolves its own dependency graph once before capturing its
module baseline; it does not claim to certify a pgjobdb consumer graph. The
PostgreSQL owner must integrate the overlay into its disposable native runner.
Use `-mod=vendor -overlay=/absolute/path/to/overlay.json` on all applicable Go
compilations. The overlay must have exactly the two absolute vendor-source keys,
with values pointing to the verified replacement files outside the module cache.

The verifier executes direct/parent and HTTP availability matrices, submit-only
HTTP delegation, and ready/cancelled/executed restart with boundary checks on toy
and SQLite. It requires all 236 named acceptance cases to pass, rather than merely
checking a zero process exit. It records failed and skipped test names; either a
failure, skip or missing required case fails acceptance. `tests.json` and
`tests.stderr` retain command results. `result.json` records runtime version,
correction revision, source locations/hashes and integrity separately.

## Local preparation and rejection controls

This command executes the exact-pin compatibility checks with local candidate
bytes. It deliberately records `publication_verified: false` and cannot establish
fetchable publication:

```sh
GOWORK=off python3 scripts/verify-conformance-backport.py \
  --source-dir "$PWD" \
  --availability-sha256 32ada631aff7c870142e1d35fb66fc3edfe86c92a2a2307ef5b87f1d007cd0c0 \
  --restart-sha256 9932df1800035cbe6926007ee5a72c21e7247445c6b262b72e7e5fd3ac9c0e8a \
  --work-dir /tmp/jobdb-backport-local
python3 scripts/verify-conformance-backport.py --self-test
```

The self-test exercises ten mechanism rejection controls: changed source,
replacement and module bytes; extra/missing inventory; missing/extra/unexpected
overlay paths; wrong runtime pin; and a replacement module. These controls do not
establish runtime behavior. An overlay compilation probe alone does not establish
conformance either.

Keep original PostgreSQL failures as failed acceptance evidence. JobDB's local
passes cannot certify PostgreSQL's fractional conversion, adjacent-microsecond
eligibility, original identity or durable reopening. pgjobdb owns integration and
reruns of all four native selections: service-free, installer, conformance and
runtime, with failures and skips reported independently.
