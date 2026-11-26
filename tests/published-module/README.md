# Published immutable-goal consumer

`goal_publication_test.go` uses the public toy and SQLite APIs to exercise selected
admission, accepted replay after closure, protected failure publication and replay,
frozen member snapshots, late execution success, chapter-free typed reads and
protected restart rejection. SQLite also closes and reopens its temporary file.
No required scenario skips. The local candidate check is:

```sh
go test -count=1 -v ./tests/published-module/...
python3 -m unittest discover -s scripts -p 'test_check_published_goals.py' -v
```

After the managed recipe publishes, save its actual native merge outputs JSON,
including `merged_hash`, `publication_disposition: published` and
`publication_branch: refs/heads/main`. The preserved `native_merge_task` wrapper
is also accepted. Then run:

```sh
python3 scripts/check-published-goals.py \
  --merge-receipt /absolute/path/native-merge.json \
  --output /absolute/path/new-external-evidence
```

The runner creates a fresh external module and initially empty module and build
caches, uses `GOPROXY=direct`, disables workspace/GOFLAGS/Go environment-file
overrides, and copies only the authored consumer test and identity probe. Go
resolves the full merged hash and acquires the exact canonical version normally.
The runner compares both origin hashes, module/go.mod checksums, consumer sums,
selected packages and linked build information, then runs both backend scenarios.
It does not replace modules, edit caches or publish Git. A supplied independent
Go download receipt can also be compared with `--module-receipt PATH`.

Each attempt needs a new evidence directory. `report.md`, `result.json`, native
and module receipts, isolation settings, and numbered stdout/stderr captures
retain actual commands and exit statuses, including acquisition failures and
unused module-inventory errors. A required acquisition, build, identity or
behavior failure fails acceptance. Blocked tests remain unexecuted. The temporary
consumer/caches are removed after the attempt; captures and consumer go.mod/go.sum
remain available. Do not label local source execution or controlled checker
fixtures as published-module acceptance. Publication completes only the bounded
JobDB delivery; downstream integration and deployment are separate work.

The separate retained-read check uses the existing publication verifier:

```sh
python3 -B scripts/verify-published-module.py FULL_PUBLISHED_HASH /absolute/path/retained-read-consumption.md
```

Run from a checkout at that exact published hash. The external consumer downloads
that module into a fresh cache with workspace mode disabled and no replacement.
It runs the public retained-SQLite denial/revocation fixture and separately builds
the exact `f885723cc03ce9c8b52365390dca9601109d2c7e` baseline client (`v0.0.94`).
The report requires named passing behavioral cases, records repository, main ref,
full commit, canonical module version, origins, checksums and linked build identity.

Current main cannot reopen the baseline format-11 database without conversion.
The report explicitly marks that compatibility failed, while checking unchanged
baseline files and baseline-reader recovery. Passing HTTP/client acceptance does
not establish full baseline API or storage compatibility. No conversion is run.

## Job metadata consumer

After managed publication, set `FULL_PUBLISHED_HASH` from the native merge
receipt and run from a checkout at that commit:

```sh
set -eu
python3 -B scripts/verify-published-module.py "$FULL_PUBLISHED_HASH" module-consumption.md

test "$(git rev-parse HEAD)" = "$FULL_PUBLISHED_HASH"
consumer=$(mktemp -d)
cp tests/published-module/job_metadata_publication_test.go "$consumer/"
(
  cd "$consumer"
  export GOENV=off GOWORK=off GOFLAGS='' GOTOOLCHAIN=local
  export GOMODCACHE="$consumer/module-cache" GOCACHE="$consumer/build-cache"
  export GOPRIVATE='github.com/colony-2/*' GOPROXY=direct
  test ! -e "$GOMODCACHE"
  module=github.com/colony-2/jobdb
  go mod init example.invalid/jobdb-metadata-consumer
  go list -m -json "$module@$FULL_PUBLISHED_HASH" > module-resolution.json
  version=$(jq -er --arg module "$module" --arg hash "$FULL_PUBLISHED_HASH" '
    select(.Path == $module and .Origin.VCS == "git" and .Origin.Hash == $hash
      and (.Origin.URL | length > 0) and .Replace == null) | .Version' module-resolution.json)
  go mod download -json "$module@$version" > module-download.json
  jq -e --arg module "$module" --arg hash "$FULL_PUBLISHED_HASH" --arg version "$version" '
    .Path == $module and .Version == $version and .Origin.VCS == "git"
    and .Origin.Hash == $hash and (.Origin.URL | length > 0) and .Replace == null and .Error == null
    and (.Sum | startswith("h1:")) and (.GoModSum | startswith("h1:"))' module-download.json
  go mod edit -require="$module@$version"
  go mod tidy
  go list -m -json "$module" > module-selected.json
  jq -e --arg version "$version" '.Version == $version and .Replace == null' module-selected.json
  go test -mod=readonly -count=1 -v -run '^TestPublishedJobMetadata$' > smoke.log 2>&1
  cat smoke.log
  rg -q '^--- PASS: TestPublishedJobMetadata ' smoke.log
)
```

Keep `module-consumption.md`, the resolution, download and selected module
records, `go.mod`, `go.sum`, and `smoke.log` together as release evidence. A test
run from this repository only checks local code, not the published dependency.
