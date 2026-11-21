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
