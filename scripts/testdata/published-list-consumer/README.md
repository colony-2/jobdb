# Last lease update compatibility

`pkg/jobdb/runtime/remote/last_lease_update_http_test.go` exercises the tenant
and project job lists using only public API, so the same test can be copied into
a clean Go module after publication.

For old-peer checks, extract JobDB commit
`4a587c1052ac1f2584fe952da4c5e8de3c4474ec` into a temporary directory. Copy
`testdata/baseline.go` there and build it in that directory with
`GOWORK=off go build -o /absolute/path/to/baseline baseline.go`. Then run:

```sh
JOBDB_LAST_LEASE_BASELINE=/absolute/path/to/baseline go test -count=1 -v \
  ./pkg/jobdb/runtime/remote -run '^(TestPublishedLastLeaseUpdateLists|TestPinnedLastLeaseUpdateOldServer)$'
```

After managed publication, use the actual merge receipt:

```sh
python3 -B scripts/check-published-last-lease-update.py \
  --merge-receipt /path/to/merge-receipt.json --output /path/to/new-evidence-dir
```

`result.json` reports the SHA, selected version, and outcome. The verifier
disables Git URL rewrites; if direct GitHub verification is unavailable, the
result fails rather than accepting mirror-only evidence.
