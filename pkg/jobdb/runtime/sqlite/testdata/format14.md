# Format 14 fixture

`format14.db` and `format14_pages.json` were produced by running
`format14_seed_test.go` in the SQLite package at
`4a587c1052ac1f2584fe952da4c5e8de3c4474ec`, not by relabeling a new schema.
The seed uses real submissions and lease operations, then fills an expiry-only
row and removes selected events to simulate partial history.

To regenerate, extract that commit to a temporary directory, copy the seed
into its `pkg/jobdb/runtime/sqlite/`, and run
`FORMAT14_OUT=/absolute/path/to/current/testdata go test ./pkg/jobdb/runtime/sqlite -run '^TestSeedFormat14$' -count=1`
from the temporary directory.
