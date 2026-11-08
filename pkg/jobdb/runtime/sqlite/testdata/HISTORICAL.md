The historical compatibility driver must be compiled against authentic source
revision `f92ae253a5da7a6b81944d0bdf4cadb085772140`, not the current module.
The producer job `3EhcYPdVGfpaXtxMoxXYrAIQ15A` records this as its initial candidate.
The source is retrievable with `git fetch origin f92ae253a5da7a6b81944d0bdf4cadb085772140`.

Export that revision to a scratch directory, copy `historical-writer.go` to its
root, then run `go build -o /tmp/jobdb-historical-writer historical-writer.go`
there. In the current checkout, run:

```sh
JOBDB_HISTORICAL_WRITER=/tmp/jobdb-historical-writer go test ./pkg/jobdb/runtime/sqlite -run TestLeaseEventsHistoricalMixedWriter -count=1
```

The driver uses the genuine old runtime to submit, acquire, renew and complete a
job, creating chapters and goal/cell state. The test invokes it before and after
the new runtime installs the additive event extension. It verifies reopening,
legacy writes, preserved signing keys and partial history. Without the binary,
this specific test explicitly skips; other compatibility checks remain active.
