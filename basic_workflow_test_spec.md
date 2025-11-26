# Basic Workflow Integration Test Spec

Goal: prove the simplest end-to-end flow works with two engines sharing a job worker but splitting task capabilities.

## High-Level Scenario
- Start embedded Postgres via the fergusstrange `embedded-postgres` helper and capture its DSN.
- Install/prepare the pgwf schema using the pgwf installer against that Postgres instance.
- Start an embedded Strata daemon via strata-go and capture its base URL.
- Build two engines (`e1`, `e2`) with `swf.EngineBuilder` + `swf/impl.Builder` using the same tenant ID, the Postgres DSN, and the Strata URL.
- Define two task workers: `t1` and `t2`. They read/write simple numeric data in `TaskData` to keep assertions deterministic (e.g., `t1` adds 1, `t2` doubles).
- Define one job worker `pipe` whose `Run` calls `DoTask` in the sequence [`t1`, `t2`, `t1`, `t2`], threading the previous output as the next input.
- Register the job worker in both engines, but only register `t1` in `e1` and only `t2` in `e2`.
- Start both engines’ `Run` loops, start a job from `e1` with initial data, and assert the job completes with the expected final value.

## Detailed Test Steps
1) **Boot test dependencies**
   - Start embedded Postgres with the fergusstrange `embedded-postgres` library; wait until it reports ready; expose DSN for GORM/pgwf.
   - Run the pgwf schema installer (from `pgwf-go/pkg/pgwf`) against that DSN to prepare tables.
   - Launch an embedded Strata daemon (strata-go helper); wait for health/readiness; record base URL.
2) **Build engines**
   - Instantiate two `EngineBuilder`s with the same tenant ID, `WithPostgresDSN(dsn)`, `WithStrata(baseURL)`, and `WithMaxActive` small (e.g., 4).
   - Register workers with `PlusWorkers`:
     - `e1`: job worker `pipe`, task worker `t1`.
     - `e2`: job worker `pipe`, task worker `t2`.
   - Call `Build(impl.Builder)` for each builder and assert no error.
3) **Define workers**
   - `t1.Run`: read `TaskData` as integer `n`; return `n+1`.
   - `t2.Run`: read integer `n`; return `n*2`.
   - `pipe.Run`: sequence through `DoTask("t1")`, `DoTask("t2")`, `DoTask("t1")`, `DoTask("t2")`; each `DoTask` passes along the prior output; final output returned to persist as the job’s last chapter.
4) **Run workflow**
   - Start both engines’ `Run(ctx)` loops (background contexts with cancel).
   - Kick off a job via `e1.StartJob` with `JobType` = `pipe.Name()`, `SingletonKey` empty, and initial `JobData` containing integer `1`.
   - Poll for completion by:
     - Checking pgwf job status for the job ID reaching “done”/no outstanding leases, and
     - Fetching the Strata story chapters to ensure four task chapters exist and the final value equals `(((1+1)*2)+1)*2 = 8`.
5) **Assertions**
   - The job reaches a terminal state without retries or cancellations.
   - Exactly four task chapters exist in Strata with ordinals 1–4 and bodies matching the expected intermediate values.
   - Both engines exercised: `t1` executions occur only on `e1`; `t2` executions occur only on `e2` (verify via logs or worker IDs saved with chapters).
6) **Cleanup**
   - Cancel contexts, stop embedded Strata, and shut down embedded Postgres.

## Current Blockers/Gaps in Code
- Fixed: engine builder’s worker slice construction no longer injects zero `WorkSet`s; capability registration avoids empty entries.
- Fixed: run loop now keys off the lease’s capability to pick the `WorkSet`, and story keys use `{tenantId, jobId}` consistently (including task handle finishes).
- Fixed: story ordinals are monotonic starting at 1 (job start is 0), and data unmarshalling now targets a real map.
- Pending verification: confirm pgwf leases expose `NextNeed()` (or adapt to the correct accessor) so the runner selects the right `WorkSet` for each lease.

## Acceptance Criteria
- The test reliably passes end-to-end: starting the job yields the expected final value and leaves no stuck leases.
- Logs or chapter metadata demonstrate `e1` ran `t1` and `e2` ran `t2`.
- All blockers above are addressed or temporarily patched in the test harness so the scenario can execute.
