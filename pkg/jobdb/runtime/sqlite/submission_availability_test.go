package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestSubmissionAvailabilityBoundaryAndReopen(t *testing.T) {
	for _, kind := range []string{"completion", "exact-success", "goal-success"} {
		for _, timeFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/time-first=%v", kind, timeFirst), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 15*time.Second)
				defer cancel()
				cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
				rt, err := NewFromConfig(ctx, cfg)
				require.NoError(t, err)
				defer func() { require.NoError(t, rt.Close(ctx)) }()
				req := availabilityRangeRequest("producer", nil)
				p, err := celltest.Submit(rt, ctx, req)
				require.NoError(t, err)
				dep := jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}
				if kind == "completion" {
					dep.Condition = jobdb.JobPrereqComplete
				}
				if kind == "goal-success" {
					dep.Target = jobdb.DependencyTarget{GoalID: p.GoalID}
				}
				at := time.Now().UTC().Add(3 * time.Second)
				req = availabilityRangeRequest("consumer", &at)
				req.Job.Dependencies = []jobdb.Dependency{dep}
				h, err := celltest.Submit(rt, ctx, req)
				require.NoError(t, err)
				check := func(resolved bool) {
					t.Helper()
					row, err := rt.loadJobRow(ctx, h.JobKey)
					require.NoError(t, err)
					for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
						status, err := statusFromRow(ctx, rt.db, row, at.Add(offset))
						require.NoError(t, err)
						want := jobdb.JobStatusPendingJobs
						if resolved {
							want = jobdb.JobStatusReady
							if offset < 0 {
								want = jobdb.JobStatusAwaitingFuture
							}
						}
						require.Equal(t, want, status)
					}
					rows, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{h.JobKey}})
					require.NoError(t, err)
					require.Len(t, rows.Jobs, 1)
					require.True(t, at.Equal(rows.Jobs[0].AvailableAt))
				}
				acquire := func() jobdb.ExecutionLease {
					t.Helper()
					lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "consumer", Routes: []jobdb.Route{{JobType: "work"}}})
					require.NoError(t, err)
					return lease
				}
				deny := func() {
					t.Helper()
					for range 3 {
						require.Nil(t, acquire())
					}
				}
				waitForTime := func() {
					t.Helper()
					timer := time.NewTimer(time.Until(at))
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				check(false)
				deny()
				reopen := func() { require.NoError(t, rt.Close(ctx)); rt, err = NewFromConfig(ctx, cfg); require.NoError(t, err) }
				if timeFirst {
					reopen()
					check(false)
					deny()
					waitForTime()
					// Elapsed availability must not bypass the persisted dependency.
					deny()
				}
				l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: p.JobKey, WorkerID: "producer", Routes: []jobdb.Route{{JobType: "work"}}})
				require.NoError(t, err)
				require.NotNil(t, l)
				ch := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}
				require.NoError(t, l.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &ch}))
				check(true)
				reopen()
				check(true)
				var lease jobdb.ExecutionLease
				if !timeFirst {
					// The dependency was released before time, and that release
					// survived reopening. Observe real denial in the reopened runtime.
					require.True(t, time.Now().Before(at), "fixture missed dependency-before-time order")
					denied := 0
					for range 3 {
						lease = acquire()
						if lease != nil {
							// An acquisition may span the availability boundary.
							require.False(t, time.Now().Before(at), "lease before availability")
							break
						}
						if time.Now().Before(at) {
							denied++
						}
					}
					require.Positive(t, denied, "no denied acquisition after reopening")
					waitForTime()
				}
				if lease == nil {
					require.Eventually(t, func() bool { lease = acquire(); return lease != nil }, 5*time.Second, 10*time.Millisecond)
				}
				require.False(t, time.Now().Before(at))
				require.Equal(t, h.JobKey, lease.Job().JobKey)
			})
		}
	}
}
