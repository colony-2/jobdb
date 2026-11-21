package toyimpl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestSubmissionAvailabilityBoundary(t *testing.T) {
	for _, kind := range []string{"completion", "exact-success", "goal-success"} {
		for _, resolved := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resolved=%v", kind, resolved), func(t *testing.T) {
				rt := New(WithProjects(celltest.UnrestrictedProjects{}))
				ctx := celltest.Context(context.Background())
				p, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: "producer", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
				require.NoError(t, err)
				dep := jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}
				if kind == "completion" {
					dep.Condition = jobdb.JobPrereqComplete
				}
				if kind == "goal-success" {
					dep.Target = jobdb.DependencyTarget{GoalID: p.GoalID}
				}
				at := time.Now().UTC().Add(time.Hour)
				h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: "consumer", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), AvailableAt: &at, Dependencies: []jobdb.Dependency{dep}}})
				require.NoError(t, err)
				if resolved {
					l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: p.JobKey, WorkerID: "producer", Routes: []jobdb.Route{{JobType: "work"}}})
					require.NoError(t, err)
					require.NotNil(t, l)
					ch := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}
					require.NoError(t, l.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &ch}))
					// Observe pending first, the transition which previously bypassed time.
					rec := rt.engine.jobRecords[h.JobKey]
					rec.status = jobdb.JobStatusPendingJobs
				}
				rec := rt.engine.jobRecords[h.JobKey]
				beforeStatus, beforeAvailable, beforeWait := rec.status, rec.availableAt, rec.dependencyWaitID
				for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
					want := jobdb.JobStatusPendingJobs
					if resolved {
						want = jobdb.JobStatusReady
						if offset < 0 {
							want = jobdb.JobStatusAwaitingFuture
						}
					}
					require.Equal(t, want, rt.engine.availableStatusLocked("tenant", at.Add(offset), rec))
					require.Equal(t, beforeStatus, rec.status)
					require.Equal(t, beforeAvailable, rec.availableAt)
					require.Equal(t, beforeWait, rec.dependencyWaitID)
				}
				if resolved {
					for _, status := range []jobdb.JobStatus{jobdb.JobStatusCompleted, jobdb.JobStatusCancelled, jobdb.JobStatusActive, jobdb.JobStatusCrashConcern} {
						copy := jobRecord{key: rec.key, status: status, availableAt: rec.availableAt, dependencyWaitID: rec.dependencyWaitID}
						copy.status = status
						if status == jobdb.JobStatusCompleted {
							copy.archived = &at
						}
						if status == jobdb.JobStatusCancelled {
							copy.cancelled = true
						}
						if status == jobdb.JobStatusActive {
							copy.leased = true
							copy.leaseExpiresAt = at.Add(time.Hour)
						}
						require.Equal(t, status, rt.engine.availableStatusLocked("tenant", at.Add(-time.Nanosecond), &copy))
					}
				}
			})
		}
	}
}
