package workflow_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubmissionAvailabilityFailedSuccessSettlement(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, overHTTP := range []bool{false, true} {
			for _, goal := range []bool{false, true} {
				for _, future := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/http=%v/goal=%v/future=%v", backend, overHTTP, goal, future), func(t *testing.T) {
						t.Parallel()
						ctx := celltest.Context(context.Background())
						rt := alternateRuntime(t, backend)
						if overHTTP {
							s := httptest.NewServer(celltest.Handler(remote.NewServer(rt)))
							t.Cleanup(s.Close)
							var err error
							rt, err = remote.New(s.URL, s.Client())
							require.NoError(t, err)
						}
						producer := wf.ExecutionWorker{JobType: "producer", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
							return nil, fmt.Errorf("producer failed")
						}}
						key := submitExecutionTest(t, rt, producer)
						info, err := rt.GetJob(ctx, key)
						require.NoError(t, err)
						var calls atomic.Int32
						consumer := wf.ExecutionWorker{JobType: "consumer", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
							calls.Add(1)
							return jobdb.NewTaskDataOrPanic(nil), nil
						}}
						dep := jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: key.JobId}, Condition: jobdb.JobPrereqSuccess}
						if goal {
							dep.Target = jobdb.DependencyTarget{GoalID: info.GoalID}
						}
						at := time.Now().UTC().Add(-time.Hour)
						if future {
							at = time.Now().UTC().Add(time.Second)
						}
						h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "consumer", Data: jobdb.NewTaskDataOrPanic(nil), AvailableAt: &at, Dependencies: []jobdb.Dependency{dep}}})
						require.NoError(t, err)
						if goal {
							_, err = rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: info.GoalID}, Message: "upstream rejected"})
							require.NoError(t, err)
						} else {
							out, err := runExecutionTest(t, rt, key, producer)
							require.NoError(t, err)
							require.Equal(t, wf.JobRunFailed, out.Status)
						}
						// Failed waits still have a settlement route under the availability gate.
						if future {
							// Observe scheduler readiness rather than assuming a wall-clock
							// sleep has crossed the not-before gate when acquisition runs.
							require.EventuallyWithT(t, func(c *assert.CollectT) {
								ready, err := rt.GetJob(ctx, h.JobKey)
								require.NoError(c, err)
								require.Equal(c, jobdb.JobStatusReady, ready.Status)
							}, 5*time.Second, 10*time.Millisecond)
						}
						out, err := runExecutionTest(t, rt, h.JobKey, consumer)
						require.NoError(t, err)
						require.Equal(t, wf.JobRunFailed, out.Status)
						require.True(t, out.LeaseAcquired)
						require.Zero(t, calls.Load())
						persisted, err := rt.GetJob(ctx, h.JobKey)
						require.NoError(t, err)
						require.Equal(t, jobdb.JobStatusCompleted, persisted.Status)
						require.NotNil(t, persisted.Data)
						_, err = persisted.Data.GetData()
						require.Error(t, err)
						if goal {
							require.ErrorIs(t, err, jobdb.ErrGoalFailed)
						}
						chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
						require.NoError(t, err)
						require.Greater(t, len(chapters), 1)
						summary, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: h.JobKey})
						require.NoError(t, err)
						require.Equal(t, jobdb.JobStatusCompleted, summary.Job.Status)
						require.True(t, at.Equal(summary.Job.AvailableAt))
						require.NotEmpty(t, summary.Job.CompletionStatus)
					})
				}
			}
		}
	}
}
