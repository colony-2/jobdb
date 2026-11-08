package workflow_test

import (
	"context"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestCancellationResultsPreserveStoredReason(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			for _, tc := range []struct{ name, reason string }{
				{"empty", ""},
				{"whitespace", " \t\n "},
				{"unicode", "  Use existing artifacts.\n请不要生成 PDF ☃\n"},
				{"long", strings.Repeat("operator feedback ☃\n", 5000)},
			} {
				for _, state := range []string{"before_execution", "external_wait"} {
					t.Run(tc.name+"/"+state, func(t *testing.T) {
						ctx := celltest.Context(context.Background())
						runs := 0
						worker := wf.ExecutionWorker{JobType: "cancellation-review", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
							runs++
							return c.DoTask(invocation("review"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
						}}
						key := submitExecutionTest(t, rt, worker)
						if state == "external_wait" {
							outcome, err := runExecutionTest(t, rt, key, worker)
							require.NoError(t, err)
							require.Equal(t, wf.JobRunSuspended, outcome.Status)
							require.NotNil(t, outcome.MissingRoute)
							require.Equal(t, "echo", outcome.MissingRoute.TaskType)
						}
						beforeRuns := runs
						require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key, Reason: tc.reason}))
						info, err := rt.GetJob(ctx, key)
						require.NoError(t, err)
						require.Equal(t, jobdb.JobStatusCancelled, info.Status)
						require.Equal(t, tc.reason, info.CompletionDetail)
						run, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key})
						require.NoError(t, err)
						require.Equal(t, tc.reason, run.Job.CompletionDetail)
						for _, attempt := range run.Attempts {
							require.Nil(t, attempt.Output, "cancellation needs no final output")
						}
						output, err := run.GetOutput(nil, key.TenantId)
						require.Nil(t, output)
						assertCancellationReason(t, err, tc.reason)
						outcome, err := runExecutionTest(t, rt, key, worker)
						require.NoError(t, err)
						require.Equal(t, wf.JobRunFailed, outcome.Status)
						require.NotNil(t, outcome.JobStatus)
						require.Equal(t, jobdb.JobStatusCancelled, *outcome.JobStatus)
						require.False(t, outcome.LeaseAcquired)
						require.Nil(t, outcome.Output)
						assertCancellationReason(t, outcome.JobError, tc.reason)
						require.Equal(t, beforeRuns, runs, "reading a cancelled result must not execute the job")
					})
				}
			}
		})
	}
}

func assertCancellationReason(t *testing.T, err error, reason string) {
	t.Helper()
	require.ErrorIs(t, err, jobdb.ErrJobCancelled)
	require.NotErrorIs(t, err, jobdb.ErrJobFailed)
	var cancelled *wf.JobCancelledError
	require.ErrorAs(t, err, &cancelled)
	require.Equal(t, reason, cancelled.Detail)
	want := "job cancelled"
	if reason != "" {
		want += ": " + reason
	}
	require.EqualError(t, err, want)
}
