package workflow_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestExecutionGoalIdentityAndBlockingWait(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			ctx := context.Background()
			var want jobdb.JobHandle
			var identities []jobdb.JobHandle
			task := executionTestTask{"echo", func(tc wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				require.Equal(t, want, tc.Job())
				child, err := tc.SubmitJob(ctx, jobdb.SubmitJob{JobID: "child", JobType: "child", Data: jobdb.NewTaskDataOrPanic(nil)})
				require.NoError(t, err)
				require.NotEmpty(t, child.GoalID)
				require.Equal(t, want.JobKey.TenantId, child.JobKey.TenantId)
				return in, nil
			}}
			w := wf.ExecutionWorker{JobType: "identity", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				identities = append(identities, c.Job())
				return c.DoTask(invocation("identity"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			var err error
			want, err = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			other, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "other", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			_, err = rt.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: "tenant", GoalID: other.GoalID}, JobID: want.JobKey.JobId})
			require.NoError(t, err)
			out, err := runExecutionTest(t, rt, want.JobKey, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			_, err = replayExecutionTest(t, rt, want.JobKey, w, task)
			require.NoError(t, err)
			require.Equal(t, []jobdb.JobHandle{want, want}, identities)
			detail, err := wf.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: want.JobKey})
			require.NoError(t, err)
			require.Equal(t, want.GoalID, detail.Job.GoalID)
			require.Equal(t, []string{other.GoalID}, detail.Job.OtherGoalIDs)

			upstream, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "upstream", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			waiter := wf.ExecutionWorker{JobType: "waiter", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				_, err := c.AwaitGoals(upstream.GoalID)
				return in, err
			}}
			key := submitExecutionTest(t, rt, waiter)
			out, err = runExecutionTest(t, rt, key, waiter)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			require.NotNil(t, out.BlockingWait)
			require.NotEmpty(t, out.BlockingWait.WaitID)
			require.Equal(t, upstream.GoalID, out.BlockingWait.Dependencies[0].Target.GoalID)
			waits, err := rt.ListDependencyWaits(ctx, jobdb.ListDependencyWaitsRequest{TenantId: "tenant", ConsumerJobID: key.JobId})
			require.NoError(t, err)
			require.Len(t, waits.Waits, 1)
			require.Equal(t, out.BlockingWait.WaitID, waits.Waits[0].WaitID)
		})
	}
}
