package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type terminalFailureJob struct{ mismatch, fail bool }

func (terminalFailureJob) Name() string { return "terminal-failure" }
func (w terminalFailureJob) Run(ctx wf.JobContext, in jobdb.JobData) (jobdb.JobData, error) {
	if w.fail {
		return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "prerequisite failed"}}
	}
	out, err := ctx.DoTask(jobdb.RunPolicy{}, "echo", in)
	if err != nil {
		return nil, err
	}
	if w.mismatch {
		ctx.(interface{ ManipulateStepForTest(int64) }).ManipulateStepForTest(1)
		return ctx.DoTask(jobdb.RunPolicy{}, "echo", jobdb.NewTaskDataOrPanic("different"))
	}
	return out, nil
}

type terminalEchoTask struct{}

func (terminalEchoTask) Name() string { return "echo" }
func (terminalEchoTask) Run(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	return in, nil
}

func TestLiveHistoryFailuresCompleteAcrossRuntimes(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, kind := range []string{"determinism", "restart-mismatch", "prerequisite"} {
			if backend == "toy" && kind != "determinism" {
				continue
			} // Toy does not create RestartExtra.
			t.Run(backend+"/"+kind, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				worker := terminalFailureJob{}
				run := func(key jobdb.JobKey, job terminalFailureJob) wf.JobRunOutcome {
					runnable, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: job, TaskWorkers: []wf.TaskWorker{terminalEchoTask{}}, WorkerID: "worker", LeaseDuration: time.Minute})
					require.NoError(t, err)
					result, err := runnable.Run(nil)
					require.NoError(t, err, "runner must persist the failure and complete its lease")
					return result
				}
				input := jobdb.NewTaskDataOrPanic("original")
				handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: input, RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 3}}}})
				require.NoError(t, err)
				expected := "input hash mismatch"
				var retained []jobdb.Chapter
				if kind == "determinism" {
					worker.mismatch = true
				} else {
					require.Equal(t, wf.JobRunCompleted, run(handle.JobKey, worker).Status)
					restart := jobdb.SubmitRestartJob{PriorJobKey: handle.JobKey, LastStepToKeep: 0, ExtraTaskInput: input, ExtraTaskOutput: input}
					if kind == "restart-mismatch" {
						restart.ExtraTaskInput = jobdb.NewTaskDataOrPanic("different")
					} else {
						prerequisite, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: input, RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 3}}}})
						require.NoError(t, err)
						require.Equal(t, wf.JobRunFailed, run(prerequisite.JobKey, terminalFailureJob{fail: true}).Status)
						restart.Prerequisites = []jobdb.JobPrerequisite{{JobID: prerequisite.JobKey.JobId, Condition: jobdb.JobPrereqSuccess}}
						expected = "did not succeed"
					}
					handle, err = rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: restart})
					require.NoError(t, err)
					retained, err = rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
					require.NoError(t, err)
					require.Len(t, retained, 2)
					require.IsType(t, jobdb.RestartExtraChapter{}, retained[1].Body)
				}
				result := run(handle.JobKey, worker)
				require.Equal(t, wf.JobRunFailed, result.Status)
				require.ErrorContains(t, result.JobError, expected)
				job, err := rt.GetJob(ctx, handle.JobKey)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusCompleted, job.Status)
				_, err = job.Data.GetData()
				require.ErrorContains(t, err, expected)
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
				require.NoError(t, err)
				require.Len(t, before, 3)
				require.IsType(t, jobdb.JobAttemptOutcomeChapter{}, before[2].Body)
				if retained != nil {
					require.Equal(t, retained, before[:2])
				}
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(worker, terminalEchoTask{}).BuildEngine()
				require.NoError(t, err)
				_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: handle.JobKey, JobWorker: worker})
				require.ErrorContains(t, err, expected)
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
				require.NoError(t, err)
				require.Equal(t, before, after, "replay must not mutate history")
			})
		}
	}
}
