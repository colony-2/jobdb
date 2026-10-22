package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type executionPermanentError struct{}

func (executionPermanentError) Error() string      { return "permanent failure" }
func (executionPermanentError) NonRetryable() bool { return true }

type executionTypedError struct{}

func (executionTypedError) Error() string { return "typed failure" }

func TestExecutionNonRetryableFailures(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"marker", "type", "catch-preserve", "catch-rewrite"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var runs, builds atomic.Int32
				call := invocation("failure")
				call.Policy.Retry.MaximumAttempts = 3
				call.Policy.Retry.NonRetryableErrorTypes = []string{"executionTypedError"}
				failure := error(fmt.Errorf("wrapped: %w", executionPermanentError{}))
				if mode == "type" {
					failure = fmt.Errorf("wrapped: %w", executionTypedError{})
				}
				if strings.HasPrefix(mode, "catch") {
					call.Catch = []wf.CatchRule{{ID: "continue", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
						var replacement error
						if mode == "catch-rewrite" {
							replacement = errors.New("retryable replacement")
						}
						return wf.CatchResult{Disposition: wf.CatchContinue, Err: replacement}, nil
					}}}
				}
				task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return nil, failure }}
				w := wf.ExecutionWorker{JobType: "permanent", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
				}}
				key := submitExecutionTest(t, rt, w)
				out, err := runExecutionTest(t, rt, key, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunFailed, out.Status)
				want := int32(1)
				if mode == "catch-rewrite" {
					want = 3
				}
				require.Equal(t, want, runs.Load())
				require.EqualValues(t, 1, builds.Load())
				_, err = replayExecutionTest(t, rt, key, w, task)
				require.Error(t, err)
				require.Equal(t, want, runs.Load())
				require.EqualValues(t, 1, builds.Load())
			})
		}
	}
}

func TestExecutionPrerequisiteAdmission(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, success := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/success=%v", backend, success), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				prerequisite := wf.ExecutionWorker{JobType: "prerequisite", Execute: func(_ wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if !success {
						return nil, errors.New("prerequisite failure")
					}
					return in, nil
				}}
				key := submitExecutionTest(t, rt, prerequisite)
				_, err := runExecutionTest(t, rt, key, prerequisite)
				require.NoError(t, err)
				var calls atomic.Int32
				w := wf.ExecutionWorker{JobType: "dependent", Execute: func(_ wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) { calls.Add(1); return in, nil }}
				h, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("job"), Prerequisites: []jobdb.JobPrerequisite{{JobID: key.JobId, Condition: jobdb.JobPrereqSuccess}}}})
				require.NoError(t, err)
				out, err := runExecutionTest(t, rt, h.JobKey, w)
				require.NoError(t, err)
				if success {
					require.Equal(t, wf.JobRunCompleted, out.Status)
					require.EqualValues(t, 1, calls.Load())
				} else {
					require.Equal(t, wf.JobRunFailed, out.Status)
					require.ErrorContains(t, out.JobError, "did not succeed")
					require.Zero(t, calls.Load())
				}
				info, err := rt.GetJob(context.Background(), h.JobKey)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusCompleted, info.Status)
				_, err = replayExecutionTest(t, rt, h.JobKey, w)
				if success {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "did not succeed")
					require.Zero(t, calls.Load())
				}
			})
		}
	}
}

func TestExecutionLiveMismatchCompletesJob(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			base := alternateRuntime(t, backend)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt := &executionFaultRuntime{WorkflowRuntime: base, cancel: cancel, fail: func(r jobdb.ExecutionRecord) bool { return r.Kind == "result" && r.ID == `/scope:"$job"#1/attempt:1` }}
			var builds atomic.Int32
			call := invocation("stable")
			w := wf.ExecutionWorker{JobType: "mismatch", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				_, _ = c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
				return in, nil // Deliberately swallow errors; the engine must still fail.
			}}
			task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { return in, nil }}
			key := submitExecutionTest(t, rt, w)
			run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "first", LeaseDuration: 500 * time.Millisecond})
			require.NoError(t, err)
			_, err = run.Run(nil)
			require.Error(t, err)
			call.Command = []byte(`{"changed":true}`)
			var out wf.JobRunOutcome
			require.Eventually(t, func() bool {
				out, err = runExecutionTest(t, base, key, w, task)
				require.NoError(t, err)
				return out.Status != wf.JobRunNotLeaseable
			}, 2*time.Second, 20*time.Millisecond)
			require.Equal(t, wf.JobRunFailed, out.Status)
			require.ErrorContains(t, out.JobError, "logical descriptor changed")
			require.EqualValues(t, 1, builds.Load())
			job, err := base.GetJob(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusCompleted, job.Status)
		})
	}
}

type executionCompletionFaultRuntime struct {
	jobdb.WorkflowRuntime
	partial bool
	fired   atomic.Bool
}
type executionCompletionFaultLease struct {
	jobdb.ExecutionLease
	runtime *executionCompletionFaultRuntime
}

func (r *executionCompletionFaultRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	lease, err := r.WorkflowRuntime.GetJobLease(ctx, req)
	if err != nil || lease == nil {
		return lease, err
	}
	return &executionCompletionFaultLease{lease, r}, nil
}
func (l *executionCompletionFaultLease) LeaseToken() string {
	if token, ok := l.ExecutionLease.(interface{ LeaseToken() string }); ok {
		return token.LeaseToken()
	}
	return ""
}
func (l *executionCompletionFaultLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	if !l.runtime.fired.CompareAndSwap(false, true) {
		return l.ExecutionLease.Complete(ctx, req)
	}
	if l.runtime.partial {
		if err := l.runtime.WorkflowRuntime.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken(), Ref: jobdb.ChapterRef{JobKey: l.Job().JobKey, Ordinal: req.Chapter.Ordinal}, Chapter: *req.Chapter, ArtifactUploads: req.ArtifactUploads}); err != nil {
			return err
		}
	} else if err := l.ExecutionLease.Complete(ctx, req); err != nil {
		return err
	}
	return errors.New("injected lost final completion acknowledgement")
}
func TestExecutionUncertainFinalCompletion(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partial=%v", backend, partial), func(t *testing.T) {
				base := alternateRuntime(t, backend)
				rt := &executionCompletionFaultRuntime{WorkflowRuntime: base, partial: partial}
				var runs atomic.Int32
				task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
				w := wf.ExecutionWorker{JobType: "completion", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(invocation("task"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				}}
				key := submitExecutionTest(t, rt, w)
				run, err := wf.GetJobForRun(context.Background(), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "first", LeaseDuration: 500 * time.Millisecond})
				require.NoError(t, err)
				result, err := run.Run(nil)
				if partial {
					require.ErrorContains(t, err, "injected lost final")
				} else {
					// The lease session reconciles an already archived result.
					require.NoError(t, err)
					require.Equal(t, wf.JobRunCompleted, result.Status)
				}
				require.True(t, rt.fired.Load())
				before, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				if partial {
					var out wf.JobRunOutcome
					require.Eventually(t, func() bool {
						out, err = runExecutionTest(t, base, key, w, task)
						require.NoError(t, err)
						return out.Status != wf.JobRunNotLeaseable
					}, 2*time.Second, 20*time.Millisecond)
					require.Equal(t, wf.JobRunCompleted, out.Status)
				}
				job, err := base.GetJob(context.Background(), key)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusCompleted, job.Status)
				_, err = replayExecutionTest(t, base, key, w, task)
				require.NoError(t, err)
				after, err := base.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.EqualValues(t, 1, runs.Load())
			})
		}
	}
}

func TestExecutionExternalCompletionValidation(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			w := wf.ExecutionWorker{JobType: "external-validation", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoTask(invocation("external"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			info, err := rt.GetJob(context.Background(), key)
			require.NoError(t, err)
			wait := info.ExecutionState.TaskWait
			require.NotNil(t, wait)
			req := jobdb.CompleteTaskIfWaitingRequest{JobKey: key, Route: jobdb.Route{JobType: w.Name(), TaskType: "echo"}, ResumeJobType: w.Name(), InputOrdinal: wait.InputOrdinal, OutputOrdinal: wait.OutputOrdinal, InputHash: wait.InputHash, Data: jobdb.NewTaskDataOrPanic("winner")}
			before, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			for _, field := range []string{"input", "output", "generation", "missing-generation", "missing-input", "route", "resume"} {
				bad := req
				switch field {
				case "input":
					bad.InputOrdinal--
				case "output":
					bad.OutputOrdinal++
				case "generation":
					bad.InputHash = "wrong"
				case "missing-generation":
					bad.InputHash = ""
				case "missing-input":
					bad.InputOrdinal = 0
				case "route":
					bad.Route.TaskType = "wrong"
				case "resume":
					bad.ResumeJobType = "wrong"
				}
				require.Error(t, rt.CompleteTaskIfWaiting(context.Background(), bad), field)
				after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after, field)
			}
			require.NoError(t, rt.CompleteTaskIfWaiting(context.Background(), req))
			out, err = runExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			value, err := replayExecutionTest(t, rt, key, w)
			require.NoError(t, err)
			raw, err := value.GetData()
			require.NoError(t, err)
			require.JSONEq(t, `"winner"`, string(raw))
		})
	}
}
