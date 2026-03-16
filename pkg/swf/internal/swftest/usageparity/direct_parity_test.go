package usageparity_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

func TestDirectGetJobRunRetryRepresentationParity(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}

		t.Run(harness.Name+"/job-retry", func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryJob{}
				ws := swftest.MustWorkSet(t, job)
				return observeViaMode(t, harness, mode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.StartJob(ctx, swf.StartJob{
						TenantId: "tenant-job-run-job-" + harness.Name,
						JobType:  job.Name(),
						JobID:    "job-retry-shape",
						Data:     swftest.NumberTaskData(1),
						RunPolicy: swf.RunPolicy{
							Retry: swf.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
						},
					})
					if err != nil {
						t.Fatalf("start job retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

					resp, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
						JobKey:         jobKey,
						IncludeOutputs: true,
					})
					if err != nil {
						t.Fatalf("get job retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})

		t.Run(harness.Name+"/task-retry", func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := swftest.MustWorkSet(t, job, task)
				return observeViaMode(t, harness, mode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.StartJob(ctx, swf.StartJob{
						TenantId: "tenant-job-run-task-" + harness.Name,
						JobType:  job.Name(),
						JobID:    "task-retry-shape",
						Data:     swftest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start task retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

					resp, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
						JobKey:               jobKey,
						IncludeInputs:        true,
						IncludeOutputs:       true,
						IncludeAttemptInputs: true,
					})
					if err != nil {
						t.Fatalf("get task retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

func TestDirectGetJobRunSynthesizedNextAttemptParity(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}

		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryJob{}
				ws := swftest.MustWorkSet(t, job)
				return observeViaMode(t, harness, mode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.StartJob(ctx, swf.StartJob{
						TenantId: "tenant-synth-next-" + harness.Name,
						JobType:  job.Name(),
						JobID:    "synth-next",
						Data:     swftest.NumberTaskData(1),
						RunPolicy: swf.RunPolicy{
							Retry: swf.RetryPolicy{
								MaximumAttempts:    2,
								BackoffCoefficient: 1,
								InitialInterval:    swf.Duration(5 * time.Second),
							},
						},
					})
					if err != nil {
						t.Fatalf("start synthesized retry via %s: %v", subject.mode, err)
					}

					var resp swf.GetJobRunResponse
					deadline := time.Now().Add(2 * time.Second)
					for time.Now().Before(deadline) {
						resp, err = subject.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
						if err != nil {
							t.Fatalf("get synthesized retry run via %s: %v", subject.mode, err)
						}
						if len(resp.Attempts) > 0 && resp.Attempts[0].Outcome.Status == swf.TaskOutcomeStatusFailed {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

func TestDirectRestartWithExtraOutputDeterminismErrorParity(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}

		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) errorObservation {
				runs := 0
				ws := swftest.MustWorkSet(t, singleEchoJob{runs: &runs}, echoTask{})
				return observeViaMode(t, harness, mode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
					origInput := swf.NewTaskDataOrPanic(map[string]string{"hello": "world"})
					jobKey, err := subject.StartJob(ctx, swf.StartJob{
						TenantId: "tenant-restart-extra-" + harness.Name,
						JobType:  ws.JobWorker.Name(),
						JobID:    "restart-extra-base",
						Data:     origInput,
					})
					if err != nil {
						t.Fatalf("start restart-extra base via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

					newInput := swf.NewTaskDataOrPanic(map[string]string{"hello": "again"})
					restartKey, err := subject.RestartJob(ctx, swf.RestartJob{
						PriorJobKey:     jobKey,
						LastStepToKeep:  0,
						JobID:           "restart-extra-copy",
						ExtraTaskInput:  newInput,
						ExtraTaskOutput: newInput,
					})
					if err != nil {
						t.Fatalf("restart with extra output via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, restartKey, swf.JobStatusCompleted)

					_, err = subject.GetJobResult(ctx, restartKey)
					if err == nil || (!errors.Is(err, swf.ErrWorkflowNotDeterministic) && !strings.Contains(err.Error(), "workflow was not deterministic")) {
						t.Fatalf("expected determinism error via %s, got %v", subject.mode, err)
					}
					return errorObservation{Error: err.Error()}
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}
