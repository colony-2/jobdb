package usageparity_test

import (
	"context"
	"strings"
	"testing"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type cancelObservation struct {
	JobKey       swf.JobKey             `json:"jobKey"`
	Status       swf.JobStatus          `json:"status"`
	ResultErr    string                 `json:"resultErr,omitempty"`
	OutputErr    string                 `json:"outputErr,omitempty"`
	JobRun       normalizedJobRun       `json:"jobRun"`
	Listed       []normalizedJobSummary `json:"listed"`
	WaitingInput normalizedTaskData     `json:"waitingInput"`
}

type errorObservation struct {
	Error string `json:"error"`
}

func TestExplicitJobIDParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, passthroughJob{name: "custom-id-job"})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				jobKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-custom-id-" + harness.Name,
					JobType:  ws.JobWorker.Name(),
					JobID:    "custom-job-id",
					Data:     swftest.NumberTaskData(7),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				result, err := subject.GetJobResult(ctx, jobKey)
				if err != nil {
					t.Fatalf("get job result via %s: %v", subject.mode, err)
				}
				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:         jobKey,
					IncludeOutputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, swf.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []swf.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list jobs via %s: %v", subject.mode, err)
				}
				return lifecycleObservation{
					JobKey:    jobKey,
					Status:    swf.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: "",
					JobRun:    normalizeJobRun(t, run, outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestCancelJobParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, pendingJob{})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) cancelObservation {
				jobKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-cancel-" + harness.Name,
					JobType:  ws.JobWorker.Name(),
					JobID:    "cancel-parity",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}

				handle := swftest.WaitForTaskHandle(t, ctx, subject.Engine(), ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})
				handleData, err := handle.Data()
				if err != nil {
					t.Fatalf("waiting task data via %s: %v", subject.mode, err)
				}

				if err := subject.CancelJob(ctx, swf.CancelJob{
					JobKey: jobKey,
					Reason: "usage parity cancel",
				}); err != nil {
					t.Fatalf("cancel via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCancelled)

				result, resultErr := subject.GetJobResult(ctx, jobKey)
				_ = result
				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, swf.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []swf.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list jobs via %s: %v", subject.mode, err)
				}

				return cancelObservation{
					JobKey:       jobKey,
					Status:       swf.JobStatusCancelled,
					ResultErr:    normalizeError(resultErr),
					OutputErr:    normalizeError(outputErr),
					JobRun:       normalizeJobRun(t, run, outputErr),
					Listed:       normalizeJobSummaries(listed.Jobs),
					WaitingInput: normalizeTaskDataResult(t, handleData),
				}
			})
		})
	}
}

func TestRestartJobParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				origKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-restart-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					JobID:    "restart-original",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start original via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, origKey, swf.JobStatusCompleted)

				restartKey, err := subject.RestartJob(ctx, swf.RestartJob{
					PriorJobKey:    origKey,
					LastStepToKeep: 0,
					JobID:          "restart-copy",
				})
				if err != nil {
					t.Fatalf("restart via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, restartKey, swf.JobStatusCompleted)

				result, err := subject.GetJobResult(ctx, restartKey)
				if err != nil {
					t.Fatalf("get restart result via %s: %v", subject.mode, err)
				}
				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:               restartKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get restart run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), restartKey.TenantId)
				listed, err := subject.ListJobs(ctx, swf.ListJobsRequest{
					TenantIds: []string{restartKey.TenantId},
					JobKeys:   []swf.JobKey{restartKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list restart job via %s: %v", subject.mode, err)
				}

				return lifecycleObservation{
					JobKey:    restartKey,
					Status:    swf.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					JobRun:    normalizeJobRun(t, run, outputErr),
					ResultErr: normalizeError(outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestRestartValidationParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness

		t.Run(harness.Name+"/negative-last-step", func(t *testing.T) {
			ws := swftest.MustWorkSet(t, passthroughJob{name: "restart-negative"})
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
				_, err := subject.RestartJob(ctx, swf.RestartJob{
					PriorJobKey:    swf.JobKey{TenantId: "tenant", JobId: "missing"},
					LastStepToKeep: -1,
				})
				if err == nil {
					t.Fatalf("expected restart validation error via %s", subject.mode)
				}
				return errorObservation{Error: err.Error()}
			})
		})

		t.Run(harness.Name+"/missing-next-chapter", func(t *testing.T) {
			ws := swftest.MustWorkSet(t, passthroughJob{name: "restart-missing-next"})
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
				jobKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-missing-next-" + harness.Name,
					JobType:  ws.JobWorker.Name(),
					JobID:    "restart-base",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start base via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				_, err = subject.RestartJob(ctx, swf.RestartJob{
					PriorJobKey:    jobKey,
					LastStepToKeep: 1,
					JobID:          "restart-missing-next-copy",
				})
				if err == nil {
					t.Fatalf("expected restart missing-next error via %s", subject.mode)
				}
				return errorObservation{Error: err.Error()}
			})
		})

		t.Run(harness.Name+"/retry-boundary", func(t *testing.T) {
			run := func(mode parityMode) errorObservation {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := swftest.MustWorkSet(t, job, task)
				return observeViaMode(t, harness, mode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
					jobKey, err := subject.StartJob(ctx, swf.StartJob{
						TenantId: "tenant-retry-boundary-" + harness.Name,
						JobType:  job.Name(),
						JobID:    "retry-boundary",
						Data:     swftest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start retry-boundary base via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

					_, err = subject.RestartJob(ctx, swf.RestartJob{
						PriorJobKey:    jobKey,
						LastStepToKeep: 1,
						JobID:          "retry-boundary-copy",
					})
					if err == nil {
						t.Fatalf("expected retry-boundary validation error via %s", subject.mode)
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

func TestPrerequisiteParityAcrossBuiltInRuntimes(t *testing.T) {
	successWorker := passthroughJob{name: "prereq-success-job"}
	failWorker := namedFailingJob{name: "prereq-fail-job", message: "prereq failed"}
	dependentWorker := passthroughJob{name: "prereq-dependent-job"}

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{
				swftest.MustWorkSet(t, successWorker),
				swftest.MustWorkSet(t, failWorker),
				swftest.MustWorkSet(t, dependentWorker),
			}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
				tenantID := "tenant-prereq-" + harness.Name

				successKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: tenantID,
					JobType:  successWorker.Name(),
					JobID:    "success",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start success prereq via %s: %v", subject.mode, err)
				}
				failKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: tenantID,
					JobType:  failWorker.Name(),
					JobID:    "fail",
					Data:     swftest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start failing prereq via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, successKey, swf.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, failKey, swf.JobStatusCompleted)

				successDependent, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-success",
					Data:     swftest.NumberTaskData(3),
					Prerequisites: []swf.JobPrerequisite{
						{JobID: successKey.JobId, Condition: swf.JobPrereqSuccess},
					},
				})
				if err != nil {
					t.Fatalf("start dependent success via %s: %v", subject.mode, err)
				}
				failedDependent, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-failed",
					Data:     swftest.NumberTaskData(4),
					Prerequisites: []swf.JobPrerequisite{
						{JobID: failKey.JobId, Condition: swf.JobPrereqSuccess},
					},
				})
				if err != nil {
					t.Fatalf("start dependent failed via %s: %v", subject.mode, err)
				}
				completeDependent, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-complete",
					Data:     swftest.NumberTaskData(5),
					Prerequisites: []swf.JobPrerequisite{
						{JobID: failKey.JobId, Condition: swf.JobPrereqComplete},
					},
				})
				if err != nil {
					t.Fatalf("start dependent complete via %s: %v", subject.mode, err)
				}

				subject.WaitForStatus(t, ctx, successDependent, swf.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, failedDependent, swf.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, completeDependent, swf.JobStatusCompleted)

				if _, err := subject.GetJobResult(ctx, successDependent); err != nil {
					t.Fatalf("expected success dependent to succeed via %s: %v", subject.mode, err)
				}
				if _, err := subject.GetJobResult(ctx, completeDependent); err != nil {
					t.Fatalf("expected complete dependent to succeed via %s: %v", subject.mode, err)
				}
				_, err = subject.GetJobResult(ctx, failedDependent)
				if err == nil {
					t.Fatalf("expected failed dependent to fail via %s", subject.mode)
				}
				if !strings.Contains(err.Error(), "prerequisite job") {
					t.Fatalf("unexpected prereq error via %s: %v", subject.mode, err)
				}
				return errorObservation{Error: err.Error()}
			})
		})
	}
}
