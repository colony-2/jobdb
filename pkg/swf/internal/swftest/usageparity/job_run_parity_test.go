package usageparity_test

import (
	"context"
	"testing"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type jobRunObservation struct {
	JobKey    swf.JobKey         `json:"jobKey"`
	Status    swf.JobStatus      `json:"status"`
	JobRun    normalizedJobRun   `json:"jobRun"`
	Result    normalizedTaskData `json:"result,omitempty"`
	ResultErr string             `json:"resultErr,omitempty"`
}

type jobRunOutputObservation struct {
	JobKey    swf.JobKey         `json:"jobKey"`
	Status    swf.JobStatus      `json:"status"`
	JobRun    normalizedJobRun   `json:"jobRun"`
	Output    normalizedTaskData `json:"output,omitempty"`
	OutputErr string             `json:"outputErr,omitempty"`
	Result    normalizedTaskData `json:"result,omitempty"`
	ResultErr string             `json:"resultErr,omitempty"`
}

func TestCompletedJobRunParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunObservation {
				jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-job-run-complete-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					JobID:    "completed-run",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				runOutput, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				return jobRunObservation{
					JobKey: jobKey,
					Status: swf.JobStatusCompleted,
					JobRun: normalizeJobRun(t, run, outputErr),
					Result: normalizeTaskDataResult(t, runOutput),
				}
			})
		})
	}
}

func TestPendingRuntimeViewParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, pendingJob{})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunObservation {
				jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-job-run-pending-" + harness.Name,
					JobType:  ws.JobWorker.Name(),
					JobID:    "pending-run",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}

				handle := swftest.WaitForTaskHandle(t, ctx, subject.Engine(), ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})
				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get pending job run via %s: %v", subject.mode, err)
				}
				runOutput, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)

				if err := handle.Finish(ctx, swftest.NumberTaskData(2)); err != nil {
					t.Fatalf("finish pending task via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				return jobRunObservation{
					JobKey:    jobKey,
					Status:    run.Job.Status,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Result:    normalizeTaskDataResult(t, runOutput),
					ResultErr: normalizeError(outputErr),
				}
			})
		})
	}
}

func TestLazyOutputLoadParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-job-run-lazy-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					JobID:    "lazy-output",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:         jobKey,
					IncludeOutputs: false,
				})
				if err != nil {
					t.Fatalf("get lazy job run via %s: %v", subject.mode, err)
				}
				output, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				result, resultErr := jobResultForTest(subject, ctx, jobKey)

				return jobRunOutputObservation{
					JobKey:    jobKey,
					Status:    swf.JobStatusCompleted,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Output:    normalizeTaskDataResult(t, output),
					OutputErr: normalizeError(outputErr),
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
				}
			})
		})
	}
}

func TestFailedGetOutputParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.FailingJob{})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-job-run-failed-" + harness.Name,
					JobType:  swftest.FailingJobName,
					JobID:    "failed-output",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start failing job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
				if err != nil {
					t.Fatalf("get failed job run via %s: %v", subject.mode, err)
				}
				output, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				result, resultErr := jobResultForTest(subject, ctx, jobKey)

				return jobRunOutputObservation{
					JobKey:    jobKey,
					Status:    swf.JobStatusCompleted,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Output:    normalizeTaskDataResult(t, output),
					OutputErr: normalizeError(outputErr),
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
				}
			})
		})
	}
}

func TestCancelledGetOutputParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.SequenceJob{Steps: []string{swftest.MissingTaskName}})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-job-run-cancelled-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					JobID:    "cancelled-output",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start cancelled job via %s: %v", subject.mode, err)
				}
				_ = swftest.WaitForTaskHandle(t, ctx, subject.Engine(), swftest.SequenceJobName, swftest.MissingTaskName, []string{jobKey.TenantId})
				if err := subject.CancelJob(ctx, swf.CancelJob{JobKey: jobKey}); err != nil {
					t.Fatalf("cancel job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCancelled)

				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
				if err != nil {
					t.Fatalf("get cancelled job run via %s: %v", subject.mode, err)
				}
				output, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				result, resultErr := jobResultForTest(subject, ctx, jobKey)

				return jobRunOutputObservation{
					JobKey:    jobKey,
					Status:    swf.JobStatusCancelled,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Output:    normalizeTaskDataResult(t, output),
					OutputErr: normalizeError(outputErr),
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
				}
			})
		})
	}
}
