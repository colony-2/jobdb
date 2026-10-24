package usageparity_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
)

type jobRunObservation struct {
	JobKey    jobdb.JobKey       `json:"jobKey"`
	Status    jobdb.JobStatus    `json:"status"`
	JobRun    normalizedJobRun   `json:"jobRun"`
	Result    normalizedTaskData `json:"result,omitempty"`
	ResultErr string             `json:"resultErr,omitempty"`
}

type jobRunOutputObservation struct {
	JobKey    jobdb.JobKey       `json:"jobKey"`
	Status    jobdb.JobStatus    `json:"status"`
	JobRun    normalizedJobRun   `json:"jobRun"`
	Output    normalizedTaskData `json:"output,omitempty"`
	OutputErr string             `json:"outputErr,omitempty"`
	Result    normalizedTaskData `json:"result,omitempty"`
	ResultErr string             `json:"resultErr,omitempty"`
}

func TestCompletedJobRunParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "completed-run",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
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
					Status: jobdb.JobStatusCompleted,
					JobRun: normalizeJobRun(t, run, outputErr),
					Result: normalizeTaskDataResult(t, runOutput),
				}
			})
		})
	}
}

func TestLazyOutputLoadParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "lazy-output",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
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
					Status:    jobdb.JobStatusCompleted,
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
	ws := jobdbtest.MustWorkSet(t, jobdbtest.FailingJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.FailingJobName,
					JobID:    "failed-output",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start failing job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey})
				if err != nil {
					t.Fatalf("get failed job run via %s: %v", subject.mode, err)
				}
				output, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				result, resultErr := jobResultForTest(subject, ctx, jobKey)

				return jobRunOutputObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
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
