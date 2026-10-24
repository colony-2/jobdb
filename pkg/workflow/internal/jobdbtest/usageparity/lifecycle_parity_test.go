package usageparity_test

import (
	"context"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
)

type cancelObservation struct {
	JobKey       jobdb.JobKey           `json:"jobKey"`
	Status       jobdb.JobStatus        `json:"status"`
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
	ws := jobdbtest.MustWorkSet(t, passthroughJob{name: "custom-id-job"})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "custom-job-id",
					Data:     jobdbtest.NumberTaskData(7),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get job result via %s: %v", subject.mode, err)
				}
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:         jobKey,
					IncludeOutputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list jobs via %s: %v", subject.mode, err)
				}
				return lifecycleObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: "",
					JobRun:    normalizeJobRun(t, run, outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestRestartJobParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				origKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "restart-original",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start original via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, origKey, jobdb.JobStatusCompleted)

				restartKey, err := subject.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
					PriorJobKey:    origKey,
					LastStepToKeep: 0,
					JobID:          "restart-copy",
				})
				if err != nil {
					t.Fatalf("restart via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, restartKey, jobdb.JobStatusCompleted)

				result, err := jobResultForTest(subject, ctx, restartKey)
				if err != nil {
					t.Fatalf("get restart result via %s: %v", subject.mode, err)
				}
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               restartKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get restart run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), restartKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{restartKey.TenantId},
					JobKeys:   []jobdb.JobKey{restartKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list restart job via %s: %v", subject.mode, err)
				}

				return lifecycleObservation{
					JobKey:    restartKey,
					Status:    jobdb.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					JobRun:    normalizeJobRun(t, run, outputErr),
					ResultErr: normalizeError(outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestPrerequisiteParityAcrossBuiltInRuntimes(t *testing.T) {
	successWorker := passthroughJob{name: "prereq-success-job"}
	failWorker := namedFailingJob{name: "prereq-fail-job", message: "prereq failed"}
	dependentWorker := passthroughJob{name: "prereq-dependent-job"}

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{
				jobdbtest.MustWorkSet(t, successWorker),
				jobdbtest.MustWorkSet(t, failWorker),
				jobdbtest.MustWorkSet(t, dependentWorker),
			}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
				tenantID := subject.built.WorkerTenantID

				successKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  successWorker.Name(),
					JobID:    "success",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start success prereq via %s: %v", subject.mode, err)
				}
				failKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  failWorker.Name(),
					JobID:    "fail",
					Data:     jobdbtest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start failing prereq via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, successKey, jobdb.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, failKey, jobdb.JobStatusCompleted)

				successDependent, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-success",
					Data:     jobdbtest.NumberTaskData(3),
					Prerequisites: []jobdb.JobPrerequisite{
						{JobID: successKey.JobId, Condition: jobdb.JobPrereqSuccess},
					},
				})
				if err != nil {
					t.Fatalf("start dependent success via %s: %v", subject.mode, err)
				}
				failedDependent, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-failed",
					Data:     jobdbtest.NumberTaskData(4),
					Prerequisites: []jobdb.JobPrerequisite{
						{JobID: failKey.JobId, Condition: jobdb.JobPrereqSuccess},
					},
				})
				if err != nil {
					t.Fatalf("start dependent failed via %s: %v", subject.mode, err)
				}
				completeDependent, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-complete",
					Data:     jobdbtest.NumberTaskData(5),
					Prerequisites: []jobdb.JobPrerequisite{
						{JobID: failKey.JobId, Condition: jobdb.JobPrereqComplete},
					},
				})
				if err != nil {
					t.Fatalf("start dependent complete via %s: %v", subject.mode, err)
				}

				subject.WaitForStatus(t, ctx, successDependent, jobdb.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, failedDependent, jobdb.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, completeDependent, jobdb.JobStatusCompleted)

				if _, err := jobResultForTest(subject, ctx, successDependent); err != nil {
					t.Fatalf("expected success dependent to succeed via %s: %v", subject.mode, err)
				}
				if _, err := jobResultForTest(subject, ctx, completeDependent); err != nil {
					t.Fatalf("expected complete dependent to succeed via %s: %v", subject.mode, err)
				}
				_, err = jobResultForTest(subject, ctx, failedDependent)
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
