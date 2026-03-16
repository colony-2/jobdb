package usageparity_test

import (
	"context"
	"testing"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type generatedStartObservation struct {
	TenantID     string             `json:"tenantId"`
	JobType      string             `json:"jobType"`
	JobIDPresent bool               `json:"jobIdPresent"`
	Status       swf.JobStatus      `json:"status"`
	Result       normalizedTaskData `json:"result"`
}

type lifecycleObservation struct {
	JobKey    swf.JobKey             `json:"jobKey"`
	Status    swf.JobStatus          `json:"status"`
	Result    normalizedTaskData     `json:"result,omitempty"`
	ResultErr string                 `json:"resultErr,omitempty"`
	JobRun    normalizedJobRun       `json:"jobRun"`
	Listed    []normalizedJobSummary `json:"listed"`
}

func TestEngineAndRuntimeConstructionParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				jobKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-construction-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					JobID:    "construction-parity",
					Data:     swftest.NumberTaskData(1),
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
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				_, runOutputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
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
					JobRun:    normalizeJobRun(t, run, runOutputErr),
					ResultErr: normalizeError(runOutputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestGeneratedJobIDConstructionParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			engineObs := observeViaMode(t, harness, engineMode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) generatedStartObservation {
				jobKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-generated-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					Data:     swftest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)
				result, err := subject.GetJobResult(ctx, jobKey)
				if err != nil {
					t.Fatalf("get job result via %s: %v", subject.mode, err)
				}
				status, err := subject.CheckJobStatus(ctx, jobKey)
				if err != nil {
					t.Fatalf("check status via %s: %v", subject.mode, err)
				}
				return generatedStartObservation{
					TenantID:     jobKey.TenantId,
					JobType:      swftest.SequenceJobName,
					JobIDPresent: jobKey.JobId != "",
					Status:       status,
					Result:       normalizeTaskDataResult(t, result),
				}
			})
			runtimeObs := observeViaMode(t, harness, runtimeMode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) generatedStartObservation {
				jobKey, err := subject.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-generated-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					Data:     swftest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)
				result, err := subject.GetJobResult(ctx, jobKey)
				if err != nil {
					t.Fatalf("get job result via %s: %v", subject.mode, err)
				}
				status, err := subject.CheckJobStatus(ctx, jobKey)
				if err != nil {
					t.Fatalf("check status via %s: %v", subject.mode, err)
				}
				return generatedStartObservation{
					TenantID:     jobKey.TenantId,
					JobType:      swftest.SequenceJobName,
					JobIDPresent: jobKey.JobId != "",
					Status:       status,
					Result:       normalizeTaskDataResult(t, result),
				}
			})
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}
