package engineconformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type explicitIDEngineEchoJob struct {
	name string
}

func (j explicitIDEngineEchoJob) Name() string { return j.name }

func (j explicitIDEngineEchoJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return data, nil
}

func TestRestartJobWithoutExtraOutputAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			origKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, origKey, swf.JobStatusCompleted)

			restartKey, err := built.Engine.SubmitRestartJob(ctx, swf.SubmitRestartJob{
				PriorJobKey:    origKey,
				LastStepToKeep: 0,
			})
			if err != nil {
				t.Fatalf("restart job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, restartKey, swf.JobStatusCompleted)

			result, err := jobResultForTest(built.Engine, ctx, restartKey)
			if err != nil {
				t.Fatalf("get restart result: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, result); got != 4 {
				t.Fatalf("unexpected restart result: got %d want 4", got)
			}
		})
	}
}

func TestEngineExplicitJobIDDuplicateSubmitAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, explicitIDEngineEchoJob{name: "engine-explicit-id-job"})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			base := swf.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobID:    "engine-explicit-id",
				JobType:  ws.JobWorker.Name(),
				Data:     swftest.NumberTaskData(7),
				Metadata: json.RawMessage(`{"queue":"blue"}`),
			}
			jobKey, err := built.Engine.SubmitJob(ctx, base)
			if err != nil {
				t.Fatalf("submit engine explicit job id: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			matching, err := built.Engine.SubmitJob(ctx, base)
			if err != nil {
				t.Fatalf("repeat engine explicit job id: %v", err)
			}
			if matching != jobKey {
				t.Fatalf("unexpected matching engine job key %+v", matching)
			}

			result, err := jobResultForTest(built.Engine, ctx, jobKey)
			if err != nil {
				t.Fatalf("get engine explicit job result: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, result); got != 7 {
				t.Fatalf("unexpected engine explicit result: got %d want 7", got)
			}
			listed, err := built.Engine.ListJobs(ctx, swf.ListJobsRequest{
				TenantIds: []string{jobKey.TenantId},
				JobKeys:   []swf.JobKey{jobKey},
				PageSize:  10,
			})
			if err != nil {
				t.Fatalf("list engine explicit job id: %v", err)
			}
			if len(listed.Jobs) != 1 {
				t.Fatalf("expected 1 engine logical job, got %d", len(listed.Jobs))
			}

			conflicting := base
			conflicting.Metadata = json.RawMessage(`{"queue":"green"}`)
			if _, err := built.Engine.SubmitJob(ctx, conflicting); !errors.Is(err, swf.ErrConflict) {
				t.Fatalf("expected engine explicit metadata conflict, got %v", err)
			}

			otherTenant := base
			otherTenant.TenantId = "tenant-engine-explicit-b-" + harness.Name
			otherKey, err := built.Engine.SubmitJob(ctx, otherTenant)
			if err != nil {
				t.Fatalf("submit engine explicit job id in other tenant: %v", err)
			}
			if otherKey == jobKey {
				t.Fatalf("expected tenant-scoped engine job keys, got %+v and %+v", jobKey, otherKey)
			}
		})
	}
}

func TestGetJobRunCompletedAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{
				JobKey:               jobKey,
				IncludeInputs:        true,
				IncludeOutputs:       true,
				IncludeAttemptInputs: true,
			})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if resp.Job.Status != swf.JobStatusCompleted {
				t.Fatalf("expected completed status, got %s", resp.Job.Status)
			}
			if resp.Start.Input == nil {
				t.Fatal("expected start input")
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Start.Input); got != 1 {
				t.Fatalf("unexpected start input: %d", got)
			}
			if len(resp.Attempts) != 1 {
				t.Fatalf("expected 1 job attempt, got %d", len(resp.Attempts))
			}
			if len(resp.Attempts[0].Tasks) != 2 {
				t.Fatalf("expected 2 task runs, got %d", len(resp.Attempts[0].Tasks))
			}
			if resp.Attempts[0].Tasks[0].TaskType != swftest.AddOneTaskName || resp.Attempts[0].Tasks[1].TaskType != swftest.DoubleTaskName {
				t.Fatalf("unexpected task types: %s, %s", resp.Attempts[0].Tasks[0].TaskType, resp.Attempts[0].Tasks[1].TaskType)
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Attempts[0].Tasks[0].Attempts[0].Output); got != 2 {
				t.Fatalf("unexpected add output: %d", got)
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Attempts[0].Tasks[1].Attempts[0].Output); got != 4 {
				t.Fatalf("unexpected double output: %d", got)
			}
			if resp.Attempts[0].Output == nil {
				t.Fatal("expected job output")
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Attempts[0].Output); got != 4 {
				t.Fatalf("unexpected job output: %d", got)
			}

			output, err := resp.GetOutput(built.Engine, jobKey.TenantId)
			if err != nil {
				t.Fatalf("GetOutput failed: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, output); got != 4 {
				t.Fatalf("unexpected GetOutput result: %d", got)
			}
		})
	}
}

func TestGetJobRunLazilyLoadsOutputAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{
				JobKey:         jobKey,
				IncludeOutputs: false,
			})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}

			output, err := resp.GetOutput(built.Engine, jobKey.TenantId)
			if err != nil {
				t.Fatalf("GetOutput failed: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, output); got != 4 {
				t.Fatalf("unexpected GetOutput result: %d", got)
			}
		})
	}
}

func TestGetJobRunGetOutputFailedAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.FailingJob{})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  swftest.FailingJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if _, err := resp.GetOutput(built.Engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobFailed) {
				t.Fatalf("expected ErrJobFailed, got %v", err)
			} else if !strings.Contains(err.Error(), "intentional failure") {
				t.Fatalf("expected failure message, got %v", err)
			}
		})
	}
}

func TestGetJobRunGetOutputCancelledAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.SequenceJob{Steps: []string{swftest.MissingTaskName}})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := swf.JobKey{
				TenantId: built.WorkerTenantID,
				JobId:    "cancelled-job",
			}
			done := swftest.MustStartJobAsync(t, built.Engine, swf.SubmitJob{
				TenantId: jobKey.TenantId,
				JobType:  swftest.SequenceJobName,
				JobID:    jobKey.JobId,
				Data:     swftest.NumberTaskData(1),
			})

			_ = swftest.WaitForTaskHandle(t, ctx, built.Engine, swftest.SequenceJobName, swftest.MissingTaskName, []string{jobKey.TenantId})

			if err := built.Engine.CancelJob(ctx, swf.CancelJob{JobKey: jobKey}); err != nil {
				t.Fatalf("cancel job: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("async start failed: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCancelled)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if _, err := resp.GetOutput(built.Engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobCancelled) {
				t.Fatalf("expected ErrJobCancelled, got %v", err)
			}
		})
	}
}

func TestGetJobRunPendingRuntimeAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.SequenceJob{Steps: []string{swftest.MissingTaskName}})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := swf.JobKey{
				TenantId: built.WorkerTenantID,
				JobId:    "pending-runtime",
			}
			done := swftest.MustStartJobAsync(t, built.Engine, swf.SubmitJob{
				TenantId: jobKey.TenantId,
				JobType:  swftest.SequenceJobName,
				JobID:    jobKey.JobId,
				Data:     swftest.NumberTaskData(1),
			})

			handle := swftest.WaitForTaskHandle(t, ctx, built.Engine, swftest.SequenceJobName, swftest.MissingTaskName, []string{jobKey.TenantId})

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{
				JobKey:               jobKey,
				IncludeInputs:        true,
				IncludeAttemptInputs: true,
			})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if len(resp.Attempts) != 1 {
				t.Fatalf("expected 1 job attempt, got %d", len(resp.Attempts))
			}
			if len(resp.Attempts[0].Tasks) != 1 {
				t.Fatalf("expected 1 task run, got %d", len(resp.Attempts[0].Tasks))
			}
			task := resp.Attempts[0].Tasks[0]
			if len(task.Attempts) != 1 {
				t.Fatalf("expected 1 task attempt, got %d", len(task.Attempts))
			}
			attempt := task.Attempts[0]
			if attempt.State == "" {
				t.Fatal("expected runtime state")
			}
			swftest.ExpectJobTypeFromNextNeed(t, attempt.Runtime.NextNeed, swftest.SequenceJobName)
			swftest.ExpectTaskSuffix(t, *attempt.Runtime.NextNeed, ":"+swftest.MissingTaskName)
			if attempt.Input == nil {
				t.Fatal("expected runtime input")
			}
			if got := swftest.MustDecodeNumberTaskIO(t, attempt.Input); got != 1 {
				t.Fatalf("unexpected runtime input: %d", got)
			}
			if _, err := resp.GetOutput(built.Engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobNotComplete) {
				t.Fatalf("expected ErrJobNotComplete, got %v", err)
			}

			if err := handle.Finish(ctx, swftest.NumberTaskData(2)); err != nil {
				t.Fatalf("finish task: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("async start failed: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)
		})
	}
}
