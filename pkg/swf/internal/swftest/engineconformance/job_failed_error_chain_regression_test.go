package engineconformance_test

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type jobFailedChainAppErrorChildJob struct{}

func (jobFailedChainAppErrorChildJob) Name() string { return "job-failed-chain-app-error-child" }

func (jobFailedChainAppErrorChildJob) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
	return nil, swf.AppError{Payload: swf.AppErrorPayload{
		Message: "child failed",
		Level:   "error",
		Attrs: map[string]interface{}{
			"nested": map[string]interface{}{"k": "v"},
		},
	}}
}

type jobFailedChainGenericChildJob struct{}

func (jobFailedChainGenericChildJob) Name() string { return "job-failed-chain-generic-child" }

func (jobFailedChainGenericChildJob) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
	return nil, fmt.Errorf("command execution failed: exit status 1")
}

type jobFailedChainTaskChildJob struct{}

func (jobFailedChainTaskChildJob) Name() string { return "job-failed-chain-task-child" }

func (jobFailedChainTaskChildJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "job-failed-chain-task-child-fail", data)
}

type jobFailedChainTaskChildFailTask struct{}

func (jobFailedChainTaskChildFailTask) Name() string { return "job-failed-chain-task-child-fail" }

func (jobFailedChainTaskChildFailTask) Run(_ swf.TaskContext, _ swf.TaskData) (swf.TaskData, error) {
	return nil, fmt.Errorf("command execution failed: exit status 1")
}

type jobFailedChainParentJob struct {
	engine swf.SWFEngine
	child  string
}

func (jobFailedChainParentJob) Name() string { return "job-failed-chain-parent" }

func (j *jobFailedChainParentJob) Run(ctx swf.JobContext, data swf.JobData) (_ swf.JobData, runErr error) {
	childKey, err := j.engine.SubmitJob(context.Background(), swf.SubmitJob{
		TenantId: ctx.GetJobKey().TenantId,
		JobType:  j.child,
		JobID:    ctx.GetJobKey().JobId + "-child",
		Data:     data,
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.AwaitJobs(childKey.JobId); err != nil {
		return nil, err
	}
	run, err := j.engine.GetJobRun(context.Background(), swf.GetJobRunRequest{
		JobKey:           childKey,
		IncludeOutputs:   true,
		IncludeArtifacts: true,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if rec := recover(); rec != nil {
			runErr = swf.AppError{Payload: swf.AppErrorPayload{
				Message: "panic in parent: " + rec.(error).Error() + "\n" + string(debug.Stack()),
				Level:   "error",
			}}
		}
	}()
	_, err = run.GetOutput(j.engine, childKey.TenantId)
	if err != nil {
		assertComparableErrorChain(err)
		if errors.Is(err, swf.ErrJobFailed) {
			return nil, err
		}
		return nil, err
	}
	return nil, nil
}

func assertComparableErrorChain(err error) {
	seen := map[error]struct{}{}
	for err != nil {
		seen[err] = struct{}{}
		switch next := err.(type) {
		case interface{ Unwrap() error }:
			err = next.Unwrap()
		default:
			err = nil
		}
	}
}

func TestGetJobRunOutputErrorChainComparableAcrossBuiltInRuntimes(t *testing.T) {
	testCases := []struct {
		name    string
		childWS swf.WorkSet
		child   string
	}{
		{
			name:    "job-app-error",
			childWS: swftest.MustWorkSet(t, jobFailedChainAppErrorChildJob{}),
			child:   "job-failed-chain-app-error-child",
		},
		{
			name:    "job-generic-error",
			childWS: swftest.MustWorkSet(t, jobFailedChainGenericChildJob{}),
			child:   "job-failed-chain-generic-child",
		},
		{
			name:    "task-generic-error",
			childWS: swftest.MustWorkSet(t, jobFailedChainTaskChildJob{}, jobFailedChainTaskChildFailTask{}),
			child:   "job-failed-chain-task-child",
		},
	}
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		for _, tc := range testCases {
			tc := tc
			t.Run(harness.Name+"/"+tc.name, func(t *testing.T) {
				parent := &jobFailedChainParentJob{child: tc.child}
				built := harness.New(t,
					tc.childWS,
					swftest.MustWorkSet(t, parent),
				)
				defer built.Shutdown(t)
				parent.engine = built.Engine

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				jobKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-job-failed-chain-" + harness.Name + "-" + tc.name,
					JobType:  parent.Name(),
					JobID:    "parent",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start parent: %v", err)
				}
				swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)
				_, err = jobResultForTest(built.Engine, ctx, jobKey)
				if err == nil {
					t.Fatal("expected parent to fail")
				}
				if strings.Contains(err.Error(), "panic in parent:") {
					t.Fatalf("captured panic stack:\n%s", err)
				}
			})
		}
	}
}
