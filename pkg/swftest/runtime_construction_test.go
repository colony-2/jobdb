package swftest_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
)

type runtimeBuilderJob struct{}

func (runtimeBuilderJob) Name() string { return "runtime-builder-job" }

func (runtimeBuilderJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "double", input)
}

type runtimeBuilderTask struct{}

func (runtimeBuilderTask) Name() string { return "double" }

func (runtimeBuilderTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	data, err := input.GetData()
	if err != nil {
		return nil, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return swf.NewTaskData(map[string]int{"value": payload["value"] * 2})
}

func TestBuildEngineWithToyRuntime(t *testing.T) {
	engine, err := swf.NewEngineBuilder().
		WithRuntime(toyruntime.New()).
		PlusWorkers(runtimeBuilderJob{}, runtimeBuilderTask{}).
		BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}

	jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "tenant-toy",
		JobType:  runtimeBuilderJob{}.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]int{"value": 21}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	result, err := engine.GetJobResult(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	got := map[string]int{}
	if err := json.Unmarshal(result.GetDataOrPanic(), &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if got["value"] != 42 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestBuildEngineWithDirectRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	embedded, err := directruntime.StartEmbeddedEngine(ctx, runtimeBuilderJob{}, runtimeBuilderTask{})
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine
	go engine.Run(ctx)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-direct",
		JobType:  runtimeBuilderJob{}.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]int{"value": 21}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for completion: %v", err)
	}

	result, err := engine.GetJobResult(ctx, jobKey)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	got := map[string]int{}
	if err := json.Unmarshal(result.GetDataOrPanic(), &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if got["value"] != 42 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestBuildEngineRequiresRuntime(t *testing.T) {
	_, err := swf.NewEngineBuilder().BuildEngine()
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), "workflow runtime is required"; got != want {
		t.Fatalf("unexpected error: got %q want %q", got, want)
	}
}
