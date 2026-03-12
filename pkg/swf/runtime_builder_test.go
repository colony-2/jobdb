package swf_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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
	ws, err := swf.AsWorkSet(runtimeBuilderJob{}, runtimeBuilderTask{})
	if err != nil {
		t.Fatalf("workset: %v", err)
	}

	engine, err := swf.NewEngineBuilder().
		WithRuntime(toyruntime.New()).
		PlusWorkers(ws.JobWorker, runtimeBuilderTask{}).
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

	dsn, stopPG, err := impl.StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("embedded postgres: %v", err)
	}
	defer stopPG()

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	if err := impl.InstallPGWF(ctx, sqlDB); err != nil {
		t.Fatalf("install pgwf: %v", err)
	}

	strataHandle, err := impl.StartEmbeddedStrata()
	if err != nil {
		t.Fatalf("embedded strata: %v", err)
	}
	defer strataHandle.Shutdown()

	strataClient, err := strataclient.New(strataclient.Config{
		BaseURL: strataHandle.BaseURL,
		APIKey:  strataHandle.APIKey,
	})
	if err != nil {
		t.Fatalf("strata client: %v", err)
	}

	engine, err := swf.NewEngineBuilder().
		WithRuntime(directruntime.New(db, strataClient)).
		PlusWorkers(runtimeBuilderJob{}, runtimeBuilderTask{}).
		BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}

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

	run, err := engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if run.Job.JobKey != jobKey {
		t.Fatalf("unexpected job run key: got %v want %v", run.Job.JobKey, jobKey)
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

func TestBuildUsesRuntimeWhenPresent(t *testing.T) {
	ws, err := swf.AsWorkSet(runtimeBuilderJob{}, runtimeBuilderTask{})
	if err != nil {
		t.Fatalf("workset: %v", err)
	}

	engine, err := swf.NewEngineBuilder().
		WithRuntime(toyruntime.New()).
		PlusWorkers(ws.JobWorker, runtimeBuilderTask{}).
		Build(nil)
	if err != nil {
		t.Fatalf("build with runtime via legacy entry point: %v", err)
	}

	jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "tenant-legacy",
		JobType:  runtimeBuilderJob{}.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]int{"value": 3}),
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
	if got["value"] != 6 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestDirectRuntimeFromConfigValidation(t *testing.T) {
	_, err := directruntime.NewFromConfig("", "http://example", "key")
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), "postgres DSN is required"; got != want {
		t.Fatalf("unexpected error: got %q want %q", got, want)
	}

	_, err = directruntime.NewFromConfig("postgres://example", "", "key")
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), "strata base URL is required"; got != want {
		t.Fatalf("unexpected error: got %q want %q", got, want)
	}

	_, err = directruntime.NewFromConfig("postgres://example", "http://example", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), "strata API key is required"; got != want {
		t.Fatalf("unexpected error: got %q want %q", got, want)
	}
}
