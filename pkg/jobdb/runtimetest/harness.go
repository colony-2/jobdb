package runtimetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type Capabilities struct {
	Leases         bool
	Schedules      bool
	SchemaRegistry bool
	RuntimeStorage bool
	LongPoll       bool
}

type Harness struct {
	Name         string
	Capabilities Capabilities
	New          func(testing.TB) Fixture
}

type Fixture struct {
	Runtime        jobdb.WorkflowRuntime
	SchemaRegistry jobdb.JobSchemaRegistry
	WorkerTenantID string
	Cleanup        func(context.Context) error
}

type builtFixture struct {
	Name           string
	Runtime        jobdb.WorkflowRuntime
	SchemaRegistry jobdb.JobSchemaRegistry
	Engine         workflow.Engine
	WorkerTenantID string

	cancel  context.CancelFunc
	runDone <-chan struct{}
	cleanup func(context.Context) error
}

func (f *builtFixture) Shutdown(t testing.TB) {
	t.Helper()
	if f == nil {
		return
	}
	if f.cancel != nil {
		f.cancel()
	}
	if f.runDone != nil {
		select {
		case <-f.runDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s engine did not stop after cancellation", f.Name)
		}
	}
	if f.cleanup != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := f.cleanup(ctx); err != nil {
			t.Fatalf("cleanup %s runtime fixture: %v", f.Name, err)
		}
	}
}

func buildFixture(t testing.TB, harness Harness, workers ...workflow.WorkSet) *builtFixture {
	t.Helper()
	if strings.TrimSpace(harness.Name) == "" {
		t.Fatal("runtimetest: harness name is required")
	}
	if harness.New == nil {
		t.Fatalf("runtimetest: %s harness constructor is required", harness.Name)
	}
	fixture := harness.New(t)
	if fixture.Runtime == nil {
		t.Fatalf("runtimetest: %s fixture runtime is required", harness.Name)
	}
	schemaRegistry := fixture.SchemaRegistry
	if schemaRegistry == nil {
		schemaRegistry, _ = fixture.Runtime.(jobdb.JobSchemaRegistry)
	}
	workerTenantID := fixture.WorkerTenantID
	if strings.TrimSpace(workerTenantID) == "" {
		workerTenantID = "tenant-worker-" + safeHarnessName(harness.Name)
	}

	builder := workflow.NewEngineBuilder().WithRuntime(fixture.Runtime)
	if len(workers) > 0 {
		builder.WithWorkerTenantId(workerTenantID)
	}
	for _, ws := range workers {
		tasks := make([]workflow.TaskWorker, 0, len(ws.TaskWorkers))
		for _, task := range ws.TaskWorkers {
			tasks = append(tasks, task)
		}
		builder.PlusWorkersWithOptions(ws.JobWorker, ws.Options, tasks...)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		cleanupFixture(t, harness.Name, fixture.Cleanup)
		t.Fatalf("build %s engine: %v", harness.Name, err)
	}

	var (
		cancel  context.CancelFunc
		runDone <-chan struct{}
	)
	if len(workers) > 0 {
		runCtx, runCancel := context.WithCancel(context.Background())
		cancel = runCancel
		done := make(chan struct{})
		runDone = done
		go func() {
			defer close(done)
			engine.Run(runCtx)
		}()
	}

	return &builtFixture{
		Name:           harness.Name,
		Runtime:        fixture.Runtime,
		SchemaRegistry: schemaRegistry,
		Engine:         engine,
		WorkerTenantID: workerTenantID,
		cancel:         cancel,
		runDone:        runDone,
		cleanup:        fixture.Cleanup,
	}
}

func cleanupFixture(t testing.TB, name string, cleanup func(context.Context) error) {
	t.Helper()
	if cleanup == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cleanup(ctx); err != nil {
		t.Fatalf("cleanup %s runtime fixture: %v", name, err)
	}
}

var unsafeHarnessName = regexp.MustCompile(`[^A-Za-z0-9_-]+`)

func safeHarnessName(name string) string {
	out := unsafeHarnessName.ReplaceAllString(strings.TrimSpace(name), "-")
	out = strings.Trim(out, "-_")
	if out == "" {
		return "runtime"
	}
	return out
}

func MustWorkSet(t testing.TB, job workflow.JobWorker, tasks ...workflow.TaskWorker) workflow.WorkSet {
	t.Helper()
	ws, err := workflow.AsWorkSet(job, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func WaitForEngineStatus(t testing.TB, ctx context.Context, engine workflow.Engine, jobKey jobdb.JobKey, want jobdb.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (jobdb.JobStatus, error) {
		job, err := engine.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForRuntimeStatus(t testing.TB, ctx context.Context, runtime jobdb.WorkflowRuntime, jobKey jobdb.JobKey, want jobdb.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (jobdb.JobStatus, error) {
		job, err := runtime.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForTaskHandle(t testing.TB, ctx context.Context, engine workflow.Engine, jobType string, taskType string, tenantIDs []string) workflow.TaskHandle {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		handles, err := engine.FindTasksWaitingForRoute(ctx, jobType, taskType, tenantIDs)
		if err == nil && len(handles) > 0 {
			return handles[0]
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("find waiting tasks: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for task handle: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s:%s task handle", jobType, taskType)
	return nil
}

func MustDecodeNumberTaskData(t testing.TB, data jobdb.TaskData) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task data")
	}
	raw, err := data.GetData()
	if err != nil {
		t.Fatalf("get task data: %v", err)
	}
	return decodeNumber(t, raw)
}

func NumberTaskData(n int) jobdb.TaskData {
	return jobdb.NewTaskDataOrPanic(map[string]int{"n": n})
}

const (
	SequenceJobName = "seq"
	AddOneTaskName  = "add"
	DoubleTaskName  = "double"
	MissingTaskName = "missing"
	FailingJobName  = "failing"
)

type sequenceJob struct {
	Steps []string
}

func (sequenceJob) Name() string { return SequenceJobName }

func (j sequenceJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	var (
		out jobdb.TaskData = data
		err error
	)
	for _, step := range j.Steps {
		out, err = ctx.DoTask(jobdb.RunPolicy{}, step, out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type addOneTask struct{}

func (addOneTask) Name() string { return AddOneTaskName }

func (addOneTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n + 1), nil
}

type doubleTask struct{}

func (doubleTask) Name() string { return DoubleTaskName }

func (doubleTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n * 2), nil
}

type failingJob struct{}

func (failingJob) Name() string { return FailingJobName }

func (failingJob) Run(_ workflow.JobContext, _ jobdb.JobData) (jobdb.JobData, error) {
	return nil, errors.New("intentional failure")
}

func decodeNumber(t testing.TB, raw json.RawMessage) int {
	t.Helper()
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode numeric payload: %v", err)
	}
	return payload["n"]
}

func waitForStatus(t testing.TB, ctx context.Context, check func(context.Context) (jobdb.JobStatus, error), want jobdb.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := check(ctx)
		if err == nil && status == want {
			return
		}
		if err != nil && !errors.Is(err, jobdb.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("check status: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for status: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("job did not reach status %s", want)
}

func numberFromTaskData(data jobdb.TaskData) (int, error) {
	if data == nil {
		return 0, fmt.Errorf("missing task data")
	}
	raw, err := data.GetData()
	if err != nil {
		return 0, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	return payload["n"], nil
}
