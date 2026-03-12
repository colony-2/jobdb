package impl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
)

// TestTaskRestartUsesCache verifies that tasks use cached results on restart
// This is the task-level equivalent of TestJobRestartUsesCache
func TestTaskRestartUsesCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskExecutionCount atomic.Int32
	taskWorker := &countingTaskWorker{
		name:    "restart-task",
		counter: &taskExecutionCount,
	}

	var jobExecutionCount atomic.Int32
	jobWorker := &taskCallingJobWorkerWithCounter{
		name:        "task-restart-job",
		taskType:    taskWorker.Name(),
		taskCounter: &taskExecutionCount,
		jobCounter:  &jobExecutionCount,
	}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)

	// Start the job
	input := swf.NewTaskDataOrPanic(map[string]string{"test": "task-cache"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Execute job once (calls DoTask which executes and caches the task)
	lease := getLeaseForJob(t, ctx, engine, jobKey)
	if lease == nil {
		t.Fatalf("no lease available")
	}

	r := newRunnerForTest(engine, lease, ws, ctx)
	r.DoJob(ctx)

	// Verify task executed exactly once
	if taskExecutionCount.Load() != 1 {
		t.Fatalf("expected 1 task execution, got %d", taskExecutionCount.Load())
	}

	// Verify the task result was cached at ordinal 1
	key := storyKeyForJob(jobKey)
	chap1, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("expected task chapter at ordinal 1: %v", err)
	}
	env1, err := decodeChapterEnvelope(chap1.Body())
	if err != nil {
		t.Fatalf("decode task chapter: %v", err)
	}
	if env1.PayloadKind != payloadKindApp {
		t.Fatalf("expected success at ordinal 1, got %s", env1.PayloadKind)
	}

	// Now restart - task should use cached result, NOT re-execute
	lease2 := getLeaseForJob(t, ctx, engine, jobKey)
	if lease2 != nil {
		r2 := newRunnerForTest(engine, lease2, ws, ctx)
		r2.DoJob(ctx)

		// CRITICAL: Task execution count should still be 1
		if taskExecutionCount.Load() != 1 {
			t.Fatalf("task re-executed on restart! expected 1 execution, got %d", taskExecutionCount.Load())
		}
	}
}

// TestExternalTaskCompletionUsesStoredInputHash verifies external completion uses the stored input hash
// instead of recomputing from the previous chapter.
func TestExternalTaskCompletionUsesStoredInputHash(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskType := "external-task"
	jobWorker := &externalTaskJobWorker{
		name:     "external-task-job",
		taskType: taskType,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)

	input := swf.NewTaskDataOrPanic(map[string]string{"form": "hello"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	lease := getLeaseForJob(t, ctx, engine, jobKey)
	if lease == nil {
		t.Fatalf("no lease available")
	}

	r := newRunnerForTest(engine, lease, jobWorker.workset, ctx)
	r.DoJob(ctx)

	handles, err := engine.FindTasksWaitingForCapability(ctx, jobWorker.Name(), taskType, []string{jobKey.TenantId})
	if err != nil {
		t.Fatalf("find waiting tasks: %v", err)
	}
	if len(handles) != 1 {
		t.Fatalf("expected 1 waiting task, got %d", len(handles))
	}

	finishData := swf.NewTaskDataOrPanic(map[string]string{"result": "done"})
	if err := handles[0].Finish(ctx, finishData); err != nil {
		t.Fatalf("finish task: %v", err)
	}

	lease2 := getLeaseForJob(t, ctx, engine, jobKey)
	if lease2 == nil {
		t.Fatalf("no lease available after task completion")
	}
	r2 := newRunnerForTest(engine, lease2, jobWorker.workset, ctx)
	r2.DoJob(ctx)

	result, err := engine.GetJobResult(ctx, jobKey)
	if err != nil {
		t.Fatalf("get job result: %v", err)
	}
	data, _ := result.GetData()
	var resultMap map[string]interface{}
	if err := json.Unmarshal(data, &resultMap); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if resultMap["result"] != "done" {
		t.Fatalf("unexpected result: %v", resultMap)
	}
}

// TestTaskRetryWithFailures verifies task retry logic works correctly
func TestTaskRetryWithFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskAttemptCount atomic.Int32
	taskWorker := &failThenSucceedTaskWorker{
		name:         "retry-task",
		failAttempts: 1,
		counter:      &taskAttemptCount,
	}

	jobWorker := &taskCallingJobWorkerSimple{
		name:     "retry-task-job",
		taskType: taskWorker.Name(),
		taskPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:    3,
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(10 * time.Millisecond),
			},
		},
	}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	// Start job (retry policy is on the task, not the job)
	input := swf.NewTaskDataOrPanic(map[string]string{"test": "task-retry"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Wait for completion
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	// Verify task executed twice (one failure, one success)
	if taskAttemptCount.Load() != 2 {
		t.Fatalf("expected 2 task attempts, got %d", taskAttemptCount.Load())
	}

	// Verify both attempts are saved as separate chapters
	key := storyKeyForJob(jobKey)

	// Ordinal 1 should have the first failed attempt
	chap1, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("expected chapter at ordinal 1: %v", err)
	}
	env1, err := decodeChapterEnvelope(chap1.Body())
	if err != nil {
		t.Fatalf("decode chapter 1: %v", err)
	}
	if env1.PayloadKind != payloadKindAppError {
		t.Fatalf("expected error at ordinal 1, got %s", env1.PayloadKind)
	}
	if env1.Meta.Attempt != 1 {
		t.Fatalf("expected attempt 1 at ordinal 1, got %d", env1.Meta.Attempt)
	}

	// Ordinal 2 should have the second successful attempt
	chap2, err := engine.strata.Chapter(ctx, key, 2)
	if err != nil {
		t.Fatalf("expected chapter at ordinal 2: %v", err)
	}
	env2, err := decodeChapterEnvelope(chap2.Body())
	if err != nil {
		t.Fatalf("decode chapter 2: %v", err)
	}
	if env2.PayloadKind != payloadKindApp {
		t.Fatalf("expected success at ordinal 2, got %s", env2.PayloadKind)
	}
	if env2.Meta.Attempt != 2 {
		t.Fatalf("expected attempt 2 at ordinal 2, got %d", env2.Meta.Attempt)
	}

	// Ordinal 3 should have the job result
	chap3, err := engine.strata.Chapter(ctx, key, 3)
	if err != nil {
		t.Fatalf("expected job result at ordinal 3: %v", err)
	}
	env3, err := decodeChapterEnvelope(chap3.Body())
	if err != nil {
		t.Fatalf("decode chapter 3: %v", err)
	}
	if env3.PayloadKind != payloadKindApp {
		t.Fatalf("expected job success at ordinal 3, got %s", env3.PayloadKind)
	}
}

// TestTaskMaxRetriesExhausted verifies tasks stop retrying after max attempts
func TestTaskMaxRetriesExhausted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskAttemptCount atomic.Int32
	taskWorker := &alwaysFailTaskWorker{
		name:    "max-retry-task",
		counter: &taskAttemptCount,
	}

	maxAttempts := 3
	jobWorker := &taskCallingJobWorkerSimple{
		name:     "max-retry-task-job",
		taskType: taskWorker.Name(),
		taskPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:    int32(maxAttempts),
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(10 * time.Millisecond),
			},
		},
	}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	input := swf.NewTaskDataOrPanic(map[string]string{"test": "task-max-retry"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Wait for completion (will complete with task error propagated to job)
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	// Verify task executed exactly maxAttempts times
	if taskAttemptCount.Load() != int32(maxAttempts) {
		t.Fatalf("expected %d task attempts, got %d", maxAttempts, taskAttemptCount.Load())
	}

	// Verify all task attempts are saved as separate chapters
	key := storyKeyForJob(jobKey)
	for i := 1; i <= maxAttempts; i++ {
		chap, err := engine.strata.Chapter(ctx, key, int64(i))
		if err != nil {
			t.Fatalf("expected task chapter at ordinal %d: %v", i, err)
		}
		env, err := decodeChapterEnvelope(chap.Body())
		if err != nil {
			t.Fatalf("decode task chapter %d: %v", i, err)
		}
		if env.Meta.Attempt != i {
			t.Fatalf("expected attempt %d at ordinal %d, got %d", i, i, env.Meta.Attempt)
		}
		if env.PayloadKind != payloadKindAppError {
			t.Fatalf("expected error at task ordinal %d, got %s", i, env.PayloadKind)
		}
	}

	// Ordinal maxAttempts+1 should have the job result (propagated task error)
	jobResultOrdinal := int64(maxAttempts + 1)
	chap, err := engine.strata.Chapter(ctx, key, jobResultOrdinal)
	if err != nil {
		t.Fatalf("expected job result at ordinal %d: %v", jobResultOrdinal, err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode job result chapter: %v", err)
	}
	// Job result should also be an error since the task failed
	if env.PayloadKind != payloadKindAppError {
		t.Fatalf("expected job error at ordinal %d, got %s", jobResultOrdinal, env.PayloadKind)
	}
}

func TestTaskInputStoredOnSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskExecutionCount atomic.Int32
	taskWorker := &countingTaskWorker{
		name:    "input-success-task",
		counter: &taskExecutionCount,
	}

	jobWorker := &taskCallingJobWorkerSimple{
		name:     "input-success-job",
		taskType: taskWorker.Name(),
		taskPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts: 1,
			},
		},
	}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	input := swf.NewTaskDataOrPanic(payload{Name: "ok", Count: 2})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	key := storyKeyForJob(jobKey)
	chap, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("expected task chapter at ordinal 1: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode task chapter: %v", err)
	}
	if env.PayloadKind != payloadKindApp {
		t.Fatalf("expected success at ordinal 1, got %s", env.PayloadKind)
	}
	if len(env.Meta.Input) == 0 {
		t.Fatalf("expected task input to be stored in metadata")
	}

	wantInput, err := input.GetData()
	if err != nil {
		t.Fatalf("get input data: %v", err)
	}
	assertJSONEqual(t, env.Meta.Input, wantInput)
}

func TestTaskInputStoredOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskAttemptCount atomic.Int32
	taskWorker := &alwaysFailTaskWorker{
		name:    "input-error-task",
		counter: &taskAttemptCount,
	}

	jobWorker := &taskCallingJobWorkerSimple{
		name:     "input-error-job",
		taskType: taskWorker.Name(),
		taskPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts: 1,
			},
		},
	}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	type payload struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
	}
	input := swf.NewTaskDataOrPanic(payload{Status: "fail", Count: 5})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	key := storyKeyForJob(jobKey)
	chap, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("expected task chapter at ordinal 1: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode task chapter: %v", err)
	}
	if env.PayloadKind != payloadKindAppError {
		t.Fatalf("expected error at ordinal 1, got %s", env.PayloadKind)
	}
	if len(env.Meta.Input) == 0 {
		t.Fatalf("expected task input to be stored in metadata")
	}

	wantInput, err := input.GetData()
	if err != nil {
		t.Fatalf("get input data: %v", err)
	}
	assertJSONEqual(t, env.Meta.Input, wantInput)
}

// TestTaskNonRetryableError verifies non-retryable task errors stop immediately
func TestTaskNonRetryableError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskAttemptCount atomic.Int32
	taskWorker := &nonRetryableTaskWorker{
		name:    "non-retryable-task",
		counter: &taskAttemptCount,
	}

	jobWorker := &taskCallingJobWorkerSimple{
		name:     "non-retryable-task-job",
		taskType: taskWorker.Name(),
		taskPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:        5,
				BackoffCoefficient:     1.0,
				InitialInterval:        swf.Duration(10 * time.Millisecond),
				NonRetryableErrorTypes: []string{"*impl.customTaskNonRetryableError"},
			},
		},
	}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	input := swf.NewTaskDataOrPanic(map[string]string{"test": "task-non-retryable"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Wait for completion
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	// Should only execute once - non-retryable errors don't retry
	if taskAttemptCount.Load() != 1 {
		t.Fatalf("expected 1 task attempt (no retry for non-retryable), got %d", taskAttemptCount.Load())
	}

	// Verify job result is the propagated non-retryable error
	_, err = engine.GetJobResult(ctx, jobKey)
	if err == nil {
		t.Fatalf("expected error result from non-retryable task error")
	}
	if !strings.Contains(err.Error(), "task-non-retryable") {
		t.Fatalf("expected error message to contain 'task-non-retryable', got: %v", err)
	}
}

func assertJSONEqual(t *testing.T, got []byte, want []byte) {
	t.Helper()

	var gotVal any
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshal got json: %v", err)
	}
	var wantVal any
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("unmarshal want json: %v", err)
	}
	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Fatalf("json mismatch: got=%s want=%s", string(got), string(want))
	}
}

// TestTaskWithMultipleRetries verifies complex retry scenarios
func TestTaskWithMultipleRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var task1Count, task2Count atomic.Int32

	// Task 1 fails once then succeeds
	task1 := &failThenSucceedTaskWorker{
		name:         "task1",
		failAttempts: 1,
		counter:      &task1Count,
	}

	// Task 2 succeeds immediately
	task2 := &countingTaskWorker{
		name:    "task2",
		counter: &task2Count,
	}

	jobWorker := &multiTaskJobWorker{
		name:      "multi-task-job",
		task1Name: task1.Name(),
		task2Name: task2.Name(),
		taskPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:    3,
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(10 * time.Millisecond),
			},
		},
	}
	ws := initWorkset(jobWorker, task1, task2)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	input := swf.NewTaskDataOrPanic(map[string]string{"test": "multi-task"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Wait for completion
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	// Task 1 should execute twice (fail, then succeed)
	if task1Count.Load() != 2 {
		t.Fatalf("expected 2 executions of task1, got %d", task1Count.Load())
	}

	// Task 2 should execute once (succeeds immediately)
	if task2Count.Load() != 1 {
		t.Fatalf("expected 1 execution of task2, got %d", task2Count.Load())
	}

	// Verify chapter structure
	key := storyKeyForJob(jobKey)
	// Ordinal 0: input
	// Ordinal 1: task1 attempt 1 (fail)
	// Ordinal 2: task1 attempt 2 (success)
	// Ordinal 3: task2 attempt 1 (success)
	// Ordinal 4: job result (success)

	for i := int64(1); i <= 4; i++ {
		_, err := engine.strata.Chapter(ctx, key, i)
		if err != nil {
			t.Fatalf("expected chapter at ordinal %d: %v", i, err)
		}
	}
}

// Test worker implementations

type countingTaskWorker struct {
	name    string
	counter *atomic.Int32
}

func (w *countingTaskWorker) Name() string { return w.name }

func (w *countingTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	w.counter.Add(1)
	input, _ := data.GetData()
	result := make(map[string]interface{})
	json.Unmarshal(input, &result)
	result["executed"] = true
	return swf.NewTaskDataOrPanic(result), nil
}

type failThenSucceedTaskWorker struct {
	name         string
	failAttempts int
	counter      *atomic.Int32
}

func (w *failThenSucceedTaskWorker) Name() string { return w.name }

func (w *failThenSucceedTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	attempt := int(w.counter.Add(1))

	if attempt <= w.failAttempts {
		return nil, swf.AppError{Payload: swf.AppErrorPayload{
			Message: fmt.Sprintf("task retry attempt %d", attempt),
			Level:   "error",
		}}
	}

	input, _ := data.GetData()
	result := make(map[string]interface{})
	json.Unmarshal(input, &result)
	result["attempt"] = attempt
	return swf.NewTaskDataOrPanic(result), nil
}

type alwaysFailTaskWorker struct {
	name    string
	counter *atomic.Int32
}

func (w *alwaysFailTaskWorker) Name() string { return w.name }

func (w *alwaysFailTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	w.counter.Add(1)
	return nil, swf.AppError{Payload: swf.AppErrorPayload{
		Message: "task always fails",
		Level:   "error",
	}}
}

type nonRetryableTaskWorker struct {
	name    string
	counter *atomic.Int32
}

func (w *nonRetryableTaskWorker) Name() string { return w.name }

func (w *nonRetryableTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	w.counter.Add(1)
	return nil, &customTaskNonRetryableError{message: "this task error is task-non-retryable"}
}

type customTaskNonRetryableError struct {
	message string
}

func (e *customTaskNonRetryableError) Error() string {
	return e.message
}

func (e *customTaskNonRetryableError) NonRetryable() bool {
	return true
}

// Job workers that call tasks

type taskCallingJobWorkerWithCounter struct {
	name        string
	taskType    string
	taskCounter *atomic.Int32
	jobCounter  *atomic.Int32
	workset     *swf.WorkSet
}

func (w *taskCallingJobWorkerWithCounter) Name() string { return w.name }

func (w *taskCallingJobWorkerWithCounter) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	w.jobCounter.Add(1)

	// Call the task
	result, err := ctx.DoTask(swf.RunPolicy{}, w.taskType, data)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type taskCallingJobWorkerSimple struct {
	name       string
	taskType   string
	taskPolicy swf.RunPolicy
	workset    *swf.WorkSet
}

func (w *taskCallingJobWorkerSimple) Name() string { return w.name }

func (w *taskCallingJobWorkerSimple) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	// Call the task with the specified retry policy
	result, err := ctx.DoTask(w.taskPolicy, w.taskType, data)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type externalTaskJobWorker struct {
	name     string
	taskType string
	workset  *swf.WorkSet
}

func (w *externalTaskJobWorker) Name() string { return w.name }

func (w *externalTaskJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	raw, err := data.GetData()
	if err != nil {
		return nil, err
	}
	var input map[string]interface{}
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, err
	}
	taskInput := map[string]interface{}{
		"form": input["form"],
		"context": map[string]interface{}{
			"request_id": "req-1",
		},
	}
	taskData := swf.NewTaskDataOrPanic(taskInput)
	return ctx.DoTask(swf.RunPolicy{}, w.taskType, taskData)
}

func TestDoTaskRescheduleSetsAlternateNeedFromInvocationTimeout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                 string
		policy               swf.RunPolicy
		expectAlternateNeed  bool
		expectedAfterSeconds int64
	}{
		{
			name:                 "invocation-timeout-set",
			policy:               swf.RunPolicy{InvocationTimeout: swf.AsDuration(2 * time.Second)},
			expectAlternateNeed:  true,
			expectedAfterSeconds: 2,
		},
		{
			name:                "no-invocation-timeout",
			policy:              swf.RunPolicy{}, // nil invocation timeout
			expectAlternateNeed: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			jobWorker := &missingTaskJobWorker{
				name:     "alt-job-" + tc.name,
				taskType: "missing-task",
				policy:   tc.policy,
			}
			ws := initWorkset(jobWorker)
			jobWorker.workset = ws

			embedded, err := StartEmbeddedEngine(ctx, jobWorker)
			if err != nil {
				t.Fatalf("start embedded engine: %v", err)
			}
			defer embedded.Shutdown()

			engine := embedded.SWFEngine.(*swfEngineImpl)

			jobKey, err := engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-alt",
				JobType:  jobWorker.Name(),
				Data:     swf.NewTaskDataOrPanic(map[string]string{"hello": "world"}),
				RunPolicy: swf.RunPolicy{
					InvocationTimeout: tc.policy.InvocationTimeout,
				},
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			lease := getLeaseForJob(t, ctx, engine, jobKey)
			if lease == nil {
				t.Fatalf("expected lease for job")
			}

			r := newRunnerForTest(engine, lease, ws, ctx)
			r.jobPolicy = normalizeRunPolicy(tc.policy)

			done := make(chan struct{})
			go func() {
				defer close(done)
				r.DoJob(ctx)
			}()

			select {
			case <-done:
			case <-ctx.Done():
				t.Fatalf("runner did not finish: %v", ctx.Err())
			}

			var altNeed sql.NullString
			var altAfter sql.NullInt64
			row := engine.udb.QueryRowContext(ctx,
				`SELECT alternate_next_need, alternate_after_seconds FROM pgwf.jobs WHERE job_id = $1`,
				lease.JobID())
			if err := row.Scan(&altNeed, &altAfter); err != nil {
				t.Fatalf("query alternate fields: %v", err)
			}

			if tc.expectAlternateNeed {
				if !altNeed.Valid {
					t.Fatalf("alternate_next_need missing")
				}
				if altNeed.String != jobWorker.name {
					t.Fatalf("alternate_next_need mismatch, got %s want %s", altNeed.String, jobWorker.name)
				}
				if !altAfter.Valid || altAfter.Int64 != tc.expectedAfterSeconds {
					t.Fatalf("alternate_after_seconds got %d want %d", altAfter.Int64, tc.expectedAfterSeconds)
				}
			} else {
				if altNeed.Valid || altAfter.Valid {
					t.Fatalf("expected no alternate fields, got need=%v after=%v", altNeed, altAfter)
				}
			}
		})
	}
}

type missingTaskJobWorker struct {
	name     string
	taskType string
	policy   swf.RunPolicy
	workset  *swf.WorkSet
}

func (w *missingTaskJobWorker) Name() string { return w.name }

func (w *missingTaskJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	_, err := ctx.DoTask(w.policy, w.taskType, data)
	return nil, err
}

// Test that when the first attempt reschedules to a missing task worker, pgwf pivots
// to the alternate need (job worker) after the invocation timeout, and on replay
// with a now-present task worker the runner records the invocation timeout payload.
func TestAlternateNeedReplayRecordsInvocationTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	invocation := swf.AsDuration(1 * time.Second)

	jobWorker := &missingTaskJobWorker{
		name:     "alt-timeout-job",
		taskType: "slow-task",
		policy: swf.RunPolicy{
			InvocationTimeout: invocation,
		},
	}
	ws := initWorkset(jobWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()
	engine := embedded.SWFEngine.(*swfEngineImpl)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-alt-timeout",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"hello": "world"}),
		RunPolicy: swf.RunPolicy{
			InvocationTimeout: invocation,
		},
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// First lease: job worker runs, reschedules to missing task and Goexits.
	lease1 := getLeaseForJob(t, ctx, engine, jobKey)
	if lease1 == nil {
		t.Fatalf("expected first lease")
	}
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		r := newRunnerForTest(engine, lease1, ws, ctx)
		r.jobPolicy = normalizeRunPolicy(jobWorker.policy)
		r.DoJob(ctx)
	}()

	<-firstDone

	// Wait past alternate-after (rounded up in pgwf to integer seconds).
	time.Sleep(2500 * time.Millisecond)

	// Before second lease, register a slow task worker to trigger invocation timeout on replay.
	slowTask := &slowTaskWorker{name: jobWorker.taskType, sleep: 1500 * time.Millisecond}
	ws.TaskWorkers[slowTask.Name()] = slowTask

	// Second lease should come via alternate need (job worker capability).
	var lease2 *pgwf.Lease
	for i := 0; i < 15; i++ {
		lease2, err = pgwf.GetWork(ctx, engine.udb, pgwf.WorkerID(engine.workerId), []pgwf.Capability{pgwf.Capability(jobWorker.Name())}, nil)
		if err != nil {
			t.Fatalf("get work attempt %d: %v", i, err)
		}
		if lease2 != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lease2 == nil {
		t.Fatalf("expected second lease via alternate need")
	}
	if lease2.NextNeed() != pgwf.Capability(jobWorker.Name()) {
		t.Fatalf("expected alternate lease to job worker, got %s", lease2.NextNeed())
	}

	// Run again; DoTask will now find worker, sleep past invocation timeout, and record timeout payload.
	r2 := newRunnerForTest(engine, lease2, ws, ctx)
	r2.jobPolicy = normalizeRunPolicy(jobWorker.policy)
	r2.DoJob(ctx)

	// Verify task chapter recorded an invocation timeout.
	chap, err := engine.strata.Chapter(ctx, storyKeyForJob(jobKey), 1)
	if err != nil {
		t.Fatalf("fetch task chapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.PayloadKind != payloadKindTimeout {
		t.Fatalf("expected timeout payload kind, got %s", env.PayloadKind)
	}
	var tp swf.TimeoutPayload
	if err := json.Unmarshal(env.Payload, &tp); err != nil {
		t.Fatalf("unmarshal timeout payload: %v", err)
	}
	if tp.Scope != swf.TimeoutScopeInvocation {
		t.Fatalf("expected invocation timeout, got %s", tp.Scope)
	}
	if !tp.Retryable {
		t.Fatalf("invocation timeout should be retryable")
	}
}

type slowTaskWorker struct {
	name  string
	sleep time.Duration
}

func (w *slowTaskWorker) Name() string { return w.name }

func (w *slowTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	time.Sleep(w.sleep)
	return data, nil
}

type multiTaskJobWorker struct {
	name       string
	task1Name  string
	task2Name  string
	taskPolicy swf.RunPolicy
	workset    *swf.WorkSet
}

func (w *multiTaskJobWorker) Name() string { return w.name }

func (w *multiTaskJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	// Call task1 with retry policy
	result1, err := ctx.DoTask(w.taskPolicy, w.task1Name, data)
	if err != nil {
		return nil, err
	}

	// Call task2 with retry policy
	result2, err := ctx.DoTask(w.taskPolicy, w.task2Name, result1)
	if err != nil {
		return nil, err
	}

	return result2, nil
}
