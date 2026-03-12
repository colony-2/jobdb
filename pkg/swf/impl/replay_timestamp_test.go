package impl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
)

type recordingReplayObserver struct {
	jobStarts  []swf.JobStartEvent
	jobEnds    []swf.JobEndEvent
	taskStarts []swf.TaskStartEvent
	taskEnds   []swf.TaskEndEvent
}

func (r *recordingReplayObserver) OnJobStart(evt swf.JobStartEvent) {
	r.jobStarts = append(r.jobStarts, evt)
}

func (r *recordingReplayObserver) OnJobEnd(evt swf.JobEndEvent) {
	r.jobEnds = append(r.jobEnds, evt)
}

func (r *recordingReplayObserver) OnTaskStart(evt swf.TaskStartEvent) {
	r.taskStarts = append(r.taskStarts, evt)
}

func (r *recordingReplayObserver) OnTaskEnd(evt swf.TaskEndEvent) {
	r.taskEnds = append(r.taskEnds, evt)
}

type singleTaskJobWorker struct {
	name    string
	workset *swf.WorkSet
}

func (w *singleTaskJobWorker) Name() string { return w.name }

func (w *singleTaskJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	taskInput := swf.NewTaskDataOrPanic(map[string]string{"task": "1"})
	return ctx.DoTask(swf.RunPolicy{}, "echo", taskInput)
}

type twoTaskJobWorker struct {
	name    string
	workset *swf.WorkSet
}

func (w *twoTaskJobWorker) Name() string { return w.name }

func (w *twoTaskJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	task1 := swf.NewTaskDataOrPanic(map[string]string{"task": "1"})
	if _, err := ctx.DoTask(swf.RunPolicy{}, "echo", task1); err != nil {
		return nil, err
	}
	task2 := swf.NewTaskDataOrPanic(map[string]string{"task": "2"})
	if _, err := ctx.DoTask(swf.RunPolicy{}, "echo", task2); err != nil {
		return nil, err
	}
	return data, nil
}

func TestReplayObserverUsesCachedChapterTimes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobWorker := &singleTaskJobWorker{name: "replay-time-job"}
	taskWorker := &echoTaskWorker{}
	jobWorker.workset = initWorkset(jobWorker, taskWorker)

	embedded, err := StartEmbeddedEngine(ctx, jobWorker, taskWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()
	engine := embedded.SWFEngine.(*swfEngineImpl)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-replay-time",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "data"}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	lease := getLeaseForJob(t, ctx, engine, jobKey)
	if lease == nil {
		t.Fatalf("expected lease")
	}
	r := newRunnerForTest(engine, lease, jobWorker.workset, ctx)
	if _, err := r.DoJob(ctx); err != nil {
		t.Fatalf("do job: %v", err)
	}

	key := storyKeyForJob(jobKey)
	chap0, err := engine.strata.Chapter(ctx, key, 0)
	if err != nil {
		t.Fatalf("chapter 0: %v", err)
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		t.Fatalf("decode chapter 0: %v", err)
	}

	taskChap, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("chapter 1: %v", err)
	}
	taskEnv, err := decodeChapterEnvelope(taskChap.Body())
	if err != nil {
		t.Fatalf("decode task chapter: %v", err)
	}

	jobChap, err := engine.strata.Chapter(ctx, key, 2)
	if err != nil {
		t.Fatalf("chapter 2: %v", err)
	}
	jobEnv, err := decodeChapterEnvelope(jobChap.Body())
	if err != nil {
		t.Fatalf("decode job chapter: %v", err)
	}

	observer := &recordingReplayObserver{}
	if _, err := engine.ReplayJobRun(ctx, swf.ReplayRunRequest{
		JobKey:   jobKey,
		Observer: observer,
	}); err != nil {
		t.Fatalf("replay job: %v", err)
	}

	if len(observer.jobStarts) != 1 {
		t.Fatalf("expected 1 job start, got %d", len(observer.jobStarts))
	}
	if len(observer.jobEnds) != 1 {
		t.Fatalf("expected 1 job end, got %d", len(observer.jobEnds))
	}
	if len(observer.taskStarts) != 1 {
		t.Fatalf("expected 1 task start, got %d", len(observer.taskStarts))
	}
	if len(observer.taskEnds) != 1 {
		t.Fatalf("expected 1 task end, got %d", len(observer.taskEnds))
	}

	if !observer.jobStarts[0].At.Equal(env0.Meta.CreatedAt) {
		t.Fatalf("job start time mismatch: got %v want %v", observer.jobStarts[0].At, env0.Meta.CreatedAt)
	}

	taskStartAt := metaStartAt(taskEnv)
	taskEndAt := metaEndAt(taskEnv)
	if !observer.taskStarts[0].At.Equal(taskStartAt) {
		t.Fatalf("task start time mismatch: got %v want %v", observer.taskStarts[0].At, taskStartAt)
	}
	if !observer.taskEnds[0].At.Equal(taskEndAt) {
		t.Fatalf("task end time mismatch: got %v want %v", observer.taskEnds[0].At, taskEndAt)
	}

	jobEndAt := metaEndAt(jobEnv)
	if !observer.jobEnds[0].At.Equal(jobEndAt) {
		t.Fatalf("job end time mismatch: got %v want %v", observer.jobEnds[0].At, jobEndAt)
	}
}

func TestReplayObserverMissingTaskUsesPriorEndTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobWorker := &twoTaskJobWorker{name: "replay-miss-job"}
	taskWorker := &echoTaskWorker{}
	workset := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = workset

	embedded, err := StartEmbeddedEngine(ctx, jobWorker, taskWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()
	engine := embedded.SWFEngine.(*swfEngineImpl)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-replay-miss",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "data"}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Pre-create only the first task chapter (ordinal 1).
	taskRunner := newRunnerForTest(engine, nil, workset, ctx)
	taskRunner.jobId = pgwf.JobID(jobKey.JobId)
	taskRunner.tenantId = jobKey.TenantId
	taskRunner.jobPolicy = normalizeRunPolicy(swf.RunPolicy{})
	taskRunner.capability = pgwf.Capability(jobWorker.Name())
	taskInput := swf.NewTaskDataOrPanic(map[string]string{"task": "1"})
	if _, err := taskRunner.DoTask(swf.RunPolicy{}, "echo", taskInput); err != nil {
		t.Fatalf("task 1: %v", err)
	}

	key := storyKeyForJob(jobKey)
	taskChap, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("chapter 1: %v", err)
	}
	taskEnv, err := decodeChapterEnvelope(taskChap.Body())
	if err != nil {
		t.Fatalf("decode task chapter: %v", err)
	}
	taskEndAt := metaEndAt(taskEnv)

	observer := &recordingReplayObserver{}
	_, err = engine.ReplayJobRun(ctx, swf.ReplayRunRequest{
		JobKey:   jobKey,
		Observer: observer,
	})
	if err == nil {
		t.Fatalf("expected replay error")
	}
	var miss swf.ReplayCacheMissError
	if !errors.As(err, &miss) {
		t.Fatalf("expected replay cache miss, got %v", err)
	}
	if miss.Reason != swf.ReplayCacheMissJobResultMissing {
		t.Fatalf("unexpected replay miss reason: %v", miss.Reason)
	}

	if len(observer.taskStarts) != 2 {
		t.Fatalf("expected 2 task starts, got %d", len(observer.taskStarts))
	}
	if len(observer.taskEnds) != 2 {
		t.Fatalf("expected 2 task ends, got %d", len(observer.taskEnds))
	}
	if len(observer.jobStarts) != 1 {
		t.Fatalf("expected 1 job start, got %d", len(observer.jobStarts))
	}
	if len(observer.jobEnds) != 1 {
		t.Fatalf("expected 1 job end, got %d", len(observer.jobEnds))
	}

	if !observer.taskEnds[1].At.Equal(taskEndAt) {
		t.Fatalf("missing task end time mismatch: got %v want %v", observer.taskEnds[1].At, taskEndAt)
	}
	if !observer.taskStarts[1].At.Equal(taskEndAt) {
		t.Fatalf("missing task start time mismatch: got %v want %v", observer.taskStarts[1].At, taskEndAt)
	}
	if !observer.jobEnds[0].At.Equal(taskEndAt) {
		t.Fatalf("job end time mismatch: got %v want %v", observer.jobEnds[0].At, taskEndAt)
	}
}
