package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// The replay contract includes the ordered observer history, not just the final
// result. Normalize data/error representations so live and decoded events compare.
type historyEvent struct {
	Kind, Task, Data, Error string
	JobKey                  JobKey
	Ordinal                 int64
	Attempt                 int
	At                      time.Time
}

type historyObserver struct{ events []historyEvent }

func historyData(data TaskData) string {
	if data == nil {
		return ""
	}
	raw, err := data.GetData()
	if err != nil {
		return "data error: " + err.Error()
	}
	return string(raw)
}

func historyError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (o *historyObserver) OnJobStart(e JobStartEvent) {
	o.events = append(o.events, historyEvent{Kind: "job-start", JobKey: e.JobKey, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Input)})
}
func (o *historyObserver) OnJobEnd(e JobEndEvent) {
	o.events = append(o.events, historyEvent{Kind: "job-end", JobKey: e.JobKey, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Output), Error: historyError(e.Err)})
}
func (o *historyObserver) OnTaskStart(e TaskStartEvent) {
	o.events = append(o.events, historyEvent{Kind: "task-start", JobKey: e.JobKey, Task: e.TaskType, Ordinal: e.Ordinal, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Input)})
}
func (o *historyObserver) OnTaskEnd(e TaskEndEvent) {
	o.events = append(o.events, historyEvent{Kind: "task-end", JobKey: e.JobKey, Task: e.TaskType, Ordinal: e.Ordinal, Attempt: e.AttemptNumber, At: e.At, Data: historyData(e.Output), Error: historyError(e.Err)})
}

type replayHistoryJobOverride struct {
	JobWorker
	run func(ExecutionContext, JobData) (JobData, error)
}

func (j replayHistoryJobOverride) Run(ctx ExecutionContext, data JobData) (JobData, error) {
	return j.run(ctx, data)
}

func TestExecutionReplayPreservesObserverHistoryAndExpiredBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := newRunnerTestRuntime()
		key := JobKey{TenantId: "tenant", JobId: "expired-replay"}
		var calls atomic.Int32
		task := failThenSucceedTask{name: "task", counter: &calls, failAttempts: 1}
		job := taskTimeoutJob{name: "job", taskType: task.Name(), policy: RunPolicy{InvocationTimeout: AsDuration(time.Minute), Retry: RetryPolicy{MaximumAttempts: 2}}}
		policy := RunPolicy{TotalTimeout: AsDuration(time.Hour)}
		ws := mustWorkSetForRunnerTest(t, job, task)
		seedJobStartForTest(t, rt, key, job.Name(), NewTaskDataOrPanic(42), policy)
		lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: job.Name()}}
		live := &historyObserver{}
		_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{Observer: live}).DoJob(context.Background())
		require.NoError(t, err)
		before, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		time.Sleep(2 * time.Hour)
		replay := &historyObserver{}
		_, err = newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: replay}).DoJob(context.Background())
		require.NoError(t, err)
		require.Equal(t, live.events, replay.events)
		require.EqualValues(t, 2, calls.Load())
		after, err := rt.ListChapters(context.Background(), ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}
