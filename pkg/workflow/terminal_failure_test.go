package workflow

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerEngineReportsUnhandledRunnerError(t *testing.T) {
	rt := newRunnerTestRuntime()
	var logs bytes.Buffer
	job := countingJobWorker{name: "job", counter: &atomic.Int32{}}
	ws := mustWorkSetForRunnerTest(t, job)
	api, err := newWorkerEngine(rt, []WorkSet{*ws}, RuntimeBuildOptions{PollTenantId: "tenant", Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	require.NoError(t, err)
	// No initial chapter: execution fails before it can complete or reschedule.
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: JobKey{TenantId: "tenant", JobId: "broken"}}, route: Route{JobType: "job"}}
	api.(*workerEngine).runLease(context.Background(), lease, "worker")
	require.Contains(t, logs.String(), "job execution failed")
	require.Contains(t, logs.String(), `"tenantId":"tenant"`)
	require.Contains(t, logs.String(), `"jobId":"broken"`)
	require.Contains(t, logs.String(), "worker")
	require.Contains(t, logs.String(), ErrChapterNotFound.Error())
	_, _, complete, rescheduled := lease.snapshot()
	require.Empty(t, complete)
	require.Empty(t, rescheduled)
}
