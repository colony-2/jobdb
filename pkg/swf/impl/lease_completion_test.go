package impl

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
)

func TestJobPrepareResultErrorDoesNotCompleteLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	jobWorker := &staticResultJobWorker{
		name:    "prepare-result-error",
		started: started,
		output:  erroringJobData{err: errors.New("bad output")},
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "input"}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	waitForSignal(t, started, 2*time.Second, "job start")

	assertJobNotCompleted(t, ctx, engine, jobKey, 500*time.Millisecond)
}

func TestJobSaveResultErrorDoesNotCompleteLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	jobWorker := &staticResultJobWorker{
		name:    "save-result-error",
		started: started,
		output:  invalidJSONJobData{data: []byte("{invalid")},
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "input"}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	waitForSignal(t, started, 2*time.Second, "job start")

	assertJobNotCompleted(t, ctx, engine, jobKey, 500*time.Millisecond)
}

func TestJobAwaitTimeoutDoesNotCompleteLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	jobWorker := &alwaysFailingJobWorker{name: "await-timeout-job", counter: &attempts}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "input"}),
		RunPolicy: swf.RunPolicy{
			TotalTimeout: swf.AsDuration(2 * time.Second),
			Retry: swf.RetryPolicy{
				MaximumAttempts:    2,
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(5 * time.Second),
			},
		},
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	waitForAttempt(t, &attempts, 1, 2*time.Second)

	assertJobNotCompleted(t, ctx, engine, jobKey, 2500*time.Millisecond)
}

func TestJobCrashConcernAfterRepeatedLeaseExpirations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	jobWorker := &trackingFailingJobWorker{
		name:    "crash-concern-job",
		counter: &attempts,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}

	if _, err := engine.udb.ExecContext(ctx, "SELECT pgwf.set_crash_concern_threshold($1)", 1); err != nil {
		t.Fatalf("set crash concern threshold: %v", err)
	}

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "input"}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	runOnceWithLease(t, ctx, engine, jobKey, jobWorker.workset)
	expireLease(t, ctx, engine, jobKey)

	runOnceWithLease(t, ctx, engine, jobKey, jobWorker.workset)
	expireLease(t, ctx, engine, jobKey)

	waitForJobStatus(t, ctx, engine, jobKey, swf.JobStatusCrashConcern, 2*time.Second)

	time.Sleep(200 * time.Millisecond)
	if attempts.Load() > 2 {
		t.Fatalf("expected worker to stop after crash concern, got %d runs", attempts.Load())
	}
}

func TestRunnerStopsKeepAliveOnExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaseCh := make(chan *pgwf.Lease, 1)
	jobWorker := &leaseCaptureJobWorker{
		name:    "stop-keepalive-on-exit",
		leaseCh: leaseCh,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	if _, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "input"}),
	}); err != nil {
		t.Fatalf("start job: %v", err)
	}

	lease := waitForLease(t, leaseCh, 2*time.Second)
	waitForKeepAliveState(t, lease, true, 2*time.Second)
	waitForKeepAliveState(t, lease, false, 2*time.Second)
}

func waitForSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, label string) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return
	case <-timer.C:
		t.Fatalf("timeout waiting for %s", label)
	}
}

func waitForAttempt(t *testing.T, counter *atomic.Int32, target int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for attempt %d", target)
}

func waitForJobStatus(t *testing.T, ctx context.Context, engine *swfEngineImpl, jobKey swf.JobKey, status swf.JobStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := engine.CheckJobStatus(ctx, jobKey)
		if err != nil {
			t.Fatalf("check job status: %v", err)
		}
		if st == status {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for status %s", status)
}

func runOnceWithLease(t *testing.T, ctx context.Context, engine *swfEngineImpl, jobKey swf.JobKey, workset *swf.WorkSet) {
	t.Helper()
	lease := getLeaseForJob(t, ctx, engine, jobKey)
	if lease == nil {
		t.Fatalf("no lease available")
	}
	r := newRunnerForTest(engine, lease, workset, ctx)
	r.DoJob(ctx)
}

func expireLease(t *testing.T, ctx context.Context, engine *swfEngineImpl, jobKey swf.JobKey) {
	t.Helper()
	_, err := engine.udb.ExecContext(
		ctx,
		"UPDATE pgwf.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2",
		jobKey.TenantId,
		jobKey.JobId,
	)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func assertJobNotCompleted(t *testing.T, ctx context.Context, engine *swfEngineImpl, jobKey swf.JobKey, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		status, err := engine.CheckJobStatus(ctx, jobKey)
		if err != nil {
			t.Fatalf("check job status: %v", err)
		}
		if status == swf.JobStatusCompleted {
			t.Fatalf("unexpected completed status")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForLease(t *testing.T, ch <-chan *pgwf.Lease, timeout time.Duration) *pgwf.Lease {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case lease := <-ch:
		if lease == nil {
			t.Fatalf("received nil lease")
		}
		return lease
	case <-timer.C:
		t.Fatalf("timeout waiting for lease")
	}
	return nil
}

func waitForKeepAliveState(t *testing.T, lease *pgwf.Lease, expected bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leaseKeepAliveStarted(lease) == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for keepAliveStarted=%v", expected)
}

func leaseKeepAliveStarted(lease *pgwf.Lease) bool {
	if lease == nil {
		return false
	}
	val := reflect.ValueOf(lease).Elem().FieldByName("keepAliveStarted")
	return *(*bool)(unsafe.Pointer(val.UnsafeAddr()))
}

type staticResultJobWorker struct {
	name    string
	started chan struct{}
	output  swf.JobData
	workset *swf.WorkSet
}

func (w *staticResultJobWorker) Name() string { return w.name }

func (w *staticResultJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	return w.output, nil
}

type erroringJobData struct {
	err error
}

func (e erroringJobData) GetData() (swf.Data, error) { return nil, e.err }
func (e erroringJobData) GetDataOrPanic() swf.Data   { panic(e.err) }
func (e erroringJobData) GetArtifacts() ([]swf.Artifact, error) {
	return nil, nil
}

type invalidJSONJobData struct {
	data []byte
}

func (d invalidJSONJobData) GetData() (swf.Data, error) { return d.data, nil }
func (d invalidJSONJobData) GetDataOrPanic() swf.Data   { return d.data }
func (d invalidJSONJobData) GetArtifacts() ([]swf.Artifact, error) {
	return nil, nil
}

type leaseCaptureJobWorker struct {
	name    string
	leaseCh chan *pgwf.Lease
	workset *swf.WorkSet
}

func (w *leaseCaptureJobWorker) Name() string { return w.name }

func (w *leaseCaptureJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if runnerCtx, ok := ctx.(*runner); ok && runnerCtx.lease != nil {
		if accessor, ok := runnerCtx.lease.(interface{ PgwfLease() *pgwf.Lease }); ok {
			if lease := accessor.PgwfLease(); lease != nil {
				select {
				case w.leaseCh <- lease:
				default:
				}
			}
		}
	}
	return erroringJobData{err: errors.New("bad output")}, nil
}

type alwaysFailingJobWorker struct {
	name    string
	counter *atomic.Int32
	workset *swf.WorkSet
}

type trackingFailingJobWorker struct {
	name    string
	counter *atomic.Int32
	ranCh   chan struct{}
	workset *swf.WorkSet
}

func (w *trackingFailingJobWorker) Name() string { return w.name }

func (w *trackingFailingJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if w.counter != nil {
		w.counter.Add(1)
	}
	select {
	case w.ranCh <- struct{}{}:
	default:
	}
	return invalidJSONJobData{data: []byte("{invalid")}, nil
}

func (w *alwaysFailingJobWorker) Name() string { return w.name }

func (w *alwaysFailingJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	w.counter.Add(1)
	return nil, swf.AppError{Payload: swf.AppErrorPayload{
		Message: "retryable failure",
		Level:   "error",
	}}
}
