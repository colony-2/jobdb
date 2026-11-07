package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func awaitAuthoritySignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("session did not make progress")
	}
}

func TestLeaseSessionRenewalFirstAuthorityLoss(t *testing.T) {
	lease := newSessionTestLease()
	cause := fmt.Errorf("renew rejected: %w", jobdb.ErrExecutionLeaseLost)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	lease.renew = func(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
		if calls.Add(1) == 1 {
			next := *lease
			next.expiry = time.Now().Add(90 * time.Millisecond)
			return &next, nil
		}
		close(entered)
		select {
		case <-release:
			return nil, cause
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	session, err := startLeaseSession(celltest.Context(context.Background()), lease)
	require.NoError(t, err)
	defer session.close()
	awaitAuthoritySignal(t, entered)
	require.Nil(t, context.Cause(session.ctx))
	close(release)
	awaitAuthoritySignal(t, session.ctx.Done())
	var renewal *jobdb.LeaseRenewalError
	require.ErrorAs(t, context.Cause(session.ctx), &renewal)
	require.ErrorIs(t, context.Cause(session.ctx), cause)
	require.ErrorIs(t, context.Cause(session.ctx), jobdb.ErrExecutionLeaseLost)
}

func TestLeaseSessionMutationAuthorityLoss(t *testing.T) {
	for _, reschedule := range []bool{false, true} {
		t.Run(fmt.Sprint(reschedule), func(t *testing.T) {
			lease := newSessionTestLease()
			cause := fmt.Errorf("mutation rejected: %w", jobdb.ErrExecutionLeaseLost)
			lease.completeErr = cause
			lease.rescheduleErr = cause
			lease.renew = func(context.Context) (jobdb.RenewableExecutionLease, error) { return lease, nil }
			session, err := startLeaseSession(celltest.Context(context.Background()), lease)
			require.NoError(t, err)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if reschedule {
					err = session.Reschedule(context.Background(), RescheduleExecutionRequest{})
				} else {
					err = session.Complete(context.Background(), CompleteExecutionRequest{})
				}
				session.close()
			}()
			awaitAuthoritySignal(t, done)
			var renewal *jobdb.LeaseRenewalError
			require.ErrorAs(t, err, &renewal)
			require.ErrorIs(t, err, cause)
			require.Same(t, err, context.Cause(session.ctx))
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		})
	}
}

func TestWorkerRunnerAppendFirstRenewableAuthorityLoss(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		t.Run(fmt.Sprint(ordinary), func(t *testing.T) {
			runtime := newRunnerTestRuntime()
			job := countingJobWorker{name: "authority", counter: &atomic.Int32{}}
			lease := newSessionTestLease()
			lease.runtime = runtime
			lease.route = Route{JobType: job.Name()}
			seedJobStartForTest(t, runtime, lease.Job().JobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})
			var calls atomic.Int64
			lease.renew = func(context.Context) (jobdb.RenewableExecutionLease, error) {
				calls.Add(1)
				next := *lease
				next.expiry = time.Now().Add(time.Hour)
				return &next, nil
			}
			cause := fmt.Errorf("append rejected: %w", jobdb.ErrExecutionLeaseLost)
			var appends atomic.Int64
			runtime.putChapterHook = func(PutChapterRequest) error { appends.Add(1); return cause }
			// Disable heartbeat detection explicitly, so only the rejected append
			// can establish authority loss. No timing race selects the detector.
			before := func(_ context.Context, l ExecutionLease) error {
				session := l.(*leaseSession)
				session.stop()
				awaitAuthoritySignal(t, session.done)
				require.Nil(t, context.Cause(session.ctx))
				return nil
			}
			var err error
			if ordinary {
				claiming := &runJobIfLeaseableLeaseRuntime{runnerTestRuntime: runtime, leaseResp: lease}
				runnable, e := GetJobForRun(celltest.Context(context.Background()), claiming, GetJobForRunRequest{JobKey: lease.Job().JobKey, JobWorker: job, BeforeRun: before})
				require.NoError(t, e)
				_, err = runnable.Run(nil)
				require.Equal(t, lease.Job().JobKey, claiming.leaseReq.JobKey)
			} else {
				runner := newWorkerRunner(runtime, mustWorkSetForRunnerTest(t, job), lease, workerRunnerOptions{})
				runner.beforeRun = before
				_, err = runner.DoJob(celltest.Context(context.Background()))
			}
			require.Equal(t, int64(1), calls.Load())
			var renewal *jobdb.LeaseRenewalError
			var control *executionControlError
			require.ErrorAs(t, err, &renewal)
			require.ErrorAs(t, err, &control)
			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.Equal(t, int64(1), appends.Load())
			_, _, complete, reschedule := lease.snapshot()
			require.Empty(t, complete)
			require.Empty(t, reschedule)
		})
	}
}

func TestRenewableAuthorityClassificationPreservesErrors(t *testing.T) {
	raw := fmt.Errorf("unsafe bearer: %w", jobdb.ErrExecutionLeaseLost)
	control := &executionControlError{raw}
	classified := classifyLeaseLoss(control)
	require.NotContains(t, classified.Error(), "unsafe bearer")
	require.ErrorIs(t, classified, raw)
	require.True(t, isExecutionControl(classified))
	require.Same(t, classified, classifyLeaseLoss(classified))
	for _, err := range []error{nil, context.Canceled, errors.New("application"), ErrWorkflowNotDeterministic} {
		require.Equal(t, err, classifyLeaseLoss(err))
	}
}
