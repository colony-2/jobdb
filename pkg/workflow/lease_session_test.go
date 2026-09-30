package workflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type sessionTestLease struct {
	*fakeExecutionLease
	expiry time.Time
	renew  func(context.Context) (jobdb.RenewableExecutionLease, error)
}

func (l *sessionTestLease) LeaseWorkerID() string  { return "owner" }
func (l *sessionTestLease) LeaseExpiry() time.Time { return l.expiry }
func (l *sessionTestLease) Renew(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
	return l.renew(ctx)
}
func newSessionTestLease() *sessionTestLease {
	return &sessionTestLease{fakeExecutionLease: &fakeExecutionLease{leaseID: "lease", job: JobHandle{JobKey: JobKey{TenantId: "tenant", JobId: "job"}}}, expiry: time.Now().Add(time.Second)}
}

func TestLeaseSessionRenewalTimeout(t *testing.T) {
	lease := newSessionTestLease()
	lease.expiry = time.Now().Add(30 * time.Millisecond)
	lease.renew = func(ctx context.Context) (jobdb.RenewableExecutionLease, error) { <-ctx.Done(); return nil, ctx.Err() }
	_, err := startLeaseSession(context.Background(), lease)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var renewal *jobdb.LeaseRenewalError
	require.ErrorAs(t, err, &renewal)
}
func TestLeaseSessionSerializesReleaseAndRenewal(t *testing.T) {
	for _, reschedule := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "reschedule"}[reschedule], func(t *testing.T) {
			lease := newSessionTestLease()
			entered := make(chan struct{})
			unblock := make(chan struct{})
			var calls atomic.Int64
			lease.renew = func(ctx context.Context) (jobdb.RenewableExecutionLease, error) {
				if calls.Add(1) == 2 {
					close(entered)
					select {
					case <-unblock:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				next := *lease
				next.expiry = time.Now().Add(120 * time.Millisecond)
				return &next, nil
			}
			session, err := startLeaseSession(context.Background(), lease)
			require.NoError(t, err)
			<-entered
			released := make(chan error, 1)
			go func() {
				if reschedule {
					released <- session.Reschedule(context.Background(), RescheduleExecutionRequest{})
				} else {
					released <- session.Complete(context.Background(), CompleteExecutionRequest{})
				}
			}()
			close(unblock)
			require.NoError(t, <-released)
			<-session.done
			require.Nil(t, context.Cause(session.ctx))
			require.Equal(t, int64(2), calls.Load())
			session.close()
		})
	}
}
func TestLeaseSessionFailureIsNotMaskedByTerminalRead(t *testing.T) {
	runtime := &runJobIfLeaseableStubRuntime{jobResp: JobInfo{Status: JobStatusCompleted}}
	for _, err := range []error{ErrExecutionLeaseLost, &jobdb.LeaseRenewalError{Err: errors.New("transport")}} {
		_, got := classifyJobRunAfterRun(context.Background(), runtime, JobKey{}, nil, nil, err)
		require.ErrorIs(t, got, err)
	}
}
func TestSuppliedLeaseRequiresRenewalSupport(t *testing.T) {
	lease := &fakeExecutionLease{leaseID: "lease", job: JobHandle{JobKey: JobKey{TenantId: "tenant", JobId: "job"}}}
	runtime := &runJobIfLeaseableStubRuntime{}
	_, err := GetJobForRunWithLease(context.Background(), runtime, lease, GetJobForRunRequest{JobKey: lease.Job().JobKey, JobWorker: runJobIfLeaseableTestJob{name: "job"}})
	require.ErrorIs(t, err, jobdb.ErrLeaseRenewalUnsupported)
	require.Equal(t, GetJobLeaseRequest{}, runtime.leaseReq)
}
