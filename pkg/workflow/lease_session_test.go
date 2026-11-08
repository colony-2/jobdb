package workflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
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

type secretSessionLease struct {
	*sessionTestLease
	resolve func(context.Context, []string) (jobdb.SecretResolveResult, error)
}

func (l *secretSessionLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	return l.resolve(ctx, names)
}

func TestLeaseSessionFreshSecretLookupAllowsRenewal(t *testing.T) {
	base := newSessionTestLease()
	started, renewed, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var count atomic.Int64
	lease := &secretSessionLease{sessionTestLease: base}
	lease.resolve = func(ctx context.Context, _ []string) (jobdb.SecretResolveResult, error) {
		close(started)
		select {
		case <-release:
			return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "value"}}}, nil
		case <-ctx.Done():
			return jobdb.SecretResolveResult{}, ctx.Err()
		}
	}
	base.renew = func(context.Context) (jobdb.RenewableExecutionLease, error) {
		n := count.Add(1)
		copyBase := *base
		copyBase.expiry = time.Now().Add(600 * time.Millisecond)
		next := &secretSessionLease{sessionTestLease: &copyBase, resolve: lease.resolve}
		if n == 2 {
			close(renewed)
		}
		return next, nil
	}
	session, err := startLeaseSession(celltest.Context(context.Background()), lease)
	require.NoError(t, err)
	defer session.close()
	runner := &workerRunner{ctx: session.ctx, lease: session}
	done := make(chan error, 1)
	go func() {
		_, err := runner.ResolveSecretsFresh(celltest.Context(context.Background()), []string{"token"})
		done <- err
	}()
	<-started
	select {
	case <-renewed:
	case <-time.After(2 * time.Second):
		t.Fatal("lookup blocked lease renewal")
	}
	close(release)
	require.NoError(t, <-done)
	cached, err := runner.ResolveSecrets(celltest.Context(context.Background()), []string{"token"})
	require.NoError(t, err)
	require.Equal(t, "value", cached.Secrets["token"].Value)
}

func TestLeaseSessionRenewalTimeout(t *testing.T) {
	lease := newSessionTestLease()
	lease.expiry = time.Now().Add(30 * time.Millisecond)
	lease.renew = func(ctx context.Context) (jobdb.RenewableExecutionLease, error) { <-ctx.Done(); return nil, ctx.Err() }
	_, err := startLeaseSession(celltest.Context(context.Background()), lease)
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
			session, err := startLeaseSession(celltest.Context(context.Background()), lease)
			require.NoError(t, err)
			<-entered
			released := make(chan error, 1)
			go func() {
				if reschedule {
					released <- session.Reschedule(celltest.Context(context.Background()), RescheduleExecutionRequest{})
				} else {
					released <- session.Complete(celltest.Context(context.Background()), CompleteExecutionRequest{})
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
	for _, status := range []JobStatus{JobStatusCompleted, JobStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			runtime := &runJobIfLeaseableStubRuntime{jobResp: JobInfo{Status: status, CompletionDetail: "operator cancelled"}}
			for _, err := range []error{ErrExecutionLeaseLost, &jobdb.LeaseRenewalError{Err: errors.New("transport")}} {
				_, got := classifyJobRunAfterRun(celltest.Context(context.Background()), runtime, JobKey{}, nil, nil, err)
				require.ErrorIs(t, got, err)
			}
			ctx, cancel := context.WithCancel(celltest.Context(context.Background()))
			cancel()
			_, got := classifyJobRunAfterRun(ctx, runtime, JobKey{}, nil, nil, ctx.Err())
			require.ErrorIs(t, got, context.Canceled)
		})
	}
}
func TestSuppliedLeaseRequiresRenewalSupport(t *testing.T) {
	lease := &fakeExecutionLease{leaseID: "lease", job: JobHandle{JobKey: JobKey{TenantId: "tenant", JobId: "job"}}}
	runtime := &runJobIfLeaseableStubRuntime{}
	_, err := GetJobForRunWithLease(celltest.Context(context.Background()), runtime, lease, GetJobForRunRequest{JobKey: lease.Job().JobKey, JobWorker: runJobIfLeaseableTestJob{name: "job"}})
	require.ErrorIs(t, err, jobdb.ErrLeaseRenewalUnsupported)
	require.Equal(t, GetJobLeaseRequest{}, runtime.leaseReq)
}
