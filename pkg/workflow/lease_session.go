package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// leaseSession owns renewal for one runner invocation. Renewal and terminal
// mutations share a lock so intentional release cannot race a heartbeat.
// Snapshot getters remain stable; capability getters follow the latest renewal.
type leaseSession struct {
	jobdb.ExecutionLease
	mu        sync.Mutex
	current   jobdb.RenewableExecutionLease
	released  bool
	ctx       context.Context
	cancel    context.CancelCauseFunc
	heartbeat context.Context
	stop      context.CancelFunc
	done      chan struct{}
}

const leaseRenewalTimeout = 10 * time.Second

func renewLease(ctx context.Context, lease jobdb.RenewableExecutionLease) (jobdb.RenewableExecutionLease, error) {
	if expiry := lease.LeaseExpiry(); !expiry.IsZero() && !expiry.After(time.Now()) {
		return nil, &jobdb.LeaseRenewalError{Err: jobdb.ErrExecutionLeaseLost}
	}
	deadline := time.Now().Add(leaseRenewalTimeout)
	if expiry := lease.LeaseExpiry(); !expiry.IsZero() && expiry.Before(deadline) {
		deadline = expiry
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	next, err := lease.Renew(callCtx)
	if err != nil {
		return nil, &jobdb.LeaseRenewalError{Err: err}
	}
	if err := callCtx.Err(); err != nil {
		return nil, &jobdb.LeaseRenewalError{Err: err}
	}
	if next == nil || next.LeaseID() != lease.LeaseID() || next.Job() != lease.Job() || next.LeaseWorkerID() == "" || (lease.LeaseWorkerID() != "" && next.LeaseWorkerID() != lease.LeaseWorkerID()) || !next.LeaseExpiry().After(time.Now()) {
		return nil, &jobdb.LeaseRenewalError{Err: jobdb.ErrExecutionLeaseLost}
	}
	return next, nil
}

func startLeaseSession(ctx context.Context, lease jobdb.RenewableExecutionLease) (*leaseSession, error) {
	next, err := renewLease(ctx, lease)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	heartbeat, stop := context.WithCancel(runCtx)
	s := &leaseSession{ExecutionLease: next, current: next, ctx: runCtx, cancel: cancel, heartbeat: heartbeat, stop: stop, done: make(chan struct{})}
	go s.loop()
	return s, nil
}

func (s *leaseSession) loop() {
	defer close(s.done)
	for {
		s.mu.Lock()
		delay := time.Until(s.current.LeaseExpiry()) / 3
		released := s.released
		s.mu.Unlock()
		if released {
			return
		}
		if delay <= 0 {
			s.cancel(&jobdb.LeaseRenewalError{Err: jobdb.ErrExecutionLeaseLost})
			return
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.heartbeat.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		s.mu.Lock()
		if s.released || s.heartbeat.Err() != nil {
			s.mu.Unlock()
			return
		}
		next, err := renewLease(s.heartbeat, s.current)
		if err == nil {
			s.current = next
		}
		// Cleanup cancellation is not a renewal failure.
		if err != nil && s.heartbeat.Err() == nil {
			s.cancel(err)
		}
		s.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (s *leaseSession) close() {
	s.stop()
	<-s.done
	s.cancel(context.Canceled)
}
func (s *leaseSession) isReleased() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.released }
func (s *leaseSession) check() error {
	if err := context.Cause(s.ctx); err != nil {
		return err
	}
	if s.released {
		return jobdb.ErrExecutionLeaseLost
	}
	return nil
}
func (s *leaseSession) LeaseToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token, ok := s.current.(interface{ LeaseToken() string }); ok {
		return token.LeaseToken()
	}
	return ""
}
func (s *leaseSession) LeaseWorkerID() string { return s.currentWorkerID() }
func (s *leaseSession) currentWorkerID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.LeaseWorkerID()
}
func (s *leaseSession) LeaseExpiry() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.LeaseExpiry()
}
func (s *leaseSession) LeaseSchemaHash() string {
	if schema, ok := s.ExecutionLease.(interface{ LeaseSchemaHash() string }); ok {
		return schema.LeaseSchemaHash()
	}
	return ""
}
func (s *leaseSession) KeepAlive(context.Context) error {
	return fmt.Errorf("runner owns lease renewal")
}
func (s *leaseSession) StopKeepAlive() { s.stop(); s.cancel(context.Canceled) }
func (s *leaseSession) operationContext(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	bounded, cancel := context.WithDeadline(ctx, s.current.LeaseExpiry())
	stop := context.AfterFunc(s.ctx, cancel)
	return bounded, func() { stop(); cancel() }
}
func (s *leaseSession) mutationError(err error) error {
	if errors.Is(err, jobdb.ErrExecutionLeaseLost) {
		s.cancel(err)
	}
	return err
}
func (s *leaseSession) Complete(ctx context.Context, req CompleteExecutionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	if err := s.current.Complete(ctx, req); err != nil {
		return s.mutationError(err)
	}
	s.released = true
	s.stop()
	return nil
}
func (s *leaseSession) Reschedule(ctx context.Context, req RescheduleExecutionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	if err := s.current.Reschedule(ctx, req); err != nil {
		return s.mutationError(err)
	}
	s.released = true
	s.stop()
	return nil
}
func (s *leaseSession) SubmitJob(ctx context.Context, req SubmitJobRequest) (JobHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return JobHandle{}, err
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	handle, err := s.current.SubmitJob(ctx, req)
	return handle, s.mutationError(err)
}
func (s *leaseSession) SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return JobHandle{}, err
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	handle, err := s.current.SubmitRestartJob(ctx, req)
	return handle, s.mutationError(err)
}

// Avoid accidental serialization of bearer material via diagnostic encoders.
func (s *leaseSession) String() string               { return "execution lease (capability redacted)" }
func (s *leaseSession) GoString() string             { return s.String() }
func (s *leaseSession) MarshalJSON() ([]byte, error) { return json.Marshal(s.String()) }
