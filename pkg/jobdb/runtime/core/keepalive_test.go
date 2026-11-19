package runtimecore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type keepaliveBarrierScheduler struct {
	Scheduler
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	exited    chan struct{}
}

func (s *keepaliveBarrierScheduler) KeepAliveLease(ctx context.Context, m LeaseMutation) (LeaseSnapshot, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.cancelled)
	<-s.release
	close(s.exited)
	return LeaseSnapshot{}, ctx.Err()
}
func TestLegacyKeepAliveStopJoinsOwnedRenewal(t *testing.T) {
	s := &keepaliveBarrierScheduler{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
	runtime := &Runtime{scheduler: s, now: time.Now}
	lease := &executionLease{runtime: runtime, expiresAt: time.Now().Add(time.Hour), snapshot: LeaseSnapshot{Identity: LeaseIdentity{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}}, Duration: time.Millisecond}}
	require.NoError(t, lease.KeepAlive(context.Background()))
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("renewal did not enter storage")
	}
	stopped := make(chan struct{})
	go func() { lease.StopKeepAlive(); close(stopped) }()
	select {
	case <-s.cancelled:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel storage")
	}
	select {
	case <-stopped:
		t.Fatal("stop returned before owned storage call exited")
	default:
	}
	close(s.release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not join renewal")
	}
	select {
	case <-s.exited:
	default:
		t.Fatal("renewal did not exit")
	}
}
func TestLegacyKeepAliveConcurrentStartStop(t *testing.T) {
	l := &executionLease{runtime: &Runtime{now: time.Now}, expiresAt: time.Now().Add(time.Hour), snapshot: LeaseSnapshot{Duration: time.Hour}}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				require.NoError(t, l.KeepAlive(context.Background()))
				l.StopKeepAlive()
				_ = l.LeaseExpiry()
			}
		}()
	}
	wg.Wait()
	l.StopKeepAlive()
}
