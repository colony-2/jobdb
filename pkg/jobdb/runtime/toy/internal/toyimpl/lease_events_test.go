package toyimpl

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestLeaseEventConcurrentRenewalAndRead(t *testing.T) {
	r := New()
	ctx := celltest.Context(context.Background())
	h, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, e)
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				require.NoError(t, l.KeepAlive(ctx))
				page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
				require.NoError(t, e)
				if len(page.Events) > 0 {
					page.Events[0].Cell = "mutated"
					if page.Events[0].PreviousLeaseExpiresAt != nil {
						*page.Events[0].PreviousLeaseExpiresAt = time.Time{}
					}
				}
			}
		}()
	}
	wg.Wait()
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 200})
	require.NoError(t, e)
	require.Len(t, page.Events, 161)
	r.engine.mu.Lock()
	state := r.engine.cells["tenant"]
	for id, c := range state.Cells {
		c.State = jobdb.CellRetired
		state.Cells[id] = c
	}
	r.engine.mu.Unlock()
	retired, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", CellNames: []string{"test"}, PageSize: 200})
	require.NoError(t, e)
	require.Equal(t, page, retired)
	require.Equal(t, "test", page.Events[0].Cell)
	require.False(t, page.Events[0].PreviousLeaseExpiresAt.IsZero())
}

func TestLeaseEventCancellationDeletionRaces(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		for iteration := 0; iteration < 8; iteration++ {
			r := New()
			ctx := celltest.Context(context.Background())
			h, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, e)
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			var lease jobdb.ExecutionLease
			go func() {
				defer wg.Done()
				<-start
				var e error
				lease, e = r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "racer", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
				require.NoError(t, e)
			}()
			go func() {
				defer wg.Done()
				<-start
				if deletion {
					_, e := r.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{h.JobKey.JobId}})
					require.NoError(t, e)
				} else {
					require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
				}
			}()
			close(start)
			wg.Wait()
			page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", IncludeHidden: true})
			require.NoError(t, e)
			if lease != nil {
				require.Len(t, page.Events, 1)
				require.Equal(t, lease.LeaseID(), page.Events[0].LeaseID)
				require.Error(t, lease.KeepAlive(ctx))
			} else {
				require.Empty(t, page.Events)
			}
		}
	}
}
