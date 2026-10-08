package runtimetest

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/crashconcern"
	"github.com/stretchr/testify/require"
)

// Exercise actual expiry through the public API. A status-only assertion misses
// schedulers that classify correctly but bypass that classification when leasing.
func runCrashConcern(t *testing.T, harnesses []Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := buildFixture(t, h)
			defer f.Shutdown(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			r := f.Runtime
			route := jobdb.Route{JobType: "crash-concern"}
			submit := func(id string) jobdb.JobKey {
				handle, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
					TenantId: f.WorkerTenantID, JobID: id, JobType: route.JobType, Data: NumberTaskData(1),
				}})
				require.NoError(t, err)
				return handle.JobKey
			}
			key := submit("flapping")
			poll := func() []jobdb.ExecutionLease {
				leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{
					TenantId: key.TenantId, WorkerID: "poller", Routes: []jobdb.Route{route}, LeaseDuration: 100 * time.Millisecond,
				})
				require.NoError(t, err)
				return leases
			}
			get := func() jobdb.ExecutionLease {
				lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
					JobKey: key, WorkerID: "targeted", Routes: []jobdb.Route{route}, LeaseDuration: 100 * time.Millisecond,
				})
				require.NoError(t, err)
				return lease
			}
			var previous string
			// Counters record expired-lease reclaims. At the threshold the live
			// lease remains ACTIVE; its expiry must prevent the next acquisition.
			for reclaims := 0; reclaims <= crashconcern.DefaultThreshold; reclaims++ {
				var lease jobdb.ExecutionLease
				if reclaims%2 == 0 {
					lease = get()
				} else {
					leases := poll()
					require.Len(t, leases, 1)
					lease = leases[0]
				}
				require.NotNil(t, lease, "isolated expiry must remain recoverable")
				require.NotEqual(t, previous, lease.LeaseID())
				previous = lease.LeaseID()
				info, err := r.GetJob(ctx, key)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusActive, info.Status)
				var expires time.Time
				if renewable, ok := lease.(jobdb.RenewableExecutionLease); ok {
					expires = renewable.LeaseExpiry()
				}
				// A remote capability may expire before the underlying lease
				// (Postgres rounds durations up to whole seconds).
				listed, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, JobKeys: []jobdb.JobKey{key}})
				require.NoError(t, err)
				require.Len(t, listed.Jobs, 1)
				if stored := listed.Jobs[0].LeaseExpiresAt; stored != nil && stored.After(expires) {
					expires = *stored
				}
				require.False(t, expires.IsZero(), "runtime must expose the lease expiry")
				timer := time.NewTimer(time.Until(expires) + 20*time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					t.Fatal(ctx.Err())
				}
				info, err = r.GetJob(ctx, key)
				require.NoError(t, err)
				want := jobdb.JobStatusReady
				if reclaims == crashconcern.DefaultThreshold {
					want = jobdb.JobStatusCrashConcern
				}
				require.Equal(t, want, info.Status)
			}
			for i := 0; i < 2; i++ {
				require.Nil(t, get())
				require.Empty(t, poll())
			}
			ready, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, Statuses: []jobdb.JobStatus{jobdb.JobStatusReady}})
			require.NoError(t, err)
			require.Empty(t, ready.Jobs)
			healthy := submit("healthy")
			leases := poll()
			require.Len(t, leases, 1, "a crash-concern job must not obstruct other ready work")
			require.Equal(t, healthy, leases[0].Job().JobKey)
		})
	}
}
