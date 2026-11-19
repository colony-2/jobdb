package runtimetest

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
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
			ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), time.Minute)
			defer cancel()
			r := f.Runtime
			route := jobdb.Route{JobType: "crash-concern"}
			submit := func(id string) jobdb.JobKey {
				handle, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test",
					TenantId: f.WorkerTenantID, JobID: id, JobType: route.JobType, Data: NumberTaskData(1),
				}})
				require.NoError(t, err)
				return handle.JobKey
			}
			key := submit("flapping")
			// Acquisition and live-state assertions perform several storage/HTTP
			// operations. Leave enough time for them on loaded native workers;
			// a 100 ms lease could expire before the ACTIVE assertion. Still wait
			// for each authoritative expiry below, including the threshold lease.
			const leaseDuration = 2 * time.Second
			poll := func() []jobdb.ExecutionLease {
				leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{
					TenantId: key.TenantId, WorkerID: "poller", Routes: []jobdb.Route{route}, LeaseDuration: leaseDuration,
				})
				require.NoError(t, err)
				return leases
			}
			get := func() jobdb.ExecutionLease {
				lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
					JobKey: key, WorkerID: "targeted", Routes: []jobdb.Route{route}, LeaseDuration: leaseDuration,
				})
				require.NoError(t, err)
				return lease
			}
			var previous string
			var expired jobdb.ExecutionLease
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
				expired = lease
				info, err := r.GetJob(ctx, key)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusActive, info.Status)
				var expires time.Time
				if renewable, ok := lease.(jobdb.RenewableExecutionLease); ok {
					expires = renewable.LeaseExpiry()
				}
				// A remote capability may expire before the underlying lease
				// (Postgres rounds durations up to whole seconds). Prefer the
				// authoritative expiry when the listing exposes it.
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
				// Some runtimes omit LeaseExpiresAt from listings. Capability
				// expiry alone does not prove backend expiry, so wait for the
				// live projection to end, bounded by the fixture context. Any
				// unexpected non-ACTIVE state still fails the assertion below.
				for {
					info, err = r.GetJob(ctx, key)
					require.NoError(t, err)
					if info.Status != jobdb.JobStatusActive {
						break
					}
					select {
					case <-time.After(20 * time.Millisecond):
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				want := jobdb.JobStatusReady
				if reclaims == crashconcern.DefaultThreshold {
					want = jobdb.JobStatusCrashConcern
				}
				require.Equal(t, want, info.Status)
				available, err := r.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{key.TenantId}, JobKeys: []jobdb.JobKey{key}, AvailableRoutes: []jobdb.Route{route},
				})
				require.NoError(t, err)
				if want == jobdb.JobStatusReady {
					require.Len(t, available.Jobs, 1, "expired lease should become discoverable")
				} else {
					require.Empty(t, available.Jobs, "crash concern is not leaseable")
				}
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
			require.ErrorIs(t, r.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: healthy}), jobdb.ErrConflict)
			require.ErrorIs(t, r.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{}), jobdb.ErrInvalidCrashConcernRequest)
			require.ErrorIs(t, r.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: jobdb.JobKey{TenantId: key.TenantId + "-other", JobId: key.JobId}}), jobdb.ErrJobNotFound)
			chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			clear := jobdb.ClearCrashConcernRequest{JobKey: key, WorkerID: "operator", Reason: "worker repaired"}
			require.NoError(t, r.ClearCrashConcern(ctx, clear))
			require.NoError(t, r.ClearCrashConcern(ctx, clear), "repeating the clear before acquisition is harmless")
			info, err := r.GetJob(ctx, key)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusReady, info.Status)
			afterChapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, chapters, afterChapters, "clear must preserve replay history")
			require.ErrorIs(t, expired.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
			recovered := get()
			require.NotNil(t, recovered)
			require.Equal(t, key, recovered.Job().JobKey)
			require.NotEqual(t, expired.LeaseID(), recovered.LeaseID())
			require.ErrorIs(t, r.ClearCrashConcern(ctx, clear), jobdb.ErrConflict)
			require.NoError(t, recovered.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: route.JobType, CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}}))
			require.ErrorIs(t, r.ClearCrashConcern(ctx, clear), jobdb.ErrConflict)
			cancelled := submit("cancelled")
			require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: cancelled}))
			require.ErrorIs(t, r.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: cancelled}), jobdb.ErrConflict)
		})
	}
}
