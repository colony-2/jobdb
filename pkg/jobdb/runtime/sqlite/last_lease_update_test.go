package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leasetest"
	"github.com/stretchr/testify/require"
)

func TestLastLeaseUpdateLifecycle(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db"), Projects: celltest.UnrestrictedProjects{}}
	runtime, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer runtime.Close(ctx)
	leasetest.Run(t, runtime, func(clock func() time.Time) { runtime.clock = clock }, true)
	request := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{"work"}}
	before, err := runtime.ListJobs(ctx, request)
	require.NoError(t, err)
	require.NoError(t, runtime.Close(ctx))
	reopened, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer reopened.Close(ctx)
	reopened.clock = runtime.clock
	after, err := reopened.ListJobs(ctx, request)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestLastLeaseUpdateFailedRenewalRollsBack(t *testing.T) {
	ctx := celltest.Context(context.Background())
	runtime, _, key := leaseEventFixture(t)
	now := time.Now().UTC().Add(time.Second)
	runtime.clock = func() time.Time { return now }
	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Hour})
	require.NoError(t, err)
	require.NotNil(t, lease)
	_, err = runtime.db.Exec(`CREATE TRIGGER reject_renewal BEFORE INSERT ON jobdb_lease_events BEGIN SELECT RAISE(ABORT,'injected event failure'); END`)
	require.NoError(t, err)
	before := compatibilitySnapshot(t, runtime.db)
	now = now.Add(time.Second)
	_, err = runtime.RenewExecutionLeaseByID(ctx, key, lease.LeaseID(), "worker", time.Minute)
	require.ErrorContains(t, err, "injected event failure")
	require.Equal(t, before, compatibilitySnapshot(t, runtime.db))
}
