package sqlite

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func sqliteResourceHarness() runtimetest.ResourceBackoffHarness {
	return runtimetest.ResourceBackoffHarness{Name: "sqlite", Durable: true, New: func(t testing.TB) runtimetest.ResourceBackoffFixture {
		ctx := celltest.Context(context.Background())
		cfg := Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "resource.db")}
		r, err := NewFromConfig(ctx, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Close(ctx) })
		var clock atomic.Int64
		clock.Store(time.Now().UnixNano())
		var random atomic.Uint64
		random.Store(math.Float64bits(.5))
		bind := func() {
			r.clock = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			r.resourceSample = func() float64 { return math.Float64frombits(random.Load()) }
		}
		bind()
		return runtimetest.ResourceBackoffFixture{Runtime: r, ClaimAppend: func(ctx context.Context, key jobdb.JobKey, at time.Time) error {
			s := appendScheduler{r}
			claim, err := s.ClaimAppend(ctx, runtimecore.ClaimAppendRequest{JobKey: key, WorkerID: runtimecore.AppendWorkerPrefix + "resource", Now: at, Duration: time.Minute})
			if err == nil {
				err = s.ReleaseAppend(ctx, claim)
			}
			return err
		}, SetTime: func(at time.Time) { clock.Store(at.UnixNano()) }, SetRandom: func(v float64) { random.Store(math.Float64bits(v)) }, Reopen: func() jobdb.ConsumerRuntime {
			require.NoError(t, r.Close(ctx))
			r, err = NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			bind()
			return r
		}, Fault: func(stage string) {
			r.resourceFault = func(at string) error {
				if stage != "" && at == stage {
					return fmt.Errorf("injected %s", at)
				}
				return nil
			}
		}, Barrier: func() (<-chan struct{}, func()) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			r.resourceFault = func(stage string) error {
				if stage == "validated" {
					once.Do(func() { close(entered); <-release })
				}
				return nil
			}
			return entered, func() { releaseOnce.Do(func() { close(release) }) }
		}}
	}}
}
func TestResourceBackoffConformance(t *testing.T) {
	runtimetest.RunResourceBackoffConformance(t, sqliteResourceHarness())
}

func TestResourceBackoffAvailabilityClockBoundaryB09b(t *testing.T) {
	f := sqliteResourceHarness().New(t)
	r := f.Runtime.(*Runtime)
	ctx := celltest.Context(context.Background())
	at := time.Now().UTC()
	f.SetTime(at)
	route := jobdb.Route{JobType: "work"}
	h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: route.JobType, Data: jobdb.NewTaskDataOrPanic("in")}})
	require.NoError(t, err)
	claim := func() jobdb.ExecutionLease {
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
		require.NoError(t, err)
		return lease
	}
	check := func(want jobdb.JobStatus, available bool) {
		t.Helper()
		base := jobdb.ListJobsRequest{TenantIds: []string{h.JobKey.TenantId}, JobKeys: []jobdb.JobKey{h.JobKey}}
		rows, err := r.ListJobs(ctx, base)
		require.NoError(t, err)
		require.Len(t, rows.Jobs, 1)
		require.Equal(t, want, rows.Jobs[0].Status)
		detail, err := r.GetJob(ctx, h.JobKey)
		require.NoError(t, err)
		require.Equal(t, want, detail.Status)
		base.AvailableRoutes = []jobdb.Route{route}
		rows, err = r.ListJobs(ctx, base)
		require.NoError(t, err)
		if available {
			require.Len(t, rows.Jobs, 1)
		} else {
			require.Empty(t, rows.Jobs)
		}
	}
	lease := claim()
	require.NotNil(t, lease)
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route}))
	check(jobdb.JobStatusReady, true)
	// A nil wait stores backend "now". A backward clock step legitimately
	// makes that instant future; discovery and acquisition must still agree.
	f.SetTime(at.Add(-time.Nanosecond))
	check(jobdb.JobStatusAwaitingFuture, false)
	require.Nil(t, claim())
	f.SetTime(at)
	check(jobdb.JobStatusReady, true)
	lease = claim()
	require.NotNil(t, lease)
	// A fixture explicitly testing ready routes can stay away from that clock
	// boundary, without weakening the independent not-before predicate.
	past := at.Add(-time.Hour)
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route, WaitUntil: &past}))
	f.SetTime(at.Add(-time.Nanosecond))
	check(jobdb.JobStatusReady, true)
	lease = claim()
	require.NotNil(t, lease)
}

func TestResourceBackoffSchemaDowngradeB10b(t *testing.T) {
	f := sqliteResourceHarness().New(t)
	ctx := celltest.Context(context.Background())
	r := f.Runtime.(*Runtime)
	h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic("in")}})
	require.NoError(t, err)
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, lease.ExecutionState().ResourceBackoff.Validate(true))
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, r, lease, "task/attempt:1", 1, jobdb.DefaultResourceBackoffPolicy())
	require.NoError(t, err)
	_, err = r.db.Exec(`PRAGMA user_version=13`)
	require.NoError(t, err)
	_, err = lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrResourceBackoffUnsupported)
	var id string
	require.NoError(t, r.db.QueryRow(`SELECT lease_id FROM jobdb_jobs WHERE job_id=?`, h.JobKey.JobId).Scan(&id))
	require.Equal(t, lease.LeaseID(), id)
	var receipts int
	require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM jobdb_resource_receipts`).Scan(&receipts))
	require.Zero(t, receipts)
}

func TestResourceBackoffCleanupB17(t *testing.T) {
	f := sqliteResourceHarness().New(t)
	r := f.Runtime.(*Runtime)
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	origin, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic("origin")}})
	require.NoError(t, err)
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: origin.JobKey}))
	// Origins and goal closures have independent permanent retention. Exercise
	// physical cleanup on an ordinary member, as the existing storage contract permits.
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "cleanup-member", GoalID: origin.GoalID, Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic("in")}})
	require.NoError(t, err)
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, r, lease, "cleanup/attempt:1", 1, jobdb.DefaultResourceBackoffPolicy())
	require.NoError(t, err)
	receipt, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
	again, err := r.ReplayResourceBackoffReceiptByID(ctx, h.JobKey, receipt.LeaseID, receipt.WorkerID, req)
	require.NoError(t, err)
	require.Equal(t, receipt, again)
	_, err = r.db.Exec(`DELETE FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, h.JobKey.TenantId, h.JobKey.JobId)
	require.NoError(t, err)
	for _, table := range []string{"jobdb_resource_sequences", "jobdb_resource_receipts"} {
		var n int
		require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM `+table).Scan(&n))
		require.Zero(t, n)
	}
	_, err = r.ReplayResourceBackoffReceiptByID(ctx, h.JobKey, receipt.LeaseID, receipt.WorkerID, req)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	_, err = r.RescheduleResourceBackoffByID(ctx, h.JobKey, receipt.LeaseID, receipt.WorkerID, req)
	require.Error(t, err)
}
