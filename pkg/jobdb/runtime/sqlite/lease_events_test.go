package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func leaseEventFixture(t *testing.T) (*Runtime, Config, jobdb.JobKey) {
	t.Helper()
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "events.db")}
	r, e := NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	t.Cleanup(func() { r.Close(ctx) })
	h, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, e)
	return r, cfg, h.JobKey
}
func TestLeaseEventInsertFailureRollsBack(t *testing.T) {
	r, _, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	_, e := r.db.Exec(`CREATE TRIGGER reject_event BEFORE INSERT ON jobdb_lease_events BEGIN SELECT RAISE(ABORT,'injected event failure'); END`)
	require.NoError(t, e)
	_, e = r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.ErrorContains(t, e, "injected event failure")
	row, e := r.loadJobRow(ctx, key)
	require.NoError(t, e)
	require.False(t, row.leaseID.Valid)
	_, e = r.db.Exec(`DROP TRIGGER reject_event`)
	require.NoError(t, e)
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	before, e := r.loadJobRow(ctx, key)
	require.NoError(t, e)
	_, e = r.db.Exec(`CREATE TRIGGER reject_event BEFORE INSERT ON jobdb_lease_events BEGIN SELECT RAISE(ABORT,'injected event failure'); END`)
	require.NoError(t, e)
	require.ErrorContains(t, l.KeepAlive(ctx), "injected event failure")
	_, e = r.RenewExecutionLeaseByID(ctx, key, l.LeaseID(), "worker", time.Second)
	require.ErrorContains(t, e, "injected event failure")
	after, e := r.loadJobRow(ctx, key)
	require.NoError(t, e)
	require.Equal(t, before, after)
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Len(t, page.Events, 1)
}
func TestLeaseEventUpgradeReopen(t *testing.T) {
	r, cfg, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	// Recreate the exact immediate-prior format by removing only the additive table.
	_, e = r.db.Exec(`DROP TABLE jobdb_lease_events; PRAGMA user_version=11`)
	require.NoError(t, e)
	require.NoError(t, r.Close(ctx))
	reopened, e := NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	defer reopened.Close(ctx)
	page, e := reopened.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Empty(t, page.Events)
	for i := 0; i < 3; i++ {
		require.NoError(t, reopened.KeepAliveLeaseByID(ctx, key, l.LeaseID(), "worker", time.Minute))
	}
	q := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 1}
	page, e = reopened.ListLeaseEvents(ctx, q)
	require.NoError(t, e)
	require.Equal(t, jobdb.LeaseEventRenewed, page.Events[0].EventType)
	require.NotEmpty(t, page.NextPageToken)
	q.PageToken = page.NextPageToken
	require.NoError(t, migrate(ctx, reopened.db))
	require.NoError(t, reopened.Close(ctx))
	reopened, e = NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	defer reopened.Close(ctx)
	page, e = reopened.ListLeaseEvents(ctx, q)
	require.NoError(t, e)
	require.Len(t, page.Events, 1)
	require.NotEmpty(t, page.NextPageToken)
	// No cascade even if the retained job fact is removed.
	_, e = reopened.db.Exec(`DELETE FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, key.TenantId, key.JobId)
	require.NoError(t, e)
	page, e = reopened.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Len(t, page.Events, 3)
}
func TestLeaseEventQueriesAndPlans(t *testing.T) {
	r, cfg, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Hour})
	require.NoError(t, e)
	for i := 0; i < 205; i++ {
		require.NoError(t, l.KeepAlive(ctx))
	}
	for _, tenant := range []string{"tenant", "other"} {
		for i := 0; i < 100; i++ {
			_, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: fmt.Sprint("job-", i), Cell: fmt.Sprint("cell-", i), CellRepo: fmt.Sprint("example.test/cell-", i), JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, e)
		}
	}
	ls, e := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "many", Routes: []jobdb.Route{{JobType: "job"}}, Limit: 100, LeaseDuration: time.Hour})
	require.NoError(t, e)
	require.Len(t, ls, 100)
	_, e = r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "other", WorkerID: "other-worker", Routes: []jobdb.Route{{JobType: "job"}}, Limit: 100, LeaseDuration: time.Hour})
	require.NoError(t, e)
	counted := &listCountingConnector{dsn: cfg.DBPath, drv: r.db.Driver()}
	db := sql.OpenDB(counted)
	defer db.Close()
	original := r.db
	r.db = db
	defer func() { r.db = original }()
	server := httptest.NewServer(celltest.Handler(remote.NewServer(r)))
	defer server.Close()
	client, e := remote.New(server.URL, server.Client())
	require.NoError(t, e)
	for _, size := range []int{1, 100, 200} {
		for _, cardinality := range []int{1, 100} {
			for _, rt := range []jobdb.WorkflowRuntime{r, client} {
				ids := []string{"job"}
				for i := 1; i < cardinality; i++ {
					ids = append(ids, fmt.Sprint("job-", i))
				}
				q := jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: ids, PageSize: size}
				counted.queries.Store(0)
				page, e := jobdb.ListLeaseEvents(ctx, rt, q)
				require.NoError(t, e)
				require.NotEmpty(t, page.Events)
				require.EqualValues(t, 2, counted.queries.Load())
				t.Logf("size=%d cardinality=%d runtime=%T reads=%d rows=%d", size, cardinality, rt, counted.queries.Load(), len(page.Events))
			}
		}
	}
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	scope, e := loadLeaseEventScope(ctx, db, "tenant")
	require.NoError(t, e)
	ids, e := scope.LeaseEventScope(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	for _, q := range []jobdb.ListLeaseEventsRequest{{TenantID: "tenant", PageSize: 100}, {TenantID: "tenant", JobIDs: []string{"job"}, PageSize: 1}, {TenantID: "tenant", GoalIDs: []string{page.Events[0].GoalID}, PageSize: 200}} {
		query, args := leaseEventQuery(q, ids, nil)
		rows, e := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
		require.NoError(t, e)
		details := []string{}
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			details = append(details, detail)
		}
		require.NoError(t, rows.Close())
		t.Logf("plan %+v: %s", q, strings.Join(details, "; "))
		require.Contains(t, strings.Join(details, " "), "INDEX")
		leaseEventExamined.Store(0)
		probe := strings.Replace(query, "WHERE e.tenant_id=?", "WHERE jobdb_test_lease_event_examined(e.event_id) AND e.tenant_id=?", 1)
		measured, e := db.QueryContext(ctx, probe, args...)
		require.NoError(t, e)
		returned := 0
		for measured.Next() {
			returned++
		}
		require.NoError(t, measured.Err())
		require.NoError(t, measured.Close())
		t.Logf("instrumented predicate rows examined=%d returned with lookahead=%d", leaseEventExamined.Load(), returned)

	}
}
func TestLeaseEventInternalClaimAndShortRenewal(t *testing.T) {
	r, _, req := waitingTaskFixture(t)
	ctx := celltest.Context(context.Background())
	require.NoError(t, r.CompleteTaskIfWaiting(ctx, req))
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Len(t, page.Events, 2)
	require.Equal(t, jobdb.LeaseAcquisitionTaskCompletion, page.Events[0].AcquisitionSource)
	require.Equal(t, jobdb.Route{JobType: "job"}, page.Events[0].Route)
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: req.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Hour})
	require.NoError(t, e)
	require.NotNil(t, l)
	next, e := r.RenewExecutionLeaseByID(ctx, req.JobKey, l.LeaseID(), "worker", time.Minute)
	require.NoError(t, e)
	require.True(t, next.LeaseExpiry().Before(l.(jobdb.RenewableExecutionLease).LeaseExpiry()))
	page, e = r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Equal(t, next.LeaseExpiry(), page.Events[0].LeaseExpiresAt)
	_, e = r.RenewExecutionLeaseByID(ctx, req.JobKey, l.LeaseID(), "wrong", time.Minute)
	require.ErrorIs(t, e, jobdb.ErrExecutionLeaseLost)
}
func TestLeaseEventConcurrentListingRenewal(t *testing.T) {
	r, _, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				require.NoError(t, l.KeepAlive(ctx))
				_, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
				require.NoError(t, e)
			}
		}()
	}
	wg.Wait()
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Len(t, page.Events, 81)
}

func TestLeaseEventTiesVisibilityAndReclaim(t *testing.T) {
	r, _, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "first", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	_, e = r.db.Exec(`UPDATE jobdb_jobs SET lease_expires_at_ns=0 WHERE tenant_id='tenant' AND job_id='job'`)
	require.NoError(t, e)
	next, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "replacement", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	require.NotNil(t, next)
	for i := 0; i < 12; i++ {
		require.NoError(t, next.KeepAlive(ctx))
	}
	tied := time.Now().UTC().UnixNano()
	_, e = r.db.Exec(`UPDATE jobdb_lease_events SET occurred_at_ns=?`, tied)
	require.NoError(t, e)
	q := jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 1}
	ids := []string{}
	for {
		page, e := r.ListLeaseEvents(ctx, q)
		require.NoError(t, e)
		for _, v := range page.Events {
			ids = append(ids, v.EventID)
			if v.WorkerID == "replacement" && v.EventType == jobdb.LeaseEventAcquired {
				require.Equal(t, l.LeaseID(), v.PreviousLeaseID)
			}
		}
		if page.NextPageToken == "" {
			break
		}
		q.PageToken = page.NextPageToken
	}
	require.Equal(t, []string{"14", "13", "12", "11", "10", "9", "8", "7", "6", "5", "4", "3", "2", "1"}, ids)
	// Seed the retained catalog state after operational retirement; retirement's
	// busy-job policy is independently covered by cell conformance.
	_, e = r.db.Exec(`UPDATE jobdb_cells SET state='RETIRED' WHERE tenant_id='tenant'`)
	require.NoError(t, e)
	retired, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", CellNames: []string{"test"}})
	require.NoError(t, e)
	require.Len(t, retired.Events, 14)
	_, e = r.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{key.JobId}})
	require.NoError(t, e)
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Empty(t, page.Events)
	page, e = r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", IncludeHidden: true})
	require.NoError(t, e)
	require.Len(t, page.Events, 14)
	require.Error(t, next.KeepAlive(ctx))
	page2, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", IncludeHidden: true})
	require.NoError(t, e)
	require.Equal(t, page, page2)
}

func TestLeaseEventCancellationDeletionRaces(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		for iteration := 0; iteration < 8; iteration++ {
			t.Run(fmt.Sprint(deletion, "-", iteration), func(t *testing.T) {
				r, _, key := leaseEventFixture(t)
				ctx := celltest.Context(context.Background())
				start := make(chan struct{})
				var wg sync.WaitGroup
				wg.Add(2)
				var lease jobdb.ExecutionLease
				go func() {
					defer wg.Done()
					<-start
					var e error
					lease, e = r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "racer", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
					require.NoError(t, e)
				}()
				go func() {
					defer wg.Done()
					<-start
					if deletion {
						_, e := r.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{key.JobId}})
						require.NoError(t, e)
					} else {
						require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key}))
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
			})
		}
	}
}
func TestLeaseEventRenewalExpiryRace(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			r, _, key := leaseEventFixture(t)
			ctx := celltest.Context(context.Background())
			l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "old", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
			require.NoError(t, e)
			deadline := time.Now().UTC().Add(3 * time.Millisecond)
			_, e = r.db.Exec(`UPDATE jobdb_jobs SET lease_expires_at_ns=? WHERE tenant_id='tenant' AND job_id='job'`, deadline.UnixNano())
			require.NoError(t, e)
			var wg sync.WaitGroup
			wg.Add(2)
			var renewed time.Time
			var renewErr error
			var replacement jobdb.ExecutionLease
			go func() {
				defer wg.Done()
				time.Sleep(time.Until(deadline) - time.Millisecond)
				renewed, renewErr = r.KeepAliveLeaseByIDWithExpiry(ctx, key, l.LeaseID(), "old", time.Minute)
			}()
			go func() {
				defer wg.Done()
				time.Sleep(time.Until(deadline))
				var e error
				replacement, e = r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "new", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
				require.NoError(t, e)
			}()
			wg.Wait()
			page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
			require.NoError(t, e)
			if renewErr == nil {
				require.Nil(t, replacement)
				require.Len(t, page.Events, 2)
				require.Equal(t, jobdb.LeaseEventRenewed, page.Events[0].EventType)
				require.Equal(t, renewed, page.Events[0].LeaseExpiresAt)
			} else {
				require.ErrorIs(t, renewErr, jobdb.ErrExecutionLeaseLost)
				require.NotNil(t, replacement)
				require.Len(t, page.Events, 2)
				require.Equal(t, l.LeaseID(), page.Events[0].PreviousLeaseID)
			}
		})
	}
}

func TestLeaseEventRenewalSamplesAfterWriterWait(t *testing.T) {
	r, _, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	tx, e := r.db.BeginTx(ctx, nil)
	require.NoError(t, e)
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() { close(started); done <- r.KeepAliveLeaseByID(ctx, key, l.LeaseID(), "worker", time.Minute) }()
	<-started
	time.Sleep(15 * time.Millisecond)
	released := time.Now().UTC()
	require.NoError(t, tx.Commit())
	require.NoError(t, <-done)
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	v := page.Events[0]
	require.False(t, v.OccurredAt.Before(released))
	require.Equal(t, v.OccurredAt.Add(time.Minute), v.LeaseExpiresAt)
}

func TestLeaseEventCrashConcernPreservesHistory(t *testing.T) {
	r, _, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, e)
	_, e = r.db.Exec(`UPDATE jobdb_jobs SET lease_expires_at_ns=0,consecutive_expirations=5 WHERE tenant_id='tenant' AND job_id='job'`)
	require.NoError(t, e)
	before, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	blocked, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "blocked", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, e)
	require.Nil(t, blocked)
	require.NoError(t, r.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: key}))
	after, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Equal(t, before, after)
	next, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "recovered", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, e)
	require.NotNil(t, next)
	after, e = r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Len(t, after.Events, 2)
	require.Equal(t, l.LeaseID(), after.Events[0].PreviousLeaseID)
}

func TestLeaseEventWideTimeBounds(t *testing.T) {
	r, _, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	_, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, e)
	past, future := time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", OccurredAfter: &past, OccurredBefore: &future})
	require.NoError(t, e)
	require.Len(t, page.Events, 1)
	page, e = r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", OccurredAfter: &future})
	require.NoError(t, e)
	require.Empty(t, page.Events)
	page, e = r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", OccurredBefore: &past})
	require.NoError(t, e)
	require.Empty(t, page.Events)
}
