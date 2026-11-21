package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func availabilityRangeRequest(id string, at *time.Time) jobdb.SubmitJobRequest {
	return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: id, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), AvailableAt: at}}
}

func TestSubmissionAvailabilityRange(t *testing.T) {
	require.NoError(t, validateSubmissionAvailability(nil))
	for _, at := range []time.Time{submissionAvailabilityMin, submissionAvailabilityMax, time.Unix(0, 0), submissionAvailabilityMin.In(time.FixedZone("offset", 3600)), submissionAvailabilityMax.In(time.FixedZone("offset", -3600))} {
		require.NoError(t, validateSubmissionAvailability(&at))
		require.True(t, at.Equal(availabilityFromNS(at.UnixNano())))
	}
	for _, at := range []time.Time{submissionAvailabilityMin.Add(-time.Nanosecond), submissionAvailabilityMax.Add(time.Nanosecond), time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC), {}} {
		require.ErrorContains(t, validateSubmissionAvailability(&at), "availableAt")
	}
	require.True(t, timeFromNS(0).IsZero(), "unrelated timestamp sentinel must remain unchanged")
	require.True(t, time.Unix(0, 0).Equal(availabilityFromNS(0)))
}

// Consumer-scoped assertions avoid mistaking preserved producers/selected goals
// for a partially published consumer.
func assertAvailabilityPublication(t *testing.T, rt *Runtime, id string, count int, selected bool) {
	t.Helper()
	for _, table := range []string{"jobdb_jobs", "jobdb_goal_preparations", "jobdb_dependency_waits", "jobdb_dependency_targets", "jobdb_submission_receipts"} {
		var n int
		require.NoError(t, rt.db.QueryRow("SELECT count(*) FROM "+table+" WHERE tenant_id='tenant' AND job_id=?", id).Scan(&n))
		want := count
		if table == "jobdb_submission_receipts" && !selected {
			want = 0
		}
		require.Equal(t, want, n, table)
	}

	var goals int
	require.NoError(t, rt.db.QueryRow(`SELECT count(*) FROM jobdb_jobs WHERE tenant_id='tenant' AND goal_origin=1`).Scan(&goals))
	wantGoals := 1
	if selected {
		wantGoals++ // independent active dependency
	} else {
		wantGoals += count
	}
	require.Equal(t, wantGoals, goals, "fresh goals must publish atomically with their consumer")
	var legacy int
	require.NoError(t, rt.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('jobdb_goals','jobdb_goal_candidates')`).Scan(&legacy))
	require.Zero(t, legacy, "membership has no secondary storage")

}

// Keep selected-goal admission idle while retaining a genuinely pending, exact-job
// dependency. Cancellation releases the producer's slot without closing its goal.
func availabilityRangeProducer(t *testing.T, rt *Runtime, ctx context.Context, selected bool) (jobdb.JobHandle, jobdb.JobHandle) {
	t.Helper()
	p, err := celltest.Submit(rt, ctx, availabilityRangeRequest("producer", nil))
	require.NoError(t, err)
	if !selected {
		return p, p
	}
	require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: p.JobKey}))
	blocker, err := celltest.Submit(rt, ctx, availabilityRangeRequest("dependency", nil))
	require.NoError(t, err)
	return p, blocker
}

func TestSubmissionAvailabilityInvalidPublicationAndRetry(t *testing.T) {
	for _, selected := range []bool{false, true} {
		for _, bad := range []time.Time{submissionAvailabilityMin.Add(-time.Nanosecond), submissionAvailabilityMax.Add(time.Nanosecond), time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC), {}} {
			t.Run(fmt.Sprintf("selected=%v/%s", selected, bad.Format(time.RFC3339Nano)), func(t *testing.T) {
				ctx := runtimetest.PrimaryGoalContext(context.Background())
				cfg := Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")}
				rt, err := NewFromConfig(ctx, cfg)
				require.NoError(t, err)
				defer func() { require.NoError(t, rt.Close(ctx)) }()
				p, dependency := availabilityRangeProducer(t, rt, ctx, selected)
				req := availabilityRangeRequest("consumer", &bad)
				req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: dependency.JobKey.JobId}, Condition: jobdb.JobPrereqComplete}}
				if selected {
					req.Job.GoalID = p.GoalID
				}
				_, err = rt.SubmitJob(ctx, req)
				require.ErrorContains(t, err, "availableAt")
				assertAvailabilityPublication(t, rt, "consumer", 0, selected)
				prep, err := goalstate.SubmitPreparation(ctx, req, "")
				require.NoError(t, err)
				_, err = rt.PrepareJob(ctx, prep)
				require.ErrorContains(t, err, "availableAt")
				assertAvailabilityPublication(t, rt, "consumer", 0, selected)
				require.NoError(t, rt.Close(ctx))
				rt, err = NewFromConfig(ctx, cfg)
				require.NoError(t, err)
				assertAvailabilityPublication(t, rt, "consumer", 0, selected)
				// Corrected retries publish once with the same normal identities.
				epoch := time.Unix(0, 0).UTC()
				req.Job.AvailableAt = &epoch
				h, err := rt.SubmitJob(ctx, req)
				require.NoError(t, err)
				again, err := rt.SubmitJob(ctx, req)
				require.NoError(t, err)
				require.Equal(t, h, again)
				assertAvailabilityPublication(t, rt, "consumer", 1, selected)
				// Invalid retries must fail even against published rows/accepted receipts.
				req.Job.AvailableAt = &bad
				_, err = rt.SubmitJob(ctx, req)
				require.ErrorContains(t, err, "availableAt")
				invalidPrep, err := goalstate.SubmitPreparation(ctx, req, "")
				require.NoError(t, err)
				_, err = rt.PrepareJob(ctx, invalidPrep)
				require.ErrorContains(t, err, "availableAt")
				row, err := rt.loadJobRow(ctx, h.JobKey)
				require.NoError(t, err)
				require.Zero(t, row.availableAtNS)
				assertAvailabilityPublication(t, rt, "consumer", 1, selected)
				// Insertion recovery cannot turn a representation error into success.
				err = rt.ensureSubmittedJobRecord(ctx, h.JobKey, req.Job.JobType, row.metadata, nil, jobPayload{}, "", &bad, nil)
				require.ErrorContains(t, err, "availableAt")
			})
		}
	}
}

func TestSubmissionAvailabilityEquivalentRetriesAndReopen(t *testing.T) {
	for _, selected := range []bool{false, true} {
		for _, at := range []time.Time{submissionAvailabilityMin, time.Unix(0, 0).UTC(), submissionAvailabilityMax} {
			t.Run(fmt.Sprintf("selected=%v/%s", selected, at.Format(time.RFC3339Nano)), func(t *testing.T) {
				ctx := runtimetest.PrimaryGoalContext(context.Background())
				cfg := Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")}
				rt, err := NewFromConfig(ctx, cfg)
				require.NoError(t, err)
				defer func() { require.NoError(t, rt.Close(ctx)) }()
				p, dependency := availabilityRangeProducer(t, rt, ctx, selected)
				req := availabilityRangeRequest("consumer", &at)
				req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: dependency.JobKey.JobId}, Condition: jobdb.JobPrereqComplete}}
				if selected {
					req.Job.GoalID = p.GoalID
				}
				h, err := rt.SubmitJob(ctx, req)
				require.NoError(t, err)
				for _, reopen := range []bool{false, true} {
					if reopen {
						require.NoError(t, rt.Close(ctx))
						rt, err = NewFromConfig(ctx, cfg)
						require.NoError(t, err)
					}
					for _, zone := range []*time.Location{time.UTC, time.FixedZone("offset", 3600)} {
						equiv := at.In(zone)
						req.Job.AvailableAt = &equiv
						again, err := rt.SubmitJob(ctx, req)
						require.NoError(t, err)
						require.Equal(t, h, again)
						rows, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{h.JobKey}})
						require.NoError(t, err)
						require.Len(t, rows.Jobs, 1)
						require.True(t, at.Equal(rows.Jobs[0].AvailableAt))
						row, err := rt.loadJobRow(ctx, h.JobKey)
						require.NoError(t, err)
						require.Equal(t, at.UnixNano(), row.availableAtNS)
						status, err := statusFromRow(ctx, rt.db, row, at.Add(-time.Nanosecond))
						require.NoError(t, err)
						require.Equal(t, jobdb.JobStatusPendingJobs, status)
					}
					assertAvailabilityPublication(t, rt, "consumer", 1, selected)
				}
			})
		}
	}
}

func TestSubmissionAvailabilityStagedReconciliation(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(fmt.Sprintf("selected=%v", selected), func(t *testing.T) {
			ctx := runtimetest.PrimaryGoalContext(context.Background())
			rt, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")})
			require.NoError(t, err)
			defer rt.Close(ctx)
			p, dependency := availabilityRangeProducer(t, rt, ctx, selected)
			at := time.Unix(0, 0).UTC()
			req := availabilityRangeRequest("consumer", &at)
			req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: dependency.JobKey.JobId}, Condition: jobdb.JobPrereqComplete}}
			if selected {
				req.Job.GoalID = p.GoalID
			}
			// Keep actual chapter-zero history while removing executable publication to
			// simulate interruption after history, before the scheduler transaction.
			h, err := rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			// Fault-inject the pre-publication state, including a new-goal origin.
			// Production deletion must retain origins; restore that protection in
			// this same transaction before exercising recovery.
			var retainDDL string
			require.NoError(t, rt.db.QueryRow(`SELECT sql FROM sqlite_master WHERE name='jobdb_jobs_goal_facts_retained'`).Scan(&retainDDL))
			tx, err := rt.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = tx.Exec(`DROP TRIGGER jobdb_jobs_goal_facts_retained`)
			require.NoError(t, err)
			for _, table := range []string{"jobdb_jobs", "jobdb_dependency_waits", "jobdb_dependency_targets", "jobdb_submission_receipts"} {
				_, err = tx.Exec("DELETE FROM " + table + " WHERE tenant_id='tenant' AND job_id='consumer'")
				require.NoError(t, err)
			}
			_, err = tx.Exec(retainDDL)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			bad := time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
			req.Job.AvailableAt = &bad
			inputHash, err := computeInputHash(ctx, req.Job.Data)
			require.NoError(t, err)
			_, handled, err := rt.reconcileExistingSubmitJob(ctx, req, h.JobKey, inputHash, nil, nil, normalizeRunPolicy(req.Job.RunPolicy), "", "")
			require.True(t, handled)
			require.ErrorContains(t, err, "availableAt")
			var count int
			for _, table := range []string{"jobdb_jobs", "jobdb_dependency_waits", "jobdb_dependency_targets", "jobdb_submission_receipts"} {
				require.NoError(t, rt.db.QueryRow("SELECT count(*) FROM "+table+" WHERE tenant_id='tenant' AND job_id='consumer'").Scan(&count))
				require.Zero(t, count, table)
			}
			req.Job.AvailableAt = &at
			again, err := rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, h, again)
			assertAvailabilityPublication(t, rt, "consumer", 1, selected)
		})
	}
}

func TestSubmissionAvailabilityParentLeaseRejectsInvalid(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	defer rt.Close(ctx)
	p, err := celltest.Submit(rt, ctx, availabilityRangeRequest("producer", nil))
	require.NoError(t, err)
	l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: p.JobKey, WorkerID: "parent", Routes: []jobdb.Route{{JobType: "work"}}})
	require.NoError(t, err)
	require.NotNil(t, l)
	bad := time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
	req := availabilityRangeRequest("consumer", &bad)
	req.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqComplete}}
	_, err = l.SubmitJob(ctx, req)
	require.ErrorContains(t, err, "availableAt")
	assertAvailabilityPublication(t, rt, "consumer", 0, false)
}

func TestSubmissionAvailabilityEpochEligibility(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	defer rt.Close(ctx)
	epoch := time.Unix(0, 0).UTC()
	h, err := celltest.Submit(rt, ctx, availabilityRangeRequest("epoch", &epoch))
	require.NoError(t, err)
	row, err := rt.loadJobRow(ctx, h.JobKey)
	require.NoError(t, err)
	require.Zero(t, row.availableAtNS)
	for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		status, err := statusFromRow(ctx, rt.db, row, epoch.Add(offset))
		require.NoError(t, err)
		want := jobdb.JobStatusReady
		if offset < 0 {
			want = jobdb.JobStatusAwaitingFuture
		}
		require.Equal(t, want, status)
	}
	run, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: h.JobKey})
	require.NoError(t, err)
	require.True(t, epoch.Equal(run.Job.AvailableAt))
	require.True(t, timeFromNS(0).IsZero())
}

func TestSubmissionAvailabilityOldTimezoneReceipt(t *testing.T) {
	ctx := runtimetest.PrimaryGoalContext(context.Background())
	rt, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	defer rt.Close(ctx)
	p, err := celltest.Submit(rt, ctx, availabilityRangeRequest("producer", nil))
	require.NoError(t, err)
	require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: p.JobKey}))
	at := time.Unix(0, 123456789).In(time.FixedZone("offset", 3600))
	req := availabilityRangeRequest("consumer", &at)
	req.Job.GoalID = p.GoalID
	h, err := rt.SubmitJob(ctx, req)
	require.NoError(t, err)
	old, err := goalstate.SubmitPreparation(ctx, req, "")
	require.NoError(t, err)
	// Model a receipt and preparation written before SQLite normalized zones.
	for _, table := range []string{"jobdb_goal_preparations", "jobdb_submission_receipts"} {
		_, err = rt.db.Exec("UPDATE "+table+" SET descriptor=? WHERE tenant_id='tenant' AND job_id='consumer'", old.Descriptor)
		require.NoError(t, err)
	}
	utc := at.UTC()
	req.Job.AvailableAt = &utc
	again, err := rt.SubmitJob(ctx, req)
	require.NoError(t, err)
	require.Equal(t, h, again)
}
