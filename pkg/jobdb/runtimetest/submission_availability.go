package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

// RunSubmissionAvailabilityConformance covers admission and stored projections
// with fresh consumers for each acquisition method and submission authority.
func RunSubmissionAvailabilityConformance(t *testing.T, harnesses ...Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			for _, parent := range []bool{false, true} {
				t.Run(fmt.Sprintf("parent=%v", parent), func(t *testing.T) {
					f := h.New(t)
					if f.Cleanup != nil {
						t.Cleanup(func() { require.NoError(t, f.Cleanup(context.Background())) })
					}
					ctx := celltest.Context(context.Background())
					parentID := ""
					submit := func(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
						return celltest.Submit(f.Runtime, ctx, req)
					}
					if parent {
						owner, err := submit(ctx, availabilityRequest("owner", "owner", nil))
						require.NoError(t, err)
						lease, err := f.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: owner.JobKey, WorkerID: "owner", Routes: []jobdb.Route{{JobType: "owner"}}, LeaseDuration: time.Minute})
						require.NoError(t, err)
						require.NotNil(t, lease)
						submit = lease.SubmitJob
						parentID = owner.JobKey.JobId
					}
					RunSubmissionAvailabilityWithSubmit(t, f.Runtime, submit, parentID)
				})
			}
		})
	}
}

func availabilityRequest(id, typ string, at *time.Time) jobdb.SubmitJobRequest {
	return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: id, JobType: typ, Data: jobdb.NewTaskDataOrPanic(nil), AvailableAt: at}}
}

// RunSubmissionAvailabilityWithSubmit also exercises restricted delegated
// submission. Observer authority is separate from the supplied submit authority.
func RunSubmissionAvailabilityWithSubmit(t *testing.T, rt jobdb.WorkflowRuntime, submit func(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error), parentID string) {
	for _, kind := range []string{"completion", "exact-success", "goal-success", "all-of"} {
		for _, timeFirst := range []bool{false, true} {
			for _, poll := range []bool{false, true} {
				name := fmt.Sprintf("%s/time-first=%v/poll=%v", kind, timeFirst, poll)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 15*time.Second)
					defer cancel()
					id := fmt.Sprintf("%s-%v-%v", kind, timeFirst, poll)
					var producers []jobdb.JobHandle
					var deps []jobdb.Dependency
					kinds := []string{kind}
					if kind == "all-of" {
						kinds = []string{"completion", "exact-success", "goal-success"}
					}
					for i, k := range kinds {
						p, err := celltest.Submit(rt, ctx, availabilityRequest(fmt.Sprintf("producer-%s-%d", id, i), "producer", nil))
						require.NoError(t, err)
						producers = append(producers, p)
						d := jobdb.Dependency{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}
						if k == "completion" {
							d.Condition = jobdb.JobPrereqComplete
						}
						if k == "goal-success" {
							d.Target = jobdb.DependencyTarget{GoalID: p.GoalID}
						}
						deps = append(deps, d)
					}
					available := time.Now().UTC().Add(3 * time.Second)
					// Force non-microsecond precision so HTTP/list round trips are exercised.
					available = available.Truncate(time.Microsecond).Add(123 * time.Nanosecond)
					req := availabilityRequest("consumer-"+id, "child", &available)
					req.Job.Dependencies = deps
					req.Job.Metadata, _ = json.Marshal(map[string]string{"consumer": id})
					h, err := submit(ctx, req)
					require.NoError(t, err)
					if parentID != "" {
						rows, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{h.JobKey}})
						require.NoError(t, err)
						require.Len(t, rows.Jobs, 1)
						require.Equal(t, parentID, rows.Jobs[0].ParentJobID)
					}
					read := func() {
						rows, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{h.JobKey}})
						require.NoError(t, err)
						require.Len(t, rows.Jobs, 1)
						require.True(t, available.Equal(rows.Jobs[0].AvailableAt), "stored availability: got %s want %s", rows.Jobs[0].AvailableAt, available)
						run, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: h.JobKey})
						require.NoError(t, err)
						require.True(t, available.Equal(run.Job.AvailableAt))
					}
					acquire := func() jobdb.ExecutionLease {
						if poll {
							leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: id, Routes: []jobdb.Route{{JobType: "child"}}, Limit: 100, LeaseDuration: time.Minute, MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"consumer"}, Values: []any{id}}}})
							require.NoError(t, err)
							if len(leases) == 0 {
								return nil
							}
							require.Len(t, leases, 1)
							require.Equal(t, h.JobKey, leases[0].Job().JobKey)
							return leases[0]
						}
						l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: id, Routes: []jobdb.Route{{JobType: "child"}}, LeaseDuration: time.Minute})
						require.NoError(t, err)
						return l
					}
					read()
					deny := func() {
						for range 3 {
							require.Nil(t, acquire())
							time.Sleep(5 * time.Millisecond)
						}
					}
					finish := func(p jobdb.JobHandle) {
						l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: p.JobKey, WorkerID: id, Routes: []jobdb.Route{{JobType: "producer"}}})
						require.NoError(t, err)
						require.NotNil(t, l)
						completeLeaseForTest(t, ctx, l, 1)
					}
					deny()
					if timeFirst {
						for time.Now().Before(available) {
							time.Sleep(10 * time.Millisecond)
						}
						deny()
					}
					for i, p := range producers {
						finish(p)
						if i < len(producers)-1 {
							deny()
						}
					}
					read()
					var l jobdb.ExecutionLease
					if !timeFirst {
						require.True(t, time.Now().Before(available), "fixture missed future observation")
						for range 3 {
							if !time.Now().Before(available) {
								break
							}
							observed := acquire()
							if observed != nil {
								// A request can span the boundary under load.
								// Such a lease is legal only after availability.
								require.False(t, time.Now().Before(available), "lease before availability")
								l = observed
								break
							}
							time.Sleep(5 * time.Millisecond)
						}
					}
					if l == nil {
						require.Eventually(t, func() bool { l = acquire(); return l != nil }, 5*time.Second, 10*time.Millisecond)
					}
					require.False(t, time.Now().Before(available))
					require.Equal(t, h.JobKey, l.Job().JobKey)
				})
			}
		}
	}
	for _, when := range []string{"omitted", "past", "equal"} {
		for _, poll := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/poll=%v", when, poll), func(t *testing.T) {
				t.Parallel()
				ctx := celltest.Context(context.Background())
				id := fmt.Sprintf("immediate-%s-%v", when, poll)
				p, err := celltest.Submit(rt, ctx, availabilityRequest("producer-"+id, "producer", nil))
				require.NoError(t, err)
				l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: p.JobKey, WorkerID: id, Routes: []jobdb.Route{{JobType: "producer"}}})
				require.NoError(t, err)
				require.NotNil(t, l)
				completeLeaseForTest(t, ctx, l, 1)
				now := time.Now().UTC()
				var at *time.Time
				if when == "past" {
					past := now.Add(-time.Hour)
					at = &past
				}
				if when == "equal" {
					at = &now
				}
				req := availabilityRequest(id, "child", at)
				req.Job.Metadata, _ = json.Marshal(map[string]string{"consumer": id})
				req.Job.Dependencies = []jobdb.Dependency{
					{Target: jobdb.DependencyTarget{JobID: p.JobKey.JobId}, Condition: jobdb.JobPrereqComplete},
					{Target: jobdb.DependencyTarget{GoalID: p.GoalID}, Condition: jobdb.JobPrereqSuccess},
				}
				h, err := submit(ctx, req)
				require.NoError(t, err)
				if poll {
					leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: id, Routes: []jobdb.Route{{JobType: "child"}}, Limit: 1, MetadataEquals: []jobdb.MetadataPredicate{{Path: []string{"consumer"}, Values: []any{id}}}})
					require.NoError(t, err)
					require.Len(t, leases, 1)
					require.Equal(t, h.JobKey, leases[0].Job().JobKey)
				} else {
					l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: id, Routes: []jobdb.Route{{JobType: "child"}}})
					require.NoError(t, err)
					require.NotNil(t, l)
				}
			})
		}
	}

}
