package runtimetest

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

// RunAvailableRoutesConformance checks read-only discovery against acquisition.
// Backend adapters should run this suite in addition to implementing the filter.
func RunAvailableRoutesConformance(t *testing.T, harnesses ...Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			for _, mode := range []string{"job", "task", "future-alternate", "due-job-alternate", "due-task-alternate", "delayed", "elapsed-delay", "dependency", "elapsed-dependency-deadline", "active", "cancelled", "completed"} {
				t.Run(mode, func(t *testing.T) {
					f := buildFixture(t, h)
					defer f.Shutdown(t)
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					r := f.Runtime
					jobRoute := jobdb.Route{JobType: "recipe:食谱"}
					taskRoute := jobdb.Route{JobType: jobRoute.JobType, TaskType: "input:collect_user_input"}
					altRoute := jobdb.Route{JobType: jobRoute.JobType, TaskType: "fallback:input"}
					submit := func(id string) jobdb.JobKey {
						handle, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: f.WorkerTenantID, JobID: id, JobType: jobRoute.JobType, Data: NumberTaskData(1), Metadata: []byte(`{"team":"blue"}`)}})
						require.NoError(t, err)
						return handle.JobKey
					}
					key := submit("candidate")
					lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "setup", Routes: []jobdb.Route{jobRoute}})
					require.NoError(t, err)
					require.NotNil(t, lease)
					stored, effective := jobRoute, jobRoute
					want := true
					reschedule := jobdb.RescheduleExecutionRequest{NextRoute: jobRoute}
					switch mode {
					case "task", "future-alternate", "due-job-alternate", "due-task-alternate":
						stored, effective = taskRoute, taskRoute
						reschedule.NextRoute = taskRoute
						reschedule.TaskWait = &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "input", ResumeJobType: jobRoute.JobType}
						if mode != "task" {
							delay := time.Duration(0)
							reschedule.AlternateRoute = &jobRoute
							if mode == "future-alternate" {
								delay = time.Hour
							} else {
								effective = jobRoute
							}
							if mode == "due-task-alternate" {
								reschedule.AlternateRoute = &altRoute
								effective = altRoute
							}
							reschedule.AlternateAfter = &delay
						}
					case "delayed", "elapsed-delay":
						until := time.Now().Add(time.Hour)
						want = false
						if mode == "elapsed-delay" {
							until = time.Now().Add(-time.Hour)
							want = true
						}
						reschedule.WaitUntil = &until
					case "dependency", "elapsed-dependency-deadline":
						dep := submit("dependency")
						reschedule.WaitForJobIDs = []string{dep.JobId}
						want = false
						if mode == "elapsed-dependency-deadline" {
							until := time.Now().Add(-time.Hour)
							reschedule.WaitUntil = &until
							want = true
						}
					case "active", "cancelled", "completed":
						want = false
					}
					switch mode {
					case "active":
					case "cancelled":
						require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key}))
					case "completed":
						final := jobdb.Chapter{Ordinal: 1, TaskType: jobRoute.JobType, CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}
						require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
					default:
						require.NoError(t, lease.Reschedule(ctx, reschedule))
					}
					base := jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, JobKeys: []jobdb.JobKey{key}}
					check := func(req jobdb.ListJobsRequest, expected bool) {
						t.Helper()
						result, err := r.ListJobs(ctx, req)
						require.NoError(t, err)
						if !expected {
							require.Empty(t, result.Jobs)
							return
						}
						require.Len(t, result.Jobs, 1)
						require.Equal(t, key, result.Jobs[0].JobKey)
						require.Equal(t, stored, *result.Jobs[0].NextRoute, "discovery must preserve the pending input route")
						if len(reschedule.WaitForJobIDs) > 0 {
							require.Equal(t, reschedule.WaitForJobIDs, result.Jobs[0].WaitFor, "discovery must not consume dependencies")
						}
						if reschedule.TaskWait != nil {
							require.Equal(t, reschedule.TaskWait, result.Jobs[0].ExecutionState.TaskWait)
						}
					}
					check(base, true) // Omitted and empty filters preserve normal inspection.
					empty := base
					empty.AvailableRoutes = []jobdb.Route{}
					check(empty, true)
					for _, route := range []jobdb.Route{jobRoute, taskRoute, altRoute, {JobType: "unrelated"}} {
						req := base
						req.AvailableRoutes = []jobdb.Route{route}
						check(req, want && route == effective)
					}
					req := base
					req.AvailableRoutes = []jobdb.Route{jobRoute, taskRoute, altRoute}
					check(req, want)
					check(req, want) // Repeated discovery never claims or consumes the alternate.
					wrongTenant := req
					wrongTenant.TenantIds = []string{"other-tenant"}
					check(wrongTenant, false)
					wrongMetadata := req
					wrongMetadata.MetadataFilter, err = jobdb.Metadata().EqualFilter("team", "red")
					require.NoError(t, err)
					check(wrongMetadata, false)
					wrongType := req
					wrongType.JobTypes = []string{"unrelated"}
					check(wrongType, false)
					wrongStatus := req
					wrongStatus.Statuses = []jobdb.JobStatus{jobdb.JobStatusActive}
					check(wrongStatus, false)
					if stored == taskRoute {
						taskFilter := req
						taskFilter.JobTasks = []jobdb.JobTaskFilter{{JobType: taskRoute.JobType, TaskType: taskRoute.TaskType}}
						check(taskFilter, want)
						taskFilter.AvailableRoutes = nil
						check(taskFilter, true)
					}
					got, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "claim", Routes: req.AvailableRoutes})
					require.NoError(t, err)
					if !want {
						require.Nil(t, got)
						return
					}
					require.NotNil(t, got)
					require.Equal(t, effective, got.Route())
					check(req, false) // The claim wins over subsequent discovery.
					require.NoError(t, got.Reschedule(ctx, reschedule))
					polled, err := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: key.TenantId, WorkerID: "poll", Routes: req.AvailableRoutes, Limit: 10})
					require.NoError(t, err)
					found := false
					for _, item := range polled {
						if item.Job().JobKey == key {
							found = true
							require.Equal(t, effective, item.Route())
						}
					}
					require.True(t, found, "PollWork must agree with discovery")
				})
			}
			t.Run("pagination-and-validation", func(t *testing.T) {
				f := buildFixture(t, h)
				defer f.Shutdown(t)
				ctx := context.Background()
				r := f.Runtime
				wanted := jobdb.Route{JobType: "wanted"}
				for i, jt := range []string{"skip", "wanted", "skip", "wanted", "skip"} {
					_, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: f.WorkerTenantID, JobType: jt, Data: NumberTaskData(i)}})
					require.NoError(t, err)
				}
				req := jobdb.ListJobsRequest{TenantIds: []string{f.WorkerTenantID}, AvailableRoutes: []jobdb.Route{wanted}, PageSize: 1}
				seen := map[jobdb.JobKey]bool{}
				for i := 0; ; i++ {
					require.Less(t, i, 5)
					page, err := r.ListJobs(ctx, req)
					require.NoError(t, err)
					require.Len(t, page.Jobs, 1, "filter must run before pagination")
					require.Equal(t, wanted, *page.Jobs[0].NextRoute)
					require.False(t, seen[page.Jobs[0].JobKey])
					seen[page.Jobs[0].JobKey] = true
					if page.NextPageToken == "" {
						break
					}
					req.PageToken = page.NextPageToken
				}
				require.Len(t, seen, 2)
				for _, invalid := range []jobdb.Route{{}, {JobType: "bad\x00name"}, {JobType: "job", TaskType: "bad\x00task"}} {
					_, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: req.TenantIds, AvailableRoutes: []jobdb.Route{invalid}})
					require.Error(t, err)
				}
			})
		})
	}
}
