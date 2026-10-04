package runtimetest

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func runCompletionSummaries(t *testing.T, harnesses []Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := buildFixture(t, h)
			defer f.Shutdown(t)
			r, ctx := f.Runtime, context.Background()
			tenant := f.WorkerTenantID
			submit := func(id string) jobdb.JobKey {
				t.Helper()
				handle, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
					TenantId: tenant, JobID: id, JobType: "completion", Data: NumberTaskData(1),
				}})
				require.NoError(t, err)
				return handle.JobKey
			}
			claim := func(key jobdb.JobKey) jobdb.ExecutionLease {
				t.Helper()
				lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "completion-worker", Routes: []jobdb.Route{{JobType: "completion"}}})
				require.NoError(t, err)
				require.NotNil(t, lease)
				return lease
			}
			check := func(key jobdb.JobKey, status, detail string) jobdb.JobSummary {
				t.Helper()
				listed, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, JobKeys: []jobdb.JobKey{key}})
				require.NoError(t, err)
				require.Len(t, listed.Jobs, 1)
				got := listed.Jobs[0]
				require.Equal(t, status, got.CompletionStatus)
				require.Equal(t, detail, got.CompletionDetail)
				return got
			}
			chapter := func(ordinal int64) jobdb.Chapter {
				return jobdb.Chapter{Ordinal: ordinal, TaskType: "completion", CreatedAt: time.Now().UTC(),
					Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}
			}
			for _, status := range []string{"success", "failed_app", "failed_system", "failed_timeout", "cancelled"} {
				t.Run(status, func(t *testing.T) {
					key := submit(status)
					check(key, "", "")
					lease := claim(key)
					final := chapter(1)
					// Completion data must come from the scheduler, independently
					// of chapter content. A committed chapter alone is insufficient.
					require.NoError(t, r.PutChapter(ctx, jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, LeaseID: lease.LeaseID(), LeaseToken: leaseTokenForTest(lease), Chapter: final}))
					check(key, "", "")
					detail := "stored detail: " + status + "\nexact text"
					if status == "success" {
						detail = ""
					}
					require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: status, Detail: detail, Chapter: &final}))
					got := check(key, status, detail)
					wantStatus := jobdb.JobStatusCompleted
					if status == "cancelled" {
						wantStatus = jobdb.JobStatusCancelled
					}
					require.Equal(t, wantStatus, got.Status)
					run, err := jobdb.GetJobRun(ctx, r, jobdb.GetJobRunRequest{JobKey: key})
					require.NoError(t, err)
					require.Equal(t, status, run.Job.CompletionStatus)
					require.Equal(t, detail, run.Job.CompletionDetail)
					// Already-terminal jobs retain their original outcome/detail.
					// Some adapters reject cancellation of archived jobs.
					_ = r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key, Reason: "late cancellation"})
					require.Equal(t, wantStatus, check(key, status, detail).Status)
				})
			}
			key := submit("retry")
			lease := claim(key)
			failed := chapter(1)
			failed.Body = jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "retry me"}}}
			require.NoError(t, r.PutChapter(ctx, jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, LeaseID: lease.LeaseID(), LeaseToken: leaseTokenForTest(lease), Chapter: failed}))
			require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "completion"}}))
			require.Equal(t, jobdb.JobStatusReady, check(key, "", "").Status)
			lease = claim(key)
			final := chapter(2)
			require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
			check(key, "success", "")

			key = submit("cancel-request")
			require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key, Reason: "operator cancelled"}))
			listed, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, JobKeys: []jobdb.JobKey{key}})
			require.NoError(t, err)
			require.Len(t, listed.Jobs, 1)
			if listed.Jobs[0].ArchivedAt == nil {
				check(key, "", "") // cancellation requested, not yet finalized
			} else {
				check(key, "cancelled", "operator cancelled")
			}

			// Completion information must not alter status filters or pagination.
			seen := map[jobdb.JobKey]bool{}
			page := ""
			for {
				listed, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, JobTypes: []string{"completion"}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}, Statuses: []jobdb.JobStatus{jobdb.JobStatusCompleted}, PageSize: 2, PageToken: page})
				require.NoError(t, err)
				for _, job := range listed.Jobs {
					require.False(t, seen[job.JobKey])
					seen[job.JobKey] = true
					require.NotEmpty(t, job.CompletionStatus)
					require.Equal(t, jobdb.JobStatusCompleted, job.Status)
				}
				page = listed.NextPageToken
				if page == "" {
					break
				}
			}
			require.Len(t, seen, 5)
			if h.Capabilities.Schedules {
				info, err := r.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{
					TenantId: tenant, ScheduleId: "completion-schedule", RequestTime: time.Now().Add(-2 * time.Hour),
					Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Hour},
					Target:  jobdb.ScheduleTarget{JobType: "completion", Data: NumberTaskData(1)},
				})
				require.NoError(t, err)
				require.NotNil(t, info.NextJobKey)
				lease := claim(*info.NextJobKey)
				final := chapter(1)
				require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_system", Detail: "scheduled failure", Chapter: &final}))
				runs, err := r.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
				require.NoError(t, err)
				found := false
				for _, run := range runs.Runs {
					if run.JobKey == *info.NextJobKey {
						found = true
						require.Equal(t, "failed_system", run.CompletionStatus)
						require.Equal(t, "scheduled failure", run.CompletionDetail)
					}
				}
				require.True(t, found)
			}
		})
	}
}
