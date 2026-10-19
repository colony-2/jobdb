package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestJobDeletionFiltersAndLeases(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend)
			admin := rt.(jobdb.JobDeletionRuntime)
			submit := func(tenant, id, metadata string) jobdb.JobKey {
				h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: id, JobType: "cleanup", Data: jobdb.NewTaskDataOrPanic("input"), Metadata: json.RawMessage(metadata)}})
				require.NoError(t, err)
				return h.JobKey
			}
			key := submit("tenant", "selected", `{"recipe":"build","env":"dev"}`)
			submit("other", "selected", `{"recipe":"build","env":"dev"}`)
			submit("tenant", "wrong-recipe", `{"recipe":"test","env":"dev"}`)
			submit("tenant", "wrong-env", `{"recipe":"build","env":"prod"}`)
			listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{key}})
			require.NoError(t, err)
			require.Len(t, listed.Jobs, 1)
			created := listed.Jobs[0].CreatedAt
			filter, err := jobdb.Metadata().EqualFilter("recipe", "build")
			require.NoError(t, err)
			filter, err = filter.EqualFilter("env", "dev")
			require.NoError(t, err)
			req := jobdb.DeleteJobsRequest{TenantID: "tenant", MetadataFilter: filter, CreatedBefore: &created, DryRun: true}
			result, err := admin.DeleteJobs(ctx, req)
			require.NoError(t, err)
			require.Zero(t, result.MatchedCount, "cutoff must be exclusive")
			cutoff := created.Add(time.Nanosecond)
			req.CreatedBefore = &cutoff
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, Routes: []jobdb.Route{{JobType: "cleanup"}}, WorkerID: "holder", LeaseDuration: time.Hour})
			require.NoError(t, err)
			require.NotNil(t, lease)
			before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			result, err = admin.DeleteJobs(ctx, req)
			require.NoError(t, err)
			require.Equal(t, jobdb.DeleteJobsResponse{MatchedCount: 1}, result)
			require.NoError(t, lease.KeepAlive(ctx), "dry run must preserve leases")
			req.DryRun = false
			result, err = admin.DeleteJobs(ctx, req)
			require.NoError(t, err)
			require.Equal(t, jobdb.DeleteJobsResponse{MatchedCount: 1, DeletedCount: 1}, result)
			require.Error(t, lease.KeepAlive(ctx))
			require.Error(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "cleanup"}}))
			require.Error(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success"}))
			_, err = lease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "cleanup", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.Error(t, err, "revoked lease cannot submit children")
			info, err := rt.GetJob(ctx, key)
			require.NoError(t, err, "direct reads must survive deletion")
			require.Equal(t, jobdb.JobStatusCancelled, info.Status)
			after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Equal(t, before, after)
			lease, err = rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, Routes: []jobdb.Route{{JobType: "cleanup"}}, WorkerID: "new", LeaseDuration: time.Hour})
			require.NoError(t, err)
			require.Nil(t, lease)
			listed, err = rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{key}, Stores: []jobdb.JobStore{jobdb.JobStoreActive, jobdb.JobStoreArchived}})
			require.NoError(t, err)
			require.Empty(t, listed.Jobs, "explicit listing selectors must not expose hidden jobs")
			result, err = admin.DeleteJobs(ctx, req)
			require.NoError(t, err)
			require.Equal(t, jobdb.DeleteJobsResponse{}, result, "repeat is a no-op")
			listed, err = rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, PageSize: 1})
			require.NoError(t, err)
			visible := append([]jobdb.JobSummary(nil), listed.Jobs...)
			for listed.NextPageToken != "" {
				listed, err = rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, PageSize: 1, PageToken: listed.NextPageToken})
				require.NoError(t, err)
				visible = append(visible, listed.Jobs...)
			}
			require.Len(t, visible, 2)
			other, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"other"}})
			require.NoError(t, err)
			require.Len(t, other.Jobs, 1)
			leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", Routes: []jobdb.Route{{JobType: "cleanup"}}, WorkerID: "poll", Limit: 10, LeaseDuration: time.Hour})
			require.NoError(t, err)
			require.Len(t, leases, 2)
			for _, l := range leases {
				require.NotEqual(t, key, l.Job().JobKey)
			}
		})
	}
}

func TestJobDeletionBeyondListingPage(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend)
			admin := rt.(jobdb.JobDeletionRuntime)
			for i := 0; i < jobdb.MaxListJobsPageSize+3; i++ {
				_, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: fmt.Sprint(i), JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
				require.NoError(t, err)
			}
			result, err := admin.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{"0", "0", "missing", "1"}})
			require.NoError(t, err)
			require.EqualValues(t, 2, result.DeletedCount)
			result, err = admin.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", All: true})
			require.NoError(t, err)
			require.EqualValues(t, jobdb.MaxListJobsPageSize+1, result.DeletedCount)
			listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
			require.NoError(t, err)
			require.Empty(t, listed.Jobs)
		})
	}
}

func TestJobDeletionHidesScheduleRuns(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend)
			schedule := jobdb.ScheduleKey{TenantId: "tenant", ScheduleId: "cleanup"}
			_, err := rt.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{TenantId: schedule.TenantId, ScheduleId: schedule.ScheduleId,
				Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Hour},
				Target:  jobdb.ScheduleTarget{JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}, Paused: true})
			require.NoError(t, err)
			h, err := rt.TriggerSchedule(ctx, jobdb.TriggerScheduleRequest{ScheduleKey: schedule, RequestID: "manual"})
			require.NoError(t, err)
			runs, err := rt.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: schedule})
			require.NoError(t, err)
			require.Len(t, runs.Runs, 1)
			_, err = rt.(jobdb.JobDeletionRuntime).DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{h.JobKey.JobId}})
			require.NoError(t, err)
			runs, err = rt.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: schedule})
			require.NoError(t, err)
			require.Empty(t, runs.Runs)
			info, err := rt.GetSchedule(ctx, schedule)
			require.NoError(t, err)
			require.Equal(t, jobdb.ScheduleStatePaused, info.State)
		})
	}
}

func TestJobDeletionPreservesThinCloneReplayAndArtifacts(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			rt := alternateRuntime(t, backend)
			admin := rt.(jobdb.JobDeletionRuntime)
			contents := strings.Repeat("shared artifact\n", 200)
			var runs atomic.Int32
			task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				runs.Add(1)
				path := filepath.Join(t.TempDir(), "shared.txt")
				if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
					return nil, err
				}
				art, err := jobdb.NewArtifactFromFile("shared.txt", path)
				if err != nil {
					return nil, err
				}
				return &jobdb.SimpleTaskData{Data: json.RawMessage(`"output"`), Artifacts: []jobdb.Artifact{art}}, nil
			}}
			worker := wf.ExecutionWorker{JobType: "thin-clone", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				out, err := c.DoTask(invocation("prefix"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				if err != nil {
					return nil, err
				}
				return c.DoScope("restart", wf.ScopeOptions{OnRestart: func(json.RawMessage) error { return nil }}, func(wf.ExecutionContext) (jobdb.JobData, error) { return out, nil })
			}}
			key := submitExecutionTest(t, rt, worker)
			out, err := runExecutionTest(t, rt, key, worker, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			clone := func(source jobdb.JobKey) jobdb.JobKey {
				h, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: executionRestartTarget(t, rt, source, "restart")}}})
				require.NoError(t, err)
				return h.JobKey
			}
			firstClone := clone(key)
			for _, pair := range [][2]jobdb.JobKey{{key, firstClone}, {firstClone, {}}} {
				source, child := pair[0], pair[1]
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source})
				require.NoError(t, err)
				result, err := admin.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: source.TenantId, JobIDs: []string{source.JobId}})
				require.NoError(t, err)
				require.EqualValues(t, 1, result.DeletedCount)
				info, err := rt.GetJob(ctx, source)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusCompleted, info.Status)
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source})
				require.NoError(t, err)
				require.Equal(t, before, after)
				_, err = replayExecutionTest(t, rt, source, worker, task)
				require.NoError(t, err)
				if child.JobId == "" {
					child = clone(source)
				} // Also allow restart from a hidden source.
				out, err = runExecutionTest(t, rt, child, worker, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
				value, err := replayExecutionTest(t, rt, child, worker, task)
				require.NoError(t, err)
				arts, err := value.GetArtifacts()
				require.NoError(t, err)
				require.Len(t, arts, 1)
				reader, err := arts[0].Open()
				require.NoError(t, err)
				data, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				require.Equal(t, contents, string(data))
			}
			require.EqualValues(t, 1, runs.Load(), "shared work must not rerun")
			listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}})
			require.NoError(t, err)
			require.Len(t, listed.Jobs, 1, "deletion must not cascade to descendants")
		})
	}
}
