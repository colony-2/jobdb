package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestExecutionOversizedPayloadTerminatesJob(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"task", "swallow", "final", "external"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var runs atomic.Int32
				big := jobdb.NewTaskDataOrPanic(map[string]any{"history": strings.Repeat("DO-NOT-PERSIST", 30000)})
				call := invocation("too-big")
				call.Policy.Retry.MaximumAttempts = 5
				call.Catch = []wf.CatchRule{{ID: "swallow", Match: func(error) bool { t.Error("fatal persistence errors must bypass catch handlers"); return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
					return wf.CatchResult{Disposition: wf.CatchHandled}, nil
				}}}
				w := wf.ExecutionWorker{JobType: "oversized", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					if mode == "final" {
						runs.Add(1)
						return big, nil
					}
					out, err := c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
					if mode == "swallow" {
						return jobdb.NewTaskDataOrPanic("ignored"), nil
					}
					return out, err
				}}
				key := submitExecutionTest(t, rt, w)
				task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return big, nil }}
				if mode == "external" {
					out, err := runExecutionTest(t, rt, key, w)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunSuspended, out.Status)
					engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
					require.NoError(t, err)
					handle, err := engine.GetWaitingTask(context.Background(), key)
					require.NoError(t, err)
					err = handle.Finish(context.Background(), big)
					require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
					info, err := rt.GetJob(context.Background(), key)
					require.NoError(t, err)
					require.Equal(t, jobdb.JobStatusCompleted, info.Status)
					_, err = replayExecutionTest(t, rt, key, w)
					require.True(t, jobdb.IsChapterPayloadFailure(err))
					require.Zero(t, runs.Load())
				} else {
					out, err := runExecutionTest(t, rt, key, w, task)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunFailed, out.Status)
					require.True(t, jobdb.IsChapterPayloadFailure(out.JobError))
					require.EqualValues(t, 1, runs.Load())
					_, err = replayExecutionTest(t, rt, key, w, task)
					require.True(t, jobdb.IsChapterPayloadFailure(err))
					require.EqualValues(t, 1, runs.Load())
				}
				chapters, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				last := chapters[len(chapters)-1]
				require.True(t, jobdb.ChapterPayloadFailure(last))
				raw, err := json.Marshal(last)
				require.NoError(t, err)
				require.Less(t, len(raw), 8192)
				require.NotContains(t, string(raw), "DO-NOT-PERSIST")
				require.Contains(t, string(raw), "/history")
			})
		}
	}
}

func TestConfiguredApplicationLimitRejectsWithoutPublishing(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var rt jobdb.WorkflowRuntime = toy.New(toy.WithMaxChapterApplicationPayloadBytes(64))
			if backend != "toy" {
				local, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(t.TempDir(), "job.db"), MaxChapterApplicationPayloadBytes: 64})
				require.NoError(t, err)
				t.Cleanup(func() { _ = local.Close(ctx) })
				rt = local
			}
			if backend == "remote" {
				server := httptest.NewServer(remote.NewServer(rt))
				t.Cleanup(server.Close)
				client, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				rt = client
			}
			key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: key.TenantId, JobID: key.JobId, JobType: "test", Data: jobdb.NewTaskDataOrPanic(strings.Repeat("x", 63))}}
			_, err := rt.SubmitJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
			_, err = rt.GetJob(ctx, key)
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			req.Job.Data = jobdb.NewTaskDataOrPanic(strings.Repeat("x", 62))
			_, err = rt.SubmitJob(ctx, req)
			require.NoError(t, err)
			leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "test", Routes: []jobdb.Route{{JobType: "test"}}, Limit: 1})
			require.NoError(t, err)
			require.Len(t, leases, 1)
			chapter := jobdb.Chapter{Ordinal: 1, TaskType: "task", Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"` + strings.Repeat("x", 63) + `"`)}}}}
			put := jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, LeaseID: leases[0].LeaseID(), Chapter: chapter}
			if token, ok := leases[0].(interface{ LeaseToken() string }); ok {
				put.LeaseToken = token.LeaseToken()
			}
			if backend != "remote" {
				put.ArtifactUploads = []jobdb.ArtifactUpload{{Name: "must-not-open", Open: func() (io.ReadCloser, error) {
					t.Error("oversized writes must not open uploads")
					return nil, fmt.Errorf("unexpected open")
				}}}
			}
			err = rt.PutChapter(ctx, put)
			require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
			chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Len(t, chapters, 1)
			// A rejected low-level append leaves the valid lease and ordinal usable.
			put.ArtifactUploads = nil
			put.Chapter.Body = jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}
			require.NoError(t, rt.PutChapter(ctx, put))
			restart := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, JobID: "restart", LastStepToKeep: 0, ExtraTaskOutput: jobdb.NewTaskDataOrPanic(strings.Repeat("x", 63))}}
			_, err = rt.SubmitRestartJob(ctx, restart)
			require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
			restart.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic("small")
			_, err = rt.SubmitRestartJob(ctx, restart)
			require.NoError(t, err)
			schedule := jobdb.UpsertScheduleRequest{TenantId: "tenant", ScheduleId: "schedule", Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Hour}, Target: jobdb.ScheduleTarget{JobType: "test", Data: jobdb.NewTaskDataOrPanic(strings.Repeat("x", 63))}}
			_, err = rt.UpsertSchedule(ctx, schedule)
			require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
			_, err = rt.GetSchedule(ctx, jobdb.ScheduleKey{TenantId: "tenant", ScheduleId: "schedule"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)

		})
	}
}
