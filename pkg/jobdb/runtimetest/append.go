package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func RunStartOrAppendConformance(t *testing.T, harnesses ...Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := h.New(t)
			if f.Cleanup != nil {
				t.Cleanup(func() { require.NoError(t, f.Cleanup(context.Background())) })
			}
			rt := f.Runtime
			ctx := celltest.Context(context.Background())
			_, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
			require.NoError(t, err)
			request := func(key string) jobdb.StartOrAppendJobRequest {
				return jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: key, MaxInputs: 100, Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("start")}, Append: json.RawMessage(`null`)}}
			}
			t.Run("concurrent-first-creator", func(t *testing.T) {
				const n = 24
				handles := make([]jobdb.JobHandle, n)
				errs := make([]error, n)
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := 0; i < n; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						q := request("burst")
						q.Job.Append = json.RawMessage(fmt.Sprint(i))
						q.Job.Job.Data = jobdb.NewTaskDataOrPanic(i)
						handles[i], errs[i] = rt.StartOrAppendJob(ctx, q)
					}(i)
				}
				close(start)
				wg.Wait()
				for i := range handles {
					require.NoError(t, errs[i])
					require.Equal(t, handles[0], handles[i])
					require.Regexp(t, `^C2[0-9A-Za-z]{27}$`, handles[i].JobKey.JobId)
					_, err := ksuid.Parse(strings.TrimPrefix(handles[i].JobKey.JobId, "C2"))
					require.NoError(t, err)
				}
				jobs, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, AppendKeys: []string{"burst"}})
				require.NoError(t, err)
				require.Len(t, jobs.Jobs, 1)
				chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handles[0].JobKey})
				require.NoError(t, err)
				require.Len(t, chapters, n)
				seen := map[string]bool{}
				for i, ch := range chapters {
					require.Equal(t, int64(i), ch.Ordinal)
					var raw []byte
					if i == 0 {
						raw = ch.Body.(jobdb.JobStartChapter).Append.Data
					} else {
						raw = ch.Body.(jobdb.AdditionalInputChapter).Input.Data
					}
					seen[string(raw)] = true
				}
				require.Len(t, seen, n)
			})
			t.Run("per-call-cap-and-worker-lease", func(t *testing.T) {
				q := request("cap")
				q.Job.MaxInputs = 1
				a, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				q.Job.MaxInputs = 2
				b, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				require.Equal(t, a, b)
				c, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				require.NotEqual(t, a.JobKey, c.JobKey)
				info, err := rt.GetJob(ctx, c.JobKey)
				require.NoError(t, err)
				require.Contains(t, info.WaitFor, a.JobKey.JobId)
				blocked, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: c.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "batch"}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.Nil(t, blocked)
				require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: a.JobKey}))
				worker, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: c.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "batch"}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.NotNil(t, worker)
				d, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				require.NotEqual(t, c.JobKey, d.JobKey)
				e, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				require.Equal(t, d, e)
				// Lease-scoped requests share the chain but never record a parent.
				q.Job.AppendKey = "leased"
				q.Job.Job.TenantId = ""
				a, err = worker.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				list, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{a.JobKey}})
				require.NoError(t, err)
				require.Len(t, list.Jobs, 1)
				require.Empty(t, list.Jobs[0].ParentJobID)
			})
			t.Run("worker-chapter-permanently-closes-prefix", func(t *testing.T) {
				q := request("sealed")
				a, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				worker, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: a.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "batch"}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.NotNil(t, worker)
				err = rt.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: worker.LeaseID(), LeaseToken: leaseTokenForTest(worker), Ref: jobdb.ChapterRef{JobKey: a.JobKey, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "task", Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}})
				require.NoError(t, err)
				require.NoError(t, worker.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "batch"}}))
				b, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				require.NotEqual(t, a.JobKey, b.JobKey)
			})
			t.Run("schema-budget-availability-and-discovery", func(t *testing.T) {
				q := request("schema")
				q.Job.Job.Schema = &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":true,"appendShape":{"type":"integer"}}`)}
				q.Job.Append = json.RawMessage(`"wrong"`)
				_, err := rt.StartOrAppendJob(ctx, q)
				require.Error(t, err)
				q.Job.Append = json.RawMessage(`1`)
				at := time.Now().Add(time.Hour)
				q.Job.Job.AvailableAt = &at
				a, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				q.Job.Job.Schema = nil
				q.Job.Job.AvailableAt = nil
				q.Job.Append = json.RawMessage(`"wrong"`)
				_, err = rt.StartOrAppendJob(ctx, q)
				require.Error(t, err)
				q.Job.Append = json.RawMessage(`2`)
				b, err := rt.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				require.Equal(t, a, b)
				info, err := rt.GetJob(ctx, a.JobKey)
				require.NoError(t, err)
				require.Equal(t, "schema", info.AppendKey)
				require.Equal(t, jobdb.JobStatusAwaitingFuture, info.Status)
				listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, AppendKeys: []string{"schema"}})
				require.NoError(t, err)
				require.Len(t, listed.Jobs, 1)
				require.Equal(t, "schema", listed.Jobs[0].AppendKey)
				q = request("large")
				q.Job.Append = json.RawMessage(`"` + strings.Repeat("x", int(jobdb.DefaultMaxChapterApplicationPayloadBytes)) + `"`)
				_, err = rt.StartOrAppendJob(ctx, q)
				require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
			})

			t.Run("validation", func(t *testing.T) {
				for _, change := range []func(*jobdb.StartOrAppendJob){func(q *jobdb.StartOrAppendJob) { q.AppendKey = "" }, func(q *jobdb.StartOrAppendJob) { q.Append = nil }, func(q *jobdb.StartOrAppendJob) { q.Job.JobID = "chosen" }, func(q *jobdb.StartOrAppendJob) { q.Job.GoalID = "goal" }, func(q *jobdb.StartOrAppendJob) { q.MaxInputs = -1 }, func(q *jobdb.StartOrAppendJob) { q.MaxInputs = 1001 }} {
					q := request("invalid")
					change(&q.Job)
					_, err := rt.StartOrAppendJob(ctx, q)
					require.ErrorIs(t, err, jobdb.ErrInvalidAppendRequest)
				}
			})
		})
	}
}
