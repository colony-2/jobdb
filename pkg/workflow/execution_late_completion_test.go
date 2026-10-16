package workflow_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// A past deadline makes timeout work eligible; it does not consume the waiting
// slot. Seed an already-expired wait so transport speed cannot race the fixture.
func TestExecutionExpiredWaitCompletionAcrossRuntimes(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"reply-first", "timeout-claim-first", "cancelled", "stale-generation"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				rt := alternateRuntime(t, backend)
				key := submitExecutionTest(t, rt, wf.ExecutionWorker{JobType: "late-reply"})
				route := jobdb.Route{JobType: "late-reply"}
				lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "fixture", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.NotNil(t, lease)
				wait := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "wait", ID: "external/attempt:1", At: time.Now().Add(-2 * time.Minute), Deadline: time.Now().Add(-time.Minute), Descriptor: json.RawMessage(`"external"`), Input: &jobdb.ExecutionSnapshot{Data: json.RawMessage(`"original-input"`)}}
				raw, err := json.Marshal(wait)
				require.NoError(t, err)
				hash := fmt.Sprintf("%x", sha256.Sum256(raw))
				token := ""
				if transport, ok := lease.(interface{ LeaseToken() string }); ok {
					token = transport.LeaseToken()
				}
				require.NoError(t, rt.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "__execution__", CreatedAt: wait.At, Body: jobdb.ExecutionChapter{Record: raw}}}))
				due := time.Duration(0)
				primary := jobdb.Route{JobType: route.JobType, TaskType: "external"}
				require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: primary, TaskWait: &jobdb.TaskWait{InputOrdinal: 1, OutputOrdinal: 2, InputHash: hash, ResumeJobType: route.JobType}, AlternateRoute: &route, AlternateAfter: &due}))
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
				require.NoError(t, err)
				pending, err := engine.GetWaitingTask(ctx, key)
				require.NoError(t, err)
				require.Equal(t, "external", pending.TaskType())
				switch mode {
				case "timeout-claim-first":
					timeoutLease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "timeout", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					require.NotNil(t, timeoutLease, "job-only workers must pick up expired missing tasks")
					require.Equal(t, route, timeoutLease.Route())
				case "cancelled":
					require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: key}))
				}
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				answer := jobdb.NewTaskDataOrPanic("late-answer")
				if mode == "stale-generation" {
					err = rt.CompleteTaskIfWaiting(ctx, jobdb.CompleteTaskIfWaitingRequest{JobKey: key, Route: primary, ResumeJobType: route.JobType, InputOrdinal: 1, OutputOrdinal: 2, InputHash: "stale", Data: answer})
				} else {
					err = pending.Finish(ctx, answer)
				}
				if mode != "reply-first" {
					require.ErrorIs(t, err, jobdb.ErrConflict)
					after, readErr := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
					require.NoError(t, readErr)
					require.Equal(t, before, after, "rejected completion must not append an outcome")
					return
				}
				require.NoError(t, err)
				records := executionRecords(t, rt, key)
				require.Len(t, records, 2)
				require.Equal(t, "result", records[1].Kind)
				require.Equal(t, wait.ID, records[1].ID)
				require.Equal(t, wait.Input, records[1].Input)
				require.JSONEq(t, `"late-answer"`, string(records[1].Output.Data))
				require.Error(t, pending.Finish(ctx, jobdb.NewTaskDataOrPanic("duplicate")))
				require.Equal(t, records, executionRecords(t, rt, key))
			})
		}
	}
}

func TestExecutionLateCompletionAndTimeoutReplay(t *testing.T) {
	for _, replyFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("reply-first=%t", replyFirst), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rt := alternateRuntime(t, "toy")
				var builds atomic.Int32
				call := invocation("external")
				call.Policy.TotalTimeout = jobdb.AsDuration(time.Minute)
				w := wf.ExecutionWorker{JobType: "late-replay", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
				}}
				key := submitExecutionTest(t, rt, w)
				out, err := runExecutionTest(t, rt, key, w)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunSuspended, out.Status)
				time.Sleep(2 * time.Minute)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).BuildEngine()
				require.NoError(t, err)
				pending, err := engine.GetWaitingTask(context.Background(), key)
				require.NoError(t, err)
				if replyFirst {
					require.NoError(t, pending.Finish(context.Background(), jobdb.NewTaskDataOrPanic("late-answer")))
				}
				out, err = runExecutionTest(t, rt, key, w)
				require.NoError(t, err)
				if replyFirst {
					require.Equal(t, wf.JobRunCompleted, out.Status, "%v", out.JobError)
					value, err := replayExecutionTest(t, rt, key, w)
					require.NoError(t, err)
					raw, err := value.GetData()
					require.NoError(t, err)
					require.JSONEq(t, `"late-answer"`, string(raw))
				} else {
					require.Equal(t, wf.JobRunFailed, out.Status)
					var timeout jobdb.TimeoutError
					require.ErrorAs(t, out.JobError, &timeout)
					require.Error(t, pending.Finish(context.Background(), jobdb.NewTaskDataOrPanic("after-timeout")))
					_, err = replayExecutionTest(t, rt, key, w)
					require.ErrorAs(t, err, &timeout)
				}
				require.EqualValues(t, 1, builds.Load(), "recovery and replay must reuse the original input")
			})
		})
	}
}
