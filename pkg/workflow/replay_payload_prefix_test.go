package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Count calls that reach the backing runtime, including upload publication.
// Replay's wrapper must reject mutations before any of these are called.
type prefixRuntime struct {
	jobdb.WorkflowRuntime
	writes, uploads, submissions, lifecycle atomic.Int32
}

func (r *prefixRuntime) PutChapter(c context.Context, q jobdb.PutChapterRequest) error {
	r.writes.Add(1)
	r.uploads.Add(int32(len(q.ArtifactUploads)))
	return r.WorkflowRuntime.PutChapter(c, q)
}
func (r *prefixRuntime) SubmitJob(c context.Context, q jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	r.submissions.Add(1)
	return r.WorkflowRuntime.SubmitJob(c, q)
}
func (r *prefixRuntime) SubmitRestartJob(c context.Context, q jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	r.submissions.Add(1)
	return r.WorkflowRuntime.SubmitRestartJob(c, q)
}
func (r *prefixRuntime) PollWork(c context.Context, q jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	r.lifecycle.Add(1)
	return r.WorkflowRuntime.PollWork(c, q)
}
func (r *prefixRuntime) GetJobLease(c context.Context, q jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	r.lifecycle.Add(1)
	return r.WorkflowRuntime.GetJobLease(c, q)
}
func (r *prefixRuntime) CancelJob(c context.Context, q jobdb.CancelJobRequest) error {
	r.lifecycle.Add(1)
	return r.WorkflowRuntime.CancelJob(c, q)
}
func (r *prefixRuntime) ClearCrashConcern(c context.Context, q jobdb.ClearCrashConcernRequest) error {
	r.lifecycle.Add(1)
	return r.WorkflowRuntime.ClearCrashConcern(c, q)
}
func (r *prefixRuntime) CompleteTaskIfWaiting(c context.Context, q jobdb.CompleteTaskIfWaitingRequest) error {
	r.lifecycle.Add(1)
	return r.WorkflowRuntime.CompleteTaskIfWaiting(c, q)
}
func (r *prefixRuntime) counts() []int32 {
	return []int32{r.writes.Load(), r.uploads.Load(), r.submissions.Load(), r.lifecycle.Load()}
}

// This is the existing worker-override technique: intercept a scope call before
// admitting its body, using state reconstructed by earlier accepted results.
type prefixScopeContext struct {
	wf.ExecutionContext
	before func(string) error
}

func (c prefixScopeContext) DoScope(site string, o wf.ScopeOptions, body func(wf.ExecutionContext) (jobdb.JobData, error)) (jobdb.JobData, error) {
	if err := c.before(site); err != nil {
		return nil, err
	}
	return c.ExecutionContext.DoScope(site, o, func(child wf.ExecutionContext) (jobdb.JobData, error) {
		return body(prefixScopeContext{child, c.before})
	})
}
func storedPrefixFailure(t *testing.T, chapters []jobdb.Chapter) jobdb.AppErrorPayload {
	t.Helper()
	last := chapters[len(chapters)-1]
	require.True(t, jobdb.ChapterPayloadFailure(last))
	return last.Body.(jobdb.JobAttemptOutcomeChapter).Outcome.(jobdb.AppErrorOutcome).Error
}

func TestReplayPayloadAcceptedPrefix(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, mode := range []string{"root", "local", "external", "external-long", "external-input"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				external := strings.HasPrefix(mode, "external")
				acceptedWait := external && mode != "external-input"
				ctx := celltest.Context(context.Background())
				rt := &prefixRuntime{WorkflowRuntime: alternateRuntime(t, backend)}
				var priorRuns, rejectedRuns, selectedBodies, followingRuns atomic.Int32
				big := jobdb.NewTaskDataOrPanic(strings.Repeat("REJECTED", 40000), jobdb.NewArtifactFromBytes("rejected.txt", []byte("unpublished")))
				if mode == "external-input" {
					big = jobdb.NewTaskDataOrPanic(strings.Repeat("oversized-input", 30000), jobdb.NewArtifactFromBytes("rejected.txt", []byte("unpublished")))
				}
				prior := executionTestTask{"prior", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
					n := priorRuns.Add(1)
					return jobdb.NewTaskDataOrPanic(map[string]int{"value": int(n) + 40}, jobdb.NewArtifactFromBytes("prior.txt", []byte("retained"))), nil
				}}
				rejected := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) { rejectedRuns.Add(1); return big, nil }}
				following := executionTestTask{"following", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) { followingRuns.Add(1); return nil, nil }}
				var state int
				var priorState []int
				var priorBuilds atomic.Int32
				notifications := 0
				var change string
				w := wf.ExecutionWorker{JobType: "prefix", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					priorState = nil
					if change == "unconsumed" {
						return in, nil
					}
					var out jobdb.TaskData
					var err error
					var call wf.TaskCall
					for _, site := range []string{"prior-first", "prior-second"} {
						call = invocation(site)
						call.TaskType = "prior"
						if change == "mismatch" {
							call.Command = []byte(`{"changed":true}`)
						}
						out, err = c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { priorBuilds.Add(1); return in, nil })
						if err != nil {
							return nil, err
						}
						raw, err := out.GetData()
						if err != nil {
							return nil, err
						}
						var v map[string]int
						if err = json.Unmarshal(raw, &v); err != nil {
							return nil, err
						}
						state = v["value"]
						priorState = append(priorState, state)
					}
					if change == "missing" || change == "builder" {
						call = invocation("unavailable")
						_, err = c.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
							if change == "builder" {
								return nil, errors.New("builder failed")
							}
							return in, nil
						})
						if err != nil {
							return nil, err
						}
					}
					if _, err = c.DoScope("selected", wf.ScopeOptions{AttemptTimeout: time.Minute}, func(wf.ExecutionContext) (jobdb.JobData, error) { selectedBodies.Add(1); return out, nil }); err != nil {
						return nil, err
					}
					if mode == "root" {
						return big, nil
					}
					rejectSite := "reject"
					if mode == "external-long" {
						rejectSite = strings.Repeat("nested", 40)
					}
					call = invocation(rejectSite)
					call.Policy.Retry.MaximumAttempts = 3
					call.Catch = []wf.CatchRule{{ID: "never", Match: func(error) bool { t.Error("terminal failure reached catch"); return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
						t.Error("terminal failure reached handler")
						return wf.CatchResult{}, nil
					}}}
					_, err = c.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
						if change == "reject-builder" {
							return nil, errors.New("rejected task builder failed")
						}
						if mode == "external-input" {
							return big, nil
						}
						return in, nil
					})
					if err != nil && change != "swallow" {
						return nil, err
					}
					call = invocation("following")
					call.TaskType = "following"
					out, err = c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
					if change == "swallow" {
						return in, nil
					}
					return out, err
				}}
				w.ObserveTaskResults = func(c context.Context) (context.Context, func(wf.TaskEndEvent)) {
					notifications = 0
					return c, func(wf.TaskEndEvent) { notifications++ }
				}
				key := submitExecutionTest(t, rt, w)
				tasks := []wf.TaskWorker{prior, following}
				if !external {
					tasks = append(tasks, rejected)
				}
				// Encoding a rejected payload can outlast the shared helper's one-second
				// lease while Reschedule/Complete blocks renewal under the session lock.
				// This fixture tests retained-prefix replay, not authority expiration.
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{
					JobKey: key, JobWorker: w, TaskWorkers: tasks, WorkerID: "execution-test",
					LeaseDuration: 10 * time.Second, ConsumerVersions: map[string]string{"c2j": "X", "pulse": "Y"},
				})
				require.NoError(t, err)
				outcome, err := run.Run(nil)
				require.NoError(t, err)
				if acceptedWait {
					require.Equal(t, wf.JobRunSuspended, outcome.Status)
					engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w).BuildEngine()
					require.NoError(t, err)
					handle, err := engine.GetWaitingTask(ctx, key)
					require.NoError(t, err)
					require.ErrorIs(t, handle.Finish(ctx, big), jobdb.ErrChapterApplicationPayloadTooLarge)
				} else {
					require.Equal(t, wf.JobRunFailed, outcome.Status)
				}
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				failure := storedPrefixFailure(t, before)
				if mode == "external-input" {
					raw, err := json.Marshal(failure.Attrs["payloadSize"])
					require.NoError(t, err)
					var size jobdb.ChapterApplicationPayloadError
					require.NoError(t, json.Unmarshal(raw, &size))
					require.Equal(t, "wait", size.RecordKind)
					require.EqualValues(t, 450002, size.ActualBytes)
				}
				if mode == "external-long" {
					raw, err := json.Marshal(failure.Attrs["payloadSize"])
					require.NoError(t, err)
					var size jobdb.ChapterApplicationPayloadError
					require.NoError(t, json.Unmarshal(raw, &size))
					require.True(t, strings.HasSuffix(size.LogicalID, "..."))
					require.LessOrEqual(t, len(size.LogicalID), 160)
				}
				info, err := rt.GetJob(ctx, key)
				require.NoError(t, err)
				_, artifactErr := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: key, Ordinal: before[len(before)-1].Ordinal, Name: "rejected.txt"})
				require.Error(t, artifactErr, "rejected upload must not be published")
				historyJSON, err := json.Marshal(before)
				require.NoError(t, err)
				require.NotContains(t, string(historyJSON), "REJECTED")
				require.NotContains(t, string(historyJSON), "oversized-input")
				require.NotContains(t, string(historyJSON), "rejected.txt")
				leaseEvents, leaseEventsErr := rt.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: key.TenantId, JobIDs: []string{key.JobId}, PageSize: 100})
				if leaseEventsErr != nil {
					require.ErrorIs(t, leaseEventsErr, jobdb.ErrLeaseEventsUnsupported)
				}
				counts := rt.counts()
				var retained []jobdb.ArtifactKey
				resultOrdinals := map[string]int64{}
				for _, ch := range before {
					if b, ok := ch.Body.(jobdb.ExecutionChapter); ok {
						rec, err := jobdb.DecodeExecutionRecord(b.Record)
						require.NoError(t, err)
						if rec.Kind == "result" {
							resultOrdinals[rec.ID] = ch.Ordinal
						}
						if rec.Output != nil {
							for _, a := range rec.Output.Artifacts {
								if a.Key != nil {
									retained = append(retained, *a.Key)
									require.Equal(t, "retained", readExecutionArtifact(t, rt, key.TenantId, *a.Key))
								}
							}
						}
					}
				}
				require.NotEmpty(t, retained)
				engine, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w, prior, rejected, following).BuildEngine()
				require.NoError(t, err)
				inspection := w
				reached := 0
				overrideCalls := 0
				inspection.Execute = func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					overrideCalls++
					return w.Run(prefixScopeContext{c, func(site string) error {
						if site == "selected" {
							reached++
							require.Equal(t, 42, state)
							require.Equal(t, []int{41, 42}, priorState)
							return context.Canceled
						}
						return nil
					}}, in)
				}
				observer := &unfinishedObserver{}
				state = 0
				out, err := engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, JobWorker: inspection, Observer: observer})
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, out)
				require.Equal(t, 1, reached)
				require.EqualValues(t, 1, selectedBodies.Load())
				require.Len(t, observer.starts, 2)
				require.Len(t, observer.ends, 2)
				require.Empty(t, observer.jobEnds)
				require.Equal(t, "prior", observer.ends[0].TaskType)
				require.Equal(t, 1, overrideCalls)
				require.Equal(t, 2, notifications)
				require.Less(t, observer.ends[0].Ordinal, observer.ends[1].Ordinal)
				for i, event := range observer.ends {
					raw, err := event.Output.GetData()
					require.NoError(t, err)
					require.JSONEq(t, fmt.Sprintf(`{"value":%d}`, 41+i), string(raw))
					require.Equal(t, resultOrdinals[event.InvocationID], event.Ordinal)
					require.Equal(t, event.Ordinal, observer.starts[i].Ordinal)
					inputRaw, err := observer.starts[i].Input.GetData()
					require.NoError(t, err)
					require.JSONEq(t, `"job"`, string(inputRaw))
					require.NoError(t, event.Err)
				}
				// An unavailable earlier task must not be relabelled as the later rejection.
				for _, testChange := range []string{"mismatch", "missing", "builder", "unconsumed"} {
					change = testChange
					state = 0
					reached = 0
					_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, JobWorker: inspection})
					if change == "mismatch" || change == "unconsumed" || acceptedWait {
						require.ErrorIs(t, err, wf.ErrWorkflowNotDeterministic)
					} else if change == "builder" {
						require.ErrorContains(t, err, "builder failed")
					} else {
						var miss wf.ReplayCacheMissError
						require.ErrorAs(t, err, &miss)
					}
					require.Zero(t, reached)
					require.False(t, jobdb.IsChapterPayloadFailure(err))
				}
				change = ""
				// An earlier input-builder error takes precedence over the rejected wait.
				if mode == "external-input" {
					change = "reject-builder"
					out, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key})
					require.Nil(t, out)
					require.ErrorContains(t, err, "rejected task builder failed")
					require.False(t, jobdb.IsChapterPayloadFailure(err))
				}
				// A nonexistent selection cannot report a successful inspection.
				for _, target := range []string{"none", "full", "swallow"} {
					results := 0
					change = ""
					if target == "swallow" {
						change = "swallow"
					}
					full := w
					full.ObserveTaskResults = func(c context.Context) (context.Context, func(wf.TaskEndEvent)) {
						return c, func(wf.TaskEndEvent) { results++ }
					}
					absentReached := 0
					if target == "none" {
						full.Execute = func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
							return w.Run(prefixScopeContext{c, func(site string) error {
								if site == "absent" {
									absentReached++
									return context.Canceled
								}
								return nil
							}}, in)
						}
					}
					observer = &unfinishedObserver{}
					out, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, JobWorker: full, Observer: observer})
					require.Nil(t, out, target)
					var miss wf.ReplayCacheMissError
					require.False(t, errors.As(err, &miss), "terminal rejection must not expose rejected input through a cache miss")
					require.True(t, jobdb.IsChapterPayloadFailure(err), "%s: %v", target, err)
					var app jobdb.AppError
					require.ErrorAs(t, err, &app)
					wantJSON, marshalErr := json.Marshal(failure)
					require.NoError(t, marshalErr)
					gotJSON, marshalErr := json.Marshal(app.Payload)
					require.NoError(t, marshalErr)
					require.JSONEq(t, string(wantJSON), string(gotJSON))
					require.Zero(t, absentReached)
					require.Len(t, observer.starts, 2)
					require.Equal(t, 2, results)
					require.Len(t, observer.ends, 2)
					require.Empty(t, observer.jobEnds)
				}
				change = ""
				// Guarded operations before the boundary must keep their read-only error.
				for _, op := range []string{"submit", "restart", "yield", "secrets", "configuration"} {
					guarded := w
					guarded.Execute = func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
						return w.Run(prefixScopeContext{c, func(site string) error {
							if site != "selected" {
								return nil
							}
							switch op {
							case "submit":
								_, err := c.SubmitJob(ctx, jobdb.SubmitJob{})
								return err
							case "restart":
								_, err := c.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{})
								return err
							case "yield":
								return c.Yield(ctx, wf.YieldRequest{})
							case "secrets":
								_, err := c.ResolveSecrets(ctx, []string{"x"})
								return err
							default:
								_, err := c.GetConfiguration(ctx)
								return err
							}
						}}, in)
					}
					_, err = engine.ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: key, JobWorker: guarded})
					require.ErrorIs(t, err, wf.ErrReplayShouldNeverMutate, op)
				}
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
				afterInfo, err := rt.GetJob(ctx, key)
				require.NoError(t, err)
				require.Equal(t, info, afterInfo)
				if leaseEventsErr == nil {
					afterEvents, err := rt.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: key.TenantId, JobIDs: []string{key.JobId}, PageSize: 100})
					require.NoError(t, err)
					require.Equal(t, leaseEvents, afterEvents)
				}
				require.Equal(t, counts, rt.counts(), "no backing writes, uploads, submissions or lifecycle calls")
				for _, key := range retained {
					require.Equal(t, "retained", readExecutionArtifact(t, rt, "tenant", key))
				}
				// A terminal job cannot be resumed to repeat accepted effects.
				resume, err := runExecutionTest(t, rt, key, w, prior, rejected, following)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunFailed, resume.Status)
				require.True(t, jobdb.IsChapterPayloadFailure(resume.JobError))
				require.Equal(t, counts[:3], rt.counts()[:3], "resume must not write or submit")
				require.EqualValues(t, 2, priorRuns.Load())
				require.EqualValues(t, 2, priorBuilds.Load())
				require.Zero(t, followingRuns.Load())
				if mode == "local" {
					require.EqualValues(t, 1, rejectedRuns.Load())
				} else {
					require.Zero(t, rejectedRuns.Load())
				}
			})
		}
	}
}

func (r *prefixRuntime) StartOrAppendJob(c context.Context, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	panic("unexpected StartOrAppendJob in test stub")
}
