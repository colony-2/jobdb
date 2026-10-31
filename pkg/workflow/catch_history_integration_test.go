package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func catchAt(t *testing.T, h wf.CatchHistory, site string) wf.CatchAttempt {
	t.Helper()
	for _, a := range h.Attempts {
		if a.Site == site {
			return a
		}
	}
	t.Fatalf("missing site %q in %+v", site, h)
	return wf.CatchAttempt{}
}
func catchGap(a wf.CatchAttempt, field, reason string) bool {
	for _, g := range a.Gaps {
		if g.Field == field && g.Reason == reason {
			return true
		}
	}
	return false
}
func readCatches(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey) wf.CatchHistory {
	t.Helper()
	h, err := wf.ReadCatchHistory(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
	require.NoError(t, err)
	return h
}

func TestCatchHistoryDispositions(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		for _, mode := range []string{"handled", "continue-tag", "route-tag", "handled-error", "unmatched", "predicate-panic", "handler-error", "replacement", "propagate", "continue", "retry", "exhausted", "invalid", "success"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				recording, inspection := catchHistoryRuntimes(t, backend)
				var callbacks atomic.Int32
				call := invocation("caught/\"scope:\"#2")
				call.Catch = []wf.CatchRule{{ID: "rule/with:punctuation", Match: func(error) bool {
					callbacks.Add(1)
					if mode == "predicate-panic" {
						panic("predicate failure")
					}
					return mode != "unmatched"
				}, Handle: func(c wf.ExecutionContext, _ error) (wf.CatchResult, error) {
					callbacks.Add(1)
					switch mode {
					case "handler-error":
						return wf.CatchResult{}, errors.New("handler broke")
					case "replacement":
						return wf.CatchResult{Disposition: wf.CatchPropagate, Err: errors.New("replacement")}, nil
					case "propagate":
						return wf.CatchResult{Disposition: wf.CatchPropagate}, nil
					case "continue", "retry", "exhausted":
						return wf.CatchResult{Disposition: wf.CatchContinue}, nil
					case "invalid":
						return wf.CatchResult{Disposition: "invalid"}, nil
					case "handled-error":
						return wf.CatchResult{Disposition: wf.CatchHandled, Err: errors.New("accepted error")}, nil
					}
					output, err := c.DoTask(invocation("artifact-producer"), func(context.Context) (jobdb.TaskData, error) {
						return jobdb.NewTaskDataOrPanic(map[string]string{"action": mode}), nil
					})
					return wf.CatchResult{Disposition: wf.CatchHandled, Output: output}, err
				}}}
				if mode == "retry" || mode == "exhausted" {
					call.Policy.Retry = jobdb.RetryPolicy{MaximumAttempts: 2, InitialInterval: jobdb.Duration(time.Millisecond)}
				}
				var runs atomic.Int32
				task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
					n := runs.Add(1)
					raw, _ := in.GetData()
					if string(raw) != `"job"` {
						return jobdb.NewTaskDataOrPanic(map[string]string{"action": mode}, jobdb.NewArtifactFromBytes("result.txt", []byte("caught output"))), nil
					}
					if mode == "success" || mode == "retry" && n == 2 {
						return jobdb.NewTaskDataOrPanic("success"), nil
					}
					return nil, errors.New("original failure")
				}}
				w := wf.ExecutionWorker{JobType: "catch-cases", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { callbacks.Add(1); return in, nil })
				}}
				key := submitExecutionTest(t, recording, w)
				// Invalid disposition is an engine error; its buffered selection may remain
				// invisible. All other executions have an authoritative final outcome.
				run, err := wf.GetJobForRun(context.Background(), recording, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "cases", LeaseDuration: time.Minute})
				require.NoError(t, err)
				out, runErr := run.Run(nil)
				require.NoError(t, runErr)
				if mode == "invalid" {
					require.Equal(t, wf.JobRunFailed, out.Status)
					require.Contains(t, out.JobError.Error(), "invalid catch disposition")
				}
				before, err := inspection.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				beforeBytes, err := json.Marshal(before)
				require.NoError(t, err)
				defer func() {
					final, err := inspection.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
					require.NoError(t, err)
					finalBytes, err := json.Marshal(final)
					require.NoError(t, err)
					require.Equal(t, beforeBytes, finalBytes, "all inspection and returned-buffer mutations must preserve stored records, metadata and chapter envelopes")
				}()
				summary, err := inspection.GetJob(context.Background(), key)
				require.NoError(t, err)
				calls, executions := callbacks.Load(), runs.Load()
				// Inspection machinery has no engine or callback registration.
				h := readCatches(t, inspection, key)
				if mode == "success" {
					require.Empty(t, h.Attempts)
					return
				}
				a := catchAt(t, h, call.Site)
				require.Equal(t, wf.CatchBoundaryTask, a.BoundaryKind)
				require.Equal(t, key, a.Boundary.JobKey)
				require.NotEmpty(t, a.Boundary.JobAttemptID)
				require.NotEmpty(t, a.ParentID)
				require.Equal(t, 1, a.Boundary.AttemptNumber)
				require.NotNil(t, a.Original)
				require.Equal(t, "AppError", a.Original.Kind)
				require.Contains(t, string(a.Original.Data), "original failure")
				require.True(t, catchGap(a, wf.CatchGapOriginalOrigin, wf.CatchGapNotRecorded))
				switch mode {
				case "unmatched":
					require.Equal(t, wf.CatchSelectionUnmatched, a.Selection)
					require.Empty(t, a.RuleID)
					require.Nil(t, a.Accepted)
				case "predicate-panic":
					require.Equal(t, wf.CatchSelectionPredicateError, a.Selection)
					require.NotNil(t, a.PredicateError)
					require.Contains(t, string(a.PredicateError.Data), "predicate failure")
					require.Nil(t, a.Accepted)
				case "invalid":
					require.NotEqual(t, wf.CatchHandlingAccepted, a.Handling)
					require.Nil(t, a.Accepted)
				default:
					require.Equal(t, wf.CatchSelectionSelected, a.Selection)
					require.Equal(t, call.Catch[0].ID, a.RuleID)
					require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
					require.NotNil(t, a.Accepted)
					require.Equal(t, wf.CatchProvenanceUnknown, a.Accepted.Provenance)
					require.True(t, catchGap(a, wf.CatchGapAcceptedProvenance, wf.CatchGapNotRecorded))
					switch mode {
					case "handled", "continue-tag", "route-tag":
						require.Equal(t, wf.CatchHandled, a.Accepted.Disposition)
						require.Nil(t, a.Accepted.Failure)
						require.JSONEq(t, fmt.Sprintf(`{"action":%q}`, mode), string(a.Accepted.Output.Data))
						require.Len(t, a.Accepted.Output.Artifacts, 1)
						art := a.Accepted.Output.Artifacts[0]
						require.NotNil(t, art.Key)
						require.Equal(t, "caught output", readExecutionArtifact(t, inspection, key.TenantId, *art.Key))
					case "handled-error":
						require.Equal(t, wf.CatchHandled, a.Accepted.Disposition)
						require.NotNil(t, a.Accepted.Failure)
					case "handler-error", "replacement", "propagate":
						require.Equal(t, wf.CatchPropagate, a.Accepted.Disposition)
						require.NotNil(t, a.Accepted.Failure)
					default:
						require.Equal(t, wf.CatchContinue, a.Accepted.Disposition)
						require.NotNil(t, a.Accepted.Failure)
					}
				}
				if mode == "retry" || mode == "exhausted" {
					require.True(t, a.Retry.Retry)
					require.NotNil(t, a.Retry.WakeAt)
					require.EqualValues(t, 2, runs.Load())
					if mode == "exhausted" {
						var found bool
						for _, later := range h.Attempts {
							if later.Site == call.Site && later.Boundary.AttemptNumber == 2 {
								found = true
								require.Equal(t, a.Boundary.BoundaryID, later.Boundary.BoundaryID)
								require.NotEqual(t, a.Boundary.AttemptID, later.Boundary.AttemptID)
								require.False(t, later.Retry.Retry)
								require.Nil(t, later.Retry.WakeAt)
							}
						}
						require.True(t, found)
					}
				} else if mode != "invalid" {
					require.NotNil(t, a.Retry)
					require.False(t, a.Retry.Retry)
					require.Nil(t, a.Retry.WakeAt)
				}
				if mode == "handled" || mode == "continue-tag" || mode == "route-tag" || mode == "retry" {
					require.Equal(t, wf.JobRunCompleted, out.Status)
				} else if mode != "invalid" {
					require.Equal(t, wf.JobRunFailed, out.Status)
				}
				// Mutate application callbacks after recording; no inspection may consult them.
				call.Catch[0].Match = func(error) bool { panic("inspection predicate") }
				call.Catch[0].Handle = func(wf.ExecutionContext, error) (wf.CatchResult, error) { panic("inspection handler") }
				for flags := 0; flags < 16; flags++ {
					other, err := wf.ReadCatchHistory(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: flags&1 != 0, IncludeOutputs: flags&2 != 0, IncludeArtifacts: flags&4 != 0, IncludeAttemptInputs: flags&8 != 0})
					require.NoError(t, err)
					require.Equal(t, h, other)
				}
				require.Equal(t, calls, callbacks.Load())
				require.Equal(t, executions, runs.Load())
				after, err := inspection.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
				summaryAfter, err := inspection.GetJob(context.Background(), key)
				require.NoError(t, err)
				require.Equal(t, summary, summaryAfter)
				// Returned buffers, including pointers within artifact descriptors, are owned.
				saved := readCatches(t, inspection, key)
				a.Original.Data[0] = '!'
				if a.Accepted != nil {
					if a.Accepted.Output != nil {
						a.Accepted.Output.Data[0] = '!'
						if len(a.Accepted.Output.Artifacts) > 0 {
							a.Accepted.Output.Artifacts[0].Name = "changed"
							a.Accepted.Output.Artifacts[0].Key.Name = "changed"
						}
					}
					if a.Accepted.Failure != nil {
						a.Accepted.Failure.Data[0] = '!'
					}
				}
				require.Equal(t, saved, readCatches(t, inspection, key))
				savedAttempt := catchAt(t, saved, call.Site)
				if savedAttempt.Accepted != nil && savedAttempt.Accepted.Output != nil && len(savedAttempt.Accepted.Output.Artifacts) > 0 {
					require.Equal(t, "caught output", readExecutionArtifact(t, inspection, key.TenantId, *savedAttempt.Accepted.Output.Artifacts[0].Key))
				}
			})
		}
	}
}

func TestCatchHistoryCompactIdentity(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			var callbacks atomic.Int32
			rule := wf.CatchRule{ID: "same", Match: func(error) bool { callbacks.Add(1); return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				callbacks.Add(1)
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("ok")}, nil
			}}
			w := wf.ExecutionWorker{JobType: "compact-catches", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				// Organizational parents are omitted; references must still be exact.
				return c.DoScope("omitted / parent", wf.ScopeOptions{}, func(p wf.ExecutionContext) (jobdb.JobData, error) {
					for i := 0; i < 3; i++ {
						_, err := p.DoScope("repeat", wf.ScopeOptions{Catch: []wf.CatchRule{rule}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("same") })
						if err != nil {
							return nil, err
						}
					}
					return nil, errors.New("later unrelated failure")
				})
			}}
			key := submitExecutionTest(t, recording, w)
			out, err := runExecutionTest(t, recording, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			h := readCatches(t, inspection, key)
			ids := map[string]bool{}
			parent := ""
			count := 0
			for _, a := range h.Attempts {
				if a.Site != "repeat" {
					continue
				}
				count++
				require.Equal(t, wf.CatchBoundaryScope, a.BoundaryKind)
				require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
				require.Equal(t, "same", a.RuleID)
				require.False(t, ids[a.Boundary.BoundaryID])
				ids[a.Boundary.BoundaryID] = true
				if parent == "" {
					parent = a.ParentID
				}
				require.Equal(t, parent, a.ParentID)
			}
			require.Equal(t, 3, count)
			require.NotEmpty(t, parent)
			before := callbacks.Load()
			require.Equal(t, h, readCatches(t, inspection, key))
			require.Equal(t, before, callbacks.Load())
			// Fixture expected facts are independent of opaque identity encoding.
			info, err := jobdb.GetJobRun(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			ord := map[int64]int{}
			for _, e := range info.Execution {
				if e.Record.Kind == "handled" {
					ord[e.Ordinal]++
				}
			}
			require.Len(t, ord, 1)
		})
	}
}

func TestCatchHistoryOrdinaryRetryAndRootAncestry(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			w := wf.ExecutionWorker{JobType: "root-retry", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				call := invocation("ordinary-retry")
				call.Policy.Retry = jobdb.RetryPolicy{MaximumAttempts: 2, InitialInterval: jobdb.Duration(time.Millisecond)}
				return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			}}
			task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) { return nil, errors.New("retry me") }}
			handle, err := recording.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("job"), RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 2, InitialInterval: jobdb.Duration(time.Millisecond)}}}})
			require.NoError(t, err)
			out, err := runExecutionTest(t, recording, handle.JobKey, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			h := readCatches(t, inspection, handle.JobKey)
			roots := map[string]bool{}
			children := map[string]int{}
			attemptIDs := map[string]bool{}
			for _, a := range h.Attempts {
				require.False(t, attemptIDs[a.Boundary.AttemptID])
				attemptIDs[a.Boundary.AttemptID] = true
				if a.Site == "$job" {
					require.Empty(t, a.ParentID)
					require.Equal(t, a.Boundary.AttemptID, a.Boundary.JobAttemptID)
					roots[a.Boundary.JobAttemptID] = true
				} else {
					require.Equal(t, "ordinary-retry", a.Site)
					children[a.Boundary.JobAttemptID]++
					require.NotEqual(t, wf.CatchHandlingAccepted, a.Handling)
					require.Nil(t, a.Accepted)
					require.Equal(t, wf.CatchSelectionUnmatched, a.Selection)
				}
				require.NotNil(t, a.Retry)
				require.Equal(t, a.Boundary.AttemptNumber == 1, a.Retry.Retry)
			}
			require.Len(t, roots, 2)
			require.Len(t, children, 2)
			for root, n := range children {
				require.True(t, roots[root])
				require.Equal(t, 2, n)
			}
		})
	}
}

// Assert the public data contract, including a consumer's unknown-state fallback.
func TestCatchHistoryPublicConstants(t *testing.T) {
	require.Equal(t, "selected", wf.CatchSelectionSelected)
	require.Equal(t, "accepted", wf.CatchHandlingAccepted)
	require.Equal(t, "predicate_error", wf.CatchSelectionPredicateError)
	require.Equal(t, "unknown", wf.CatchProvenanceUnknown)
	a := wf.CatchAttempt{Selection: "future", Handling: "future", Accepted: &wf.CatchAcceptedResult{Disposition: wf.CatchHandled}}
	successful := a.Selection == wf.CatchSelectionSelected && a.Handling == wf.CatchHandlingAccepted && a.Accepted != nil && a.Accepted.Disposition == wf.CatchHandled && a.Accepted.Failure == nil
	require.False(t, successful)
	_, err := json.Marshal(wf.CatchHistory{})
	require.NoError(t, err)
}

func TestCatchHistoryNestedHandlerBoundaries(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			recoverRule := wf.CatchRule{ID: "same", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("ok")}, nil
			}}
			w := wf.ExecutionWorker{JobType: "nested-handler", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("outer", wf.ScopeOptions{Catch: []wf.CatchRule{recoverRule}}, func(o wf.ExecutionContext) (jobdb.JobData, error) {
					innerRule := recoverRule
					innerRule.ID = `strange/attempt:19/scope:"fake"#1`
					innerRule.Handle = func(h wf.ExecutionContext, _ error) (wf.CatchResult, error) {
						_, err := h.DoScope("handler-child", wf.ScopeOptions{Catch: []wf.CatchRule{recoverRule}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("identical failure") })
						if err != nil {
							return wf.CatchResult{}, err
						}
						return wf.CatchResult{Disposition: wf.CatchPropagate}, nil
					}
					return o.DoScope("inner", wf.ScopeOptions{Catch: []wf.CatchRule{innerRule}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("identical failure") })
				})
			}}
			key := submitExecutionTest(t, recording, w)
			out, err := runExecutionTest(t, recording, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			h := readCatches(t, inspection, key)
			require.Len(t, h.Attempts, 3)
			outer, inner, child := catchAt(t, h, "outer"), catchAt(t, h, "inner"), catchAt(t, h, "handler-child")
			require.Equal(t, outer.Boundary.BoundaryID, inner.ParentID)
			require.Equal(t, inner.Boundary.BoundaryID, child.ParentID)
			require.Equal(t, outer.Boundary.JobAttemptID, child.Boundary.JobAttemptID)
			require.Equal(t, wf.CatchPropagate, inner.Accepted.Disposition)
			require.NotNil(t, inner.Accepted.Failure)
			require.Nil(t, outer.Accepted.Failure)
			require.Nil(t, child.Accepted.Failure)
			for _, a := range h.Attempts {
				require.True(t, catchGap(a, wf.CatchGapOriginalOrigin, wf.CatchGapNotRecorded))
				require.Equal(t, wf.CatchProvenanceUnknown, a.Accepted.Provenance)
			}
		})
	}
}
