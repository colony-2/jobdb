package workflow_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Independent input and output jobs contain genuinely accepted application
// bytes. An error-only rejection marker cannot satisfy these fixtures.
func TestReplayRetainedOversizedPayload(t *testing.T) {
	for _, transport := range []string{"sqlite", "remote"} {
		for _, field := range []string{"input", "output"} {
			t.Run(transport+"/"+field, func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				path := filepath.Join(t.TempDir(), "legacy.db")
				historical, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: path, MaxChapterApplicationPayloadBytes: 64 * 1024})
				require.NoError(t, err)
				large := jobdb.NewTaskDataOrPanic(strings.Repeat(field, 1500))
				largeRaw, err := large.GetData()
				require.NoError(t, err)
				require.Greater(t, len(largeRaw), 128)
				input := jobdb.NewTaskDataOrPanic("small")
				if field == "input" {
					input = large
				}
				var callbacks atomic.Int32
				var inputSeen, dependentState int
				var reconstructedInput []byte
				w := wf.ExecutionWorker{JobType: "legacy", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					raw, err := in.GetData()
					if err != nil {
						return nil, err
					}
					inputSeen = len(raw)
					reconstructedInput = append([]byte(nil), raw...)
					out, err := c.DoTask(invocation("retained"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
					if err != nil {
						return nil, err
					}
					raw, err = out.GetData()
					if err != nil {
						return nil, err
					}
					dependentState = len(raw)
					return out, nil
				}}
				task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
					callbacks.Add(1)
					if field == "output" {
						return large, nil
					}
					return jobdb.NewTaskDataOrPanic("derived"), nil
				}}
				h, err := celltest.Submit(historical, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: w.Name(), Data: input}})
				require.NoError(t, err)
				result, err := runExecutionTest(t, historical, h.JobKey, w, task)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, result.Status)
				wantInput, wantState := inputSeen, dependentState
				wantInputBytes := append([]byte(nil), reconstructedInput...)
				before, err := historical.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
				require.NoError(t, err)
				var retainedOrdinal int64
				if field == "output" {
					for _, ch := range before {
						if body, ok := ch.Body.(jobdb.ExecutionChapter); ok {
							rec, err := jobdb.DecodeExecutionRecord(body.Record)
							require.NoError(t, err)
							if rec.Kind == "result" && strings.Contains(rec.ID, `/task:"retained"`) {
								require.Equal(t, []byte(largeRaw), []byte(rec.Output.Data))
								retainedOrdinal = ch.Ordinal
							}
						}
					}
					require.Positive(t, retainedOrdinal)
				}
				require.NoError(t, historical.Close(ctx))
				current, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: path, MaxChapterApplicationPayloadBytes: 128})
				require.NoError(t, err)
				defer current.Close(ctx)
				var rt jobdb.WorkflowRuntime = current
				if transport == "remote" {
					server := httptest.NewServer(celltest.Handler(remote.NewServer(current)))
					defer server.Close()
					rt, err = remote.New(server.URL, server.Client())
					require.NoError(t, err)
				}
				chapter, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: retainedOrdinal})
				require.NoError(t, err)
				if field == "input" {
					require.Equal(t, []byte(largeRaw), []byte(chapter.Body.(jobdb.JobStartChapter).Input.Data))
				} else {
					rec, err := jobdb.DecodeExecutionRecord(chapter.Body.(jobdb.ExecutionChapter).Record)
					require.NoError(t, err)
					require.Equal(t, []byte(largeRaw), []byte(rec.Output.Data))
				}
				inputSeen, dependentState = 0, 0
				reconstructedInput = nil
				out, err := replayExecutionTest(t, rt, h.JobKey, w, task)
				require.NoError(t, err)
				raw, err := out.GetData()
				require.NoError(t, err)
				if field == "output" {
					require.Equal(t, []byte(largeRaw), []byte(raw))
				} else {
					require.JSONEq(t, `"derived"`, string(raw))
				}
				require.Equal(t, wantInputBytes, reconstructedInput)
				require.Equal(t, wantInput, inputSeen)
				require.Equal(t, wantState, dependentState)
				require.EqualValues(t, 1, callbacks.Load())
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
				require.NoError(t, err)
				require.Equal(t, before, after)
				// The compatibility read path must not relax today's write boundary.
				_, err = celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: w.Name(), Data: large}})
				require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
			})
		}
	}
}
