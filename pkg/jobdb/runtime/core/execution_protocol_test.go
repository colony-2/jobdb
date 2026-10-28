package runtimecore_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type executionCompletionChapters struct {
	*taskCompletionChapters
	input runtimecore.EncodedChapter
}

func (c *executionCompletionChapters) Get(_ context.Context, _ runtimecore.ChapterLogKey, ordinal int64) (runtimecore.EncodedChapter, error) {
	if ordinal == c.input.Ordinal {
		return c.input, nil
	}
	return runtimecore.EncodedChapter{}, jobdb.ErrChapterNotFound
}
func (c *executionCompletionChapters) Count(context.Context, runtimecore.ChapterLogKey) (int64, error) {
	return c.input.Ordinal + 1, nil
}

func TestExecutionCompletionIndependentStores(t *testing.T) {
	for _, mode := range []string{"success", "stale-generation", "missing-input", "expired"} {
		t.Run(mode, func(t *testing.T) {
			wait := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "wait", ID: "scope/task/attempt:1", Activation: "activation", At: time.Now().UTC(), Descriptor: json.RawMessage(`"external"`), Input: &jobdb.ExecutionSnapshot{Data: json.RawMessage(`{"input":"explicit"}`)}}
			if mode == "expired" {
				wait.Deadline = time.Now().Add(-time.Second)
			}
			raw, err := json.Marshal(wait)
			require.NoError(t, err)
			hash, err := runtimecodec.ExecutionIdentity(wait)
			require.NoError(t, err)
			scheduler := &taskCompletionScheduler{waiting: runtimecore.WaitingTaskSnapshot{JobType: "task-route", Task: runtimecore.TaskWork{TaskType: "external", ResumeJobType: "job-route", InputOrdinal: 3, OutputOrdinal: 4, InputHash: hash}}}
			encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: jobdb.Chapter{Ordinal: 3, TaskType: "__execution__", Body: jobdb.ExecutionChapter{Record: raw}}})
			require.NoError(t, err)
			chapters := &executionCompletionChapters{taskCompletionChapters: &taskCompletionChapters{scheduler: scheduler}, input: encoded}
			rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: scheduler, Chapters: chapters, Schemas: newMemorySchemaStore()})
			require.NoError(t, err)
			req := jobdb.CompleteTaskIfWaitingRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Route: jobdb.Route{JobType: "task-route", TaskType: "external"}, ResumeJobType: "job-route", InputOrdinal: 3, OutputOrdinal: 4, InputHash: hash, Data: jobdb.NewTaskDataOrPanic("answer")}
			if mode == "stale-generation" {
				req.InputHash = "wrong"
			}
			if mode == "missing-input" {
				req.InputOrdinal = 0
			}
			err = rt.CompleteTaskIfWaiting(context.Background(), req)
			if mode != "success" && mode != "expired" {
				require.ErrorIs(t, err, jobdb.ErrConflict)
				require.Nil(t, chapters.output)
				require.Empty(t, scheduler.events)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, chapters.output)
			require.Equal(t, []string{"claim", "validate", "append", "reschedule"}, scheduler.events)
			ch, err := runtimecore.DecodeChapter(*chapters.output)
			require.NoError(t, err)
			body, ok := ch.Body.(jobdb.ExecutionChapter)
			require.True(t, ok)
			result, err := jobdb.DecodeExecutionRecord(body.Record)
			require.NoError(t, err)
			require.Equal(t, wait.ID, result.ID)
			require.Equal(t, wait.Input, result.Input)
			require.Equal(t, "result", result.Kind)
			require.JSONEq(t, `"answer"`, string(result.Output.Data))
		})
	}
}
