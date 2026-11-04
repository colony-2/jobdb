package toyimpl

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestRestartDoesNotRevalidateInheritedChapters(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt := New(WithMaxChapterApplicationPayloadBytes(2 * 1024 * 1024))
	source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobID: "legacy", JobType: "legacy", Data: jobdb.NewTaskDataOrPanic(strings.Repeat("x", 300000))}})
	require.NoError(t, err)
	leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "writer", Routes: []jobdb.Route{{JobType: "legacy"}}, Limit: 1})
	require.NoError(t, err)
	require.Len(t, leases, 1)
	ch := jobdb.Chapter{Ordinal: 1, TaskType: "task", Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: json.RawMessage(`null`)}}}}
	require.NoError(t, rt.PutChapter(ctx, jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: source.JobKey, Ordinal: 1}, LeaseID: leases[0].LeaseID(), Chapter: ch}))
	rt.engine.maxChapterApplicationPayloadBytes = 0
	for _, schema := range []json.RawMessage{nil, json.RawMessage(`{"chapterShape":false}`)} {
		req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, LastStepToKeep: 0}}
		if schema != nil {
			req.Job.Schema = &jobdb.JobSchemaSelector{Schema: schema}
		}
		clone, err := rt.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		inherited, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: clone.JobKey, Ordinal: 0})
		require.NoError(t, err)
		original, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: source.JobKey, Ordinal: 0})
		require.NoError(t, err)
		require.Equal(t, original, inherited)
	}
}
