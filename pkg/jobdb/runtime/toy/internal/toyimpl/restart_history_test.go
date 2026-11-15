package toyimpl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"io"
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

func TestRestartRetainedArtifactContents(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt := New()
	submit := func(tenant string) jobdb.JobHandle {
		h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: tenant, JobID: "source", JobType: "recipe", Data: jobdb.NewTaskDataOrPanic("original", jobdb.NewArtifactFromBytes("recipe", []byte(tenant)), jobdb.NewArtifactFromBytes("empty", []byte{}))}})
		require.NoError(t, err)
		l, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "writer", Routes: []jobdb.Route{{JobType: "recipe"}}})
		require.NoError(t, err)
		for i := int64(1); i <= 3; i++ {
			data := []byte(fmt.Sprintf("%s-%d", tenant, i))
			ch := jobdb.Chapter{Ordinal: i, TaskType: "task", Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: json.RawMessage(`null`)}}}}
			require.NoError(t, rt.PutChapter(ctx, jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: i}, LeaseID: l.LeaseID(), Chapter: ch, ArtifactUploads: []jobdb.ArtifactUpload{{Name: "result", Size: int64(len(data)), Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}}}))
		}
		return h
	}
	for _, tenant := range []string{"first", "second"} {
		source := submit(tenant)
		history, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source.JobKey})
		require.NoError(t, err)
		req := jobdb.SubmitRestartJobRequest{RequestID: "copy", Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, JobID: "destination", LastStepToKeep: 2}}
		dest, err := rt.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		copied, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: dest.JobKey})
		require.NoError(t, err)
		require.Equal(t, history[:3], copied)
		for ordinal, ch := range copied {
			for _, art := range ch.Artifacts {
				sourceKey := runtimeArtifactKey{jobKey: source.JobKey, ordinal: int64(ordinal), name: art.Name}
				destKey := runtimeArtifactKey{jobKey: dest.JobKey, ordinal: int64(ordinal), name: art.Name}
				require.Equal(t, rt.engine.runtimeArtifacts[sourceKey], rt.engine.runtimeArtifacts[destKey])
				reader, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: dest.JobKey, Ordinal: int64(ordinal), Name: art.Name})
				require.NoError(t, err)
				rc, err := reader.Open()
				require.NoError(t, err)
				got, err := io.ReadAll(rc)
				require.NoError(t, err)
				require.NoError(t, rc.Close())
				require.Equal(t, string(rt.engine.runtimeArtifacts[sourceKey]), string(got))
			}
		}
		_, ok := rt.engine.runtimeArtifacts[runtimeArtifactKey{jobKey: dest.JobKey, ordinal: 3, name: "result"}]
		require.False(t, ok)
		again, err := rt.SubmitRestartJob(ctx, req)
		require.NoError(t, err)
		require.Equal(t, dest, again)
		req.RequestID = "conflict"
		req.Job.LastStepToKeep = 1
		_, err = rt.SubmitRestartJob(ctx, req)
		require.ErrorIs(t, err, jobdb.ErrExistingJobMismatch)
		unchanged, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: dest.JobKey})
		require.NoError(t, err)
		require.Equal(t, copied, unchanged)
		unchanged, err = rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source.JobKey})
		require.NoError(t, err)
		require.Equal(t, history, unchanged)
	}
}

func TestRestartMissingArtifactAndPublicationFailureLeaveNoDestination(t *testing.T) {
	for _, failure := range []string{"missing-bytes", "decode", "publication"} {
		t.Run(failure, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			rt := New()
			source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobID: "source", JobType: "recipe", Data: jobdb.NewTaskDataOrPanic("original", jobdb.NewArtifactFromBytes("a", []byte("first")), jobdb.NewArtifactFromBytes("b", []byte("second")))}})
			require.NoError(t, err)
			req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, JobID: "destination"}}
			if failure == "missing-bytes" {
				delete(rt.engine.runtimeArtifacts, runtimeArtifactKey{jobKey: source.JobKey, ordinal: 0, name: "b"})
			}
			if failure == "decode" {
				ch := rt.engine.runtimeChapters[source.JobKey][0]
				ch.Metadata = jobdb.ChapterMetadata{Fields: map[string]jobdb.ChapterMetadataValue{"run_policy": {Kind: jobdb.ChapterMetadataString, String: "bad"}}}
				rt.engine.runtimeChapters[source.JobKey][0] = ch
			}
			if failure == "publication" {
				// Prepare an explicit destination, then invalidate the prepared goal's
				// dependency. Publication must fail after artifact staging.
				p, err := goalstate.RestartPreparation(ctx, req, "")
				require.NoError(t, err)
				_, err = rt.PrepareJob(ctx, p)
				require.NoError(t, err)
				prepared := rt.engine.preparedGoals[jobdb.JobKey{TenantId: "tenant", JobId: "destination"}]
				prepared.Request.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: "missing"}, Condition: jobdb.JobPrereqSuccess}}
				rt.engine.preparedGoals[jobdb.JobKey{TenantId: "tenant", JobId: "destination"}] = prepared
			}
			_, err = rt.SubmitRestartJob(ctx, req)
			require.Error(t, err)
			dest := jobdb.JobKey{TenantId: "tenant", JobId: "destination"}
			require.NotContains(t, rt.engine.jobRecords, dest)
			require.NotContains(t, rt.engine.runtimeChapters, dest)
			require.NotContains(t, rt.engine.preparedGoals, dest)
			for key := range rt.engine.runtimeArtifacts {
				require.NotEqual(t, dest, key.jobKey)
			}
			require.Contains(t, rt.engine.jobRecords, source.JobKey)
		})
	}
}
