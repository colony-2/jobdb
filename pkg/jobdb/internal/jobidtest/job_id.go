package jobidtest

import (
	"context"
	"io"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

// Run exercises the allocation policy only for JobDB-owned runtime fixtures.
func Run(test *testing.T, runtime jobdb.ConsumerRuntime, fullRuntime bool) {
	test.Helper()
	ctx := celltest.Context(context.Background())
	const tenant, artifactName, content = "job-id-tenant", "input.txt", "saved input"
	request := func(id string) jobdb.SubmitJobRequest {
		data := jobdb.NewTaskDataOrPanic("original")
		if fullRuntime {
			data = jobdb.NewTaskDataOrPanic("original", jobdb.NewArtifactFromBytes(artifactName, []byte(content)))
		}
		return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: tenant, Cell: "work", CellRepo: "example.test/work", JobType: "work", JobID: id,
			Data: data,
		}}
	}
	lookup := func(key jobdb.JobKey) {
		test.Helper()
		require.Equal(test, tenant, key.TenantId)
		info, err := runtime.GetJob(ctx, key)
		require.NoError(test, err)
		require.Equal(test, "work", info.Cell)
		_, err = runtime.GetJob(ctx, jobdb.JobKey{TenantId: "another-tenant", JobId: key.JobId})
		require.ErrorIs(test, err, jobdb.ErrJobNotFound)
	}

	generated, err := celltest.Submit(runtime, ctx, request(""))
	require.NoError(test, err)
	require.Regexp(test, `^C2[0-9A-Za-z]{27}$`, generated.JobKey.JobId)
	lookup(generated.JobKey)

	const legacyID = "0ujsswThIGTUYm2K8FjOOfXtY1K"
	legacy, err := runtime.SubmitJob(ctx, request(legacyID))
	require.NoError(test, err)
	require.Equal(test, legacyID, legacy.JobKey.JobId)
	lookup(legacy.JobKey)
	again, err := runtime.SubmitJob(ctx, request(legacyID))
	require.NoError(test, err)
	require.Equal(test, legacy, again)
	chapters, err := runtime.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: legacy.JobKey})
	require.NoError(test, err)
	require.Len(test, chapters, 1)
	require.JSONEq(test, `"original"`, string(chapters[0].Body.(jobdb.JobStartChapter).Input.Data))
	if fullRuntime {
		artifact, err := runtime.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: legacy.JobKey, Ordinal: 0, Name: artifactName})
		require.NoError(test, err)
		reader, err := artifact.Open()
		require.NoError(test, err)
		data, err := io.ReadAll(reader)
		require.NoError(test, reader.Close())
		require.NoError(test, err)
		require.Equal(test, content, string(data))
	}

	restartReq := jobdb.SubmitRestartJobRequest{RequestID: "restart", Job: jobdb.SubmitRestartJob{
		PriorJobKey: legacy.JobKey, Cell: legacy.Cell, LastStepToKeep: 0,
	}}
	restarted, err := runtime.SubmitRestartJob(ctx, restartReq)
	require.NoError(test, err)
	require.Regexp(test, `^C2[0-9A-Za-z]{27}$`, restarted.JobKey.JobId)
	lookup(restarted.JobKey)
	again, err = runtime.SubmitRestartJob(ctx, restartReq)
	require.NoError(test, err)
	require.Equal(test, restarted, again)
	if !fullRuntime {
		return
	}

	failReq := jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: tenant, GoalID: generated.GoalID}, Message: "test failure allocation"}
	failed, err := runtime.FailGoal(ctx, failReq)
	require.NoError(test, err)
	require.Regexp(test, `^C2[0-9A-Za-z]{27}$`, failed.FailureJobID)
	closure := jobdb.JobKey{TenantId: tenant, JobId: failed.FailureJobID}
	lookup(closure)
	info, err := runtime.GetJob(ctx, closure)
	require.NoError(test, err)
	require.Equal(test, jobdb.JobRecordGoalFailure, info.RecordKind)
	replay, err := runtime.FailGoal(ctx, failReq)
	require.NoError(test, err)
	require.Equal(test, failed, replay)
}
