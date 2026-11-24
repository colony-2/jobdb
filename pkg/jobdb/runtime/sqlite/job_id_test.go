package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobidtest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestJobIDCoreRuntime(test *testing.T) {
	runCoreSQLiteConformance(test, func(test *testing.T, harnesses ...runtimetest.Harness) {
		for _, harness := range harnesses {
			test.Run(harness.Name, func(test *testing.T) {
				fixture := harness.New(test)
				jobidtest.Run(test, fixture.Runtime, false)
			})
		}
	})
}

func TestJobIDLegacyRestartReservation(test *testing.T) {
	ctx := celltest.Context(context.Background())
	config := Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(test.TempDir(), "jobdb.db")}
	runtime, err := NewFromConfig(ctx, config)
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, runtime.Close(ctx)) })
	source, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "tenant", Cell: "work", CellRepo: "example.test/work", JobType: "work", JobID: "source",
		Data: jobdb.NewTaskDataOrPanic(nil),
	}})
	require.NoError(test, err)
	request := jobdb.SubmitRestartJobRequest{RequestID: "saved-restart", Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, Cell: source.Cell}}
	preparation, err := goalstate.RestartPreparation(ctx, request, "")
	require.NoError(test, err)
	preparation.Key.JobId = "0ujsswThIGTUYm2K8FjOOfXtY1K"
	reserved, err := runtime.PrepareJob(ctx, preparation)
	require.NoError(test, err)
	require.False(test, reserved.Published)
	_, err = runtime.GetJob(ctx, reserved.Key)
	require.ErrorIs(test, err, jobdb.ErrJobNotFound)
	require.NoError(test, runtime.Close(ctx))
	runtime, err = NewFromConfig(ctx, config)
	require.NoError(test, err)
	accepted, err := runtime.SubmitRestartJob(ctx, request)
	require.NoError(test, err)
	require.Equal(test, reserved.Key, accepted.JobKey)
	require.NoError(test, runtime.Close(ctx))
	runtime, err = NewFromConfig(ctx, config)
	require.NoError(test, err)
	replayed, err := runtime.SubmitRestartJob(ctx, request)
	require.NoError(test, err)
	require.Equal(test, accepted, replayed)
	_, err = runtime.GetJob(ctx, reserved.Key)
	require.NoError(test, err)
}
