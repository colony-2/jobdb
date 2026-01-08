package swf_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/colony-2/swf-go/pkg/swf/toy"
	"github.com/stretchr/testify/require"
)

const (
	artifactCleanupJobName  = "artifact-cleanup-job"
	artifactCleanupTaskName = "artifact-cleanup-task"
)

func TestArtifactCleanupAcrossEngines(t *testing.T) {
	t.Run("toy", func(t *testing.T) {
		ctx := context.Background()
		tempDir := t.TempDir()
		fileNames := []string{"artifact-a.txt", "artifact-b.txt"}
		filePaths := artifactPaths(tempDir, fileNames)

		jobWorker := &artifactCleanupJob{}
		taskWorker := &artifactCleanupTask{dir: tempDir, fileNames: fileNames}
		ws, err := swf.AsWorkSet(jobWorker, taskWorker)
		require.NoError(t, err)
		engine := toy.NewToyEngine([]swf.WorkSet{*ws})

		runArtifactCleanupScenario(t, ctx, engine, filePaths)
	})

	t.Run("real", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPGWF(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgwf schema: %v", err)
		}

		baseURL, strata := startStrata(t)
		defer strata.Shutdown()
		waitForStrataReady(t, baseURL)

		tempDir := t.TempDir()
		fileNames := []string{"artifact-a.txt", "artifact-b.txt"}
		filePaths := artifactPaths(tempDir, fileNames)

		jobWorker := &artifactCleanupJob{}
		taskWorker := &artifactCleanupTask{dir: tempDir, fileNames: fileNames}
		engine, err := swf.NewEngineBuilder().
			WithPostgresDSN(postgresDSN).
			WithStrata(baseURL).
			WithStrataAPIKey(strata.APIKey).
			PlusWorkers(jobWorker, taskWorker).
			Build(impl.Builder)
		require.NoError(t, err)

		go engine.Run(ctx)
		runArtifactCleanupScenario(t, ctx, engine, filePaths)
	})
}

type artifactCleanupJob struct{}

func (j *artifactCleanupJob) Name() string { return artifactCleanupJobName }

func (j *artifactCleanupJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, artifactCleanupTaskName, input)
}

type artifactCleanupTask struct {
	dir       string
	fileNames []string
}

func (t *artifactCleanupTask) Name() string { return artifactCleanupTaskName }

func (t *artifactCleanupTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(t.fileNames))
	for _, name := range t.fileNames {
		path := filepath.Join(t.dir, name)
		if err := os.WriteFile(path, []byte("artifact:"+name), 0644); err != nil {
			return nil, err
		}

		pathCopy := path
		nameCopy := name
		artifact := swf.NewArtifact(nameCopy, func() (io.ReadCloser, int64, error) {
			f, err := os.Open(pathCopy)
			if err != nil {
				return nil, 0, err
			}
			info, err := f.Stat()
			if err != nil {
				f.Close()
				return nil, 0, err
			}
			return f, info.Size(), nil
		}, func() error {
			return os.Remove(pathCopy)
		})
		artifacts = append(artifacts, artifact)
	}

	return &swf.SimpleTaskData{
		Data:      []byte(`{"ok":true}`),
		Artifacts: artifacts,
	}, nil
}

func artifactPaths(dir string, names []string) []string {
	paths := make([]string, 0, len(names))
	for _, name := range names {
		paths = append(paths, filepath.Join(dir, name))
	}
	return paths
}

func runArtifactCleanupScenario(t *testing.T, ctx context.Context, engine swf.SWFEngine, filePaths []string) {
	t.Helper()

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  artifactCleanupJobName,
		Data:     swf.NewTaskDataOrPanic(map[string]string{"hello": "world"}),
	})
	require.NoError(t, err)

	require.NoError(t, swf.WaitForJobToComplete(ctx, 30*time.Second, jobKey, engine))

	status, err := engine.CheckJobStatus(ctx, jobKey)
	require.NoError(t, err)
	require.Equal(t, swf.JobStatusCompleted, status)

	_, err = engine.GetJobResult(ctx, jobKey)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, path := range filePaths {
			if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)
}
