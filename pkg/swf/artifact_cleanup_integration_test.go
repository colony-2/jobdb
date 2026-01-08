package swf_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArtifactCleanupAfterUpload verifies that artifact cleanup only happens
// after the upload to strata completes successfully. This prevents race conditions
// where files are deleted before they can be uploaded.
func TestArtifactCleanupAfterUpload(t *testing.T) {
	t.Run("file artifact cleanup waits for upload", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPGWF(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgwf schema: %v", err)
		}

		// Start Strata
		baseURL, strata := startStrata(t)
		defer strata.Shutdown()
		waitForStrataReady(t, baseURL)

		// Create a temporary file artifact
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "test-output.txt")
		testData := []byte("test data for upload")
		err := os.WriteFile(testFile, testData, 0644)
		require.NoError(t, err)

		// Track whether cleanup was called and when
		var cleanupCalled atomic.Bool
		var cleanupTime time.Time

		// Create artifact with cleanup tracking
		artifact := swf.NewArtifact("test-output.txt", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(testFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			cleanupCalled.Store(true)
			cleanupTime = time.Now()
			return os.Remove(testFile)
		})

		// Job that produces the artifact via a task
		jobWorker := &artifactProducerJob{artifact: artifact}
		taskWorker := &artifactProducingTask{artifact: artifact}

		// Build engine with the job and task worker
		engine, err := swf.NewEngineBuilder().
			WithPostgresDSN(postgresDSN).
			WithStrata(baseURL).
			WithStrataAPIKey(strata.APIKey).
			PlusWorkers(jobWorker, taskWorker).
			Build(impl.Builder)
		require.NoError(t, err)

		// Run engine in background
		go engine.Run(ctx)

		// Start the job
		startTime := time.Now()
		jobKey, err := engine.StartJob(ctx, swf.StartJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]string{"key": "value"}),
		})
		require.NoError(t, err)

		// Wait for job to complete
		require.Eventually(t, func() bool {
			status, _ := engine.CheckJobStatus(ctx, jobKey)
			return status == swf.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		// Verify cleanup was called
		assert.True(t, cleanupCalled.Load(), "artifact cleanup should be called")

		// Verify file no longer exists (cleanup removed it)
		_, err = os.Stat(testFile)
		assert.True(t, os.IsNotExist(err), "file should be cleaned up after upload")

		// Verify the artifact was actually uploaded by reading it back
		result, err := engine.GetJobResult(ctx, jobKey)
		require.NoError(t, err)

		artifacts, err := result.GetArtifacts()
		require.NoError(t, err)
		require.Len(t, artifacts, 1, "result should have one artifact")

		// Read the artifact from storage (not from local file)
		uploadedData, err := artifacts[0].Bytes(ctx)
		require.NoError(t, err)
		assert.Equal(t, testData, uploadedData, "uploaded artifact should match original data")

		// Log timing for debugging
		t.Logf("Job started: %v", startTime)
		t.Logf("Cleanup called: %v", cleanupTime)
		t.Logf("Time to cleanup: %v", cleanupTime.Sub(startTime))
	})

	t.Run("cleanup happens even on task error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPGWF(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgwf schema: %v", err)
		}

		// Start Strata
		baseURL, strata := startStrata(t)
		defer strata.Shutdown()
		waitForStrataReady(t, baseURL)

		// Create a temporary file artifact
		tempDir := t.TempDir()
		testFile := filepath.Join(tempDir, "error-test.txt")
		err := os.WriteFile(testFile, []byte("error test"), 0644)
		require.NoError(t, err)

		var cleanupCalled atomic.Bool
		artifact := swf.NewArtifact("error-test.txt", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(testFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			cleanupCalled.Store(true)
			return os.Remove(testFile)
		})

		// Job that fails but has artifact input
		jobWorker := &failingJobWithArtifacts{}
		taskWorker := &failingTask{}

		// Build engine with the job and task worker
		engine, err := swf.NewEngineBuilder().
			WithPostgresDSN(postgresDSN).
			WithStrata(baseURL).
			WithStrataAPIKey(strata.APIKey).
			PlusWorkers(jobWorker, taskWorker).
			Build(impl.Builder)
		require.NoError(t, err)

		// Run engine in background
		go engine.Run(ctx)

		// Start the job with artifact
		jobKey, err := engine.StartJob(ctx, swf.StartJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data: &swf.SimpleTaskData{
				Data:      []byte(`{}`),
				Artifacts: []swf.Artifact{artifact},
			},
		})
		require.NoError(t, err)

		// Wait for job to complete (with error)
		require.Eventually(t, func() bool {
			status, _ := engine.CheckJobStatus(ctx, jobKey)
			return status == swf.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		// Verify cleanup was still called despite error
		assert.True(t, cleanupCalled.Load(), "artifact cleanup should be called even on error")

		// Verify file was cleaned up
		_, err = os.Stat(testFile)
		assert.True(t, os.IsNotExist(err), "file should be cleaned up even after job error")
	})
}

// artifactProducerJob calls a task that produces an artifact as output
type artifactProducerJob struct {
	artifact swf.Artifact
}

func (j *artifactProducerJob) Name() string { return "artifact-producer" }

func (j *artifactProducerJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	// Call a task that produces the artifact
	return ctx.DoTask(swf.RunPolicy{}, "produce-artifact", input)
}

// artifactProducingTask produces an artifact
type artifactProducingTask struct {
	artifact swf.Artifact
}

func (t *artifactProducingTask) Name() string { return "produce-artifact" }

func (t *artifactProducingTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return &swf.SimpleTaskData{
		Data:      []byte(`{"produced":true}`),
		Artifacts: []swf.Artifact{t.artifact},
	}, nil
}

// failingJobWithArtifacts calls a task that receives artifacts as input but fails
type failingJobWithArtifacts struct{}

func (j *failingJobWithArtifacts) Name() string { return "failing-with-artifacts" }

func (j *failingJobWithArtifacts) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	// Call a task that will fail
	return ctx.DoTask(swf.RunPolicy{}, "failing-task", input)
}

// failingTask receives artifacts but fails
type failingTask struct{}

func (t *failingTask) Name() string { return "failing-task" }

func (t *failingTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "intentional failure", Level: "error"}}
}
