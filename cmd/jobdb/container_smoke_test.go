//go:build container_smoke

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
)

const (
	containerSmokeTenantID = "container-smoke"
	containerSmokeJobID    = "persistence-check"
	containerSmokeJobType  = "container-smoke-job"
)

func TestContainerRuntimePersistenceAndArtifactBoundary(t *testing.T) {
	baseURL := os.Getenv("JOBDB_CONTAINER_SMOKE_URL")
	if baseURL == "" {
		t.Fatal("JOBDB_CONTAINER_SMOKE_URL is required")
	}
	runtime, err := remoteruntime.New(baseURL, nil)
	if err != nil {
		t.Fatalf("build remote runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch phase := os.Getenv("JOBDB_CONTAINER_SMOKE_PHASE"); phase {
	case "seed":
		seedContainerSmokeData(t, ctx, runtime)
	case "verify":
		verifyContainerSmokeData(t, ctx, runtime)
	default:
		t.Fatalf("unknown JOBDB_CONTAINER_SMOKE_PHASE %q", phase)
	}
}

func seedContainerSmokeData(t *testing.T, ctx context.Context, runtime jobdb.WorkflowRuntime) {
	t.Helper()
	handle, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: containerSmokeTenantID,
			JobID:    containerSmokeJobID,
			JobType:  containerSmokeJobType,
			Data:     jobdb.NewTaskDataOrPanic(map[string]string{"phase": "seed"}),
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit smoke job: %v", err)
	}

	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
		JobKey:        handle.JobKey,
		WorkerID:      "container-smoke-worker",
		Capabilities:  []string{containerSmokeJobType},
		LeaseDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("get smoke job lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected smoke job lease")
	}

	artifacts := []jobdb.Artifact{
		jobdb.NewArtifactFromBytes("inline.bin", bytes.Repeat([]byte{'i'}, 4096)),
		jobdb.NewArtifactFromBytes("remote.bin", bytes.Repeat([]byte{'r'}, 4097)),
	}
	uploads := make([]jobdb.ArtifactUpload, 0, len(artifacts))
	for _, artifact := range artifacts {
		uploads = append(uploads, jobdb.ArtifactUpload{
			Name: artifact.Name(),
			Size: artifact.Size(),
			Open: artifact.Open,
		})
	}

	if err := runtime.PutChapter(ctx, jobdb.PutChapterRequest{
		LeaseID:    lease.LeaseID(),
		LeaseToken: containerSmokeLeaseToken(t, lease),
		Ref: jobdb.ChapterRef{
			JobKey:  handle.JobKey,
			Ordinal: 1,
		},
		Chapter: jobdb.Chapter{
			Ordinal:   1,
			TaskType:  containerSmokeJobType,
			CreatedAt: time.Now().UTC(),
			Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
				Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"stored":true}`)},
			}},
		},
		ArtifactUploads: uploads,
	}); err != nil {
		t.Fatalf("put smoke chapter: %v", err)
	}
}

func verifyContainerSmokeData(t *testing.T, ctx context.Context, runtime jobdb.WorkflowRuntime) {
	t.Helper()
	jobKey := jobdb.JobKey{TenantId: containerSmokeTenantID, JobId: containerSmokeJobID}
	chapter, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: jobKey, Ordinal: 1})
	if err != nil {
		t.Fatalf("get persisted smoke chapter: %v", err)
	}
	if got, want := len(chapter.Artifacts), 2; got != want {
		t.Fatalf("persisted artifact count = %d, want %d", got, want)
	}

	for _, tc := range []struct {
		name string
		size int
		fill byte
	}{
		{name: "inline.bin", size: 4096, fill: 'i'},
		{name: "remote.bin", size: 4097, fill: 'r'},
	} {
		reader, err := runtime.OpenArtifact(ctx, jobdb.ArtifactRef{
			JobKey:  jobKey,
			Ordinal: 1,
			Name:    tc.name,
		})
		if err != nil {
			t.Fatalf("open persisted artifact %s: %v", tc.name, err)
		}
		rc, err := reader.Open()
		if err != nil {
			t.Fatalf("open persisted artifact reader %s: %v", tc.name, err)
		}
		data, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			t.Fatalf("read persisted artifact %s: %v", tc.name, err)
		}
		if !bytes.Equal(data, bytes.Repeat([]byte{tc.fill}, tc.size)) {
			t.Fatalf("persisted artifact %s content mismatch", tc.name)
		}
	}
}

func containerSmokeLeaseToken(t *testing.T, lease jobdb.ExecutionLease) string {
	t.Helper()
	withToken, ok := lease.(interface{ LeaseToken() string })
	if !ok || withToken.LeaseToken() == "" {
		t.Fatal("remote lease did not include an authorization token")
	}
	return withToken.LeaseToken()
}
