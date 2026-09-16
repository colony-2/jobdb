package runtimecore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

type getTestScheduler struct {
	runtimecore.Scheduler
	job runtimecore.StoredJob
}

func (s getTestScheduler) GetJob(context.Context, jobdb.JobKey) (runtimecore.StoredJob, error) {
	return s.job, nil
}

func TestRuntimeGetJobUsesFinalChapterForArchivedData(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	chapter := jobdb.Chapter{
		Ordinal: 0, TaskType: "collect", CreatedAt: time.Now().UTC(),
		Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		}},
	}
	encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: chapter})
	if err != nil {
		t.Fatal(err)
	}
	scheduler := getTestScheduler{job: runtimecore.StoredJob{
		JobKey: key, Store: jobdb.JobStoreArchived,
		Status: jobdb.JobStatusCompleted, SchemaHash: "schema-hash",
	}}
	runtime, err := runtimecore.NewRuntime(runtimecore.Config{
		Scheduler: scheduler, Chapters: readTestChapters{encoded: encoded},
		Schemas: readTestSchemas{},
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := runtime.GetJob(context.Background(), key)
	if err != nil || info.Status != jobdb.JobStatusCompleted || info.SchemaHash != "schema-hash" {
		t.Fatalf("archived job = %+v, %v", info, err)
	}
	data, err := info.Data.GetData()
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatalf("archived job data = %s, %v", data, err)
	}
	scheduler.job.Store = jobdb.JobStoreActive
	runtime, err = runtimecore.NewRuntime(runtimecore.Config{
		Scheduler: scheduler, Chapters: readTestChapters{encoded: encoded},
		Schemas: readTestSchemas{},
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err = runtime.GetJob(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := info.Data.GetData(); !errors.Is(err, jobdb.ErrJobNotComplete) {
		t.Fatalf("active job data error = %v", err)
	}
}
