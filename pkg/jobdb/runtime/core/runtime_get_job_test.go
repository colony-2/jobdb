package runtimecore_test

import "github.com/colony-2/jobdb/pkg/jobdb/celltest"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	storagepb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
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
		RouteJobType: "collect", WorkKind: runtimecore.WorkKindTask,
		TaskWork:      &runtimecore.TaskWork{TaskType: "download", InputHash: "hash", InputOrdinal: 1, OutputOrdinal: 2, ResumeJobType: "collect"},
		WaitForJobIDs: []string{"dependency"},
		Completion:    &runtimecore.CompletionSnapshot{Status: "success", Detail: "retained detail"},
	}}
	runtime, err := runtimecore.NewRuntime(runtimecore.Config{Projects: celltest.UnrestrictedProjects{}, Configurations: &configuration.MemoryStore{Projects: celltest.UnrestrictedProjects{}},
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
	if info.NextRoute == nil || info.NextRoute.TaskType != "download" || info.ExecutionState.TaskWait == nil || info.ExecutionState.TaskWait.InputHash != "hash" || len(info.WaitFor) != 1 || info.WaitFor[0] != "dependency" || info.CompletionStatus != "success" || info.CompletionDetail != "retained detail" {
		t.Fatalf("missing detail fields: %+v", info)
	}
	data, err := info.Data.GetData()
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatalf("archived job data = %s, %v", data, err)
	}
	scheduler.job.Store = jobdb.JobStoreActive
	runtime, err = runtimecore.NewRuntime(runtimecore.Config{Projects: celltest.UnrestrictedProjects{}, Configurations: &configuration.MemoryStore{Projects: celltest.UnrestrictedProjects{}},
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

func TestRuntimeGetJobCancelledRetainedChapters(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "cancelled"}
	record, err := json.Marshal(jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "header", Versions: &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: "fixture"}})
	require.NoError(t, err)
	bodies := []jobdb.ChapterBody{
		jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`"input"`)}, Append: &jobdb.ApplicationInputBytes{Data: []byte(`{"n":1}`)}},
		jobdb.AdditionalInputChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`{"n":2}`)}},
		jobdb.ExecutionChapter{Record: record},
		jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"retained"`)}},
		jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`"ordinary"`)}},
		jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"result"`)}}},
		jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "application"}}},
		jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.SystemErrorOutcome{Error: jobdb.SystemErrorPayload{Message: "system"}}},
		jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.TimeoutOutcome{Timeout: jobdb.TimeoutPayload{}}},
	}
	for _, body := range bodies {
		t.Run(fmt.Sprintf("%T", body), func(t *testing.T) {
			ch := jobdb.Chapter{Ordinal: 2, TaskType: "batch", Body: body, Artifacts: []jobdb.StoredArtifact{{Name: "retained", Digest: "digest", Size: 7}}}
			encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: ch})
			require.NoError(t, err)
			rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: getTestScheduler{job: runtimecore.StoredJob{JobKey: key, Store: jobdb.JobStoreArchived, Status: jobdb.JobStatusCancelled, Completion: &runtimecore.CompletionSnapshot{Status: "cancelled", Detail: " \nexact reason\n "}}}, Chapters: readTestChapters{encoded: encoded}, Schemas: readTestSchemas{}})
			require.NoError(t, err)
			info, err := rt.GetJob(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusCancelled, info.Status)
			require.Equal(t, " \nexact reason\n ", info.CompletionDetail)
			_, err = info.Data.GetData()
			switch b := body.(type) {
			case jobdb.TaskAttemptOutcomeChapter:
				require.True(t, jobdb.IsAppError(err), "%v", err)
			case jobdb.JobAttemptOutcomeChapter:
				switch b.Outcome.(type) {
				case jobdb.SystemErrorOutcome:
					require.True(t, jobdb.IsSystemError(err))
				case jobdb.TimeoutOutcome:
					var timeout *jobdb.TimeoutError
					require.ErrorAs(t, err, &timeout)
				default:
					require.NoError(t, err)
				}
			default:
				require.NoError(t, err)
			}
			artifacts, err := info.Data.GetArtifacts()
			require.NoError(t, err)
			require.Len(t, artifacts, 1)
			artifactKey, keyErr := artifacts[0].ArtifactKey()
			require.NoError(t, keyErr)
			require.Equal(t, jobdb.ArtifactKey{JobId: key.JobId, TaskOrdinal: 2, Name: "retained", SizeBytes: 7}, artifactKey)
		})
	}
}

func TestRuntimeGetJobCancelledCorruption(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "corrupt"}
	for _, kind := range []string{"framing", "unknown", "unknown-variant", "missing-chapter", "grouped", "control", "job-outcome", "task-outcome"} {
		t.Run(kind, func(t *testing.T) {
			encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: jobdb.Chapter{Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`1`)}, Append: &jobdb.ApplicationInputBytes{Data: []byte(`2`)}}}})
			require.NoError(t, err)
			var record storagepb.ChapterRecord
			require.NoError(t, proto.Unmarshal(encoded.Payload, &record))
			switch kind {
			case "framing":
				encoded.Payload = []byte{255}
			case "unknown":
				record.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "unknown-variant":
				record.ClearJobStart()
				record.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "missing-chapter":
				record.ClearJobStart()
			case "grouped":
				record.GetJobStart().GetAppend().SetData([]byte(`{"private-marker":`))
			case "control":
				record.SetExecution(&storagepb.ExecutionChapter{})
			case "job-outcome":
				record.SetJobAttemptOutcome(&storagepb.JobAttemptOutcomeChapter{})
			case "task-outcome":
				record.SetTaskAttemptOutcome(&storagepb.TaskAttemptOutcomeChapter{})
			}
			if kind != "framing" {
				encoded.Payload, err = proto.Marshal(&record)
				require.NoError(t, err)
			}
			rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: getTestScheduler{job: runtimecore.StoredJob{JobKey: key, Store: jobdb.JobStoreArchived, Status: jobdb.JobStatusCancelled}}, Chapters: readTestChapters{encoded: encoded}, Schemas: readTestSchemas{}})
			require.NoError(t, err)
			info, err := rt.GetJob(context.Background(), key)
			if err == nil {
				_, err = info.Data.GetData()
			}
			var failure *jobdb.ExecutionInfrastructureError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, jobdb.ExecutionInfrastructureDurableDecode, failure.Reason)
			require.Equal(t, &key, failure.JobKey)
		})
	}
}
