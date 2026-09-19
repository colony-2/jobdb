package runtimecore_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

type listTestScheduler struct {
	runtimecore.Scheduler
	rows []runtimecore.StoredJob
}

func (s listTestScheduler) ListJobs(_ context.Context, _ runtimecore.ListJobsRequest) (runtimecore.ListJobsResponse, error) {
	return runtimecore.ListJobsResponse{Jobs: s.rows}, nil
}

func TestRuntimeListJobsKeepsArchivedTaskSummary(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Microsecond)
	archived := created.Add(time.Minute)
	expiry := archived.Add(time.Hour)
	row := runtimecore.StoredJob{
		JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"},
		Store:  jobdb.JobStoreArchived, Status: jobdb.JobStatusCompleted,
		JobType: "collect", RouteJobType: "collect", WorkKind: runtimecore.WorkKindTask,
		TaskWork: &runtimecore.TaskWork{TaskType: "download", ResumeJobType: "collect",
			InputOrdinal: 2, OutputOrdinal: 3, InputHash: "sha256:input"},
		RunPolicy:     jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 4}},
		ClientPayload: json.RawMessage(`{}`), ClientPayloadRevision: 1,
		AppMetadata: json.RawMessage(`{"source":"api"}`), SchemaHash: "schema-hash",
		ParentJobID: "parent", WaitForJobIDs: []string{"prereq"},
		AvailableAt: created, ExpiresAt: &expiry, CreatedAt: created,
		ArchivedAt: &archived, CancelRequested: false,
	}
	runtime, err := runtimecore.NewRuntime(runtimecore.Config{
		Scheduler: listTestScheduler{rows: []runtimecore.StoredJob{row}},
		Chapters:  readTestChapters{}, Schemas: readTestSchemas{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ListJobs(context.Background(), jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
	if err != nil || len(result.Jobs) != 1 {
		t.Fatalf("list jobs = %+v, %v", result, err)
	}
	got := result.Jobs[0]
	if got.Status != jobdb.JobStatusCompleted || got.JobType != "collect" ||
		got.NextRoute == nil || *got.NextRoute != (jobdb.Route{JobType: "collect", TaskType: "download"}) ||
		got.ExecutionState.TaskWait == nil || got.ExecutionState.TaskWait.InputOrdinal != 2 ||
		got.ExecutionState.TaskWait.OutputOrdinal != 3 ||
		got.ExecutionState.TaskWait.ResumeJobType != "collect" ||
		got.ExecutionState.TaskWait.InputHash != "sha256:input" ||
		string(got.ClientPayload) != `{}` || string(got.Metadata) != `{"source":"api"}` ||
		got.SchemaHash != "schema-hash" || got.ParentJobID != "parent" ||
		got.ArchivedAt == nil || !got.ArchivedAt.Equal(archived) ||
		got.ExpiresAt == nil || !got.ExpiresAt.Equal(expiry) ||
		len(got.WaitFor) != 1 || got.WaitFor[0] != "prereq" {
		t.Fatalf("archived task summary = %+v", got)
	}
}
