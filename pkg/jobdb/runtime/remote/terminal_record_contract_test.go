package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/stretchr/testify/require"
)

// This fixture tests the actual HTTP adapter against the agreed read contract.
// It is not protected publication or SQLite migration evidence.
type terminalReadContractRuntime struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
	summary      jobdb.JobSummary
	info         jobdb.JobInfo
	chapterReads atomic.Int32
}

func (r *terminalReadContractRuntime) GetJob(_ context.Context, key jobdb.JobKey) (jobdb.JobInfo, error) {
	if key != r.summary.JobKey {
		return jobdb.JobInfo{}, jobdb.ErrJobNotFound
	}
	return r.info, nil
}
func (r *terminalReadContractRuntime) ListJobs(_ context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	if len(req.TenantIds) != 1 || req.TenantIds[0] != r.summary.JobKey.TenantId {
		return jobdb.ListJobsResponse{}, nil
	}
	return jobdb.ListJobsResponse{Jobs: []jobdb.JobSummary{r.summary}}, nil
}
func (r *terminalReadContractRuntime) ListChapters(context.Context, jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	r.chapterReads.Add(1)
	return nil, jobdb.ErrChapterNotFound
}

func terminalHTTPFixture(kind jobdb.JobRecordKind) *terminalReadContractRuntime {
	at := time.Date(2026, 11, 12, 15, 16, 17, 890, time.UTC)
	failure := &jobdb.GoalFailure{Code: "goal_failed", GoalID: "goal", Message: "exact\n保存 <message>", FailedAt: at,
		TerminalJobIDs: []string{"a", "b"}, NonTerminalJobIDs: []string{"live"}}
	return &terminalReadContractRuntime{
		summary: jobdb.JobSummary{Cell: "work", CellID: "cell", CellRepo: "example.test/work", JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "failure"},
			RecordKind: kind, TerminalFailure: jobdb.CloneGoalFailure(failure), GoalID: "goal", JobType: jobdb.GoalFailureJobType,
			Status: jobdb.JobStatusCompleted, CompletionStatus: "failed", CompletionDetail: failure.Message, CreatedAt: at, ArchivedAt: &at},
		info: jobdb.JobInfo{Cell: "work", CellID: "cell", CellRepo: "example.test/work", GoalID: "goal", RecordKind: kind,
			TerminalFailure: jobdb.CloneGoalFailure(failure), Status: jobdb.JobStatusCompleted, CompletionStatus: "failed", CompletionDetail: failure.Message},
	}
}

func TestTerminalRecordHTTPReadContract(t *testing.T) {
	for _, kind := range []jobdb.JobRecordKind{jobdb.JobRecordGoalFailure, jobdb.JobRecordMigratedGoalFailure} {
		t.Run(string(kind), func(t *testing.T) {
			backend := terminalHTTPFixture(kind)
			server := httptest.NewServer(NewServer(backend))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			info, err := rt.GetJob(context.Background(), backend.summary.JobKey)
			require.NoError(t, err)
			require.Equal(t, kind, info.RecordKind)
			require.Equal(t, backend.info.TerminalFailure, info.TerminalFailure)
			data, err := info.Data.GetData()
			require.Nil(t, data)
			require.ErrorIs(t, err, jobdb.ErrJobFailed)
			require.ErrorIs(t, err, jobdb.ErrGoalFailed)
			var failure *jobdb.GoalFailedError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, *backend.info.TerminalFailure, failure.Failure)
			list, err := rt.ListJobs(context.Background(), jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, GoalIDs: []string{"goal"}})
			require.NoError(t, err)
			require.Len(t, list.Jobs, 1)
			require.Equal(t, kind, list.Jobs[0].RecordKind)
			require.Equal(t, backend.summary.TerminalFailure, list.Jobs[0].TerminalFailure)
			run, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: backend.summary.JobKey})
			require.NoError(t, err)
			require.Equal(t, kind, run.Job.RecordKind)
			require.Equal(t, backend.summary.CellID, run.Job.CellID)
			require.Empty(t, run.Attempts)
			require.Empty(t, run.Execution)
			require.Zero(t, run.Start)
			output, err := run.GetOutput(nil, "tenant")
			require.Nil(t, output)
			require.ErrorIs(t, err, jobdb.ErrGoalFailed)
			require.ErrorIs(t, err, jobdb.ErrJobFailed)
			require.Zero(t, backend.chapterReads.Load())
			_, err = rt.GetJob(context.Background(), jobdb.JobKey{TenantId: "other", JobId: "failure"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			response, err := server.Client().Get(server.URL + "/v1/tenants/tenant/jobs/failure")
			require.NoError(t, err)
			defer response.Body.Close()
			var wire map[string]any
			require.NoError(t, json.NewDecoder(response.Body).Decode(&wire))
			require.Equal(t, string(kind), wire["recordKind"])
			require.Contains(t, wire, "terminalFailure")
			require.Nil(t, wire["data"])
		})
	}
}

func TestTerminalRecordHTTPRejectsMalformedDetail(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimeapi.JobInfo)
	}{
		{"unknown", func(i *runtimeapi.JobInfo) { i.RecordKind = "UNKNOWN" }},
		{"missing-snapshot", func(i *runtimeapi.JobInfo) { i.TerminalFailure = nil }},
		{"mismatched-goal", func(i *runtimeapi.JobInfo) { i.GoalId = "another" }},
		{"wrong-completion", func(i *runtimeapi.JobInfo) { s := "success"; i.CompletionStatus = &s }},
		{"task-data", func(i *runtimeapi.JobInfo) { i.Data = &runtimeapi.StoredTaskData{} }},
		{"runnable", func(i *runtimeapi.JobInfo) { i.NextRoute = &runtimeapi.Route{JobType: "work"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := terminalHTTPFixture(jobdb.JobRecordGoalFailure)
			wire, err := jobInfoToAPI(context.Background(), backend.info)
			require.NoError(t, err)
			tc.mutate(&wire)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(wire)
			}))
			defer server.Close()
			rt, err := New(server.URL, server.Client())
			require.NoError(t, err)
			_, err = rt.GetJob(context.Background(), backend.summary.JobKey)
			require.ErrorIs(t, err, jobdb.ErrInvalidJobRecord)
		})
	}
}

func TestGoalFailureJobIDWireDoesNotReplaceSnapshotJobID(t *testing.T) {
	goal := jobdb.GoalInfo{FailureJobID: "marker", Failure: &jobdb.GoalFailure{JobID: "recorded-source", Message: "reason"}}
	raw, err := json.Marshal(goal)
	require.NoError(t, err)
	var decoded jobdb.GoalInfo
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, "marker", decoded.FailureJobID)
	// JobID already has independent exact-job dependency meaning.
	// The additive marker identity must not repurpose it.
	require.Equal(t, "recorded-source", decoded.Failure.JobID)
}
