package jobdb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func protectedRecordFixture(kind JobRecordKind) (JobSummary, JobInfo) {
	at := time.Date(2026, 11, 12, 12, 34, 56, 1234, time.UTC)
	failure := &GoalFailure{Code: "goal_failed", GoalID: "goal", Message: "No recovery\n保存 <reason>", FailedAt: at,
		TerminalJobIDs: []string{"old"}, NonTerminalJobIDs: []string{"running"}}
	summary := JobSummary{Cell: "work", CellID: "cell", CellRepo: "example.test/work", RecordKind: kind, TerminalFailure: CloneGoalFailure(failure),
		JobKey: JobKey{TenantId: "tenant", JobId: "failure"}, GoalID: "goal", JobType: GoalFailureJobType,
		Status: JobStatusCompleted, CompletionStatus: "failed", CompletionDetail: failure.Message, CreatedAt: at, ArchivedAt: &at}
	info := JobInfo{Cell: summary.Cell, CellID: summary.CellID, CellRepo: summary.CellRepo, GoalID: summary.GoalID,
		RecordKind: kind, TerminalFailure: CloneGoalFailure(failure), Status: summary.Status,
		CompletionStatus: summary.CompletionStatus, CompletionDetail: summary.CompletionDetail}
	return summary, info
}

type protectedReadRuntime struct {
	WorkflowRuntime
	summary JobSummary
	info    JobInfo
	err     error
}

func (r protectedReadRuntime) ListJobs(context.Context, ListJobsRequest) (ListJobsResponse, error) {
	return ListJobsResponse{Jobs: []JobSummary{r.summary}}, r.err
}
func (r protectedReadRuntime) GetJob(context.Context, JobKey) (JobInfo, error) { return r.info, r.err }
func (protectedReadRuntime) ListChapters(context.Context, ListChaptersRequest) ([]Chapter, error) {
	panic("protected detail must not read execution chapters")
}

func TestProtectedRecordRunAndOutputWithoutChapters(t *testing.T) {
	for _, kind := range []JobRecordKind{JobRecordGoalFailure, JobRecordMigratedGoalFailure} {
		t.Run(string(kind), func(t *testing.T) {
			summary, info := protectedRecordFixture(kind)
			run, err := GetJobRun(context.Background(), protectedReadRuntime{summary: summary, info: info}, GetJobRunRequest{JobKey: summary.JobKey})
			require.NoError(t, err)
			require.Equal(t, kind, run.Job.RecordKind)
			require.Equal(t, summary.GoalID, run.Job.GoalID)
			require.Equal(t, summary.CellID, run.Job.CellID)
			require.Equal(t, summary.CellRepo, run.Job.CellRepo)
			require.Equal(t, *info.TerminalFailure, *run.Job.TerminalFailure)
			require.Empty(t, run.Execution)
			require.Empty(t, run.Attempts)
			require.Zero(t, run.Start)
			output, err := run.GetOutput(nil, summary.JobKey.TenantId)
			require.Nil(t, output)
			require.ErrorIs(t, err, ErrJobFailed)
			require.ErrorIs(t, err, ErrGoalFailed)
			var goalError *GoalFailedError
			require.ErrorAs(t, err, &goalError)
			require.Equal(t, *info.TerminalFailure, goalError.Failure)
			goalError.Failure.TerminalJobIDs[0] = "changed-error"
			require.Equal(t, "old", run.Job.TerminalFailure.TerminalJobIDs[0])
			run.Job.TerminalFailure.TerminalJobIDs[0] = "changed-response"
			require.Equal(t, "old", info.TerminalFailure.TerminalJobIDs[0])
		})
	}
}

func TestProtectedRecordRejectsMalformedOrUnknownViews(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*JobSummary, *JobInfo)
	}{
		{"unknown-summary", func(s *JobSummary, _ *JobInfo) { s.RecordKind = "FUTURE_KIND" }},
		{"unknown-detail", func(_ *JobSummary, i *JobInfo) { i.RecordKind = "FUTURE_KIND" }},
		{"missing-summary-snapshot", func(s *JobSummary, _ *JobInfo) { s.TerminalFailure = nil }},
		{"missing-detail-snapshot", func(_ *JobSummary, i *JobInfo) { i.TerminalFailure = nil }},
		{"missing-goal", func(_ *JobSummary, i *JobInfo) { i.GoalID = "" }},
		{"wrong-snapshot-goal", func(_ *JobSummary, i *JobInfo) { i.TerminalFailure.GoalID = "another" }},
		{"wrong-message", func(_ *JobSummary, i *JobInfo) { i.CompletionDetail = "another" }},
		{"wrong-time", func(_ *JobSummary, i *JobInfo) { i.TerminalFailure.FailedAt = time.Time{} }},
		{"wrong-code", func(_ *JobSummary, i *JobInfo) { i.TerminalFailure.Code = "job_dependency_failed" }},
		{"running", func(_ *JobSummary, i *JobInfo) { i.Status = JobStatusActive }},
		{"successful", func(_ *JobSummary, i *JobInfo) { i.CompletionStatus = "success" }},
		{"no-ownership", func(_ *JobSummary, i *JobInfo) { i.CellID = "" }},
		{"runnable", func(_ *JobSummary, i *JobInfo) { i.NextRoute = &Route{JobType: "work"} }},
		{"missing-archive", func(s *JobSummary, _ *JobInfo) { s.ArchivedAt = nil }},
		{"detail-is-execution", func(_ *JobSummary, i *JobInfo) { i.RecordKind = JobRecordExecution; i.TerminalFailure = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			summary, info := protectedRecordFixture(JobRecordGoalFailure)
			tc.mutate(&summary, &info)
			_, err := GetJobRun(context.Background(), protectedReadRuntime{summary: summary, info: info}, GetJobRunRequest{JobKey: summary.JobKey})
			require.ErrorIs(t, err, ErrInvalidJobRecord)
		})
	}
	_, info := protectedRecordFixture(JobRecordGoalFailure)
	for _, message := range []string{" ", string([]byte{0xff}), strings.Repeat("x", (16<<10)+1)} {
		info.CompletionDetail = message
		info.TerminalFailure.Message = message
		require.ErrorIs(t, info.ValidateRecord(), ErrInvalidJobRecord)
	}
	info.RecordKind = JobRecordMigratedGoalFailure
	require.ErrorIs(t, info.ValidateRecord(), ErrInvalidJobRecord)
	// Valid historical text and snapshot identities remain exact, including
	// references that no longer represent live secondary membership.
	info.TerminalFailure.Message = "  historical\nreason  "
	info.CompletionDetail = info.TerminalFailure.Message
	info.TerminalFailure.TerminalJobIDs = []string{"dropped-secondary"}
	require.NoError(t, info.ValidateRecord())
	require.Equal(t, "dropped-secondary", info.TerminalFailure.TerminalJobIDs[0])
}

func TestProtectedRecordReadErrorsRemainErrors(t *testing.T) {
	summary, info := protectedRecordFixture(JobRecordGoalFailure)
	failure := errors.New("detail denied")
	_, err := GetJobRun(context.Background(), protectedReadRuntime{summary: summary, info: info, err: failure}, GetJobRunRequest{JobKey: summary.JobKey})
	require.ErrorIs(t, err, failure)
}

func TestOrdinaryFailureTypeDoesNotImplyProtectedClosure(t *testing.T) {
	for _, kind := range []JobRecordKind{"", JobRecordExecution} {
		run := GetJobRunResponse{Job: JobRunSummary{RecordKind: kind, JobType: GoalFailureJobType,
			Status: JobStatusCompleted, CompletionStatus: "failed", CompletionDetail: "No recovery"}}
		_, err := run.GetOutput(nil, "tenant")
		require.ErrorIs(t, err, ErrJobNotComplete)
		require.NotErrorIs(t, err, ErrGoalFailed)
		run.Job.TerminalFailure = &GoalFailure{Code: "goal_failed"}
		_, err = run.GetOutput(nil, "tenant")
		require.ErrorIs(t, err, ErrInvalidJobRecord)
	}
}

func TestCloneGoalFailurePreservesHistoricalShape(t *testing.T) {
	failure := &GoalFailure{TerminalJobIDs: nil, NonTerminalJobIDs: []string{}}
	copy := CloneGoalFailure(failure)
	require.Equal(t, failure, copy)
	require.Nil(t, copy.TerminalJobIDs)
	require.NotNil(t, copy.NonTerminalJobIDs)
}
