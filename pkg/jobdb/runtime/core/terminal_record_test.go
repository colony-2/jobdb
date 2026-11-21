package runtimecore_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type terminalContractScheduler struct {
	runtimecore.Scheduler
	row       runtimecore.StoredJob
	discarded int
}

func (s *terminalContractScheduler) GetJob(context.Context, jobdb.JobKey) (runtimecore.StoredJob, error) {
	return s.row, nil
}
func (s *terminalContractScheduler) ListJobs(context.Context, runtimecore.ListJobsRequest) (runtimecore.ListJobsResponse, error) {
	return runtimecore.ListJobsResponse{Jobs: []runtimecore.StoredJob{s.row}}, nil
}
func (s *terminalContractScheduler) PrepareJob(_ context.Context, p runtimecore.JobPreparation) (runtimecore.PreparedJob, error) {
	return runtimecore.PreparedJob{Key: p.Key, GoalID: "new-goal"}, nil
}
func (s *terminalContractScheduler) DiscardJobPreparation(context.Context, jobdb.JobKey) error {
	s.discarded++
	return nil
}

type emptyTerminalContractChapters struct {
	runtimecore.ChapterLog
	counts int
}

func (c *emptyTerminalContractChapters) Count(context.Context, runtimecore.ChapterLogKey) (int64, error) {
	c.counts++
	return 0, nil
}

func terminalContractStored(kind jobdb.JobRecordKind) runtimecore.StoredJob {
	at := time.Date(2026, 11, 12, 13, 14, 15, 0, time.UTC)
	failure := &jobdb.GoalFailure{Code: "goal_failed", GoalID: "goal", Message: "exact\nreason 保存", FailedAt: at,
		TerminalJobIDs: []string{"previous"}, NonTerminalJobIDs: []string{"active"}}
	return runtimecore.StoredJob{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "failure"},
		Cell:       jobdb.ResolvedCell{Cell: "work", CellID: "cell", CellRepo: "example.test/work"},
		RecordKind: kind, TerminalFailure: failure, GoalID: "goal", JobType: jobdb.GoalFailureJobType,
		Store: jobdb.JobStoreArchived, Status: jobdb.JobStatusCompleted, CreatedAt: at, ArchivedAt: &at,
		Completion: &runtimecore.CompletionSnapshot{Status: "failed", Detail: failure.Message}}
}

func newTerminalContractCore(t *testing.T, s *terminalContractScheduler, c runtimecore.ChapterLog) *runtimecore.Runtime {
	t.Helper()
	rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: s, Chapters: c, Schemas: readTestSchemas{}})
	require.NoError(t, err)
	return rt
}

func TestCoreTerminalRecordInspectionWithoutChapters(t *testing.T) {
	for _, kind := range []jobdb.JobRecordKind{jobdb.JobRecordGoalFailure, jobdb.JobRecordMigratedGoalFailure} {
		t.Run(string(kind), func(t *testing.T) {
			s := &terminalContractScheduler{row: terminalContractStored(kind)}
			chapters := &emptyTerminalContractChapters{}
			rt := newTerminalContractCore(t, s, chapters)
			info, err := rt.GetJob(context.Background(), s.row.JobKey)
			require.NoError(t, err)
			require.Nil(t, info.NextRoute)
			require.Equal(t, s.row.TerminalFailure, info.TerminalFailure)
			data, err := info.Data.GetData()
			require.Nil(t, data)
			require.ErrorIs(t, err, jobdb.ErrJobFailed)
			require.ErrorIs(t, err, jobdb.ErrGoalFailed)
			var goalError *jobdb.GoalFailedError
			require.ErrorAs(t, err, &goalError)
			require.Equal(t, *s.row.TerminalFailure, goalError.Failure)
			info.TerminalFailure.TerminalJobIDs[0] = "mutated"
			require.Equal(t, "previous", s.row.TerminalFailure.TerminalJobIDs[0])
			run, err := jobdb.GetJobRun(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: s.row.JobKey})
			require.NoError(t, err)
			require.Equal(t, s.row.RecordKind, run.Job.RecordKind)
			require.Equal(t, s.row.Cell.CellID, run.Job.CellID)
			require.Empty(t, run.Execution)
			require.Empty(t, run.Attempts)
			require.Zero(t, run.Start)
			require.Zero(t, chapters.counts)
		})
	}
}

func TestCoreTerminalRecordRejectsRestartBeforeHistory(t *testing.T) {
	s := &terminalContractScheduler{row: terminalContractStored(jobdb.JobRecordGoalFailure)}
	chapters := &emptyTerminalContractChapters{}
	rt := newTerminalContractCore(t, s, chapters)
	for _, boundary := range []*jobdb.ExecutionRestart{nil, {ScopeID: "point"}} {
		_, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{
			PriorJobKey: s.row.JobKey, JobID: "new", ExecutionRestart: boundary}})
		require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
	}
	require.Equal(t, 2, s.discarded)
	require.Zero(t, chapters.counts)
}

func TestCoreRejectsInvalidProtectedFactsAndPreservesOrdinaryArchiveCheck(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimecore.StoredJob)
	}{
		{"unknown", func(r *runtimecore.StoredJob) { r.RecordKind = "UNKNOWN" }},
		{"missing-failure", func(r *runtimecore.StoredJob) { r.TerminalFailure = nil }},
		{"active-store", func(r *runtimecore.StoredJob) { r.Store = jobdb.JobStoreActive }},
		{"missing-archive", func(r *runtimecore.StoredJob) { r.ArchivedAt = nil }},
		{"fake-type", func(r *runtimecore.StoredJob) { r.JobType = "recipe" }},
		{"runnable", func(r *runtimecore.StoredJob) { r.RouteJobType = "work" }},
		{"task", func(r *runtimecore.StoredJob) { r.TaskWork = &runtimecore.TaskWork{TaskType: "task"} }},
		{"no-owner", func(r *runtimecore.StoredJob) { r.Cell.CellID = "" }},
		{"missing-completion", func(r *runtimecore.StoredJob) { r.Completion = nil }},
		{"wrong-detail", func(r *runtimecore.StoredJob) { r.Completion.Detail = "different" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &terminalContractScheduler{row: terminalContractStored(jobdb.JobRecordGoalFailure)}
			tc.mutate(&s.row)
			chapters := &emptyTerminalContractChapters{}
			rt := newTerminalContractCore(t, s, chapters)
			_, err := rt.GetJob(context.Background(), s.row.JobKey)
			require.ErrorIs(t, err, jobdb.ErrInvalidJobRecord)
			_, err = rt.ListJobs(context.Background(), jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
			require.ErrorIs(t, err, jobdb.ErrInvalidJobRecord)
			require.Zero(t, chapters.counts)
		})
	}
	for _, kind := range []jobdb.JobRecordKind{"", jobdb.JobRecordExecution} {
		s := &terminalContractScheduler{row: terminalContractStored(kind)}
		s.row.TerminalFailure = nil
		chapters := &emptyTerminalContractChapters{}
		rt := newTerminalContractCore(t, s, chapters)
		_, err := rt.GetJob(context.Background(), s.row.JobKey)
		require.ErrorContains(t, err, "has no chapters")
		require.Equal(t, 1, chapters.counts)
	}
}
