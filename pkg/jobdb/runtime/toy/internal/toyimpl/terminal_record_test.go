package toyimpl

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

// These explicit retained-row fixtures test readers, not FailGoal publication.
func TestToySeededTerminalRecordReaders(t *testing.T) {
	for _, kind := range []jobdb.JobRecordKind{jobdb.JobRecordGoalFailure, jobdb.JobRecordMigratedGoalFailure} {
		t.Run(string(kind), func(t *testing.T) {
			r := New(WithProjects(celltest.UnrestrictedProjects{}))
			ctx := runtimetest.PrimaryGoalContext(context.Background())
			source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			at := time.Date(2026, 11, 12, 14, 15, 16, 0, time.UTC)
			key := jobdb.JobKey{TenantId: "tenant", JobId: "failure"}
			failure := &jobdb.GoalFailure{Code: "goal_failed", GoalID: source.GoalID, Message: "exact\nreason 保存", FailedAt: at, NonTerminalJobIDs: []string{source.JobKey.JobId}}
			r.engine.mu.Lock()
			rec := &jobRecord{crashBudgetRemaining: 5, key: key, cell: r.engine.jobRecords[source.JobKey].cell, goalID: source.GoalID,
				recordKind: kind, terminalFailure: jobdb.CloneGoalFailure(failure), jobType: jobdb.GoalFailureJobType,
				status: jobdb.JobStatusCompleted, completionStatus: "failed", completionDetail: failure.Message, createdAt: at, archived: &at}
			r.engine.jobRecords[key] = rec
			r.engine.mu.Unlock()

			info, err := r.GetJob(ctx, key)
			require.NoError(t, err)
			require.Equal(t, kind, info.RecordKind)
			require.Equal(t, failure, info.TerminalFailure)
			require.Nil(t, info.NextRoute)
			data, err := info.Data.GetData()
			require.Nil(t, data)
			require.ErrorIs(t, err, jobdb.ErrJobFailed)
			require.ErrorIs(t, err, jobdb.ErrGoalFailed)
			info.TerminalFailure.NonTerminalJobIDs[0] = "changed"
			require.Equal(t, failure, rec.terminalFailure)
			list, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{key}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}})
			require.NoError(t, err)
			require.Len(t, list.Jobs, 1)
			require.Equal(t, kind, list.Jobs[0].RecordKind)
			require.Equal(t, failure, list.Jobs[0].TerminalFailure)
			for _, shared := range []bool{false, true} {
				var run jobdb.GetJobRunResponse
				if shared {
					run, err = jobdb.GetJobRun(ctx, r, jobdb.GetJobRunRequest{JobKey: key})
				} else {
					run, err = r.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: key})
				}
				require.NoError(t, err)
				require.Equal(t, kind, run.Job.RecordKind)
				require.Equal(t, failure, run.Job.TerminalFailure)
				require.Zero(t, run.Start)
				require.Empty(t, run.Execution)
				require.Empty(t, run.Attempts)
				output, err := run.GetOutput(nil, key.TenantId)
				require.Nil(t, output)
				require.ErrorIs(t, err, jobdb.ErrJobFailed)
				require.ErrorIs(t, err, jobdb.ErrGoalFailed)
			}
			chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			require.Empty(t, chapters)
			_, err = r.GetChapter(ctx, jobdb.ChapterRef{JobKey: key, Ordinal: 0})
			require.ErrorIs(t, err, jobdb.ErrChapterNotFound)
			_, err = r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, JobID: "restart", Cell: source.Cell, CellRepo: source.CellRepo}})
			require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
			require.Nil(t, r.engine.getJobRecord(jobdb.JobKey{TenantId: key.TenantId, JobId: "restart"}))

			rec.mu.Lock()
			rec.recordKind = "UNKNOWN"
			rec.mu.Unlock()
			_, err = r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.ErrorIs(t, err, jobdb.ErrInvalidJobRecord)
			rec.mu.Lock()
			rec.recordKind = jobdb.JobRecordExecution
			rec.terminalFailure = nil
			rec.mu.Unlock()
			_, err = r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
		})
	}
}
