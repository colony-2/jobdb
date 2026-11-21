package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type terminalReplayContractRuntime struct {
	WorkflowRuntime
	info JobInfo
}

func (r terminalReplayContractRuntime) GetJob(context.Context, JobKey) (JobInfo, error) {
	return r.info, nil
}
func (terminalReplayContractRuntime) GetChapter(context.Context, ChapterRef) (Chapter, error) {
	panic("protected record replay reached execution history")
}

func TestReplayRejectsTerminalRecordBeforeHistoryOrCallbacks(t *testing.T) {
	for _, kind := range []jobdb.JobRecordKind{jobdb.JobRecordGoalFailure, jobdb.JobRecordMigratedGoalFailure} {
		info := JobInfo{RecordKind: kind, Cell: "work", CellID: "cell", CellRepo: "example.test/work", GoalID: "goal",
			Status: JobStatusCompleted, CompletionStatus: "failed", CompletionDetail: "reason",
			TerminalFailure: &jobdb.GoalFailure{Code: "goal_failed", GoalID: "goal", Message: "reason", FailedAt: time.Now().UTC()}}
		value, err := replayRuntimeJob(context.Background(), terminalReplayContractRuntime{info: info}, &replayWorkSet{engine: &workerEngine{}, jobKey: JobKey{TenantId: "tenant", JobId: "failure"}})
		require.Nil(t, value)
		require.ErrorIs(t, err, jobdb.ErrJobRecordOperationUnsupported)
		info.RecordKind = "UNKNOWN"
		_, err = replayRuntimeJob(context.Background(), terminalReplayContractRuntime{info: info}, &replayWorkSet{engine: &workerEngine{}, jobKey: JobKey{TenantId: "tenant", JobId: "failure"}})
		require.ErrorIs(t, err, jobdb.ErrInvalidJobRecord)
	}
}
