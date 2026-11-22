package toyimpl

import (
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// The engine mutex protects source records; the transaction stages protected
// insertion, first closure and detached waits until all validations succeed.
type toyFailureTransaction struct{ *toyGoalStore }

func (s *toyFailureTransaction) InsertGoalFailure(g jobdb.GoalInfo, failure jobdb.GoalFailure) (jobdb.JobKey, error) {
	key, err := s.engine.idGenerator(g.GoalKey.TenantId)
	if err != nil {
		return jobdb.JobKey{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobKey{}, err
	}
	if key.TenantId != g.GoalKey.TenantId {
		return jobdb.JobKey{}, fmt.Errorf("%w: generated failure job changed tenant", jobdb.ErrInvalidJobRecord)
	}
	if s.engine.jobRecords[key] != nil {
		return jobdb.JobKey{}, jobdb.NewExistingJobMismatchError("generated failure job collides with retained job")
	}
	if _, exists := s.engine.preparedGoals[key]; exists {
		return jobdb.JobKey{}, jobdb.NewExistingJobMismatchError("generated failure job collides with preparation")
	}
	at := failure.FailedAt
	record := &jobRecord{crashBudgetRemaining: 5, key: key, goalID: g.GoalKey.GoalID,
		cell:       jobdb.ResolvedCell{Cell: g.Cell, CellID: g.CellID, CellRepo: g.CellRepo},
		recordKind: jobdb.JobRecordGoalFailure, terminalFailure: jobdb.CloneGoalFailure(&failure),
		jobType: jobdb.GoalFailureJobType, status: jobdb.JobStatusCompleted,
		completionStatus: "failed", completionDetail: failure.Message,
		createdAt: at, availableAt: at, finished: at, archived: &at}
	if err := validateProtectedToyRecord(record); err != nil {
		return jobdb.JobKey{}, err
	}
	if err := (jobdb.JobRunSummary{Cell: g.Cell, CellID: g.CellID, CellRepo: g.CellRepo,
		JobKey: key, GoalID: record.goalID, RecordKind: record.recordKind, TerminalFailure: record.terminalFailure,
		JobType: record.jobType, Status: record.status, CompletionStatus: record.completionStatus,
		CompletionDetail: record.completionDetail, CreatedAt: at, ArchivedAt: &at}).ValidateRecord(); err != nil {
		return jobdb.JobKey{}, err
	}
	s.records[key] = record
	return key, nil
}
