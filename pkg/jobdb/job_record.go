package jobdb

import (
	"errors"
	"fmt"
	"slices"
)

// JobRecordKind distinguishes executed work from protected scheduler records.
// An absent legacy discriminator means EXECUTION. Job type or missing history
// alone never identifies a protected record.
type JobRecordKind string

const (
	JobRecordExecution           JobRecordKind = "EXECUTION"
	JobRecordGoalFailure         JobRecordKind = "GOAL_FAILURE"
	JobRecordMigratedGoalFailure JobRecordKind = "MIGRATED_GOAL_FAILURE"
	GoalFailureJobType                         = "jobdb.goal_failure"
)

var (
	ErrInvalidJobRecord = errors.New("invalid job record")
	// ErrJobRecordOperationUnsupported rejects execution operations on a known
	// protected record. Reading it does not grant execution authority.
	ErrJobRecordOperationUnsupported = errors.New("operation is unsupported for this job record")
)

func (k JobRecordKind) IsTerminalRecord() bool {
	return k == JobRecordGoalFailure || k == JobRecordMigratedGoalFailure
}

// Validate rejects unknown discriminators without inferring a kind from data.
func (k JobRecordKind) Validate() error {
	switch k {
	case "", JobRecordExecution, JobRecordGoalFailure, JobRecordMigratedGoalFailure:
		return nil
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidJobRecord, k)
	}
}

// CloneGoalFailure detaches the saved member snapshot from its storage owner.
func CloneGoalFailure(f *GoalFailure) *GoalFailure {
	if f == nil {
		return nil
	}
	c := *f
	c.TerminalJobIDs = slices.Clone(f.TerminalJobIDs)
	c.NonTerminalJobIDs = slices.Clone(f.NonTerminalJobIDs)
	return &c
}

func validateJobRecord(kind JobRecordKind, goalID string, status JobStatus, completion, detail string, failure *GoalFailure) error {
	if err := kind.Validate(); err != nil {
		return err
	}
	if !kind.IsTerminalRecord() {
		if failure != nil {
			return fmt.Errorf("%w: execution record has protected failure", ErrInvalidJobRecord)
		}
		return nil
	}
	if goalID == "" || status != JobStatusCompleted || completion != "failed" || failure == nil {
		return fmt.Errorf("%w: terminal failure requires goal, completed lifecycle, failed outcome and snapshot", ErrInvalidJobRecord)
	}
	// Migrated snapshots retain their historical contents. Do not normalize
	// membership or reorder their arrays while validating the saved outcome.
	if failure.Code != "goal_failed" || failure.GoalID != goalID || failure.FailedAt.IsZero() || failure.Message != detail {
		return fmt.Errorf("%w: terminal failure snapshot disagrees with job", ErrInvalidJobRecord)
	}
	if err := validateGoalFailureMessage(failure.Message); err != nil {
		return fmt.Errorf("%w: invalid failure message", ErrInvalidJobRecord)
	}
	return nil
}

// ValidateRecord checks the discriminator and protected terminal outcome.
// It does not establish storage protection or grant lifecycle authority.
func (i JobInfo) ValidateRecord() error {
	if err := validateJobRecord(i.RecordKind, i.GoalID, i.Status, i.CompletionStatus, i.CompletionDetail, i.TerminalFailure); err != nil {
		return err
	}
	if i.RecordKind.IsTerminalRecord() && (i.Cell == "" || i.CellID == "" || i.CellRepo == "" || i.NextRoute != nil ||
		len(i.WaitFor) != 0 || i.BlockingWait != nil || i.ExecutionState.TaskWait != nil) {
		return fmt.Errorf("%w: protected detail requires retained ownership and no executable route or wait", ErrInvalidJobRecord)
	}
	return nil
}

func (s JobSummary) ValidateRecord() error {
	if err := validateJobRecord(s.RecordKind, s.GoalID, s.Status, s.CompletionStatus, s.CompletionDetail, s.TerminalFailure); err != nil {
		return err
	}
	if s.RecordKind.IsTerminalRecord() && (s.Cell == "" || s.CellID == "" || s.CellRepo == "" || s.ArchivedAt == nil ||
		s.JobType != GoalFailureJobType || s.NextRoute != nil || s.LeaseExpiresAt != nil || len(s.WaitFor) != 0 ||
		s.BlockingWait != nil || s.BlockingWaitSummary != nil || s.ExecutionState.TaskWait != nil) {
		return fmt.Errorf("%w: protected summary requires archived ownership and no executable route, lease or wait", ErrInvalidJobRecord)
	}
	return nil
}

func (s JobRunSummary) ValidateRecord() error {
	if err := validateJobRecord(s.RecordKind, s.GoalID, s.Status, s.CompletionStatus, s.CompletionDetail, s.TerminalFailure); err != nil {
		return err
	}
	if s.RecordKind.IsTerminalRecord() && (s.Cell == "" || s.CellID == "" || s.CellRepo == "" || s.ArchivedAt == nil ||
		s.JobType != GoalFailureJobType || s.BlockingWait != nil) {
		return fmt.Errorf("%w: protected run requires archived ownership and no execution wait", ErrInvalidJobRecord)
	}
	return nil
}

// TerminalRecordFailure returns a detached typed failed-job result after the
// caller has validated a protected terminal record. No task output is created.
func (i JobInfo) TerminalRecordFailure() error {
	if err := i.ValidateRecord(); err != nil {
		return err
	}
	if !i.RecordKind.IsTerminalRecord() {
		return fmt.Errorf("%w: execution record has no protected failure", ErrInvalidJobRecord)
	}
	return terminalRecordFailure(i.TerminalFailure)
}

func terminalRecordFailure(failure *GoalFailure) error {
	return &JobFailedError{Cause: &GoalFailedError{Failure: *CloneGoalFailure(failure)}}
}
