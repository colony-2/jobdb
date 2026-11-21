package jobdb

import (
	"context"
	"errors"
	"fmt"
)

// ExecutionInfrastructureReason identifies failures of durable execution, rather
// than failures returned by an application's task.
type ExecutionInfrastructureReason string

const (
	ExecutionInfrastructureDeterminism   ExecutionInfrastructureReason = "determinism"
	ExecutionInfrastructureDurableDecode ExecutionInfrastructureReason = "durable_decode"
	ExecutionInfrastructureRuntimeIO     ExecutionInfrastructureReason = "runtime_io"
)

// ExecutionInfrastructureError preserves the local cause and the identity known
// at its source. Ordinal is a physical chapter position; nil means unknown.
// It does not imply that an operation which returned an error did not commit.
type ExecutionInfrastructureError struct {
	Reason    ExecutionInfrastructureReason
	Operation string
	JobKey    *JobKey
	Ordinal   *int64
	Err       error
}

func (e *ExecutionInfrastructureError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("execution infrastructure: %s (%s)", e.Reason, e.Operation)
	}
	return fmt.Sprintf("execution infrastructure: %s (%s): %v", e.Reason, e.Operation, e.Err)
}
func (e *ExecutionInfrastructureError) Unwrap() error { return e.Err }
func IsExecutionInfrastructureError(err error) bool {
	var e *ExecutionInfrastructureError
	return errors.As(err, &e) || errors.Is(err, ErrWorkflowNotDeterministic)
}

// ClassifyExecutionInfrastructure adds source context without replacing an
// existing classification (in particular a different prerequisite identity).
func ClassifyExecutionInfrastructure(err error, reason ExecutionInfrastructureReason, operation string, key *JobKey, ordinal *int64) error {
	if err == nil {
		return nil
	}
	var e *ExecutionInfrastructureError
	if errors.As(err, &e) {
		return err
	}
	if reason == ExecutionInfrastructureRuntimeIO && errors.Is(err, ErrWorkflowNotDeterministic) {
		reason = ExecutionInfrastructureDeterminism
	}
	if key != nil {
		copy := *key
		key = &copy
	}
	if ordinal != nil {
		copy := *ordinal
		ordinal = &copy
	}
	return &ExecutionInfrastructureError{Reason: reason, Operation: operation, JobKey: key, Ordinal: ordinal, Err: err}
}

// ClassifyRuntimeIO is for known runtime I/O boundaries, after input validation.
// Semantic rejections and standalone authority/cancellation retain their behavior.
func ClassifyRuntimeIO(err error, operation string, key *JobKey, ordinal *int64) error {
	if err == nil || IsExecutionInfrastructureError(err) {
		return ClassifyExecutionInfrastructure(err, ExecutionInfrastructureRuntimeIO, operation, key, ordinal)
	}
	var renewal *LeaseRenewalError
	var cell *CellError
	if errors.As(err, &renewal) || errors.As(err, &cell) {
		return err
	}
	for _, semantic := range []error{context.Canceled, context.DeadlineExceeded, ErrExecutionLeaseLost, ErrLeasePermissionDenied, ErrConflict, ErrJobNotFound, ErrChapterNotFound, ErrJobNotComplete, ErrJobCancelled, ErrJobSchemaValidation, ErrJobSchemaArchived, ErrJobSchemaNotFound, ErrGoalNotFound, ErrGoalConflict, ErrGoalFailed, ErrInvalidDependency, ErrDependencyWaitNotFound, ErrDependencyWaitConflict, ErrDependencyCycle, ErrInvalidAppendRequest, ErrAppendBusy, ErrPrimaryGoalSelectionUnsupported, ErrPrimaryGoalSelectionForbidden, ErrJobGoalSemanticsUnsupported, ErrJobRecordOperationUnsupported, ErrInvalidJobRecord, ErrSubmissionReceiptUnavailable} {
		if errors.Is(err, semantic) {
			return err
		}
	}
	var timeout TimeoutError
	if IsAppError(err) || IsSystemError(err) || errors.As(err, &timeout) || IsChapterPayloadFailure(err) {
		return err
	}
	return ClassifyExecutionInfrastructure(err, ExecutionInfrastructureRuntimeIO, operation, key, ordinal)
}

// RemoteExecutionCause is a bounded, safe descriptor, not a reconstructed Go
// backend error. Arbitrary local backend types remain available only locally.
type RemoteExecutionCause struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Truncated bool   `json:"-"`
}

func (e *RemoteExecutionCause) Error() string { return e.Message }
