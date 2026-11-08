package jobdb

// JobCancelledError preserves the persisted completion detail of a cancelled
// job. It matches ErrJobCancelled through errors.Is, independently of its detail.
type JobCancelledError struct {
	// Detail is the exact stored completion detail, without trimming or truncation.
	// It is empty when the cancellation has no supplied reason.
	Detail string
}

func (e *JobCancelledError) Error() string {
	if e == nil || e.Detail == "" {
		return ErrJobCancelled.Error()
	}
	return ErrJobCancelled.Error() + ": " + e.Detail
}

func (e *JobCancelledError) Unwrap() error { return ErrJobCancelled }
