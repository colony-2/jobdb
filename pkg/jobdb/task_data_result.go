package jobdb

// TaskDataResultCarrier exposes an underlying TaskData plus a deferred error.
// It is used by GetJob implementations that want to preserve lazy access while
// still allowing callers to recover the old `(TaskData, error)` shape when
// needed.
type TaskDataResultCarrier interface {
	TaskData
	TaskDataResult() (TaskData, error)
}

// ExtractTaskDataResult unwraps a TaskDataResultCarrier when present.
// For ordinary TaskData values it returns the value unchanged with a nil error.
func ExtractTaskDataResult(data TaskData) (TaskData, error) {
	if data == nil {
		return nil, nil
	}
	if carrier, ok := data.(TaskDataResultCarrier); ok {
		return carrier.TaskDataResult()
	}
	return data, nil
}
