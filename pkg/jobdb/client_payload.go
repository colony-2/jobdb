package jobdb

import (
	"encoding/json"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
)

type ClientPayloadUpdate = clientpayload.Update

// TaskWait is the explicit pending task route, never application payload.
type TaskWait struct {
	InputOrdinal  int64  `json:"inputOrdinal"`
	OutputOrdinal int64  `json:"outputOrdinal"`
	InputHash     string `json:"inputHash"`
	ResumeJobType string `json:"resumeJobType"`
}

// ExecutionState contains immutable policy and explicit current task coordinates.
type ExecutionState struct {
	RunPolicy RunPolicy `json:"runPolicy"`
	TaskWait  *TaskWait `json:"taskWait,omitempty"`
}

func CloneExecutionState(s ExecutionState) ExecutionState {
	raw, _ := json.Marshal(s)
	var out ExecutionState
	_ = json.Unmarshal(raw, &out)
	return out
}

// RescheduleTaskWait validates the complete target route. Task routes require
// coordinates on each operation; a job route clears them.
func RescheduleTaskWait(next Route, task *TaskWait) (*TaskWait, error) {
	if err := next.Validate(); err != nil {
		return nil, err
	}
	if next.TaskType == "" {
		if task != nil {
			return nil, fmt.Errorf("job route cannot have task coordinates")
		}
		return nil, nil
	}
	if task == nil || task.InputOrdinal < 0 || task.OutputOrdinal < 0 || task.InputHash == "" || ValidateIdentifier(task.ResumeJobType) != nil {
		return nil, fmt.Errorf("task route requires complete task coordinates")
	}
	copy := *task
	return &copy, nil
}
