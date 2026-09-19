package runtimecore

import "github.com/colony-2/jobdb/pkg/jobdb"

type TaskWork struct {
	TaskType      string
	ResumeJobType string
	InputOrdinal  int64
	OutputOrdinal int64
	InputHash     string
}

func executionState(policy jobdb.RunPolicy, task *TaskWork) jobdb.ExecutionState {
	state := jobdb.ExecutionState{RunPolicy: policy}
	if task != nil {
		state.TaskWait = &jobdb.TaskWait{InputOrdinal: task.InputOrdinal, OutputOrdinal: task.OutputOrdinal, InputHash: task.InputHash, ResumeNeed: task.ResumeJobType}
	}
	return jobdb.CloneExecutionState(state)
}
