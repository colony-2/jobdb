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
		state.TaskWait = &jobdb.TaskWait{InputOrdinal: task.InputOrdinal, OutputOrdinal: task.OutputOrdinal, InputHash: task.InputHash, ResumeJobType: task.ResumeJobType}
	}
	return jobdb.CloneExecutionState(state)
}

func resourceExecutionState(policy jobdb.RunPolicy, task *TaskWork, budget *int64, capability *jobdb.ResourceBackoffCapability, resume *jobdb.ResourceResume) jobdb.ExecutionState {
	s := executionState(policy, task)
	s.CrashBudgetRemaining = budget
	s.ResourceBackoff = capability
	s.ResourceResume = resume
	return jobdb.CloneExecutionState(s)
}
