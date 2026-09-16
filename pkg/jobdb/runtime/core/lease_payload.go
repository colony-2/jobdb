package runtimecore

import (
	"encoding/json"
	"fmt"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

// TaskWork is the current task route stored by a scheduler backend.
type TaskWork struct {
	TaskType      string
	ResumeJobType string
	InputOrdinal  int64
	OutputOrdinal int64
	InputHash     string
}

// LeasePayloadProjection contains the first-class scheduler fields used to
// construct the existing ExecutionLease.Payload JSON view. OpaquePresent
// distinguishes an explicit empty application payload from no application
// payload, because the former is visible as {} rather than a generated view.
type LeasePayloadProjection struct {
	RunPolicy     jobdb.RunPolicy
	TaskWork      *TaskWork
	Opaque        json.RawMessage
	OpaquePresent bool
}

// ProjectLeasePayload preserves the historical lease payload JSON contract.
// An explicit application JSON object wins over the generated run policy and
// task_wait view, matching existing JobDB direct and remote behavior.
func ProjectLeasePayload(req LeasePayloadProjection) (json.RawMessage, error) {
	payload := runtimecodec.SchedulerPayload{RunPolicy: req.RunPolicy}
	if req.TaskWork != nil {
		payload.TaskWait = &runtimecodec.TaskWait{
			InputStep:  req.TaskWork.InputOrdinal,
			OutputStep: req.TaskWork.OutputOrdinal,
			Next:       req.TaskWork.ResumeJobType,
			InputHash:  req.TaskWork.InputHash,
		}
	}
	if req.OpaquePresent {
		if !json.Valid(req.Opaque) {
			return nil, fmt.Errorf("opaque lease payload must be valid JSON")
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(req.Opaque, &fields); err != nil || fields == nil {
			return nil, fmt.Errorf("opaque lease payload must be a JSON object")
		}
		payload.VisiblePayload = append(json.RawMessage(nil), req.Opaque...)
	}
	return runtimecodec.SchedulerPayloadJSONView(payload)
}
