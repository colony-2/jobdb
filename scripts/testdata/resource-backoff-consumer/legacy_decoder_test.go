// Frozen execution reader from 67cce3c7a7954cc334b06f5d881a2b16d3f9c490.
// Only package names and references to shared model types are qualified.
package resourceconsumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"io"
	"strings"
	"time"
)

// beforeResourceExecutionPatternVersion pins the semantics of the client execution protocol.
const beforeResourceExecutionPatternVersion uint32 = 4

type beforeResourceExecutionVersions struct {
	beforeResourceExecutionPatternVersion uint32            `json:"execution_pattern_version"`
	JobDBVersion                          string            `json:"jobdb_version"`
	ConsumerVersions                      map[string]string `json:"consumer_versions,omitempty"`
}

// beforeResourceExecutionSnapshot is an immutable value captured by the engine, never a live
// resource or secret cache. jobdb.Artifact bytes survive worker and backend changes.
type beforeResourceExecutionSnapshot struct {
	Data      json.RawMessage                   `json:"data"`
	Artifacts []beforeResourceExecutionArtifact `json:"artifacts,omitempty"`
}
type beforeResourceExecutionArtifact struct {
	Key    *jobdb.ArtifactKey `json:"key,omitempty"`
	Name   string             `json:"name"`
	source jobdb.Artifact
}

// beforeResourceExecutionRecord is the versioned execution history wire contract. Unknown
// fields and kinds are rejected rather than interpreted as a different event.
type beforeResourceExecutionRecord struct {
	Version           uint32                           `json:"version"`
	Kind              string                           `json:"kind"`
	ID                string                           `json:"id,omitempty"`
	Activation        string                           `json:"activation,omitempty"`
	Versions          *beforeResourceExecutionVersions `json:"versions,omitempty"`
	At                time.Time                        `json:"at"`
	Descriptor        json.RawMessage                  `json:"descriptor,omitempty"`
	Attempt           int                              `json:"attempt,omitempty"`
	Deadline          time.Time                        `json:"deadline,omitempty"`
	TotalDeadline     time.Time                        `json:"total_deadline,omitempty"`
	Input             *beforeResourceExecutionSnapshot `json:"input,omitempty"`
	Output            *beforeResourceExecutionSnapshot `json:"output,omitempty"`
	ErrorKind         string                           `json:"error_kind,omitempty"`
	Error             json.RawMessage                  `json:"error,omitempty"`
	Rule              string                           `json:"rule,omitempty"`
	Disposition       string                           `json:"disposition,omitempty"`
	Retryable         *bool                            `json:"retryable,omitempty"`
	Retry             bool                             `json:"retry,omitempty"`
	WakeAt            time.Time                        `json:"wake_at,omitempty"`
	Source            *jobdb.JobKey                    `json:"source,omitempty"`
	Payload           json.RawMessage                  `json:"payload,omitempty"`
	Dependencies      []jobdb.Dependency               `json:"dependencies,omitempty"`
	Prerequisites     []jobdb.JobPrerequisite          `json:"prerequisites,omitempty"`
	AlternateAt       *time.Time                       `json:"alternate_at,omitempty"`
	ExecutionTaskType string                           `json:"execution_task_type,omitempty"`
}

func beforeResourceDecodeExecutionRecord(raw []byte) (beforeResourceExecutionRecord, error) {
	var r beforeResourceExecutionRecord
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, fmt.Errorf("invalid execution record: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return r, fmt.Errorf("trailing execution data")
	}
	if r.Version != beforeResourceExecutionPatternVersion {
		return r, fmt.Errorf("unsupported execution version %d (current %d)", r.Version, beforeResourceExecutionPatternVersion)
	}
	if r.Version == 1 && (r.Kind == "dispatch" || r.AlternateAt != nil || r.ExecutionTaskType != "") {
		return r, fmt.Errorf("task dispatch records require execution version 2")
	}
	switch r.Kind {
	case "header", "activation":
		if r.Versions == nil || r.Versions.beforeResourceExecutionPatternVersion != r.Version || r.Versions.JobDBVersion == "" {
			return r, fmt.Errorf("invalid execution versions")
		}
	case "dependency_binding", "checkpoint", "dispatch", "join_result", "admission", "scope", "task", "attempt", "result", "catch", "handled", "decision", "end", "wait", "restart", "continuation", "sleep", "join":
		if r.ID == "" {
			return r, fmt.Errorf("execution %s requires identity", r.Kind)
		}
	default:
		return r, fmt.Errorf("unknown execution kind %q", r.Kind)
	}
	if r.Versions != nil {
		for k, v := range r.Versions.ConsumerVersions {
			if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
				return r, fmt.Errorf("invalid consumer versions")
			}
		}
	}
	switch r.Kind {
	case "dependency_binding":
		var wait jobdb.DependencyWaitInfo
		if json.Unmarshal(r.Payload, &wait) != nil || wait.WaitID == "" || wait.State == jobdb.DependencyPending || wait.State == "" {
			return r, fmt.Errorf("invalid dependency binding")
		}
	case "dispatch":
		if r.ExecutionTaskType == "" {
			return r, fmt.Errorf("execution dispatch requires task type")
		}
	case "scope", "task":
		if !json.Valid(r.Descriptor) {
			return r, fmt.Errorf("execution %s requires a descriptor", r.Kind)
		}
	case "attempt":
		if r.Attempt < 1 {
			return r, fmt.Errorf("execution attempt must be positive")
		}
	case "join_result", "admission", "result", "handled", "end":
		if r.Output == nil && r.ErrorKind == "" {
			return r, fmt.Errorf("execution outcome requires output or error")
		}
		if r.Version < 2 && r.Output != nil && r.ErrorKind != "" {
			return r, fmt.Errorf("execution diagnostics alongside errors require version 2")
		}
	case "wait":
		if r.Input == nil || !json.Valid(r.Descriptor) {
			return r, fmt.Errorf("execution wait requires input and task type")
		}
	case "restart":
		if r.Source == nil || r.Source.Validate() != nil {
			return r, fmt.Errorf("execution restart requires source job")
		}
	case "continuation":
		if !json.Valid(r.Payload) {
			return r, fmt.Errorf("execution continuation requires budgets")
		}
	}
	if r.ErrorKind != "" {
		switch r.ErrorKind {
		case "AppError", "SystemError", "Timeout":
		default:
			return r, fmt.Errorf("invalid execution error kind")
		}
		if !json.Valid(r.Error) {
			return r, fmt.Errorf("invalid execution error payload")
		}
	}
	if r.Disposition != "" && r.Disposition != "handled" && r.Disposition != "continue" && r.Disposition != "propagate" {
		return r, fmt.Errorf("invalid execution disposition")
	}
	for _, snapshot := range []*beforeResourceExecutionSnapshot{r.Input, r.Output} {
		if snapshot != nil {
			if !json.Valid(snapshot.Data) {
				return r, fmt.Errorf("invalid execution snapshot")
			}
			names := map[string]bool{}
			for _, a := range snapshot.Artifacts {
				if a.Name == "" || names[a.Name] {
					return r, fmt.Errorf("invalid execution artifact names")
				}
				names[a.Name] = true
				if a.Key == nil || a.Key.Validate() != nil || a.Key.SizeBytes < 0 {
					return r, fmt.Errorf("execution artifact %q requires a durable key and known size", a.Name)
				}
			}
		}
	}
	return r, nil
}
