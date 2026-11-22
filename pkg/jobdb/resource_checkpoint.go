package jobdb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ResourceCheckpoint records original engine coordinates and durable input. Its
// distinct history kind makes incompatible execution readers reject it.
type ResourceCheckpoint struct {
	Frames            []ResourceCheckpointFrame `json:"frames,omitempty"`
	Version           int                       `json:"version"`
	Invocation        string                    `json:"invocation"`
	Attempt           int                       `json:"attempt"`
	Policy            ResourceBackoffPolicy     `json:"policy"`
	ExecutionTaskType string                    `json:"executionTaskType,omitempty"`
	PayloadRevision   int64                     `json:"payloadRevision"`
}

func ResourceCheckpointHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func ValidateResourceCheckpoint(ch Chapter, req ResourceBackoffRequest) error {
	body, ok := ch.Body.(ExecutionChapter)
	if !ok || ch.Ordinal != req.Resume.CheckpointOrdinal || ResourceCheckpointHash(body.Record) != req.Resume.CheckpointHash {
		return fmt.Errorf("%w: checkpoint selection", ErrInvalidResourceBackoff)
	}
	rec, err := DecodeExecutionRecord(body.Record)
	if err != nil {
		return err
	}
	if req.Resume.TaskWait != nil && rec.Resource != nil && rec.Resource.ExecutionTaskType != req.Resume.Route.TaskType {
		return fmt.Errorf("%w: checkpoint implementation", ErrInvalidResourceBackoff)
	}
	if rec.Kind != "resource_checkpoint" || rec.ID != req.Invocation || rec.Resource == nil || rec.Resource.Attempt != req.Attempt || rec.Resource.PayloadRevision != req.Resume.PayloadRevision {
		return fmt.Errorf("%w: checkpoint coordinates", ErrInvalidResourceBackoff)
	}
	p, _ := json.Marshal(normalizedResourcePolicy(rec.Resource.Policy))
	q, _ := json.Marshal(normalizedResourcePolicy(req.Policy))
	if string(p) != string(q) {
		return fmt.Errorf("%w: checkpoint policy", ErrInvalidResourceBackoff)
	}
	return nil
}

// ResourceCheckpointFrame preserves an admitted unfinished task across sibling
// suspension. Completed tasks remain in ordinary immutable history.
type ResourceCheckpointFrame struct {
	Invocation        string             `json:"invocation"`
	Attempt           int                `json:"attempt"`
	Input             *ExecutionSnapshot `json:"input"`
	Deadline          time.Time          `json:"deadline"`
	TotalDeadline     time.Time          `json:"totalDeadline"`
	ExecutionTaskType string             `json:"executionTaskType"`
}

// ValidateResourceTaskCheckpoint compares an execution-task continuation to its
// immutable declared input and original timers. Legacy manual task chapters
// retain their existing generation check in the scheduler.
func ValidateResourceTaskCheckpoint(checkpoint, declaration Chapter, req ResourceBackoffRequest) error {
	body, ok := declaration.Body.(ExecutionChapter)
	if !ok {
		return nil
	}
	original, err := DecodeExecutionRecord(body.Record)
	if err != nil {
		return err
	}
	if original.Kind != "wait" {
		return fmt.Errorf("%w: task declaration", ErrInvalidResourceBackoff)
	}
	selected, err := DecodeExecutionRecord(checkpoint.Body.(ExecutionChapter).Record)
	if err != nil {
		return err
	}
	p, _ := json.Marshal(original.Input)
	q, _ := json.Marshal(selected.Input)
	if string(p) != string(q) {
		return fmt.Errorf("%w: task input", ErrInvalidResourceBackoff)
	}
	if !original.TotalDeadline.Equal(selected.TotalDeadline) ||
		!original.TotalDeadline.IsZero() && (selected.Deadline.IsZero() || selected.Deadline.After(original.TotalDeadline)) {
		return fmt.Errorf("%w: task total deadline", ErrInvalidResourceBackoff)
	}
	if original.ID != req.Invocation {
		// A worker can run an ordinary recipe retry locally while retaining the
		// acquired external generation. Its new attempt owns fresh recorded timers;
		// the logical invocation and all ancestor attempts must still match exactly.
		index := strings.LastIndex(original.ID, "/attempt:")
		if index < 0 {
			return fmt.Errorf("%w: task attempt", ErrInvalidResourceBackoff)
		}
		previous, err := strconv.Atoi(original.ID[index+len("/attempt:"):])
		if err != nil || previous < 1 || req.Attempt <= previous || req.Invocation != fmt.Sprintf("%s/attempt:%d", original.ID[:index], req.Attempt) {
			return fmt.Errorf("%w: task attempt", ErrInvalidResourceBackoff)
		}
		return nil
	}
	if !original.Deadline.Equal(selected.Deadline) {
		return fmt.Errorf("%w: task input or deadline", ErrInvalidResourceBackoff)
	}
	return nil
}
