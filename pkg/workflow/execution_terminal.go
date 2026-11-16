package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// terminalJobCompletion validates the boundary before announcing a final job
// attempt's end. An accepted root result alone does not establish completion.
func (c *executionContext) terminalJobCompletion() error {
	e := c.run
	if !e.r.replay || e.terminal == nil {
		return nil
	}
	if next := e.peek(); next != nil {
		return e.control(fmt.Errorf("%w: unconsumed execution record at %d", ErrWorkflowNotDeterministic, next.ordinal))
	}
	if err := c.terminalBoundary("job_outcome", "", 0, nil, ""); err != nil {
		return err
	}
	return e.control(ReplayCacheMissError{JobKey: e.r.jobKey, Ordinal: e.ordinal, Reason: ReplayCacheMissJobResultMissing})
}

// terminalBoundary stops at a rejected append, not at an arbitrary cache miss.
// The marker is outside events: it proves failure, never a durable result.
// positions and taskType constrain external outcomes with incomplete identity diagnostics.
func (c *executionContext) terminalBoundary(kind, id string, attempt int, positions map[int64]bool, taskType string) error {
	e := c.run
	e.mu.Lock()
	defer e.mu.Unlock()
	return c.terminalBoundaryLocked(kind, id, attempt, positions, taskType)
}

func (c *executionContext) terminalBoundaryLocked(kind, id string, attempt int, positions map[int64]bool, taskType string) error {
	e := c.run
	if !e.r.replay || e.terminal == nil {
		return nil
	}
	if err := c.checkLocked(); err != nil {
		return err
	}
	if !e.terminalPosition || e.peekLocked() != nil {
		return nil
	}
	var app jobdb.AppError
	if !errors.As(e.terminal, &app) {
		return nil
	}
	// Decode a private view without altering the stored diagnostics or error.
	raw, err := json.Marshal(app.Payload.Attrs["payloadSize"])
	var size jobdb.ChapterApplicationPayloadError
	if err != nil || json.Unmarshal(raw, &size) != nil || size.Ordinal != e.final.Ordinal {
		return nil
	}
	if attempt == 0 {
		if i := strings.LastIndex(id, "/attempt:"); i >= 0 {
			attempt, _ = strconv.Atoi(id[i+len("/attempt:"):])
		}
	}
	if positions != nil && (kind != "result" || !positions[e.final.Ordinal]) {
		return nil
	}
	if size.Attempt != 0 && size.Attempt != attempt {
		return nil
	}
	exact := kind != "job_outcome" && size.RecordKind == kind && size.LogicalID == id && !strings.HasSuffix(size.LogicalID, "...")
	if !exact {
		if positions != nil {
			// The retained wait supplies the identity when diagnostics omit or
			// shorten it. Never use a shortened ID's prefix to establish it.
			if (size.RecordKind != "" && size.RecordKind != kind) ||
				(size.LogicalID != "" && size.LogicalID != id && !strings.HasSuffix(size.LogicalID, "...")) ||
				size.TaskType != taskType || e.final.TaskType != taskType {
				return nil
			}
		} else {
			// A legacy root outcome may have no execution identity. Only its
			// root result boundary and original job type can use that fact.
			rootOutcome := kind == "job_outcome" || (kind == "result" &&
				id == `/scope:"$job"#1/attempt:`+strconv.Itoa(attempt) && attempt > 0)
			if size.RecordKind != "" || size.LogicalID != "" || !rootOutcome ||
				size.TaskType != e.r.worker.JobWorker.Name() || e.final.TaskType != size.TaskType {
				return nil
			}
		}
	}
	return e.controlLocked(e.terminal)
}
