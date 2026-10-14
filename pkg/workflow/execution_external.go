package workflow

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (c *executionContext) externalTask(call TaskCall, id string, input *jobdb.ExecutionSnapshot, deadline, total time.Time) (jobdb.ExecutionRecord, error) {
	rec, err := c.event("wait", id, func() *jobdb.ExecutionRecord {
		return &jobdb.ExecutionRecord{Input: input, Deadline: deadline, TotalDeadline: total, Descriptor: json.RawMessage(fmt.Sprintf("%q", call.TaskType))}
	})
	if err != nil {
		return jobdb.ExecutionRecord{}, err
	}
	if next := c.run.peek(); next != nil {
		return c.event("result", id, func() *jobdb.ExecutionRecord { return nil })
	}
	if c.run.r.replay {
		return jobdb.ExecutionRecord{}, ReplayCacheMissError{JobKey: c.GetJobKey(), Ordinal: c.run.nextOrdinal(), Reason: ReplayCacheMissTaskResultMissing}
	}
	if !rec.Deadline.IsZero() && !time.Now().Before(rec.Deadline) {
		scope := TimeoutScopeInvocation
		retryable := true
		if !rec.TotalDeadline.IsZero() && !rec.Deadline.Before(rec.TotalDeadline) {
			scope = TimeoutScopeTotal
			retryable = false
		}
		return c.finishBody("result", id, nil, NewTimeoutError("task", 0, scope, nil, retryable), rec.Input)
	}
	// The unique persisted wait record is the generation. Its ordinal and digest
	// bind completion to this invocation, even when successive inputs are equal.
	ordinal := c.run.nextOrdinal() - 1
	for _, event := range c.run.events {
		if event.record.Kind == "wait" && event.record.ID == id {
			ordinal = event.ordinal
		}
	}
	raw, _ := json.Marshal(rec)
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	req := RescheduleExecutionRequest{NextRoute: Route{JobType: c.run.r.worker.JobWorker.Name(), TaskType: call.TaskType}, TaskWait: &TaskWait{InputOrdinal: ordinal, OutputOrdinal: ordinal + 1, InputHash: hash, ResumeJobType: c.run.r.worker.JobWorker.Name()}}
	if !rec.Deadline.IsZero() {
		route := Route{JobType: c.run.r.worker.JobWorker.Name()}
		delay := max(time.Until(rec.Deadline), 0)
		req.AlternateRoute = &route
		req.AlternateAfter = &delay
	}
	err = c.run.r.lease.Reschedule(c.ctx, req)
	if err != nil {
		return jobdb.ExecutionRecord{}, c.run.control(err)
	}
	return jobdb.ExecutionRecord{}, c.run.suspend()
}
