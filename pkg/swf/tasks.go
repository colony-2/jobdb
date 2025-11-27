package swf

import (
	"context"
	"log/slog"
	"time"
)

type taskRunApi interface {
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]TaskHandle, error)
}

type TaskContext struct {
	JobId  JobId
	Step   int64
	Logger *slog.Logger
}

// AwaitDuration pauses task execution for the specified duration.
// The engine may override this in the future to reschedule work.
func (TaskContext) AwaitDuration(waitFor Duration) error {
	time.Sleep(waitFor.ToDuration())
	return nil
}

type Worker interface {
	worker()
}

type TaskHandle interface {
	JobId() JobId
	Data() (TaskData, error)
	Finish(ctx context.Context, taskData TaskData) error
}

type TaskCompletion struct {
	JobId JobId
	Step  int64
	Error error
}
