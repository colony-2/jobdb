package internal

import (
	"context"
	"log/slog"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
)

// RunnerBackend abstracts external interactions used by runner.
type RunnerBackend interface {
	// GetChapter returns the chapter at ordinal or a not-found error.
	GetChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
	// SaveChapter persists the chapter payload and artifacts.
	SaveChapter(ctx context.Context, key story.Key, chap story.Chapter) error
	// GetJobAttemptOutcome retrieves the job attempt outcome chapter (may be specialized).
	GetJobAttemptOutcome(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
	// AwaitUntil blocks or reschedules until wakeAt; replay backends should not wait.
	AwaitUntil(ctx context.Context, wakeAt time.Time, info AwaitInfo) error
	// AwaitJobs blocks or reschedules until dependencies complete.
	// Returns whether the call rescheduled execution.
	AwaitJobs(ctx context.Context, jobIds []string, info AwaitInfo) (bool, error)
	// AfterSaveTaskOutput allows backend-specific wrapping of output artifacts
	// (e.g., fallback artifacts) after a successful save.
	AfterSaveTaskOutput(output jobdb.TaskData, dataBytes jobdb.Data, artifacts []jobdb.Artifact, digests []string, key story.Key, ordinal int64, logger *slog.Logger) (jobdb.TaskData, error)
}

// AwaitInfo provides context for awaits.
type AwaitInfo struct {
	JobKey   jobdb.JobKey
	TaskType string // empty for job-level awaits
	Ordinal  int64
	Attempt  int
}

// Lease is the internal lease interface used by runner.
type Lease interface {
	KeepAlive(ctx context.Context) error
	StopKeepAlive()
	CompleteWithStatus(ctx context.Context, status pgwf.CompletionStatus, completionDetail string) error
	Reschedule(ctx context.Context, deps pgwf.JobDependencies, payload any) error
	NextNeed() pgwf.Capability
	Payload() []byte
}
