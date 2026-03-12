package swf

import (
	"log/slog"
	"time"
)

// WorkflowRuntime constructs an SWF engine without exposing backend-specific objects
// through the main engine package.
type WorkflowRuntime interface {
	BuildEngine(workers []WorkSet, opts RuntimeBuildOptions) (SWFEngine, error)
}

type RuntimeBuildOptions struct {
	Logger                *slog.Logger
	MaxActive             int
	AwaitRecycleThreshold time.Duration
}
