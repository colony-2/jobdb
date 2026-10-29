package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ExecutionWorker implements JobWorker with an execution callback.
type ExecutionWorker struct {
	JobType string
	Execute func(ExecutionContext, JobData) (JobData, error)
}

func (w ExecutionWorker) Name() string { return w.JobType }
func (w ExecutionWorker) Run(ctx ExecutionContext, input JobData) (JobData, error) {
	if w.Execute == nil {
		return nil, fmt.Errorf("execution worker callback is required")
	}
	return w.Execute(ctx, input)
}

type ExecutionContext interface {
	Context() context.Context
	GetJobKey() JobKey
	// Job returns the authoritative tenant, primary goal and job identity.
	Job() JobHandle
	Logger() *slog.Logger
	ExecutionVersions() jobdb.ExecutionVersions
	// DoTask invokes durable work. Its input builder must be deterministic and
	// side-effect-free: inspection may reconstruct unfinished input by calling
	// it. Recorded results and external waits reuse their stored input instead.
	// Put execution, preparation, and external effects in the TaskWorker.
	DoTask(TaskCall, func(context.Context) (TaskData, error)) (TaskData, error)
	DoScope(string, ScopeOptions, func(ExecutionContext) (JobData, error)) (JobData, error)
	AwaitDuration(Duration) error
	AwaitJobs(...string) error
	// AwaitGoals waits for successful goal outcomes and returns their committed job bindings.
	AwaitGoals(...string) ([]jobdb.DependencyResolution, error)
	// AwaitDependencies supports mixed exact-job and goal dependencies.
	AwaitDependencies(...jobdb.Dependency) ([]jobdb.DependencyResolution, error)
	// GetConfiguration returns a copy of the first successful tenant snapshot
	// in this live job attempt. Tasks share it; whole-job retries reset it.
	GetConfiguration(context.Context) (ConfigurationSnapshot, error)
	// ResolveSecrets caches allowed values and omissions by name for this live
	// job attempt. Nothing is recorded; offline replay rejects these reads.
	ResolveSecrets(context.Context, []string) (SecretResolveResult, error)
	// ResolveSecretsFresh refreshes only the requested names in this attempt's cache.
	ResolveSecretsFresh(context.Context, []string) (SecretResolveResult, error)
	ClientPayload() json.RawMessage
	ClientPayloadRevision() int64
	SubmitJob(context.Context, SubmitJob) (JobHandle, error)
	SubmitRestartJob(context.Context, SubmitRestartJobRequest) (JobHandle, error)
	// Yield reschedules and stops execution. Read JobRunOutcome.Handoff after
	// Run returns for confirmation; post-Yield user code may still be running.
	Yield(context.Context, YieldRequest) error
}

// TaskCall identifies durable work. Once recorded, its timeout and retry
// policy is authoritative for replay and recovery. Site, task type, contract
// version, command, catch rule IDs, and alternate declaration must still match.
type TaskCall struct {
	Site            string              `json:"site"`
	TaskType        string              `json:"task_type"`
	ContractVersion uint32              `json:"contract_version"`
	Command         json.RawMessage     `json:"command"`
	Policy          RunPolicy           `json:"policy"`
	Catch           []CatchRule         `json:"-"`
	Alternate       *TaskAlternateAfter `json:"alternate,omitempty"`
}

// TaskAlternateAfter makes another implementation eligible after a durable
// external wait. A locally registered primary still executes immediately.
// After starts at the first wait and is preserved across recovery. Acquisition
// of the alternate commits its selection for subsequent task retries. The due
// time is not a strict cutoff for external answers; normal lease fencing applies.
type TaskAlternateAfter struct {
	TaskType string        `json:"task_type"`
	After    time.Duration `json:"after"`
}

// ScopeOptions supplies policy for a new scope occurrence. Existing occurrences
// retain their recorded timeout and retry settings during replay and recovery;
// callbacks must retain the recorded catch rule IDs and restart-hook presence.
// An organizational scope omitted from compact history remains unlimited and
// non-retrying when reconstructed. Newly entered scopes use current options.
type ScopeOptions struct {
	TotalTimeout   time.Duration
	AttemptTimeout time.Duration
	Retry          RetryPolicy
	Catch          []CatchRule
	// OnRestart runs with fresh consumer state before the selected scope body.
	OnRestart func(json.RawMessage) error
}

type CatchRule struct {
	ID     string
	Match  func(error) bool
	Handle func(ExecutionContext, error) (CatchResult, error)
}
type CatchDisposition string

const (
	CatchHandled   CatchDisposition = "handled"
	CatchContinue  CatchDisposition = "continue"
	CatchPropagate CatchDisposition = "propagate"
)

type CatchResult struct {
	Disposition CatchDisposition
	Output      TaskData
	Err         error
}

func copyConsumerVersions(in map[string]string) (map[string]string, error) {
	for k, v := range in {
		if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("consumer version names and values must be nonempty")
		}
	}
	return maps.Clone(in), nil
}

// IsExecutionSuspended recognizes the suspension control error through wrappers.
// Propagate it to stop task execution. It does not order post-Yield code before
// Run returns; use JobRunOutcome.Handoff to publish handoff notifications.
func IsExecutionSuspended(err error) bool {
	return errors.Is(err, errExecutionSuspended)
}
