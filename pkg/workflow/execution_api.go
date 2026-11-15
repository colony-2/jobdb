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
	// ObserveTaskResults optionally creates invocation-local task-result state.
	// See TaskResultObserverProvider for context, ordering and lifetime requirements.
	ObserveTaskResults func(context.Context) (context.Context, func(TaskEndEvent))
}

// TaskResultObserverProvider optionally observes accepted task-attempt results.
// The runner calls NewTaskResultObserver once per execution or replay invocation,
// before orchestration and outside whole-job retries. The returned nonnil context
// must derive from the input, preserving cancellation, lease authority and values;
// every attempt inherits it even when the returned callback is nil.
//
// Callback delivery is synchronous and serialized within an invocation, before
// existing task-end observers, catches, retries and subsequent orchestration on
// that path. Concurrent branches gain no additional scheduling order. Events use
// accepted stored results after reconciliation, including failed attempts, and
// exclude synthetic catch/scope outputs and uncommitted work. Replay and resume
// reconstruct fresh state only as retained results are consumed, not in advance.
//
// Allocate mutable state in the factory, not on a shared worker. Callbacks should
// do prompt deterministic bookkeeping, avoid execution reentry and never wait on
// asynchronous listeners. Synchronize any other concurrent access to that state.
// This is not durable exactly-once external delivery and adds no panic recovery.
// Worker wrappers must forward this optional capability to preserve it.
type TaskResultObserverProvider interface {
	NewTaskResultObserver(context.Context) (context.Context, func(TaskEndEvent))
}

// NewTaskResultObserver delegates to ObserveTaskResults. A nil factory returns
// the incoming context unchanged and no callback.
func (w ExecutionWorker) NewTaskResultObserver(ctx context.Context) (context.Context, func(TaskEndEvent)) {
	if w.ObserveTaskResults == nil {
		return ctx, nil
	}
	return w.ObserveTaskResults(ctx)
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
	// The builder runs synchronously on the orchestration goroutine. TaskWorker.Run
	// runs in a separate goroutine and may outlive cancellation; it must not share
	// mutable state with orchestration or another attempt. Put execution,
	// preparation, and external effects in the TaskWorker.
	DoTask(TaskCall, func(context.Context) (TaskData, error)) (TaskData, error)
	// DoScope runs its body, restart hook, and catch callbacks synchronously.
	// Cancellation does not return control until the callback and its defers finish.
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
	// Run returns for confirmation. Orchestration must propagate the returned
	// error to unwind; post-Yield task worker code may still be running.
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
// Propagate it to stop execution. Post-Yield task worker code may outlive Run;
// use JobRunOutcome.Handoff to publish handoff notifications.
func IsExecutionSuspended(err error) bool {
	return errors.Is(err, errExecutionSuspended)
}
