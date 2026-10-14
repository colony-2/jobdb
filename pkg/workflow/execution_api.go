package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ExecutionWorker selects the versioned invocation/scope engine. It is the
// temporary registration surface while consumers migrate off JobWorker.Run.
type ExecutionWorker struct {
	JobType string
	Execute func(ExecutionContext, JobData) (JobData, error)
}

func (w ExecutionWorker) Name() string { return w.JobType }
func (w ExecutionWorker) Run(JobContext, JobData) (JobData, error) {
	return nil, fmt.Errorf("execution worker requires the versioned engine")
}

type ExecutionContext interface {
	Context() context.Context
	GetJobKey() JobKey
	Logger() *slog.Logger
	ExecutionVersions() jobdb.ExecutionVersions
	DoTask(TaskCall, func(context.Context) (TaskData, error)) (TaskData, error)
	DoScope(string, ScopeOptions, func(ExecutionContext) (JobData, error)) (JobData, error)
	AwaitDuration(Duration) error
	AwaitJobs(...string) error
	GetConfiguration(context.Context) (ConfigurationSnapshot, error)
	ResolveSecrets(context.Context, []string) (SecretResolveResult, error)
	ClientPayload() json.RawMessage
	ClientPayloadRevision() int64
	SubmitJob(context.Context, SubmitJob) (JobKey, error)
	SubmitRestartJob(context.Context, SubmitRestartJob) (JobKey, error)
	Yield(context.Context, RescheduleExecutionRequest) error
}

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

func (w ExecutionWorker) executionDefinition() ExecutionWorker { return w }
