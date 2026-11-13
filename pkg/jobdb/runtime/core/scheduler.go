package runtimecore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Scheduler persists job state and performs atomic lease
// mutations for runtime core.
type Scheduler interface {
	jobdb.LeaseEventReader
	jobdb.CellRuntime
	CreateCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.CreateCellRequest) (jobdb.Cell, error)
	ResolveCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
	LookupCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
	GoalScheduler
	CreateJob(ctx context.Context, req CreateJobRequest) (StoredJob, error)
	GetJob(ctx context.Context, key jobdb.JobKey) (StoredJob, error)
	ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)
	CancelJob(ctx context.Context, req CancelJobMutation) (StoredJob, error)
	// ClearCrashConcern atomically resets only the consecutive expiry counter.
	// Reject live leases and final/hidden jobs without mutating them.
	ClearCrashConcern(context.Context, jobdb.ClearCrashConcernRequest) error

	AcquireWork(ctx context.Context, req WorkRequest) ([]LeaseSnapshot, error)
	AcquireJobLease(ctx context.Context, req JobLeaseRequest) (*LeaseSnapshot, error)
	ValidateLease(ctx context.Context, identity LeaseIdentity) (LeaseSnapshot, error)
	KeepAliveLease(ctx context.Context, mutation LeaseMutation) (LeaseSnapshot, error)
	CompleteLease(ctx context.Context, mutation CompletionMutation) (StoredJob, error)
	RescheduleLease(ctx context.Context, mutation RescheduleMutation) (StoredJob, error)

	GetWaitingTask(ctx context.Context, key jobdb.JobKey) (WaitingTaskSnapshot, error)
	ClaimTask(ctx context.Context, req ClaimTaskRequest) (LeaseIdentity, error)
}

// CreateJobRequest contains the immutable job facts and initial route written
// after runtime core has validated and encoded the first chapter.
type CreateJobRequest struct {
	// SelectedGoalID requires the scheduler to atomically validate authority,
	// ownership, openness and cycles and retain the immutable accepted receipt.
	SelectedGoalID        string
	Cell                  jobdb.ResolvedCell
	GoalID                string
	ShareGoal             bool
	Dependencies          []jobdb.Dependency
	ClientPayload         json.RawMessage
	ClientPayloadRevision int64
	InitialPayloadDigest  string
	JobKey                jobdb.JobKey
	JobType               string
	ParentJobID           string
	RunPolicy             jobdb.RunPolicy
	AppMetadata           json.RawMessage
	SchemaHash            string
	WaitForJobIDs         []string
	AvailableAt           *time.Time
	ExpiresAt             *time.Time
	CreatedAt             time.Time
	WorkerID              string
}

// WorkKind identifies whether the current route runs a job or a task.
type WorkKind string

const (
	WorkKindJob  WorkKind = "JOB"
	WorkKindTask WorkKind = "TASK"
)

// AlternateRoute is an optional delayed route for a stalled job.
type AlternateRoute struct {
	JobType  string
	TaskType string
	After    time.Duration
}

// StoredJob joins immutable facts with current or archived scheduler state.
type StoredJob struct {
	Cell                  jobdb.ResolvedCell
	BlockingWaitSummary   *jobdb.DependencyWaitSummary
	BlockingWait          *jobdb.DependencyWaitInfo
	GoalID                string
	OtherGoalIDs          []string
	JobKey                jobdb.JobKey
	Store                 jobdb.JobStore
	JobType               string
	Status                jobdb.JobStatus
	RouteJobType          string
	WorkKind              WorkKind
	TaskWork              *TaskWork
	AlternateRoute        *AlternateRoute
	RunPolicy             jobdb.RunPolicy
	ClientPayload         json.RawMessage
	ClientPayloadRevision int64
	InitialPayloadDigest  string
	AppMetadata           json.RawMessage
	SchemaHash            string
	ParentJobID           string
	WaitForJobIDs         []string
	AvailableAt           time.Time
	ExpiresAt             *time.Time
	LeaseExpiresAt        *time.Time
	LeaseWorkerID         string
	CreatedAt             time.Time
	UpdatedAt             time.Time
	ArchivedAt            *time.Time
	CancelRequested       bool
	Completion            *CompletionSnapshot
	Lease                 *LeaseIdentity
}

// CompletionSnapshot describes a terminal scheduler outcome.
type CompletionSnapshot struct {
	Status    string
	Detail    string
	ErrorKind string
	Retryable *bool
}

// ListJobsRequest is the scheduler-side form of job listing.
type ListJobsRequest struct {
	CellNames []string
	GoalIDs   []string
	// IncludeHidden must be applied before pagination by deletion-capable stores.
	IncludeHidden bool
	TenantIds     []string
	Statuses      []jobdb.JobStatus
	Stores        []jobdb.JobStore
	JobTypes      []string
	JobTasks      []jobdb.JobTaskFilter
	// AvailableRoutes uses the same eligibility and effective-route rules as
	// AcquireWork at Now, before pagination. It must not claim or change jobs.
	// Empty disables this filter; all other filters still intersect it.
	AvailableRoutes []jobdb.Route
	Now             time.Time
	JobKeys         []jobdb.JobKey
	ParentJobIDs    []string
	RootOnly        bool
	MetadataEquals  []jobdb.MetadataPredicate
	CreatedAfter    *time.Time
	CreatedBefore   *time.Time
	PageSize        int
	PageToken       string
}

// ListJobsResponse is the scheduler-side job list result.
type ListJobsResponse struct {
	Jobs          []StoredJob
	NextPageToken string
}

// WorkRequest asks the scheduler to atomically lease queue work.
type WorkRequest struct {
	CellNames      []string
	TenantId       string
	WorkerID       string
	Selector       WorkSelector
	Limit          int
	LeaseDuration  time.Duration
	MetadataEquals []jobdb.MetadataPredicate
	Now            time.Time
}

// WorkSelector groups typed public routes into job and task selectors.
type WorkSelector struct {
	JobTypes []string
	Tasks    []jobdb.JobTaskFilter
}

// JobLeaseRequest asks the scheduler to atomically lease one specific job.
type JobLeaseRequest struct {
	JobKey        jobdb.JobKey
	WorkerID      string
	Selector      WorkSelector
	LeaseDuration time.Duration
	Now           time.Time
}

// LeaseIdentity identifies the current owner of a live scheduler lease.
type LeaseIdentity struct {
	JobKey    jobdb.JobKey
	LeaseID   string
	WorkerID  string
	ExpiresAt time.Time
}

// LeaseSnapshot is the typed lease returned by the scheduler.
type LeaseSnapshot struct {
	Cell                  jobdb.ResolvedCell
	GoalID                string
	Identity              LeaseIdentity
	JobType               string
	RouteJobType          string
	WorkKind              WorkKind
	TaskWork              *TaskWork
	RunPolicy             jobdb.RunPolicy
	ClientPayload         json.RawMessage
	ClientPayloadRevision int64
	InitialPayloadDigest  string
	SchemaHash            string
	Duration              time.Duration
}

// LeaseMutation renews a live lease.
type LeaseMutation struct {
	Identity LeaseIdentity
	Duration time.Duration
	Now      time.Time
}

// CompletionMutation completes a live lease.
type CompletionMutation struct {
	ClientPayloadUpdate *jobdb.ClientPayloadUpdate
	Identity            LeaseIdentity
	Status              string
	Detail              string
	ErrorKind           string
	Retryable           *bool
	Now                 time.Time
}

// RescheduleMutation returns a live lease to the scheduler queue.
// With WaitForJobIDs, WaitUntil is an alternative wakeup deadline: all jobs
// terminating OR reaching the deadline makes work eligible. Without dependencies,
// WaitUntil is a not-before time.
type RescheduleMutation struct {
	DependencyWait      *jobdb.DependencyWait
	Identity            LeaseIdentity
	RouteJobType        string
	WorkKind            WorkKind
	TaskWork            *TaskWork
	WaitUntil           *time.Time
	WaitForJobIDs       []string
	ClientPayloadUpdate *jobdb.ClientPayloadUpdate
	AlternateRoute      *AlternateRoute
	Now                 time.Time
}

// WaitingTaskSnapshot describes a job waiting for task output.
type WaitingTaskSnapshot struct {
	JobType string
	Task    TaskWork
}

// CancelJobMutation atomically cancels a job.
type CancelJobMutation struct {
	JobKey   jobdb.JobKey
	Reason   string
	WorkerID string
	Now      time.Time
}
