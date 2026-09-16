package runtimecore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Scheduler persists job and schedule state and performs atomic lease
// mutations for runtime core.
type Scheduler interface {
	CreateJob(ctx context.Context, req CreateJobRequest) (StoredJob, error)
	GetJob(ctx context.Context, key jobdb.JobKey) (StoredJob, error)
	ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)
	CancelJob(ctx context.Context, req CancelJobMutation) (StoredJob, error)

	AcquireWork(ctx context.Context, req WorkRequest) ([]LeaseSnapshot, error)
	AcquireJobLease(ctx context.Context, req JobLeaseRequest) (*LeaseSnapshot, error)
	ValidateLease(ctx context.Context, identity LeaseIdentity) (LeaseSnapshot, error)
	KeepAliveLease(ctx context.Context, mutation LeaseMutation) (LeaseSnapshot, error)
	CompleteLease(ctx context.Context, mutation CompletionMutation) (StoredJob, error)
	RescheduleLease(ctx context.Context, mutation RescheduleMutation) (StoredJob, error)

	GetWaitingTask(ctx context.Context, key jobdb.JobKey) (WaitingTaskSnapshot, error)
	CompleteTaskWork(ctx context.Context, mutation CompleteTaskWorkMutation) (StoredJob, error)

	UpsertSchedule(ctx context.Context, mutation StoredScheduleMutation) (StoredSchedule, error)
	GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (StoredSchedule, error)
	ListSchedules(ctx context.Context, req ListSchedulesRequest) (ListSchedulesResponse, error)
	MutateSchedule(ctx context.Context, mutation ScheduleStateMutation) (StoredSchedule, error)
	ListScheduleRuns(ctx context.Context, req ListScheduleRunsRequest) (ListScheduleRunsResponse, error)
}

// CreateJobRequest contains the immutable job facts and initial route written
// after runtime core has validated and encoded the first chapter.
type CreateJobRequest struct {
	JobKey        jobdb.JobKey
	JobType       string
	ParentJobID   string
	RunPolicy     jobdb.RunPolicy
	AppMetadata   json.RawMessage
	Schedule      *jobdb.ScheduleOccurrenceMetadata
	SchemaHash    string
	WaitForJobIDs []string
	AvailableAt   *time.Time
	ExpiresAt     *time.Time
	CreatedAt     time.Time
	WorkerID      string
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
	JobKey              jobdb.JobKey
	Store               jobdb.JobStore
	JobType             string
	Status              jobdb.JobStatus
	RouteJobType        string
	WorkKind            WorkKind
	TaskWork            *TaskWork
	AlternateRoute      *AlternateRoute
	RunPolicy           jobdb.RunPolicy
	LeasePayload        json.RawMessage
	LeasePayloadVisible bool
	AppMetadata         json.RawMessage
	SchemaHash          string
	ParentJobID         string
	Schedule            *jobdb.ScheduleOccurrenceMetadata
	WaitForJobIDs       []string
	AvailableAt         time.Time
	ExpiresAt           *time.Time
	LeaseExpiresAt      *time.Time
	LeaseWorkerID       string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ArchivedAt          *time.Time
	CancelRequested     bool
	Completion          *CompletionSnapshot
	Lease               *LeaseIdentity
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
	TenantIds      []string
	Statuses       []jobdb.JobStatus
	Stores         []jobdb.JobStore
	JobTypes       []string
	JobTasks       []jobdb.JobTaskFilter
	JobKeys        []jobdb.JobKey
	ParentJobIDs   []string
	RootOnly       bool
	MetadataEquals []jobdb.MetadataPredicate
	CreatedAfter   *time.Time
	CreatedBefore  *time.Time
	PageSize       int
	PageToken      string
}

// ListJobsResponse is the scheduler-side job list result.
type ListJobsResponse struct {
	Jobs          []StoredJob
	NextPageToken string
}

// WorkRequest asks the scheduler to atomically lease queue work.
type WorkRequest struct {
	TenantId       string
	WorkerID       string
	Selector       WorkSelector
	Limit          int
	LeaseDuration  time.Duration
	MetadataEquals []jobdb.MetadataPredicate
	Now            time.Time
}

// WorkSelector names job routes and task routes without encoded capability
// strings. Runtime core parses public worker capabilities into this form.
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
	Identity            LeaseIdentity
	JobType             string
	RouteJobType        string
	WorkKind            WorkKind
	TaskWork            *TaskWork
	RunPolicy           jobdb.RunPolicy
	LeasePayload        json.RawMessage
	LeasePayloadVisible bool
	SchemaHash          string
	Duration            time.Duration
}

// LeaseMutation renews a live lease.
type LeaseMutation struct {
	Identity LeaseIdentity
	Duration time.Duration
	Now      time.Time
}

// CompletionMutation completes a live lease.
type CompletionMutation struct {
	Identity LeaseIdentity
	Status   string
	Detail   string
	Now      time.Time
}

// RescheduleMutation returns a live lease to the scheduler queue.
type RescheduleMutation struct {
	Identity          LeaseIdentity
	RouteJobType      string
	WorkKind          WorkKind
	TaskWork          *TaskWork
	WaitUntil         *time.Time
	WaitForJobIDs     []string
	LeasePayload      json.RawMessage
	ClearLeasePayload bool
	AlternateRoute    *AlternateRoute
	Now               time.Time
}

// WaitingTaskSnapshot describes a job waiting for task output.
type WaitingTaskSnapshot struct {
	JobType string
	Task    TaskWork
}

// CompleteTaskWorkMutation atomically resumes a job waiting on task output.
type CompleteTaskWorkMutation struct {
	JobKey            jobdb.JobKey
	WorkerID          string
	Task              WaitingTaskSnapshot
	LeasePayload      json.RawMessage
	ClearLeasePayload bool
	Now               time.Time
}

// CancelJobMutation atomically cancels a job.
type CancelJobMutation struct {
	JobKey   jobdb.JobKey
	Reason   string
	WorkerID string
	Now      time.Time
}

// StoredScheduleMutation is the scheduler-side schedule upsert payload after
// runtime core has validated and snapshotted the target.
type StoredScheduleMutation struct {
	ScheduleKey   jobdb.ScheduleKey
	State         jobdb.ScheduleState
	SpecHash      string
	Trigger       jobdb.ScheduleTrigger
	Target        jobdb.ScheduleTarget
	OverlapPolicy jobdb.ScheduleOverlapPolicy
	FailurePolicy jobdb.ScheduleFailurePolicy
	NextFireAt    *time.Time
	NextJobKey    *jobdb.JobKey

	ExpectedGeneration *int64
	RequestTime        time.Time
	WorkerID           string
}

// ScheduleStateMutation changes a schedule's lifecycle state.
type ScheduleStateMutation struct {
	ScheduleKey        jobdb.ScheduleKey
	State              jobdb.ScheduleState
	ExpectedGeneration *int64
	RequestTime        time.Time
	WorkerID           string
}

// StoredSchedule is the scheduler's logical view of a schedule.
type StoredSchedule struct {
	ScheduleKey   jobdb.ScheduleKey
	State         jobdb.ScheduleState
	Generation    int64
	SpecHash      string
	Trigger       jobdb.ScheduleTrigger
	Target        jobdb.ScheduleTarget
	OverlapPolicy jobdb.ScheduleOverlapPolicy
	FailurePolicy jobdb.ScheduleFailurePolicy
	NextFireAt    *time.Time
	NextJobKey    *jobdb.JobKey
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ListSchedulesRequest is the scheduler-side form of schedule listing.
type ListSchedulesRequest struct {
	TenantId       string
	ScheduleIDs    []string
	States         []jobdb.ScheduleState
	TargetJobTypes []string
	PageSize       int
	PageToken      string
}

// ListSchedulesResponse is the scheduler-side schedule list result.
type ListSchedulesResponse struct {
	Schedules     []StoredSchedule
	NextPageToken string
}

// ListScheduleRunsRequest is the scheduler-side form of schedule-run listing.
type ListScheduleRunsRequest struct {
	ScheduleKey     jobdb.ScheduleKey
	ScheduledAfter  *time.Time
	ScheduledBefore *time.Time
	Statuses        []jobdb.JobStatus
	PageSize        int
	PageToken       string
}

// ListScheduleRunsResponse is the scheduler-side schedule-run list result.
type ListScheduleRunsResponse struct {
	Runs          []StoredJob
	NextPageToken string
}
