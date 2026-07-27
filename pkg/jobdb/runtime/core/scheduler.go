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

// CreateJobRequest is the scheduler-side job fact created after runtime core
// has validated and encoded the initial chapter.
type CreateJobRequest struct {
	JobKey        jobdb.JobKey
	JobType       string
	ParentJobID   string
	NextNeed      string
	Payload       json.RawMessage
	Metadata      json.RawMessage
	SchemaHash    string
	Prerequisites []jobdb.JobPrerequisite
	WaitForJobIDs []string
	AvailableAt   *time.Time
	CreatedAt     time.Time
	WorkerID      string
}

// StoredJob is the scheduler's logical view of a job.
type StoredJob struct {
	JobKey          jobdb.JobKey
	JobType         string
	Status          jobdb.JobStatus
	NextNeed        string
	Payload         json.RawMessage
	Metadata        json.RawMessage
	SchemaHash      string
	ParentJobID     string
	Prerequisites   []jobdb.JobPrerequisite
	WaitForJobIDs   []string
	AvailableAt     time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ArchivedAt      *time.Time
	CancelRequested bool
	Completion      *CompletionSnapshot
	Lease           *LeaseIdentity
	TaskWait        *WaitingTaskSnapshot
}

// CompletionSnapshot describes a terminal scheduler outcome.
type CompletionSnapshot struct {
	Status string
	Detail string
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
	Capabilities   []string
	Limit          int
	LeaseDuration  time.Duration
	MetadataEquals []jobdb.MetadataPredicate
	Now            time.Time
}

// JobLeaseRequest asks the scheduler to atomically lease one specific job.
type JobLeaseRequest struct {
	JobKey        jobdb.JobKey
	WorkerID      string
	Capabilities  []string
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

// LeaseSnapshot is the logical lease returned by the scheduler.
type LeaseSnapshot struct {
	Identity   LeaseIdentity
	Capability string
	Payload    json.RawMessage
	SchemaHash string
	Duration   time.Duration
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
	Identity       LeaseIdentity
	NextNeed       string
	WaitUntil      *time.Time
	WaitForJobIDs  []string
	Payload        json.RawMessage
	AlternateNeed  string
	AlternateAfter *time.Duration
	Now            time.Time
}

// WaitingTaskSnapshot describes a job waiting for task output.
type WaitingTaskSnapshot struct {
	Capability    string
	ResumeNeed    string
	InputOrdinal  int64
	OutputOrdinal int64
	InputHash     string
}

// CompleteTaskWorkMutation atomically resumes a job waiting on task output.
type CompleteTaskWorkMutation struct {
	JobKey  jobdb.JobKey
	Task    WaitingTaskSnapshot
	Payload json.RawMessage
	Now     time.Time
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
