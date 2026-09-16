package runtimeadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func (s Scheduler) CreateJob(ctx context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	policy, err := runPolicyToPgjobdb(req.RunPolicy)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	input := pgjobdb.SubmitJobRequest{
		TenantID: pgjobdb.TenantID(req.JobKey.TenantId), JobID: pgjobdb.JobID(req.JobKey.JobId),
		WorkerID: pgjobdb.WorkerID(req.WorkerID), JobType: pgjobdb.JobType(req.JobType),
		RunPolicy: policy, AppMetadata: append(json.RawMessage(nil), req.AppMetadata...),
		Runtime: pgjobdb.RuntimeMetadata{
			SchemaHash: req.SchemaHash, ParentJobID: pgjobdb.JobID(req.ParentJobID),
		},
		AvailableAt: req.AvailableAt, ExpiresAt: req.ExpiresAt,
	}
	for _, id := range req.WaitForJobIDs {
		input.WaitFor = append(input.WaitFor, pgjobdb.JobID(id))
	}
	if req.Schedule != nil {
		history, err := json.Marshal(req.Schedule.FailureHistory)
		if err != nil {
			return runtimecore.StoredJob{}, fmt.Errorf("encode schedule failure history: %w", err)
		}
		reason := "automatic"
		if req.Schedule.Manual {
			reason = "manual"
		}
		input.Runtime.Schedule = &pgjobdb.ScheduleOccurrence{
			ScheduleID: req.Schedule.ScheduleId, Generation: req.Schedule.Generation,
			SpecHash: req.Schedule.SpecHash, ScheduledAt: req.Schedule.ScheduledAt,
			RunID: req.Schedule.RunId, Reason: reason, Manual: req.Schedule.Manual,
			BackfillID: req.Schedule.BackfillId, PreviousJobID: req.Schedule.PreviousJobId,
			FailureHistory: history,
		}
	}
	if _, err := pgjobdb.SubmitJob(ctx, s.DB, input); err != nil {
		return runtimecore.StoredJob{}, err
	}
	return s.GetJob(ctx, req.JobKey)
}

func (s Scheduler) CancelJob(ctx context.Context, req runtimecore.CancelJobMutation) (runtimecore.StoredJob, error) {
	if err := pgjobdb.CancelJob(ctx, s.DB, pgjobdb.CancelJobRequest{
		TenantID: pgjobdb.TenantID(req.JobKey.TenantId), JobID: pgjobdb.JobID(req.JobKey.JobId),
		WorkerID: pgjobdb.WorkerID(req.WorkerID), Reason: req.Reason,
	}); err != nil {
		return runtimecore.StoredJob{}, err
	}
	return s.GetJob(ctx, req.JobKey)
}

func (s Scheduler) CompleteLease(ctx context.Context, mutation runtimecore.CompletionMutation) (runtimecore.StoredJob, error) {
	identity := identityToPgjobdb(mutation.Identity)
	if err := pgjobdb.CompleteJob(ctx, s.DB, identity, pgjobdb.Completion{
		Status: pgjobdb.CompletionStatus(mutation.Status), Detail: mutation.Detail,
		ErrorKind: mutation.ErrorKind, Retryable: mutation.Retryable,
	}); err != nil {
		return runtimecore.StoredJob{}, translateLeaseError(err)
	}
	return s.GetJob(ctx, mutation.Identity.JobKey)
}

func (s Scheduler) RescheduleLease(ctx context.Context, mutation runtimecore.RescheduleMutation) (runtimecore.StoredJob, error) {
	request := pgjobdb.RescheduleRequest{
		RouteJobType:      pgjobdb.JobType(mutation.RouteJobType),
		WorkKind:          pgjobdb.WorkKind(mutation.WorkKind),
		AvailableAt:       mutation.WaitUntil,
		LeasePayload:      append(json.RawMessage(nil), mutation.LeasePayload...),
		ClearLeasePayload: mutation.ClearLeasePayload,
		Alternate:         &pgjobdb.AlternateRoute{},
	}
	for _, id := range mutation.WaitForJobIDs {
		request.WaitFor = append(request.WaitFor, pgjobdb.JobID(id))
	}
	if mutation.TaskWork != nil {
		request.Task = taskToPgjobdb(*mutation.TaskWork)
	}
	if mutation.AlternateRoute != nil {
		request.Alternate = &pgjobdb.AlternateRoute{
			JobType:  pgjobdb.JobType(mutation.AlternateRoute.JobType),
			TaskType: pgjobdb.TaskType(mutation.AlternateRoute.TaskType),
			After:    mutation.AlternateRoute.After,
		}
	}
	if err := pgjobdb.RescheduleJob(ctx, s.DB, identityToPgjobdb(mutation.Identity), request); err != nil {
		return runtimecore.StoredJob{}, translateLeaseError(err)
	}
	return s.GetJob(ctx, mutation.Identity.JobKey)
}

func (s Scheduler) GetWaitingTask(ctx context.Context, key jobdb.JobKey) (runtimecore.WaitingTaskSnapshot, error) {
	row, err := s.GetJob(ctx, key)
	if err != nil {
		return runtimecore.WaitingTaskSnapshot{}, err
	}
	if row.WorkKind != runtimecore.WorkKindTask || row.TaskWork == nil ||
		row.Store != jobdb.JobStoreActive {
		return runtimecore.WaitingTaskSnapshot{}, jobdb.ErrConflict
	}
	return runtimecore.WaitingTaskSnapshot{JobType: row.RouteJobType, Task: *row.TaskWork}, nil
}

func (s Scheduler) CompleteTaskWork(ctx context.Context, mutation runtimecore.CompleteTaskWorkMutation) (runtimecore.StoredJob, error) {
	if err := pgjobdb.CompleteTaskWork(ctx, s.DB, pgjobdb.CompleteTaskWorkRequest{
		TenantID:          pgjobdb.TenantID(mutation.JobKey.TenantId),
		JobID:             pgjobdb.JobID(mutation.JobKey.JobId),
		WorkerID:          pgjobdb.WorkerID(mutation.WorkerID),
		JobType:           pgjobdb.JobType(mutation.Task.JobType),
		Task:              *taskToPgjobdb(mutation.Task.Task),
		LeasePayload:      append(json.RawMessage(nil), mutation.LeasePayload...),
		ClearLeasePayload: mutation.ClearLeasePayload,
	}); err != nil {
		return runtimecore.StoredJob{}, err
	}
	return s.GetJob(ctx, mutation.JobKey)
}

func identityToPgjobdb(identity runtimecore.LeaseIdentity) pgjobdb.LeaseIdentity {
	return pgjobdb.LeaseIdentity{
		TenantID: pgjobdb.TenantID(identity.JobKey.TenantId),
		JobID:    pgjobdb.JobID(identity.JobKey.JobId),
		LeaseID:  identity.LeaseID, WorkerID: pgjobdb.WorkerID(identity.WorkerID),
	}
}

func taskToPgjobdb(task runtimecore.TaskWork) *pgjobdb.TaskWork {
	return &pgjobdb.TaskWork{
		TaskType:      pgjobdb.TaskType(task.TaskType),
		ResumeJobType: pgjobdb.JobType(task.ResumeJobType),
		InputOrdinal:  task.InputOrdinal, OutputOrdinal: task.OutputOrdinal,
		InputHash: task.InputHash,
	}
}

func runPolicyToPgjobdb(policy jobdb.RunPolicy) (pgjobdb.RunPolicy, error) {
	initial, err := durationToMillis(time.Duration(policy.Retry.InitialInterval))
	if err != nil {
		return pgjobdb.RunPolicy{}, err
	}
	maximum, err := durationToMillis(time.Duration(policy.Retry.MaximumInterval))
	if err != nil {
		return pgjobdb.RunPolicy{}, err
	}
	out := pgjobdb.RunPolicy{Retry: pgjobdb.RetryPolicy{
		InitialIntervalMillis: initial, BackoffCoefficient: policy.Retry.BackoffCoefficient,
		MaximumIntervalMillis: maximum, MaximumAttempts: policy.Retry.MaximumAttempts,
		NonRetryableErrorTypes: append([]string(nil), policy.Retry.NonRetryableErrorTypes...),
	}}
	if policy.InvocationTimeout != nil {
		value, err := durationToMillis(policy.InvocationTimeout.ToDuration())
		if err != nil {
			return pgjobdb.RunPolicy{}, err
		}
		out.InvocationTimeoutMillis = &value
	}
	if policy.TotalTimeout != nil {
		value, err := durationToMillis(policy.TotalTimeout.ToDuration())
		if err != nil {
			return pgjobdb.RunPolicy{}, err
		}
		out.TotalTimeoutMillis = &value
	}
	return out, nil
}

func durationToMillis(duration time.Duration) (int64, error) {
	if duration < 0 || duration%time.Millisecond != 0 {
		return 0, fmt.Errorf("run policy duration must be a nonnegative whole number of milliseconds")
	}
	return int64(duration / time.Millisecond), nil
}
