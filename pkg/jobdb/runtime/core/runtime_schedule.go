package runtimecore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// UpsertSchedule snapshots target input and persists the schedule generation.
func (r *Runtime) UpsertSchedule(ctx context.Context, req jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error) {
	if err := r.validate(); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := jobdb.ValidateScheduleRequest(req); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	target, err := snapshotScheduleTarget(ctx, req.Target)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	specHash, err := jobdb.ScheduleSpecHash(req.Trigger, target.toTarget(), req.OverlapPolicy, req.FailurePolicy)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	now := scheduleRequestTime(req.RequestTime, r.now)
	next, err := jobdb.InitialScheduleFire(req.Trigger, now)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	state := jobdb.ScheduleStateActive
	if req.Paused {
		state = jobdb.ScheduleStatePaused
		next = nil
	}
	key := jobdb.ScheduleKey{TenantId: req.TenantId, ScheduleId: req.ScheduleId}
	generation := int64(1)
	var expected *int64
	if current, err := r.scheduler.GetSchedule(ctx, key); err == nil {
		if current.State == jobdb.ScheduleStateArchived {
			return jobdb.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot be updated", jobdb.ErrConflict)
		}
		generation = current.Generation + 1
		expected = &current.Generation
	} else if !errors.Is(err, jobdb.ErrJobNotFound) {
		return jobdb.ScheduleInfo{}, err
	}
	if req.ExpectedGeneration != nil {
		if expected == nil || *expected != *req.ExpectedGeneration {
			return jobdb.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", jobdb.ErrConflict)
		}
	}
	triggerJSON, err := json.Marshal(req.Trigger)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	failureJSON, err := json.Marshal(req.FailurePolicy)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	var nextKey *jobdb.JobKey
	if next != nil {
		nextKey = &jobdb.JobKey{TenantId: key.TenantId,
			JobId: jobdb.ScheduleRunJobID(key.ScheduleId, generation, *next)}
	}
	stored, err := r.scheduler.UpsertSchedule(ctx, StoredScheduleMutation{
		ScheduleKey: key, State: state, SpecHash: specHash,
		TriggerSnapshot: triggerJSON, TargetJobType: target.JobType,
		TargetSnapshot: targetJSON, OverlapPolicy: jobdb.NormalizeScheduleOverlapPolicy(req.OverlapPolicy),
		FailurePolicySnapshot: failureJSON, NextFireAt: next, NextJobKey: nextKey,
		ExpectedGeneration: expected, RequestTime: now, WorkerID: req.WorkerID,
	})
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	info, err := decodeSchedule(stored)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if state == jobdb.ScheduleStateActive && stored.NextFireAt != nil && stored.NextJobKey != nil {
		if _, err := r.submitScheduledOccurrenceWithID(ctx, info, stored.NextJobKey.JobId,
			*stored.NextFireAt, "", "", false, req.WorkerID); err != nil {
			return jobdb.ScheduleInfo{}, err
		}
	}
	return info, nil
}

func (r *Runtime) GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (jobdb.ScheduleInfo, error) {
	if err := r.validate(); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	stored, err := r.scheduler.GetSchedule(ctx, key)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	return decodeSchedule(stored)
}

func (r *Runtime) ListSchedules(ctx context.Context, req jobdb.ListSchedulesRequest) (jobdb.ListSchedulesResponse, error) {
	if err := r.validate(); err != nil {
		return jobdb.ListSchedulesResponse{}, err
	}
	if req.TenantId == "" {
		return jobdb.ListSchedulesResponse{}, fmt.Errorf("tenantId is required")
	}
	stored, err := r.scheduler.ListSchedules(ctx, ListSchedulesRequest{
		TenantId: req.TenantId, ScheduleIDs: req.ScheduleIds,
		States: req.States, TargetJobTypes: req.TargetJobTypes,
		PageSize: req.PageSize, PageToken: req.PageToken,
	})
	if err != nil {
		return jobdb.ListSchedulesResponse{}, err
	}
	out := jobdb.ListSchedulesResponse{Schedules: make([]jobdb.ScheduleInfo, 0, len(stored.Schedules)),
		NextPageToken: stored.NextPageToken}
	for _, item := range stored.Schedules {
		info, err := decodeSchedule(item)
		if err != nil {
			return jobdb.ListSchedulesResponse{}, err
		}
		out.Schedules = append(out.Schedules, info)
	}
	return out, nil
}

func (r *Runtime) PauseSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return r.mutateSchedule(ctx, req, jobdb.ScheduleStatePaused)
}

func (r *Runtime) ResumeSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return r.mutateSchedule(ctx, req, jobdb.ScheduleStateActive)
}

func (r *Runtime) ArchiveSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return r.mutateSchedule(ctx, req, jobdb.ScheduleStateArchived)
}

func (r *Runtime) mutateSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest, state jobdb.ScheduleState) (jobdb.ScheduleInfo, error) {
	if err := r.validate(); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if err := req.ScheduleKey.Validate(); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	current, err := r.GetSchedule(ctx, req.ScheduleKey)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if current.State == jobdb.ScheduleStateArchived && state != jobdb.ScheduleStateArchived {
		return jobdb.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot change state", jobdb.ErrConflict)
	}
	if req.ExpectedGeneration != nil && current.Generation != *req.ExpectedGeneration {
		return jobdb.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", jobdb.ErrConflict)
	}
	now := scheduleRequestTime(req.RequestTime, r.now)
	var next *time.Time
	var nextKey *jobdb.JobKey
	if state == jobdb.ScheduleStateActive {
		next, err = jobdb.InitialScheduleFire(current.Trigger, now)
		if err != nil {
			return jobdb.ScheduleInfo{}, err
		}
		if next != nil {
			nextKey = &jobdb.JobKey{TenantId: req.ScheduleKey.TenantId,
				JobId: jobdb.ScheduleRunJobID(req.ScheduleKey.ScheduleId, current.Generation+1, *next)}
		}
	}
	stored, err := r.scheduler.MutateSchedule(ctx, ScheduleStateMutation{
		ScheduleKey: req.ScheduleKey, State: state, NextFireAt: next, NextJobKey: nextKey,
		ExpectedGeneration: &current.Generation, RequestTime: now, WorkerID: req.WorkerID,
	})
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	info, err := decodeSchedule(stored)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if state == jobdb.ScheduleStateActive && stored.NextFireAt != nil && stored.NextJobKey != nil {
		if _, err := r.submitScheduledOccurrenceWithID(ctx, info, stored.NextJobKey.JobId,
			*stored.NextFireAt, "", "", false, req.WorkerID); err != nil {
			return jobdb.ScheduleInfo{}, err
		}
	}
	return info, nil
}

func (r *Runtime) TriggerSchedule(ctx context.Context, req jobdb.TriggerScheduleRequest) (jobdb.JobHandle, error) {
	info, err := r.GetSchedule(ctx, req.ScheduleKey)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if info.State == jobdb.ScheduleStateArchived {
		return jobdb.JobHandle{}, fmt.Errorf("%w: archived schedule cannot be triggered", jobdb.ErrConflict)
	}
	now := scheduleRequestTime(req.RequestTime, r.now)
	jobID := jobdb.ScheduleManualJobID(req.ScheduleKey.ScheduleId, req.RequestID)
	key, err := r.submitScheduledOccurrenceWithID(ctx, info, jobID, now, "", "", true, req.WorkerID)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	return jobdb.JobHandle{JobKey: key}, nil
}

func (r *Runtime) ListScheduleRuns(ctx context.Context, req jobdb.ListScheduleRunsRequest) (jobdb.ListScheduleRunsResponse, error) {
	if err := r.validate(); err != nil {
		return jobdb.ListScheduleRunsResponse{}, err
	}
	if err := req.ScheduleKey.Validate(); err != nil {
		return jobdb.ListScheduleRunsResponse{}, err
	}
	stored, err := r.scheduler.ListScheduleRuns(ctx, ListScheduleRunsRequest{
		ScheduleKey: req.ScheduleKey, ScheduledAfter: req.ScheduledAfter,
		ScheduledBefore: req.ScheduledBefore, Statuses: req.Statuses,
		PageSize: req.PageSize, PageToken: req.PageToken,
	})
	if err != nil {
		return jobdb.ListScheduleRunsResponse{}, err
	}
	out := jobdb.ListScheduleRunsResponse{Runs: make([]jobdb.ScheduleRunSummary, 0, len(stored.Runs)),
		NextPageToken: stored.NextPageToken}
	for _, row := range stored.Runs {
		if row.Schedule == nil || row.Schedule.ScheduleId != req.ScheduleKey.ScheduleId {
			continue
		}
		summary, err := jobSummaryFromStored(row)
		if err != nil {
			return jobdb.ListScheduleRunsResponse{}, err
		}
		reason := ""
		if row.Completion != nil {
			var detail struct {
				ReasonCode string `json:"reasonCode"`
			}
			_ = json.Unmarshal([]byte(row.Completion.Detail), &detail)
			reason = detail.ReasonCode
		}
		out.Runs = append(out.Runs, jobdb.ScheduleRunSummary{
			JobSummary: summary, ScheduleId: req.ScheduleKey.ScheduleId,
			ScheduledAt: row.Schedule.ScheduledAt, ReasonCode: reason,
		})
	}
	return out, nil
}

func (r *Runtime) submitScheduledOccurrence(ctx context.Context, info jobdb.ScheduleInfo, at time.Time, previousID, bits string, manual bool, workerID string) (jobdb.JobKey, error) {
	at = at.UTC().Truncate(time.Microsecond)
	return r.submitScheduledOccurrenceWithID(ctx, info,
		jobdb.ScheduleRunJobID(info.ScheduleId, info.Generation, at), at,
		previousID, bits, manual, workerID)
}

func (r *Runtime) submitScheduledOccurrenceWithID(ctx context.Context, info jobdb.ScheduleInfo, jobID string, at time.Time, previousID, bits string, manual bool, workerID string) (jobdb.JobKey, error) {
	at = at.UTC().Truncate(time.Microsecond)
	runID := jobdb.ScheduleRunID(at)
	if manual {
		runID = jobID
	}
	occurrence := &jobdb.ScheduleOccurrenceMetadata{
		ScheduleId: info.ScheduleId, Kind: jobdb.ScheduleMetadataKind,
		Generation: info.Generation, SpecHash: info.SpecHash,
		ScheduledAt: at.UTC(), RunId: runID, Manual: manual,
		PreviousJobId: previousID, FailureHistory: jobdb.ScheduleFailureHistory{
			Bits: bits, WindowSize: info.FailurePolicy.WindowSize,
		},
	}
	prereqs := []jobdb.JobPrerequisite(nil)
	if previousID != "" && info.OverlapPolicy == jobdb.ScheduleOverlapSerial {
		prereqs = append(prereqs, jobdb.JobPrerequisite{JobID: previousID,
			Condition: jobdb.JobPrereqComplete})
	}
	handle, err := r.submitJobWithSchedule(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: info.TenantId, JobID: jobID, JobType: info.Target.JobType,
			Data: info.Target.Data, RunPolicy: info.Target.RunPolicy,
			Metadata: info.Target.Metadata, Prerequisites: prereqs, AvailableAt: &at,
		}, WorkerID: workerID,
	}, "", occurrence)
	if err != nil {
		return jobdb.JobKey{}, err
	}
	return handle.JobKey, nil
}

func scheduleRequestTime(request time.Time, now func() time.Time) time.Time {
	if request.IsZero() {
		return now().UTC().Truncate(time.Microsecond)
	}
	return request.UTC().Truncate(time.Microsecond)
}
