package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/lib/pq"
)

func (s Scheduler) UpsertSchedule(ctx context.Context, req runtimecore.StoredScheduleMutation) (runtimecore.StoredSchedule, error) {
	nextJob, err := nextJobID(req.ScheduleKey.TenantId, req.NextJobKey)
	if err != nil {
		return runtimecore.StoredSchedule{}, err
	}
	stored, err := pgjobdb.UpsertSchedule(ctx, s.DB, pgjobdb.UpsertScheduleRequest{
		TenantID:   pgjobdb.TenantID(req.ScheduleKey.TenantId),
		ScheduleID: req.ScheduleKey.ScheduleId,
		State:      pgjobdb.ScheduleState(req.State), SpecHash: req.SpecHash,
		Trigger:        append(json.RawMessage(nil), req.TriggerSnapshot...),
		TargetJobType:  pgjobdb.JobType(req.TargetJobType),
		TargetSnapshot: append(json.RawMessage(nil), req.TargetSnapshot...),
		OverlapPolicy:  string(req.OverlapPolicy),
		FailurePolicy:  append(json.RawMessage(nil), req.FailurePolicySnapshot...),
		NextFireAt:     req.NextFireAt, NextJobID: nextJob,
		ExpectedGeneration: req.ExpectedGeneration,
	})
	if err != nil {
		return runtimecore.StoredSchedule{}, translateScheduleMutationError(err)
	}
	return scheduleFromPgjobdb(*stored), nil
}

func translateScheduleMutationError(err error) error {
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && pgErr.Code == "P0001" &&
		(strings.Contains(pgErr.Message, "generation mismatch") ||
			strings.Contains(pgErr.Message, "archived schedule")) {
		return fmt.Errorf("%w: %s", jobdb.ErrConflict, pgErr.Message)
	}
	return err
}

func (s Scheduler) GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (runtimecore.StoredSchedule, error) {
	stored, err := pgjobdb.GetSchedule(ctx, s.DB, pgjobdb.TenantID(key.TenantId), key.ScheduleId)
	if errors.Is(err, pgjobdb.ErrScheduleNotFound) {
		return runtimecore.StoredSchedule{}, jobdb.ErrJobNotFound
	}
	if err != nil {
		return runtimecore.StoredSchedule{}, err
	}
	return scheduleFromPgjobdb(*stored), nil
}

func (s Scheduler) ListSchedules(ctx context.Context, req runtimecore.ListSchedulesRequest) (runtimecore.ListSchedulesResponse, error) {
	opts := pgjobdb.ListSchedulesOptions{
		TenantID:    pgjobdb.TenantID(req.TenantId),
		ScheduleIDs: append([]string(nil), req.ScheduleIDs...),
		PageSize:    req.PageSize, PageToken: req.PageToken,
	}
	for _, state := range req.States {
		opts.States = append(opts.States, pgjobdb.ScheduleState(state))
	}
	for _, jobType := range req.TargetJobTypes {
		opts.TargetJobTypes = append(opts.TargetJobTypes, pgjobdb.JobType(jobType))
	}
	listed, err := pgjobdb.ListSchedules(ctx, s.DB, opts)
	if err != nil {
		return runtimecore.ListSchedulesResponse{}, err
	}
	out := runtimecore.ListSchedulesResponse{
		Schedules:     make([]runtimecore.StoredSchedule, 0, len(listed.Schedules)),
		NextPageToken: listed.NextPageToken,
	}
	for _, item := range listed.Schedules {
		out.Schedules = append(out.Schedules, scheduleFromPgjobdb(item))
	}
	return out, nil
}

func (s Scheduler) MutateSchedule(ctx context.Context, req runtimecore.ScheduleStateMutation) (runtimecore.StoredSchedule, error) {
	nextJob, err := nextJobID(req.ScheduleKey.TenantId, req.NextJobKey)
	if err != nil {
		return runtimecore.StoredSchedule{}, err
	}
	mutation := pgjobdb.ScheduleMutationRequest{
		TenantID:   pgjobdb.TenantID(req.ScheduleKey.TenantId),
		ScheduleID: req.ScheduleKey.ScheduleId,
		NextFireAt: req.NextFireAt, NextJobID: nextJob,
		ExpectedGeneration: req.ExpectedGeneration,
	}
	var stored *pgjobdb.Schedule
	switch req.State {
	case jobdb.ScheduleStateActive:
		stored, err = pgjobdb.ResumeSchedule(ctx, s.DB, mutation)
	case jobdb.ScheduleStatePaused:
		stored, err = pgjobdb.PauseSchedule(ctx, s.DB, mutation)
	case jobdb.ScheduleStateArchived:
		stored, err = pgjobdb.ArchiveSchedule(ctx, s.DB, mutation)
	default:
		return runtimecore.StoredSchedule{}, fmt.Errorf("unsupported schedule control state %q", req.State)
	}
	if err != nil {
		return runtimecore.StoredSchedule{}, translateScheduleMutationError(err)
	}
	return scheduleFromPgjobdb(*stored), nil
}

func (s Scheduler) ListScheduleRuns(ctx context.Context, req runtimecore.ListScheduleRunsRequest) (runtimecore.ListScheduleRunsResponse, error) {
	opts := pgjobdb.ListScheduleRunsOptions{
		TenantID:       pgjobdb.TenantID(req.ScheduleKey.TenantId),
		ScheduleID:     req.ScheduleKey.ScheduleId,
		ScheduledAfter: req.ScheduledAfter, ScheduledBefore: req.ScheduledBefore,
		PageSize: req.PageSize, PageToken: req.PageToken,
	}
	for _, status := range req.Statuses {
		opts.Statuses = append(opts.Statuses, pgjobdb.JobStatus(status))
	}
	listed, err := pgjobdb.ListScheduleRuns(ctx, s.DB, opts)
	if err != nil {
		return runtimecore.ListScheduleRunsResponse{}, err
	}
	out := runtimecore.ListScheduleRunsResponse{
		Runs:          make([]runtimecore.StoredJob, 0, len(listed.Runs)),
		NextPageToken: listed.NextPageToken,
	}
	for _, item := range listed.Runs {
		row, err := storedJobFromDetail(item)
		if err != nil {
			return runtimecore.ListScheduleRunsResponse{}, err
		}
		out.Runs = append(out.Runs, row)
	}
	return out, nil
}

func nextJobID(tenant string, key *jobdb.JobKey) (*pgjobdb.JobID, error) {
	if key == nil {
		return nil, nil
	}
	if key.TenantId != tenant || key.JobId == "" {
		return nil, fmt.Errorf("schedule next job must belong to the schedule tenant")
	}
	id := pgjobdb.JobID(key.JobId)
	return &id, nil
}

func scheduleFromPgjobdb(item pgjobdb.Schedule) runtimecore.StoredSchedule {
	out := runtimecore.StoredSchedule{
		ScheduleKey: jobdb.ScheduleKey{TenantId: string(item.TenantID), ScheduleId: item.ScheduleID},
		State:       jobdb.ScheduleState(item.State), Generation: item.Generation,
		SpecHash:              item.SpecHash,
		TriggerSnapshot:       append(json.RawMessage(nil), item.Trigger...),
		TargetJobType:         string(item.TargetJobType),
		TargetSnapshot:        append(json.RawMessage(nil), item.TargetSnapshot...),
		OverlapPolicy:         jobdb.ScheduleOverlapPolicy(item.OverlapPolicy),
		FailurePolicySnapshot: append(json.RawMessage(nil), item.FailurePolicy...),
		NextFireAt:            item.NextFireAt, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
	}
	if item.NextJobID != nil {
		out.NextJobKey = &jobdb.JobKey{TenantId: string(item.TenantID), JobId: string(*item.NextJobID)}
	}
	return out
}
