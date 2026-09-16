package runtimecore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// preflightScheduleLease checks an occurrence before its first application
// chapter and creates the next occurrence while the lease is still held.
func (r *Runtime) preflightScheduleLease(ctx context.Context, lease *executionLease) (bool, error) {
	key := lease.snapshot.Identity.JobKey
	job, err := r.scheduler.GetJob(ctx, key)
	if err != nil {
		return false, err
	}
	if job.Schedule == nil {
		return true, nil
	}
	count, err := r.chapters.Count(ctx, ChapterLogKey{JobKey: key})
	if err != nil {
		return false, err
	}
	if count > 1 {
		return true, nil
	}
	occ := *job.Schedule
	info, err := r.GetSchedule(ctx, jobdb.ScheduleKey{
		TenantId: key.TenantId, ScheduleId: occ.ScheduleId,
	})
	if errors.Is(err, jobdb.ErrJobNotFound) {
		return r.cancelScheduledLease(ctx, lease, occ,
			"schedule_missing", "schedule missing before app start", 0, "")
	}
	if err != nil {
		return false, err
	}
	switch info.State {
	case jobdb.ScheduleStatePaused:
		return r.cancelScheduledLease(ctx, lease, occ,
			"schedule_paused", "schedule paused before app start", info.Generation, info.SpecHash)
	case jobdb.ScheduleStateArchived:
		return r.cancelScheduledLease(ctx, lease, occ,
			"schedule_archived", "schedule archived before app start", info.Generation, info.SpecHash)
	}
	if info.Generation != occ.Generation {
		return r.cancelScheduledLease(ctx, lease, occ,
			"schedule_generation_mismatch", "schedule generation changed before app start", info.Generation, info.SpecHash)
	}
	if info.SpecHash != occ.SpecHash {
		return r.cancelScheduledLease(ctx, lease, occ,
			"schedule_spec_mismatch", "schedule spec changed before app start", info.Generation, info.SpecHash)
	}
	if info.Trigger.EndAt != nil && occ.ScheduledAt.After(info.Trigger.EndAt.UTC()) {
		return r.cancelScheduledLease(ctx, lease, occ,
			"schedule_ended", "schedule ended before app start", info.Generation, info.SpecHash)
	}
	bits := occ.FailureHistory.Bits
	window := info.FailurePolicy.WindowSize
	if window <= 0 {
		window = occ.FailureHistory.WindowSize
	}
	if occ.PreviousJobId != "" {
		previous, err := r.scheduler.GetJob(ctx, jobdb.JobKey{
			TenantId: key.TenantId, JobId: occ.PreviousJobId,
		})
		success := err == nil && previous.Completion != nil && previous.Completion.Status == "success"
		bits = jobdb.AppendScheduleFailureBit(bits, success, window)
	}
	if jobdb.ScheduleFailurePolicyViolated(bits, info.FailurePolicy) {
		return r.cancelScheduledLease(ctx, lease, occ,
			"failure_policy", "schedule failure policy blocked this occurrence", info.Generation, info.SpecHash)
	}
	next, err := jobdb.NextScheduleFire(info.Trigger, r.now())
	if err != nil {
		return false, err
	}
	if next != nil {
		if _, err := r.submitScheduledOccurrence(ctx, info, *next, key.JobId,
			bits, false, lease.snapshot.Identity.WorkerID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *Runtime) cancelScheduledLease(ctx context.Context, lease *executionLease,
	occ jobdb.ScheduleOccurrenceMetadata, reason, message string,
	actualGeneration int64, actualSpecHash string) (bool, error) {
	detail := scheduleCancelDetail{
		Kind: "schedule_preflight_outcome", Status: "cancelled",
		ReasonCode: reason, Message: message, ScheduleId: occ.ScheduleId,
		ExpectedGeneration: occ.Generation, ActualGeneration: actualGeneration,
		ExpectedSpecHash: occ.SpecHash, ActualSpecHash: actualSpecHash,
		ScheduledAt: occ.ScheduledAt,
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return false, err
	}
	chapter := jobdb.Chapter{
		Ordinal: 1, TaskType: lease.Capability(), CreatedAt: r.now(),
		Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.SystemErrorOutcome{
			Error: jobdb.SystemErrorPayload{
				Message: message, Component: "jobdb.schedule_preflight", Code: reason,
			},
		}},
	}
	if err := lease.Complete(ctx, jobdb.CompleteExecutionRequest{
		Status: "cancelled", Detail: string(raw), Chapter: &chapter,
	}); err != nil {
		return false, err
	}
	return false, nil
}

type scheduleCancelDetail struct {
	Kind               string    `json:"kind"`
	Status             string    `json:"status"`
	ReasonCode         string    `json:"reasonCode"`
	Message            string    `json:"message,omitempty"`
	ScheduleId         string    `json:"scheduleId,omitempty"`
	ExpectedGeneration int64     `json:"expectedGeneration,omitempty"`
	ActualGeneration   int64     `json:"actualGeneration,omitempty"`
	ExpectedSpecHash   string    `json:"expectedSpecHash,omitempty"`
	ActualSpecHash     string    `json:"actualSpecHash,omitempty"`
	ScheduledAt        time.Time `json:"scheduledAt,omitempty"`
}
