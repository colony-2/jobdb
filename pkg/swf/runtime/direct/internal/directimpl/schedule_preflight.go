package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/jobmetadata"
)

func (r *Runtime) preflightScheduleLease(ctx context.Context, lease *executionLease) (bool, error) {
	jobKey := lease.Job().JobKey
	detail, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId), pgwf.GetJobOptions{})
	if err != nil {
		return false, err
	}
	lease.schemaHash = jobmetadata.SchemaHashFromStoredMetadata(detail.Metadata)
	occ, hasSchedule, err := swf.ExtractScheduleOccurrenceMetadata(detail.Metadata)
	if err != nil {
		return false, err
	}
	if !hasSchedule {
		return true, nil
	}
	chapterCount, err := r.storyChapterCount(ctx, jobKey)
	if err != nil {
		return false, err
	}
	if chapterCount > 1 {
		return true, nil
	}
	row, found, err := r.loadScheduleRow(ctx, swf.ScheduleKey{TenantId: jobKey.TenantId, ScheduleId: occ.ScheduleId})
	if err != nil {
		return false, err
	}
	if !found {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_missing", "schedule missing before app start", 0, ""))
	}
	if row.state == swf.ScheduleStatePaused {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_paused", "schedule paused before app start", row.generation, row.specHash))
	}
	if row.state == swf.ScheduleStateArchived {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_archived", "schedule archived before app start", row.generation, row.specHash))
	}
	if row.generation != occ.Generation {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_generation_mismatch", "schedule generation changed before app start", row.generation, row.specHash))
	}
	if row.specHash != occ.SpecHash {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_spec_mismatch", "schedule spec changed before app start", row.generation, row.specHash))
	}
	if row.trigger.EndAt != nil && occ.ScheduledAt.After(row.trigger.EndAt.UTC()) {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_ended", "schedule ended before app start", row.generation, row.specHash))
	}
	failureBits := occ.FailureHistory.Bits
	windowSize := row.failurePolicy.WindowSize
	if windowSize <= 0 {
		windowSize = occ.FailureHistory.WindowSize
	}
	if occ.PreviousJobId != "" {
		failureBits = swf.AppendScheduleFailureBit(failureBits, r.previousJobSucceeded(ctx, jobKey.TenantId, occ.PreviousJobId), windowSize)
	}
	if swf.ScheduleFailurePolicyViolated(failureBits, row.failurePolicy) {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "failure_policy", "schedule failure policy blocked this occurrence", row.generation, row.specHash))
	}
	nextFireAt, err := swf.NextScheduleFire(row.trigger, time.Now().UTC())
	if err != nil {
		return false, err
	}
	if nextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, row, *nextFireAt, jobKey.JobId, failureBits, false, lease.workerID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *Runtime) storyChapterCount(ctx context.Context, jobKey swf.JobKey) (int64, error) {
	st, err := r.loadStory(ctx, storyKeyForJob(jobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return 0, swf.ErrJobNotFound
		}
		return 0, err
	}
	return st.ChapterCount(), nil
}

func (r *Runtime) previousJobSucceeded(ctx context.Context, tenantID string, jobID string) bool {
	detail, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(tenantID), pgwf.JobID(jobID), pgwf.GetJobOptions{})
	if err != nil || detail.CompletionStatus == nil {
		return false
	}
	return *detail.CompletionStatus == completionStatusSuccess
}

func (r *Runtime) cancelScheduledLease(ctx context.Context, lease *executionLease, detail scheduleCancelDetail) (bool, error) {
	raw, err := json.Marshal(detail)
	if err != nil {
		return false, err
	}
	if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "cancelled", Detail: string(raw)}); err != nil {
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

func cancellationForOccurrence(occ swf.ScheduleOccurrenceMetadata, reason string, message string, actualGeneration int64, actualSpecHash string) scheduleCancelDetail {
	return scheduleCancelDetail{
		Kind:               "schedule_preflight_outcome",
		Status:             "cancelled",
		ReasonCode:         reason,
		Message:            message,
		ScheduleId:         occ.ScheduleId,
		ExpectedGeneration: occ.Generation,
		ActualGeneration:   actualGeneration,
		ExpectedSpecHash:   occ.SpecHash,
		ActualSpecHash:     actualSpecHash,
		ScheduledAt:        occ.ScheduledAt,
	}
}
