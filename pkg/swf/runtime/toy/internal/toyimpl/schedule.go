package toyimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func (r *Runtime) UpsertSchedule(ctx context.Context, req swf.UpsertScheduleRequest) (swf.ScheduleInfo, error) {
	if err := swf.ValidateScheduleRequest(req); err != nil {
		return swf.ScheduleInfo{}, err
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	target, err := snapshotScheduleTarget(ctx, req.Target)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	specHash, err := swf.ScheduleSpecHash(req.Trigger, target, req.OverlapPolicy, req.FailurePolicy)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	nextFireAt, err := swf.InitialScheduleFire(req.Trigger, now)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	key := swf.ScheduleKey{TenantId: req.TenantId, ScheduleId: req.ScheduleId}
	state := swf.ScheduleStateActive
	if req.Paused {
		state = swf.ScheduleStatePaused
	}
	r.engine.mu.Lock()
	existing := r.engine.schedules[key]
	if existing != nil && existing.info.State == swf.ScheduleStateArchived {
		r.engine.mu.Unlock()
		return swf.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot be updated", swf.ErrConflict)
	}
	if req.ExpectedGeneration != nil {
		if existing == nil || existing.info.Generation != *req.ExpectedGeneration {
			r.engine.mu.Unlock()
			return swf.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", swf.ErrConflict)
		}
	}
	generation := int64(1)
	createdAt := now
	if existing != nil {
		generation = existing.info.Generation + 1
		createdAt = existing.info.CreatedAt
	}
	var nextJobKey *swf.JobKey
	if state == swf.ScheduleStateActive && nextFireAt != nil {
		nextJobKey = &swf.JobKey{TenantId: key.TenantId, JobId: swf.ScheduleRunJobID(key.ScheduleId, generation, *nextFireAt)}
	}
	info := swf.ScheduleInfo{
		TenantId:       key.TenantId,
		ScheduleId:     key.ScheduleId,
		ScheduleKey:    key,
		State:          state,
		EffectiveState: state,
		Generation:     generation,
		SpecHash:       specHash,
		Trigger:        req.Trigger,
		Target:         cloneScheduleTarget(target),
		OverlapPolicy:  swf.NormalizeScheduleOverlapPolicy(req.OverlapPolicy),
		FailurePolicy:  req.FailurePolicy,
		NextFireAt:     cloneTime(nextFireAt),
		NextJobKey:     nextJobKey,
		CreatedAt:      createdAt,
		UpdatedAt:      now,
	}
	r.engine.schedules[key] = &toyScheduleRecord{info: info}
	r.engine.mu.Unlock()
	if state == swf.ScheduleStateActive && nextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, info, *nextFireAt, "", "", false, req.WorkerID); err != nil {
			return swf.ScheduleInfo{}, err
		}
	}
	return info, nil
}

func (r *Runtime) GetSchedule(ctx context.Context, key swf.ScheduleKey) (swf.ScheduleInfo, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	rec := r.engine.schedules[key]
	if rec == nil {
		return swf.ScheduleInfo{}, swf.ErrJobNotFound
	}
	return cloneScheduleInfo(rec.info), nil
}

func (r *Runtime) ListSchedules(ctx context.Context, req swf.ListSchedulesRequest) (swf.ListSchedulesResponse, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	idAllowed := stringSet(req.ScheduleIds)
	stateAllowed := scheduleStateSet(req.States)
	jobTypeAllowed := stringSet(req.TargetJobTypes)
	out := make([]swf.ScheduleInfo, 0)
	for key, rec := range r.engine.schedules {
		if req.TenantId != "" && key.TenantId != req.TenantId {
			continue
		}
		info := rec.info
		if len(idAllowed) > 0 && !idAllowed[info.ScheduleId] {
			continue
		}
		if len(stateAllowed) > 0 && !stateAllowed[info.State] {
			continue
		}
		if len(jobTypeAllowed) > 0 && !jobTypeAllowed[info.Target.JobType] {
			continue
		}
		out = append(out, cloneScheduleInfo(info))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].ScheduleId > out[j].ScheduleId
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return swf.ListSchedulesResponse{Schedules: out}, nil
}

func (r *Runtime) PauseSchedule(ctx context.Context, req swf.ScheduleMutationRequest) (swf.ScheduleInfo, error) {
	return r.mutateScheduleState(ctx, req, swf.ScheduleStatePaused, false)
}

func (r *Runtime) ResumeSchedule(ctx context.Context, req swf.ScheduleMutationRequest) (swf.ScheduleInfo, error) {
	return r.mutateScheduleState(ctx, req, swf.ScheduleStateActive, true)
}

func (r *Runtime) ArchiveSchedule(ctx context.Context, req swf.ScheduleMutationRequest) (swf.ScheduleInfo, error) {
	return r.mutateScheduleState(ctx, req, swf.ScheduleStateArchived, false)
}

func (r *Runtime) TriggerSchedule(ctx context.Context, req swf.TriggerScheduleRequest) (swf.JobHandle, error) {
	info, err := r.GetSchedule(ctx, req.ScheduleKey)
	if err != nil {
		return swf.JobHandle{}, err
	}
	if info.State == swf.ScheduleStateArchived {
		return swf.JobHandle{}, fmt.Errorf("%w: archived schedule cannot be triggered", swf.ErrConflict)
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	jobID := swf.ScheduleManualJobID(info.ScheduleId, req.RequestID)
	key, err := r.submitScheduledOccurrenceWithJobID(ctx, info, jobID, now, "", "", true, req.WorkerID)
	if err != nil {
		return swf.JobHandle{}, err
	}
	return swf.JobHandle{JobKey: key}, nil
}

func (r *Runtime) ListScheduleRuns(ctx context.Context, req swf.ListScheduleRunsRequest) (swf.ListScheduleRunsResponse, error) {
	if err := req.ScheduleKey.Validate(); err != nil {
		return swf.ListScheduleRunsResponse{}, err
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = swf.DefaultListJobsPageSize
	} else if pageSize > swf.MaxListJobsPageSize {
		pageSize = swf.MaxListJobsPageSize
	}
	var cursorTime time.Time
	var cursorJob string
	hasCursor := false
	if req.PageToken != "" {
		createdAt, jobKey, err := swf.DecodeListJobsPageToken(req.PageToken)
		if err != nil {
			return swf.ListScheduleRunsResponse{}, err
		}
		cursorTime = createdAt
		cursorJob = jobKey.String()
		hasCursor = true
	}
	statusAllowed := map[swf.JobStatus]bool(nil)
	if len(req.Statuses) > 0 {
		statusAllowed = make(map[swf.JobStatus]bool, len(req.Statuses))
		for _, status := range req.Statuses {
			statusAllowed[status] = true
		}
	}
	out := make([]swf.ScheduleRunSummary, 0, pageSize+1)
	r.engine.mu.Lock()
	for key, rec := range r.engine.jobRecords {
		if key.TenantId != req.ScheduleKey.TenantId {
			continue
		}
		rec.mu.Lock()
		occ, hasSchedule, err := swf.ExtractScheduleOccurrenceMetadata(rec.metadata)
		if err != nil {
			rec.mu.Unlock()
			r.engine.mu.Unlock()
			return swf.ListScheduleRunsResponse{}, err
		}
		if !hasSchedule || occ.ScheduleId != req.ScheduleKey.ScheduleId {
			rec.mu.Unlock()
			continue
		}
		scheduledAt := occ.ScheduledAt.UTC()
		if req.ScheduledAfter != nil && scheduledAt.Before(req.ScheduledAfter.UTC()) {
			rec.mu.Unlock()
			continue
		}
		if req.ScheduledBefore != nil && scheduledAt.After(req.ScheduledBefore.UTC()) {
			rec.mu.Unlock()
			continue
		}
		if len(statusAllowed) > 0 && !statusAllowed[rec.status] {
			rec.mu.Unlock()
			continue
		}
		if hasCursor {
			if rec.createdAt.After(cursorTime) {
				rec.mu.Unlock()
				continue
			}
			if rec.createdAt.Equal(cursorTime) && key.String() >= cursorJob {
				rec.mu.Unlock()
				continue
			}
		}
		payloadCopy := cloneJSON(rec.payload)
		job := swf.JobSummary{
			JobKey:          key,
			Status:          rec.status,
			JobType:         rec.jobType,
			NextNeed:        cloneString(rec.capability),
			WaitFor:         append([]string(nil), rec.waitFor...),
			AvailableAt:     rec.availableAt,
			CancelRequested: rec.cancelled,
			CreatedAt:       rec.createdAt,
			ArchivedAt:      cloneTime(rec.archived),
			Payload:         payloadCopy,
			Metadata:        swf.StripRuntimeMetadata(rec.metadata),
		}
		if wait, waitErr := extractWorkerTaskWait(payloadCopy); waitErr == nil && wait != nil {
			job.TaskWaitInput = &wait.InputStep
			job.TaskWaitOutput = &wait.OutputStep
			job.TaskWaitInputHash = cloneStringPtr(&wait.InputHash)
			job.TaskWaitNext = cloneStringPtr(&wait.Next)
		}
		rec.mu.Unlock()
		out = append(out, swf.ScheduleRunSummary{
			JobSummary:  job,
			ScheduleId:  req.ScheduleKey.ScheduleId,
			ScheduledAt: scheduledAt,
			ReasonCode:  scheduleReasonFromCompletionDetail(rec.completionDetail),
		})
	}
	r.engine.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].JobSummary.CreatedAt.Equal(out[j].JobSummary.CreatedAt) {
			return out[i].JobSummary.JobKey.String() > out[j].JobSummary.JobKey.String()
		}
		return out[i].JobSummary.CreatedAt.After(out[j].JobSummary.CreatedAt)
	})
	resp := swf.ListScheduleRunsResponse{Runs: out}
	if len(out) > pageSize {
		last := out[pageSize-1].JobSummary
		resp.Runs = out[:pageSize]
		if tok, err := swf.EncodeListJobsPageToken(last.CreatedAt, last.JobKey); err == nil {
			resp.NextPageToken = tok
		}
	}
	return resp, nil
}

func (r *Runtime) mutateScheduleState(ctx context.Context, req swf.ScheduleMutationRequest, state swf.ScheduleState, submitFirst bool) (swf.ScheduleInfo, error) {
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.engine.mu.Lock()
	rec := r.engine.schedules[req.ScheduleKey]
	if rec == nil {
		r.engine.mu.Unlock()
		return swf.ScheduleInfo{}, swf.ErrJobNotFound
	}
	if req.ExpectedGeneration != nil && rec.info.Generation != *req.ExpectedGeneration {
		r.engine.mu.Unlock()
		return swf.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", swf.ErrConflict)
	}
	if rec.info.State == swf.ScheduleStateArchived && state != swf.ScheduleStateArchived {
		r.engine.mu.Unlock()
		return swf.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot change state", swf.ErrConflict)
	}
	next, err := swf.InitialScheduleFire(rec.info.Trigger, now)
	if err != nil {
		r.engine.mu.Unlock()
		return swf.ScheduleInfo{}, err
	}
	rec.info.State = state
	rec.info.EffectiveState = state
	rec.info.Generation++
	rec.info.UpdatedAt = now
	rec.info.NextFireAt = nil
	rec.info.NextJobKey = nil
	if state == swf.ScheduleStateActive && next != nil {
		rec.info.NextFireAt = cloneTime(next)
		rec.info.NextJobKey = &swf.JobKey{TenantId: req.ScheduleKey.TenantId, JobId: swf.ScheduleRunJobID(req.ScheduleKey.ScheduleId, rec.info.Generation, *next)}
	}
	info := cloneScheduleInfo(rec.info)
	r.engine.mu.Unlock()
	if submitFirst && info.State == swf.ScheduleStateActive && info.NextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, info, *info.NextFireAt, "", "", false, req.WorkerID); err != nil {
			return swf.ScheduleInfo{}, err
		}
	}
	return info, nil
}

func (r *Runtime) submitScheduledOccurrence(ctx context.Context, info swf.ScheduleInfo, scheduledAt time.Time, previousJobID string, failureBits string, manual bool, workerID string) (swf.JobKey, error) {
	return r.submitScheduledOccurrenceWithJobID(ctx, info, swf.ScheduleRunJobID(info.ScheduleId, info.Generation, scheduledAt), scheduledAt, previousJobID, failureBits, manual, workerID)
}

func (r *Runtime) submitScheduledOccurrenceWithJobID(ctx context.Context, info swf.ScheduleInfo, jobID string, scheduledAt time.Time, previousJobID string, failureBits string, manual bool, workerID string) (swf.JobKey, error) {
	jobKey := swf.JobKey{TenantId: info.TenantId, JobId: jobID}
	target := info.Target
	windowSize := info.FailurePolicy.WindowSize
	if windowSize <= 0 {
		windowSize = len(failureBits)
	}
	schedulerMetadata, err := swf.MergeScheduleOccurrenceMetadata(target.Metadata, swf.ScheduleOccurrenceMetadata{
		ScheduleId:    info.ScheduleId,
		Generation:    info.Generation,
		SpecHash:      info.SpecHash,
		ScheduledAt:   scheduledAt.UTC(),
		PreviousJobId: previousJobID,
		FailureHistory: swf.ScheduleFailureHistory{
			Bits:       failureBits,
			WindowSize: windowSize,
		},
	}, manual)
	if err != nil {
		return swf.JobKey{}, err
	}
	jobData, storedArtifacts, err := r.materializeTaskData(ctx, jobKey, 0, swf.TaskData(target.Data))
	if err != nil {
		return swf.JobKey{}, err
	}
	inputHash, err := swfInputHash(ctx, jobData)
	if err != nil {
		return swf.JobKey{}, err
	}
	now := time.Now().UTC()
	metadata, err := marshalChapterMetadata(map[string]any{
		"version":    1,
		"ordinal":    int64(0),
		"task_type":  target.JobType,
		"created_at": now,
		"input_hash": inputHash,
		"attempt":    1,
		"run_policy": target.RunPolicy,
	})
	if err != nil {
		return swf.JobKey{}, err
	}
	payload, err := jobData.GetData()
	if err != nil {
		return swf.JobKey{}, err
	}
	payloadJSON, err := json.Marshal(workerJobPayload{RunPolicy: target.RunPolicy})
	if err != nil {
		return swf.JobKey{}, err
	}
	status := swf.JobStatusReady
	waitFor := []string(nil)
	if previousJobID != "" && info.OverlapPolicy == swf.ScheduleOverlapSerial {
		waitFor = []string{previousJobID}
		status = swf.JobStatusPendingJobs
	}
	if scheduledAt.After(now) && len(waitFor) == 0 {
		status = swf.JobStatusAwaitingFuture
	}
	record := &jobRecord{
		status:      status,
		jobType:     target.JobType,
		createdAt:   now,
		metadata:    schedulerMetadata,
		payload:     payloadJSON,
		capability:  target.JobType,
		chapters:    make(map[int64]*toyChapter),
		availableAt: scheduledAt.UTC(),
		waitFor:     waitFor,
	}
	record.chapters[0] = &toyChapter{TaskType: target.JobType, CreatedAt: now, Input: jobData, Output: jobData, Attempt: 1}
	start := swf.Chapter{
		Ordinal:   0,
		TaskType:  target.JobType,
		InputHash: inputHash,
		CreatedAt: now,
		Metadata:  metadata,
		Body:      swf.JobStartChapter{Input: swf.ApplicationInputBytes{Data: append([]byte(nil), payload...)}},
		Artifacts: storedArtifacts,
	}
	r.engine.mu.Lock()
	if existing := r.engine.jobRecords[jobKey]; existing != nil {
		r.engine.mu.Unlock()
		return jobKey, nil
	}
	r.engine.jobRecords[jobKey] = record
	if r.engine.runtimeChapters[jobKey] == nil {
		r.engine.runtimeChapters[jobKey] = make(map[int64]swf.Chapter)
	}
	r.engine.runtimeChapters[jobKey][0] = start
	r.engine.mu.Unlock()
	return jobKey, nil
}

func cloneScheduleInfo(info swf.ScheduleInfo) swf.ScheduleInfo {
	info.NextFireAt = cloneTime(info.NextFireAt)
	if info.NextJobKey != nil {
		next := *info.NextJobKey
		info.NextJobKey = &next
	}
	info.Target = cloneScheduleTarget(info.Target)
	return info
}

func cloneScheduleTarget(target swf.ScheduleTarget) swf.ScheduleTarget {
	target.Metadata = cloneJSON(target.Metadata)
	if target.Data != nil {
		snapshot, err := snapshotScheduleTarget(context.Background(), target)
		if err == nil {
			return snapshot
		}
	}
	return target
}

func snapshotScheduleTarget(ctx context.Context, target swf.ScheduleTarget) (swf.ScheduleTarget, error) {
	if target.Data == nil {
		target.Metadata = cloneJSON(target.Metadata)
		return target, nil
	}
	raw, err := target.Data.GetData()
	if err != nil {
		return swf.ScheduleTarget{}, err
	}
	sourceArtifacts, err := target.Data.GetArtifacts()
	if err != nil {
		return swf.ScheduleTarget{}, err
	}
	artifacts := make([]swf.Artifact, 0, len(sourceArtifacts))
	for _, artifact := range sourceArtifacts {
		if artifact == nil {
			return swf.ScheduleTarget{}, fmt.Errorf("target artifact is nil")
		}
		bytes, err := artifact.Bytes(ctx)
		if err != nil {
			return swf.ScheduleTarget{}, err
		}
		artifacts = append(artifacts, swf.NewArtifactFromBytes(artifact.Name(), append([]byte(nil), bytes...)))
	}
	target.Data = swf.JobData(&swf.SimpleTaskData{
		Data:      append([]byte(nil), raw...),
		Artifacts: artifacts,
	})
	target.Metadata = cloneJSON(target.Metadata)
	return target, nil
}

func scheduleStateSet(states []swf.ScheduleState) map[swf.ScheduleState]bool {
	if len(states) == 0 {
		return nil
	}
	out := make(map[swf.ScheduleState]bool, len(states))
	for _, state := range states {
		out[state] = true
	}
	return out
}

func stringSet(values []string) map[string]bool {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func cloneTime(src *time.Time) *time.Time {
	if src == nil {
		return nil
	}
	value := src.UTC()
	return &value
}

func scheduleReasonFromCompletionDetail(detail string) string {
	if detail == "" {
		return ""
	}
	var payload struct {
		ReasonCode string `json:"reasonCode"`
	}
	if err := json.Unmarshal([]byte(detail), &payload); err != nil {
		return ""
	}
	return payload.ReasonCode
}
