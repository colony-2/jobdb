package toyimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (r *Runtime) UpsertSchedule(ctx context.Context, req jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error) {
	if err := jobdb.ValidateScheduleRequest(req); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	target, err := snapshotScheduleTarget(ctx, req.Target)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	specHash, err := jobdb.ScheduleSpecHash(req.Trigger, target, req.OverlapPolicy, req.FailurePolicy)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	nextFireAt, err := jobdb.InitialScheduleFire(req.Trigger, now)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	key := jobdb.ScheduleKey{TenantId: req.TenantId, ScheduleId: req.ScheduleId}
	state := jobdb.ScheduleStateActive
	if req.Paused {
		state = jobdb.ScheduleStatePaused
	}
	r.engine.mu.Lock()
	existing := r.engine.schedules[key]
	if existing != nil && existing.info.State == jobdb.ScheduleStateArchived {
		r.engine.mu.Unlock()
		return jobdb.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot be updated", jobdb.ErrConflict)
	}
	if req.ExpectedGeneration != nil {
		if existing == nil || existing.info.Generation != *req.ExpectedGeneration {
			r.engine.mu.Unlock()
			return jobdb.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", jobdb.ErrConflict)
		}
	}
	generation := int64(1)
	createdAt := now
	if existing != nil {
		generation = existing.info.Generation + 1
		createdAt = existing.info.CreatedAt
	}
	var nextJobKey *jobdb.JobKey
	if state == jobdb.ScheduleStateActive && nextFireAt != nil {
		nextJobKey = &jobdb.JobKey{TenantId: key.TenantId, JobId: jobdb.ScheduleRunJobID(key.ScheduleId, generation, *nextFireAt)}
	}
	info := jobdb.ScheduleInfo{
		TenantId:       key.TenantId,
		ScheduleId:     key.ScheduleId,
		ScheduleKey:    key,
		State:          state,
		EffectiveState: state,
		Generation:     generation,
		SpecHash:       specHash,
		Trigger:        req.Trigger,
		Target:         cloneScheduleTarget(target),
		OverlapPolicy:  jobdb.NormalizeScheduleOverlapPolicy(req.OverlapPolicy),
		FailurePolicy:  req.FailurePolicy,
		NextFireAt:     cloneTime(nextFireAt),
		NextJobKey:     nextJobKey,
		CreatedAt:      createdAt,
		UpdatedAt:      now,
	}
	r.engine.schedules[key] = &toyScheduleRecord{info: info}
	r.engine.mu.Unlock()
	if state == jobdb.ScheduleStateActive && nextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, info, *nextFireAt, "", "", false, req.WorkerID); err != nil {
			return jobdb.ScheduleInfo{}, err
		}
	}
	return info, nil
}

func (r *Runtime) GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (jobdb.ScheduleInfo, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	rec := r.engine.schedules[key]
	if rec == nil {
		return jobdb.ScheduleInfo{}, jobdb.ErrJobNotFound
	}
	return cloneScheduleInfo(rec.info), nil
}

func (r *Runtime) ListSchedules(ctx context.Context, req jobdb.ListSchedulesRequest) (jobdb.ListSchedulesResponse, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	idAllowed := stringSet(req.ScheduleIds)
	stateAllowed := scheduleStateSet(req.States)
	jobTypeAllowed := stringSet(req.TargetJobTypes)
	out := make([]jobdb.ScheduleInfo, 0)
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
	return jobdb.ListSchedulesResponse{Schedules: out}, nil
}

func (r *Runtime) PauseSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return r.mutateScheduleState(ctx, req, jobdb.ScheduleStatePaused, false)
}

func (r *Runtime) ResumeSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return r.mutateScheduleState(ctx, req, jobdb.ScheduleStateActive, true)
}

func (r *Runtime) ArchiveSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return r.mutateScheduleState(ctx, req, jobdb.ScheduleStateArchived, false)
}

func (r *Runtime) TriggerSchedule(ctx context.Context, req jobdb.TriggerScheduleRequest) (jobdb.JobHandle, error) {
	info, err := r.GetSchedule(ctx, req.ScheduleKey)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if info.State == jobdb.ScheduleStateArchived {
		return jobdb.JobHandle{}, fmt.Errorf("%w: archived schedule cannot be triggered", jobdb.ErrConflict)
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	jobID := jobdb.ScheduleManualJobID(info.ScheduleId, req.RequestID)
	key, err := r.submitScheduledOccurrenceWithJobID(ctx, info, jobID, now, "", "", true, req.WorkerID)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	return jobdb.JobHandle{JobKey: key}, nil
}

func (r *Runtime) ListScheduleRuns(ctx context.Context, req jobdb.ListScheduleRunsRequest) (jobdb.ListScheduleRunsResponse, error) {
	if err := req.ScheduleKey.Validate(); err != nil {
		return jobdb.ListScheduleRunsResponse{}, err
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = jobdb.DefaultListJobsPageSize
	} else if pageSize > jobdb.MaxListJobsPageSize {
		pageSize = jobdb.MaxListJobsPageSize
	}
	var cursorTime time.Time
	var cursorJob string
	hasCursor := false
	if req.PageToken != "" {
		createdAt, jobKey, err := jobdb.DecodeListJobsPageToken(req.PageToken)
		if err != nil {
			return jobdb.ListScheduleRunsResponse{}, err
		}
		cursorTime = createdAt
		cursorJob = jobKey.String()
		hasCursor = true
	}
	statusAllowed := map[jobdb.JobStatus]bool(nil)
	if len(req.Statuses) > 0 {
		statusAllowed = make(map[jobdb.JobStatus]bool, len(req.Statuses))
		for _, status := range req.Statuses {
			statusAllowed[status] = true
		}
	}
	out := make([]jobdb.ScheduleRunSummary, 0, pageSize+1)
	r.engine.mu.Lock()
	for key, rec := range r.engine.jobRecords {
		if key.TenantId != req.ScheduleKey.TenantId {
			continue
		}
		rec.mu.Lock()
		occ, hasSchedule, err := jobdb.ExtractScheduleOccurrenceMetadata(rec.metadata)
		if err != nil {
			rec.mu.Unlock()
			r.engine.mu.Unlock()
			return jobdb.ListScheduleRunsResponse{}, err
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
		job := jobdb.JobSummary{
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
			Metadata:        jobdb.StripRuntimeMetadata(rec.metadata),
		}
		if wait, waitErr := extractWorkerTaskWait(payloadCopy); waitErr == nil && wait != nil {
			job.TaskWaitInput = &wait.InputStep
			job.TaskWaitOutput = &wait.OutputStep
			job.TaskWaitInputHash = cloneStringPtr(&wait.InputHash)
			job.TaskWaitNext = cloneStringPtr(&wait.Next)
		}
		rec.mu.Unlock()
		out = append(out, jobdb.ScheduleRunSummary{
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
	resp := jobdb.ListScheduleRunsResponse{Runs: out}
	if len(out) > pageSize {
		last := out[pageSize-1].JobSummary
		resp.Runs = out[:pageSize]
		if tok, err := jobdb.EncodeListJobsPageToken(last.CreatedAt, last.JobKey); err == nil {
			resp.NextPageToken = tok
		}
	}
	return resp, nil
}

func (r *Runtime) mutateScheduleState(ctx context.Context, req jobdb.ScheduleMutationRequest, state jobdb.ScheduleState, submitFirst bool) (jobdb.ScheduleInfo, error) {
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.engine.mu.Lock()
	rec := r.engine.schedules[req.ScheduleKey]
	if rec == nil {
		r.engine.mu.Unlock()
		return jobdb.ScheduleInfo{}, jobdb.ErrJobNotFound
	}
	if req.ExpectedGeneration != nil && rec.info.Generation != *req.ExpectedGeneration {
		r.engine.mu.Unlock()
		return jobdb.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", jobdb.ErrConflict)
	}
	if rec.info.State == jobdb.ScheduleStateArchived && state != jobdb.ScheduleStateArchived {
		r.engine.mu.Unlock()
		return jobdb.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot change state", jobdb.ErrConflict)
	}
	next, err := jobdb.InitialScheduleFire(rec.info.Trigger, now)
	if err != nil {
		r.engine.mu.Unlock()
		return jobdb.ScheduleInfo{}, err
	}
	rec.info.State = state
	rec.info.EffectiveState = state
	rec.info.Generation++
	rec.info.UpdatedAt = now
	rec.info.NextFireAt = nil
	rec.info.NextJobKey = nil
	if state == jobdb.ScheduleStateActive && next != nil {
		rec.info.NextFireAt = cloneTime(next)
		rec.info.NextJobKey = &jobdb.JobKey{TenantId: req.ScheduleKey.TenantId, JobId: jobdb.ScheduleRunJobID(req.ScheduleKey.ScheduleId, rec.info.Generation, *next)}
	}
	info := cloneScheduleInfo(rec.info)
	r.engine.mu.Unlock()
	if submitFirst && info.State == jobdb.ScheduleStateActive && info.NextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, info, *info.NextFireAt, "", "", false, req.WorkerID); err != nil {
			return jobdb.ScheduleInfo{}, err
		}
	}
	return info, nil
}

func (r *Runtime) submitScheduledOccurrence(ctx context.Context, info jobdb.ScheduleInfo, scheduledAt time.Time, previousJobID string, failureBits string, manual bool, workerID string) (jobdb.JobKey, error) {
	return r.submitScheduledOccurrenceWithJobID(ctx, info, jobdb.ScheduleRunJobID(info.ScheduleId, info.Generation, scheduledAt), scheduledAt, previousJobID, failureBits, manual, workerID)
}

func (r *Runtime) submitScheduledOccurrenceWithJobID(ctx context.Context, info jobdb.ScheduleInfo, jobID string, scheduledAt time.Time, previousJobID string, failureBits string, manual bool, workerID string) (jobdb.JobKey, error) {
	jobKey := jobdb.JobKey{TenantId: info.TenantId, JobId: jobID}
	target := info.Target
	windowSize := info.FailurePolicy.WindowSize
	if windowSize <= 0 {
		windowSize = len(failureBits)
	}
	schedulerMetadata, err := jobdb.MergeScheduleOccurrenceMetadata(target.Metadata, jobdb.ScheduleOccurrenceMetadata{
		ScheduleId:    info.ScheduleId,
		Generation:    info.Generation,
		SpecHash:      info.SpecHash,
		ScheduledAt:   scheduledAt.UTC(),
		PreviousJobId: previousJobID,
		FailureHistory: jobdb.ScheduleFailureHistory{
			Bits:       failureBits,
			WindowSize: windowSize,
		},
	}, manual)
	if err != nil {
		return jobdb.JobKey{}, err
	}
	jobData, storedArtifacts, err := r.materializeTaskData(ctx, jobKey, 0, jobdb.TaskData(target.Data))
	if err != nil {
		return jobdb.JobKey{}, err
	}
	inputHash, err := jobdbInputHash(ctx, jobData)
	if err != nil {
		return jobdb.JobKey{}, err
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
		return jobdb.JobKey{}, err
	}
	payload, err := jobData.GetData()
	if err != nil {
		return jobdb.JobKey{}, err
	}
	payloadJSON, err := json.Marshal(workerJobPayload{RunPolicy: target.RunPolicy})
	if err != nil {
		return jobdb.JobKey{}, err
	}
	status := jobdb.JobStatusReady
	waitFor := []string(nil)
	if previousJobID != "" && info.OverlapPolicy == jobdb.ScheduleOverlapSerial {
		waitFor = []string{previousJobID}
		status = jobdb.JobStatusPendingJobs
	}
	if scheduledAt.After(now) && len(waitFor) == 0 {
		status = jobdb.JobStatusAwaitingFuture
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
	start := jobdb.Chapter{
		Ordinal:   0,
		TaskType:  target.JobType,
		InputHash: inputHash,
		CreatedAt: now,
		Metadata:  metadata,
		Body:      jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: append([]byte(nil), payload...)}},
		Artifacts: storedArtifacts,
	}
	r.engine.mu.Lock()
	if existing := r.engine.jobRecords[jobKey]; existing != nil {
		r.engine.mu.Unlock()
		return jobKey, nil
	}
	r.engine.jobRecords[jobKey] = record
	if r.engine.runtimeChapters[jobKey] == nil {
		r.engine.runtimeChapters[jobKey] = make(map[int64]jobdb.Chapter)
	}
	r.engine.runtimeChapters[jobKey][0] = start
	r.engine.mu.Unlock()
	return jobKey, nil
}

func cloneScheduleInfo(info jobdb.ScheduleInfo) jobdb.ScheduleInfo {
	info.NextFireAt = cloneTime(info.NextFireAt)
	if info.NextJobKey != nil {
		next := *info.NextJobKey
		info.NextJobKey = &next
	}
	info.Target = cloneScheduleTarget(info.Target)
	return info
}

func cloneScheduleTarget(target jobdb.ScheduleTarget) jobdb.ScheduleTarget {
	target.Metadata = cloneJSON(target.Metadata)
	if target.Data != nil {
		snapshot, err := snapshotScheduleTarget(context.Background(), target)
		if err == nil {
			return snapshot
		}
	}
	return target
}

func snapshotScheduleTarget(ctx context.Context, target jobdb.ScheduleTarget) (jobdb.ScheduleTarget, error) {
	if target.Data == nil {
		target.Metadata = cloneJSON(target.Metadata)
		return target, nil
	}
	raw, err := target.Data.GetData()
	if err != nil {
		return jobdb.ScheduleTarget{}, err
	}
	sourceArtifacts, err := target.Data.GetArtifacts()
	if err != nil {
		return jobdb.ScheduleTarget{}, err
	}
	artifacts := make([]jobdb.Artifact, 0, len(sourceArtifacts))
	for _, artifact := range sourceArtifacts {
		if artifact == nil {
			return jobdb.ScheduleTarget{}, fmt.Errorf("target artifact is nil")
		}
		bytes, err := artifact.Bytes(ctx)
		if err != nil {
			return jobdb.ScheduleTarget{}, err
		}
		artifacts = append(artifacts, jobdb.NewArtifactFromBytes(artifact.Name(), append([]byte(nil), bytes...)))
	}
	target.Data = jobdb.JobData(&jobdb.SimpleTaskData{
		Data:      append([]byte(nil), raw...),
		Artifacts: artifacts,
	})
	target.Metadata = cloneJSON(target.Metadata)
	return target, nil
}

func scheduleStateSet(states []jobdb.ScheduleState) map[jobdb.ScheduleState]bool {
	if len(states) == 0 {
		return nil
	}
	out := make(map[jobdb.ScheduleState]bool, len(states))
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
