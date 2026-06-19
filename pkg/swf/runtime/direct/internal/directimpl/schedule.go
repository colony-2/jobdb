package directimpl

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/swf-go/pkg/swf"
)

type scheduleRow struct {
	tenantID      string
	scheduleID    string
	state         swf.ScheduleState
	generation    int64
	specHash      string
	trigger       swf.ScheduleTrigger
	target        storedScheduleTarget
	targetJobType string
	overlapPolicy swf.ScheduleOverlapPolicy
	failurePolicy swf.ScheduleFailurePolicy
	nextFireAt    sql.NullTime
	nextJobID     sql.NullString
	createdAt     time.Time
	updatedAt     time.Time
}

type storedScheduleTarget struct {
	JobType   string                   `json:"jobType"`
	Data      json.RawMessage          `json:"data,omitempty"`
	Artifacts []storedScheduleArtifact `json:"artifacts,omitempty"`
	RunPolicy swf.RunPolicy            `json:"runPolicy,omitempty"`
	Metadata  json.RawMessage          `json:"metadata,omitempty"`
}

type storedScheduleArtifact struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Digest string `json:"sha256,omitempty"`
	Data   []byte `json:"data,omitempty"`
}

const scheduleColumns = `
tenant_id, schedule_id, state, generation, spec_hash, trigger_json, target_json,
target_job_type, overlap_policy, failure_policy_json, next_fire_at, next_job_id,
created_at, updated_at
`

func (r *Runtime) UpsertSchedule(ctx context.Context, req swf.UpsertScheduleRequest) (swf.ScheduleInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.ScheduleInfo{}, err
	}
	if err := swf.ValidateScheduleRequest(req); err != nil {
		return swf.ScheduleInfo{}, err
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	target, err := storedTargetFromSchedule(ctx, req.Target)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	specHash, err := swf.ScheduleSpecHash(req.Trigger, target.toScheduleTarget(), req.OverlapPolicy, req.FailurePolicy)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	nextFireAt, err := swf.InitialScheduleFire(req.Trigger, now)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	state := swf.ScheduleStateActive
	if req.Paused {
		state = swf.ScheduleStatePaused
	}
	var row scheduleRow
	tx, err := r.udb.BeginTx(ctx, nil)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	defer func() { _ = tx.Rollback() }()
	existing, found, err := scanScheduleMaybe(tx.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM swf_schedules WHERE tenant_id = $1 AND schedule_id = $2`, req.TenantId, req.ScheduleId))
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	if found && existing.state == swf.ScheduleStateArchived {
		return swf.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot be updated", swf.ErrConflict)
	}
	if req.ExpectedGeneration != nil {
		if !found || existing.generation != *req.ExpectedGeneration {
			return swf.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", swf.ErrConflict)
		}
	}
	generation := int64(1)
	createdAt := now
	if found {
		generation = existing.generation + 1
		createdAt = existing.createdAt
	}
	nextJobID := sql.NullString{}
	nextAt := sql.NullTime{}
	if state == swf.ScheduleStateActive && nextFireAt != nil {
		nextJobID = sql.NullString{String: swf.ScheduleRunJobID(req.ScheduleId, generation, *nextFireAt), Valid: true}
		nextAt = sql.NullTime{Time: nextFireAt.UTC(), Valid: true}
	}
	triggerJSON, err := json.Marshal(req.Trigger)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	failureJSON, err := json.Marshal(req.FailurePolicy)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	if found {
		_, err = tx.ExecContext(ctx, `
UPDATE swf_schedules
SET state = $1, generation = $2, spec_hash = $3, trigger_json = $4, target_json = $5,
	target_job_type = $6, overlap_policy = $7, failure_policy_json = $8,
	next_fire_at = $9, next_job_id = $10, updated_at = $11
WHERE tenant_id = $12 AND schedule_id = $13`,
			state, generation, specHash, triggerJSON, targetJSON, target.JobType,
			swf.NormalizeScheduleOverlapPolicy(req.OverlapPolicy), failureJSON,
			nullTimeArg(nextAt), nullStringArg(nextJobID), now, req.TenantId, req.ScheduleId)
	} else {
		_, err = tx.ExecContext(ctx, `
INSERT INTO swf_schedules (
	tenant_id, schedule_id, state, generation, spec_hash, trigger_json, target_json,
	target_job_type, overlap_policy, failure_policy_json, next_fire_at, next_job_id,
	created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			req.TenantId, req.ScheduleId, state, generation, specHash, triggerJSON, targetJSON,
			target.JobType, swf.NormalizeScheduleOverlapPolicy(req.OverlapPolicy), failureJSON,
			nullTimeArg(nextAt), nullStringArg(nextJobID), createdAt, now)
	}
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	if err := tx.Commit(); err != nil {
		return swf.ScheduleInfo{}, err
	}
	row = scheduleRow{
		tenantID:      req.TenantId,
		scheduleID:    req.ScheduleId,
		state:         state,
		generation:    generation,
		specHash:      specHash,
		trigger:       req.Trigger,
		target:        target,
		targetJobType: target.JobType,
		overlapPolicy: swf.NormalizeScheduleOverlapPolicy(req.OverlapPolicy),
		failurePolicy: req.FailurePolicy,
		nextFireAt:    nextAt,
		nextJobID:     nextJobID,
		createdAt:     createdAt,
		updatedAt:     now,
	}
	if row.state == swf.ScheduleStateActive && nextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, row, *nextFireAt, "", "", false, req.WorkerID); err != nil {
			return swf.ScheduleInfo{}, err
		}
	}
	return scheduleInfoFromRow(row), nil
}

func (r *Runtime) GetSchedule(ctx context.Context, key swf.ScheduleKey) (swf.ScheduleInfo, error) {
	row, found, err := r.loadScheduleRow(ctx, key)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	if !found {
		return swf.ScheduleInfo{}, swf.ErrJobNotFound
	}
	return scheduleInfoFromRow(row), nil
}

func (r *Runtime) ListSchedules(ctx context.Context, req swf.ListSchedulesRequest) (swf.ListSchedulesResponse, error) {
	if req.TenantId == "" {
		return swf.ListSchedulesResponse{}, fmt.Errorf("tenantId is required")
	}
	rows, err := r.udb.QueryContext(ctx, `SELECT `+scheduleColumns+` FROM swf_schedules WHERE tenant_id = $1`, req.TenantId)
	if err != nil {
		return swf.ListSchedulesResponse{}, err
	}
	defer rows.Close()
	idAllowed := stringSet(req.ScheduleIds)
	stateAllowed := scheduleStateSet(req.States)
	jobTypeAllowed := stringSet(req.TargetJobTypes)
	out := make([]swf.ScheduleInfo, 0)
	for rows.Next() {
		row, err := scanScheduleRow(rows)
		if err != nil {
			return swf.ListSchedulesResponse{}, err
		}
		if len(idAllowed) > 0 && !idAllowed[row.scheduleID] {
			continue
		}
		if len(stateAllowed) > 0 && !stateAllowed[row.state] {
			continue
		}
		if len(jobTypeAllowed) > 0 && !jobTypeAllowed[row.targetJobType] {
			continue
		}
		out = append(out, scheduleInfoFromRow(row))
	}
	if err := rows.Err(); err != nil {
		return swf.ListSchedulesResponse{}, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].ScheduleId > out[j].ScheduleId
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	pageSize := req.PageSize
	if pageSize <= 0 || pageSize > swf.MaxListJobsPageSize {
		pageSize = swf.DefaultListJobsPageSize
	}
	if len(out) > pageSize {
		out = out[:pageSize]
	}
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
	row, found, err := r.loadScheduleRow(ctx, req.ScheduleKey)
	if err != nil {
		return swf.JobHandle{}, err
	}
	if !found {
		return swf.JobHandle{}, swf.ErrJobNotFound
	}
	if row.state == swf.ScheduleStateArchived {
		return swf.JobHandle{}, fmt.Errorf("%w: archived schedule cannot be triggered", swf.ErrConflict)
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	key, err := r.submitScheduledOccurrenceWithJobID(ctx, row, swf.ScheduleManualJobID(row.scheduleID, req.RequestID), now, "", "", true, req.WorkerID)
	if err != nil {
		return swf.JobHandle{}, err
	}
	return swf.JobHandle{JobKey: key}, nil
}

func (r *Runtime) ListScheduleRuns(ctx context.Context, req swf.ListScheduleRunsRequest) (swf.ListScheduleRunsResponse, error) {
	if err := req.ScheduleKey.Validate(); err != nil {
		return swf.ListScheduleRunsResponse{}, err
	}
	result, err := pgwf.ListJobs(ctx, r.pgwfDB(ctx), pgwf.ListJobsOptions{
		TenantIDs:       []string{req.ScheduleKey.TenantId},
		Statuses:        convertSwfStatusesToPgwf(req.Statuses),
		IncludeArchived: true,
		Limit:           normalizePageSize(req.PageSize),
		Cursor:          req.PageToken,
		SortBy:          pgwf.SortByCreatedAt,
		SortOrder:       pgwf.SortDesc,
	})
	if err != nil {
		return swf.ListScheduleRunsResponse{}, err
	}
	out := make([]swf.ScheduleRunSummary, 0, len(result.Jobs))
	for _, job := range result.Jobs {
		occ, hasSchedule, err := swf.ExtractScheduleOccurrenceMetadata(job.Metadata)
		if err != nil {
			return swf.ListScheduleRunsResponse{}, err
		}
		if !hasSchedule || occ.ScheduleId != req.ScheduleKey.ScheduleId {
			continue
		}
		scheduledAt := occ.ScheduledAt.UTC()
		if req.ScheduledAfter != nil && scheduledAt.Before(req.ScheduledAfter.UTC()) {
			continue
		}
		if req.ScheduledBefore != nil && scheduledAt.After(req.ScheduledBefore.UTC()) {
			continue
		}
		summary := swf.JobSummary{
			JobKey:          swf.JobKey{TenantId: job.TenantID, JobId: job.JobID},
			Status:          convertPgwfStatusToSwf(job.Status, job.CancelRequested, job.ArchivedAt),
			JobType:         swf.JobTypeFromNextNeed(job.NextNeed),
			NextNeed:        strPtr(job.NextNeed),
			WaitFor:         append([]string(nil), job.WaitFor...),
			AvailableAt:     job.AvailableAt,
			ExpiresAt:       job.ExpiresAt,
			LeaseExpiresAt:  job.LeaseExpiresAt,
			CancelRequested: job.CancelRequested,
			CreatedAt:       job.CreatedAt,
			ArchivedAt:      job.ArchivedAt,
			Metadata:        swf.StripRuntimeMetadata(job.Metadata),
		}
		reason := ""
		if job.CompletionDetail != nil {
			reason = scheduleReasonFromCompletionDetail(*job.CompletionDetail)
		}
		out = append(out, swf.ScheduleRunSummary{JobSummary: summary, ScheduleId: req.ScheduleKey.ScheduleId, ScheduledAt: scheduledAt, ReasonCode: reason})
	}
	return swf.ListScheduleRunsResponse{Runs: out, NextPageToken: result.NextCursor}, nil
}

func (r *Runtime) mutateScheduleState(ctx context.Context, req swf.ScheduleMutationRequest, state swf.ScheduleState, submitFirst bool) (swf.ScheduleInfo, error) {
	if err := req.ScheduleKey.Validate(); err != nil {
		return swf.ScheduleInfo{}, err
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := r.udb.BeginTx(ctx, nil)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	defer func() { _ = tx.Rollback() }()
	row, found, err := scanScheduleMaybe(tx.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM swf_schedules WHERE tenant_id = $1 AND schedule_id = $2`, req.ScheduleKey.TenantId, req.ScheduleKey.ScheduleId))
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	if !found {
		return swf.ScheduleInfo{}, swf.ErrJobNotFound
	}
	if req.ExpectedGeneration != nil && row.generation != *req.ExpectedGeneration {
		return swf.ScheduleInfo{}, fmt.Errorf("%w: schedule generation mismatch", swf.ErrConflict)
	}
	if row.state == swf.ScheduleStateArchived && state != swf.ScheduleStateArchived {
		return swf.ScheduleInfo{}, fmt.Errorf("%w: archived schedule cannot change state", swf.ErrConflict)
	}
	next, err := swf.InitialScheduleFire(row.trigger, now)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	row.state = state
	row.generation++
	row.updatedAt = now
	row.nextFireAt = sql.NullTime{}
	row.nextJobID = sql.NullString{}
	if state == swf.ScheduleStateActive && next != nil {
		row.nextFireAt = sql.NullTime{Time: next.UTC(), Valid: true}
		row.nextJobID = sql.NullString{String: swf.ScheduleRunJobID(row.scheduleID, row.generation, *next), Valid: true}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE swf_schedules
SET state = $1, generation = $2, next_fire_at = $3, next_job_id = $4, updated_at = $5
WHERE tenant_id = $6 AND schedule_id = $7`,
		row.state, row.generation, nullTimeArg(row.nextFireAt), nullStringArg(row.nextJobID), row.updatedAt, row.tenantID, row.scheduleID); err != nil {
		return swf.ScheduleInfo{}, err
	}
	if err := tx.Commit(); err != nil {
		return swf.ScheduleInfo{}, err
	}
	if submitFirst && row.state == swf.ScheduleStateActive && row.nextFireAt.Valid {
		if _, err := r.submitScheduledOccurrence(ctx, row, row.nextFireAt.Time, "", "", false, req.WorkerID); err != nil {
			return swf.ScheduleInfo{}, err
		}
	}
	return scheduleInfoFromRow(row), nil
}

func (r *Runtime) submitScheduledOccurrence(ctx context.Context, row scheduleRow, scheduledAt time.Time, previousJobID string, failureBits string, manual bool, workerID string) (swf.JobKey, error) {
	return r.submitScheduledOccurrenceWithJobID(ctx, row, swf.ScheduleRunJobID(row.scheduleID, row.generation, scheduledAt), scheduledAt, previousJobID, failureBits, manual, workerID)
}

func (r *Runtime) submitScheduledOccurrenceWithJobID(ctx context.Context, row scheduleRow, jobID string, scheduledAt time.Time, previousJobID string, failureBits string, manual bool, workerID string) (swf.JobKey, error) {
	jobKey := swf.JobKey{TenantId: row.tenantID, JobId: jobID}
	target := row.target.toScheduleTarget()
	windowSize := row.failurePolicy.WindowSize
	if windowSize <= 0 {
		windowSize = len(failureBits)
	}
	schedulerMetadata, err := swf.MergeScheduleOccurrenceMetadata(target.Metadata, swf.ScheduleOccurrenceMetadata{
		ScheduleId:    row.scheduleID,
		Generation:    row.generation,
		SpecHash:      row.specHash,
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
	prereqs := []swf.JobPrerequisite(nil)
	if previousJobID != "" && row.overlapPolicy == swf.ScheduleOverlapSerial {
		prereqs = append(prereqs, swf.JobPrerequisite{JobID: previousJobID, Condition: swf.JobPrereqComplete})
	}
	_, waitFor, err := normalizePrerequisites(jobKey, prereqs)
	if err != nil {
		return swf.JobKey{}, err
	}
	taskData := swf.TaskData(target.Data)
	inputHash, err := computeInputHash(ctx, taskData)
	if err != nil {
		return swf.JobKey{}, err
	}
	jobPolicy := normalizeRunPolicy(target.RunPolicy)
	co, err := taskDataToCreatOptions(taskData, 0, target.JobType, r.requestWorkerID(workerID), chapterTypeJobStart, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
		Attempt:       1,
		RunPolicy:     &jobPolicy,
		Metadata:      metadataForStartChapter(target.Metadata),
		Prerequisites: prereqs,
	})
	if err != nil {
		return swf.JobKey{}, err
	}
	if _, err := r.strataClient.CreateStory(ctx, storyKeyForJob(jobKey), co); err != nil {
		if !errors.Is(err, core.ErrConflict) {
			return swf.JobKey{}, err
		}
		start, exists, loadErr := r.loadExistingStartChapter(ctx, jobKey)
		if loadErr != nil {
			return swf.JobKey{}, loadErr
		}
		if !exists {
			return swf.JobKey{}, err
		}
		if compareErr := compareSubmitStartChapter(jobKey, start, target.JobType, inputHash, target.Metadata, prereqs, jobPolicy); compareErr != nil {
			return swf.JobKey{}, compareErr
		}
	}
	if artifacts, _ := taskData.GetArtifacts(); len(artifacts) > 0 {
		assignArtifactKeys(artifacts, jobKey.JobId, 0)
		for _, art := range artifacts {
			if cleanupErr := art.Cleanup(); cleanupErr != nil {
				r.logger.Warn("failed to cleanup scheduled job input artifact", "artifact", art.Name(), "error", cleanupErr)
			}
		}
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, target.JobType, schedulerMetadata, waitFor, jobPayload{RunPolicy: jobPolicy}, workerID, &scheduledAt); err != nil {
		return swf.JobKey{}, err
	}
	return jobKey, nil
}

func (r *Runtime) loadScheduleRow(ctx context.Context, key swf.ScheduleKey) (scheduleRow, bool, error) {
	return scanScheduleMaybe(r.udb.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM swf_schedules WHERE tenant_id = $1 AND schedule_id = $2`, key.TenantId, key.ScheduleId))
}

func scanScheduleMaybe(row *sql.Row) (scheduleRow, bool, error) {
	out, err := scanScheduleRow(row)
	if err == sql.ErrNoRows {
		return scheduleRow{}, false, nil
	}
	return out, err == nil, err
}

func scanScheduleRow(scanner interface{ Scan(dest ...any) error }) (scheduleRow, error) {
	var row scheduleRow
	var triggerJSON []byte
	var targetJSON []byte
	var failureJSON []byte
	var state string
	var overlap string
	if err := scanner.Scan(
		&row.tenantID,
		&row.scheduleID,
		&state,
		&row.generation,
		&row.specHash,
		&triggerJSON,
		&targetJSON,
		&row.targetJobType,
		&overlap,
		&failureJSON,
		&row.nextFireAt,
		&row.nextJobID,
		&row.createdAt,
		&row.updatedAt,
	); err != nil {
		return scheduleRow{}, err
	}
	if err := json.Unmarshal(triggerJSON, &row.trigger); err != nil {
		return scheduleRow{}, err
	}
	if err := json.Unmarshal(targetJSON, &row.target); err != nil {
		return scheduleRow{}, err
	}
	if err := json.Unmarshal(failureJSON, &row.failurePolicy); err != nil {
		return scheduleRow{}, err
	}
	row.state = swf.ScheduleState(state)
	row.overlapPolicy = swf.ScheduleOverlapPolicy(overlap)
	return row, nil
}

func storedTargetFromSchedule(ctx context.Context, target swf.ScheduleTarget) (storedScheduleTarget, error) {
	var data json.RawMessage
	var storedArtifacts []storedScheduleArtifact
	if target.Data != nil {
		raw, err := target.Data.GetData()
		if err != nil {
			return storedScheduleTarget{}, err
		}
		data = append(json.RawMessage(nil), raw...)
		artifacts, err := target.Data.GetArtifacts()
		if err != nil {
			return storedScheduleTarget{}, err
		}
		storedArtifacts = make([]storedScheduleArtifact, 0, len(artifacts))
		for _, artifact := range artifacts {
			if artifact == nil {
				return storedScheduleTarget{}, fmt.Errorf("target artifact is nil")
			}
			bytes, err := artifact.Bytes(ctx)
			if err != nil {
				return storedScheduleTarget{}, err
			}
			sum := sha256.Sum256(bytes)
			storedArtifacts = append(storedArtifacts, storedScheduleArtifact{
				Name:   artifact.Name(),
				Size:   int64(len(bytes)),
				Digest: hex.EncodeToString(sum[:]),
				Data:   append([]byte(nil), bytes...),
			})
		}
	}
	return storedScheduleTarget{
		JobType:   target.JobType,
		Data:      data,
		Artifacts: storedArtifacts,
		RunPolicy: target.RunPolicy,
		Metadata:  swf.NormalizeJSON(target.Metadata),
	}, nil
}

func (t storedScheduleTarget) toScheduleTarget() swf.ScheduleTarget {
	artifacts := make([]swf.Artifact, 0, len(t.Artifacts))
	for _, artifact := range t.Artifacts {
		artifacts = append(artifacts, swf.NewArtifactFromBytes(artifact.Name, append([]byte(nil), artifact.Data...)))
	}
	return swf.ScheduleTarget{
		JobType:   t.JobType,
		Data:      swf.JobData(&swf.SimpleTaskData{Data: append(json.RawMessage(nil), t.Data...), Artifacts: artifacts}),
		RunPolicy: t.RunPolicy,
		Metadata:  append(json.RawMessage(nil), t.Metadata...),
	}
}

func scheduleInfoFromRow(row scheduleRow) swf.ScheduleInfo {
	key := swf.ScheduleKey{TenantId: row.tenantID, ScheduleId: row.scheduleID}
	var nextFireAt *time.Time
	if row.nextFireAt.Valid {
		t := row.nextFireAt.Time.UTC()
		nextFireAt = &t
	}
	var nextJobKey *swf.JobKey
	if row.nextJobID.Valid && row.nextJobID.String != "" {
		nextJobKey = &swf.JobKey{TenantId: row.tenantID, JobId: row.nextJobID.String}
	}
	return swf.ScheduleInfo{
		TenantId:       row.tenantID,
		ScheduleId:     row.scheduleID,
		ScheduleKey:    key,
		State:          row.state,
		EffectiveState: row.state,
		Generation:     row.generation,
		SpecHash:       row.specHash,
		Trigger:        row.trigger,
		Target:         row.target.toScheduleTarget(),
		OverlapPolicy:  row.overlapPolicy,
		FailurePolicy:  row.failurePolicy,
		NextFireAt:     nextFireAt,
		NextJobKey:     nextJobKey,
		CreatedAt:      row.createdAt.UTC(),
		UpdatedAt:      row.updatedAt.UTC(),
	}
}

func nullTimeArg(v sql.NullTime) any {
	if !v.Valid {
		return nil
	}
	return v.Time.UTC()
}

func nullStringArg(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
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
