package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/google/uuid"
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
	nextFireAtNS  sql.NullInt64
	nextJobID     sql.NullString
	createdAtNS   int64
	updatedAtNS   int64
}

type storedScheduleTarget struct {
	JobType   string          `json:"jobType"`
	Data      json.RawMessage `json:"data,omitempty"`
	RunPolicy swf.RunPolicy   `json:"runPolicy,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

const scheduleColumns = `
tenant_id, schedule_id, state, generation, spec_hash, trigger_json, target_json,
target_job_type, overlap_policy, failure_policy_json, next_fire_at_ns,
next_job_id, created_at_ns, updated_at_ns
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
	specHash, err := swf.ScheduleSpecHash(req.Trigger, req.Target, req.OverlapPolicy, req.FailurePolicy)
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
	target, err := storedTargetFromSchedule(req.Target)
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	var row scheduleRow
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		existing, found, err := r.loadScheduleRowTx(ctx, tx, swf.ScheduleKey{TenantId: req.TenantId, ScheduleId: req.ScheduleId})
		if err != nil {
			return err
		}
		if found && existing.state == swf.ScheduleStateArchived {
			return fmt.Errorf("%w: archived schedule cannot be updated", swf.ErrConflict)
		}
		if req.ExpectedGeneration != nil {
			if !found || existing.generation != *req.ExpectedGeneration {
				return fmt.Errorf("%w: schedule generation mismatch", swf.ErrConflict)
			}
		}
		generation := int64(1)
		createdAt := now
		if found {
			generation = existing.generation + 1
			createdAt = timeFromNS(existing.createdAtNS)
		}
		nextJobID := sql.NullString{}
		nextNS := sql.NullInt64{}
		if state == swf.ScheduleStateActive && nextFireAt != nil {
			nextJobID = sql.NullString{String: swf.ScheduleRunJobID(req.ScheduleId, generation, *nextFireAt), Valid: true}
			nextNS = sql.NullInt64{Int64: timeToNS(*nextFireAt), Valid: true}
		}
		triggerJSON, err := json.Marshal(req.Trigger)
		if err != nil {
			return err
		}
		targetJSON, err := json.Marshal(target)
		if err != nil {
			return err
		}
		failureJSON, err := json.Marshal(req.FailurePolicy)
		if err != nil {
			return err
		}
		if found {
			_, err = tx.ExecContext(ctx, `
UPDATE swf_schedules
SET state = ?, generation = ?, spec_hash = ?, trigger_json = ?, target_json = ?,
	target_job_type = ?, overlap_policy = ?, failure_policy_json = ?,
	next_fire_at_ns = ?, next_job_id = ?, updated_at_ns = ?
WHERE tenant_id = ? AND schedule_id = ?`,
				state, generation, specHash, triggerJSON, targetJSON,
				target.JobType, swf.NormalizeScheduleOverlapPolicy(req.OverlapPolicy), failureJSON,
				nullIntArg(nextNS), nullStringArg(nextJobID), timeToNS(now), req.TenantId, req.ScheduleId)
		} else {
			_, err = tx.ExecContext(ctx, `
INSERT INTO swf_schedules (
	tenant_id, schedule_id, state, generation, spec_hash, trigger_json, target_json,
	target_job_type, overlap_policy, failure_policy_json, next_fire_at_ns,
	next_job_id, created_at_ns, updated_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				req.TenantId, req.ScheduleId, state, generation, specHash, triggerJSON, targetJSON,
				target.JobType, swf.NormalizeScheduleOverlapPolicy(req.OverlapPolicy), failureJSON,
				nullIntArg(nextNS), nullStringArg(nextJobID), timeToNS(createdAt), timeToNS(now))
		}
		if err != nil {
			return err
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
			nextFireAtNS:  nextNS,
			nextJobID:     nextJobID,
			createdAtNS:   timeToNS(createdAt),
			updatedAtNS:   timeToNS(now),
		}
		return nil
	})
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	if row.state == swf.ScheduleStateActive && nextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, row, *nextFireAt, "", "", false, req.WorkerID); err != nil {
			return swf.ScheduleInfo{}, err
		}
	}
	return scheduleInfoFromRow(row), nil
}

func (r *Runtime) GetSchedule(ctx context.Context, key swf.ScheduleKey) (swf.ScheduleInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.ScheduleInfo{}, err
	}
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
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.ListSchedulesResponse{}, err
	}
	if req.TenantId == "" {
		return swf.ListSchedulesResponse{}, fmt.Errorf("tenantId is required")
	}
	rows, err := r.db.QueryContext(ctx, `SELECT `+scheduleColumns+` FROM swf_schedules WHERE tenant_id = ?`, req.TenantId)
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
	if ctx == nil {
		ctx = context.Background()
	}
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
	rows, err := r.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM swf_jobs WHERE tenant_id = ? ORDER BY created_at_ns DESC, job_id DESC`, req.ScheduleKey.TenantId)
	if err != nil {
		return swf.ListScheduleRunsResponse{}, err
	}
	all := make([]jobRow, 0)
	for rows.Next() {
		row, err := scanJobRow(rows)
		if err != nil {
			_ = rows.Close()
			return swf.ListScheduleRunsResponse{}, err
		}
		all = append(all, row)
	}
	if err := rows.Close(); err != nil {
		return swf.ListScheduleRunsResponse{}, err
	}
	if err := rows.Err(); err != nil {
		return swf.ListScheduleRunsResponse{}, err
	}
	statusAllowed := statusSet(req.Statuses)
	now := time.Now().UTC()
	out := make([]swf.ScheduleRunSummary, 0, pageSize+1)
	for _, row := range all {
		occ, hasSchedule, err := swf.ExtractScheduleOccurrenceMetadata(row.metadata)
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
		status, err := statusFromRow(ctx, r.db, row, now)
		if err != nil {
			return swf.ListScheduleRunsResponse{}, err
		}
		if len(statusAllowed) > 0 && !statusAllowed[status] {
			continue
		}
		createdAt := timeFromNS(row.createdAtNS)
		key := swf.JobKey{TenantId: row.tenantID, JobId: row.jobID}
		if hasCursor {
			if createdAt.After(cursorTime) {
				continue
			}
			if createdAt.Equal(cursorTime) && key.String() >= cursorJob {
				continue
			}
		}
		waitFor, err := decodeWaitFor(row.waitForRaw)
		if err != nil {
			return swf.ListScheduleRunsResponse{}, err
		}
		nextNeed, _ := effectiveNextNeed(row, now)
		job := swf.JobSummary{
			JobKey:          key,
			Status:          status,
			JobType:         row.jobType,
			NextNeed:        cloneString(nextNeed),
			WaitFor:         waitFor,
			AvailableAt:     timeFromNS(row.availableAtNS),
			LeaseExpiresAt:  nullTimeFromNS(row.leaseExpiresAtNS),
			CancelRequested: row.cancelRequested,
			CreatedAt:       createdAt,
			ArchivedAt:      nullTimeFromNS(row.archivedAtNS),
			Payload:         jobPayloadVisibleJSON(row.payload),
			Metadata:        swf.StripRuntimeMetadata(row.metadata),
		}
		if tw, waitErr := extractTaskWaitFromRaw(row.payload); waitErr == nil && tw != nil {
			job.TaskWaitInput = &tw.InputStep
			job.TaskWaitOutput = &tw.OutputStep
			job.TaskWaitInputHash = cloneString(tw.InputHash)
			job.TaskWaitNext = cloneString(tw.Next)
		}
		out = append(out, swf.ScheduleRunSummary{
			JobSummary:  job,
			ScheduleId:  req.ScheduleKey.ScheduleId,
			ScheduledAt: scheduledAt,
			ReasonCode:  scheduleReasonFromNullableString(row.completionDetail),
		})
		if len(out) > pageSize {
			break
		}
	}
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
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.ScheduleInfo{}, err
	}
	if err := req.ScheduleKey.Validate(); err != nil {
		return swf.ScheduleInfo{}, err
	}
	now := req.RequestTime.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var row scheduleRow
	err := r.withTx(ctx, func(tx *sql.Tx) error {
		existing, found, err := r.loadScheduleRowTx(ctx, tx, req.ScheduleKey)
		if err != nil {
			return err
		}
		if !found {
			return swf.ErrJobNotFound
		}
		if req.ExpectedGeneration != nil && existing.generation != *req.ExpectedGeneration {
			return fmt.Errorf("%w: schedule generation mismatch", swf.ErrConflict)
		}
		if existing.state == swf.ScheduleStateArchived && state != swf.ScheduleStateArchived {
			return fmt.Errorf("%w: archived schedule cannot change state", swf.ErrConflict)
		}
		next, err := swf.InitialScheduleFire(existing.trigger, now)
		if err != nil {
			return err
		}
		existing.state = state
		existing.generation++
		existing.updatedAtNS = timeToNS(now)
		existing.nextFireAtNS = sql.NullInt64{}
		existing.nextJobID = sql.NullString{}
		if state == swf.ScheduleStateActive && next != nil {
			existing.nextFireAtNS = sql.NullInt64{Int64: timeToNS(*next), Valid: true}
			existing.nextJobID = sql.NullString{String: swf.ScheduleRunJobID(existing.scheduleID, existing.generation, *next), Valid: true}
		}
		_, err = tx.ExecContext(ctx, `
UPDATE swf_schedules
SET state = ?, generation = ?, next_fire_at_ns = ?, next_job_id = ?, updated_at_ns = ?
WHERE tenant_id = ? AND schedule_id = ?`,
			existing.state, existing.generation, nullIntArg(existing.nextFireAtNS), nullStringArg(existing.nextJobID),
			existing.updatedAtNS, existing.tenantID, existing.scheduleID)
		if err != nil {
			return err
		}
		row = existing
		return nil
	})
	if err != nil {
		return swf.ScheduleInfo{}, err
	}
	if submitFirst && row.state == swf.ScheduleStateActive && row.nextFireAtNS.Valid {
		next := timeFromNS(row.nextFireAtNS.Int64)
		if _, err := r.submitScheduledOccurrence(ctx, row, next, "", "", false, req.WorkerID); err != nil {
			return swf.ScheduleInfo{}, err
		}
	}
	return scheduleInfoFromRow(row), nil
}

func (r *Runtime) submitScheduledOccurrence(ctx context.Context, row scheduleRow, scheduledAt time.Time, previousJobID string, failureBits string, manual bool, workerID string) (swf.JobKey, error) {
	jobID := swf.ScheduleRunJobID(row.scheduleID, row.generation, scheduledAt)
	return r.submitScheduledOccurrenceWithJobID(ctx, row, jobID, scheduledAt, previousJobID, failureBits, manual, workerID)
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
	waitFor, err := encodeWaitFor(prereqJobIDs(prereqs))
	if err != nil {
		return swf.JobKey{}, err
	}
	waitForIDs, err := decodeWaitFor(waitFor)
	if err != nil {
		return swf.JobKey{}, err
	}
	taskData := swf.TaskData(target.Data)
	inputHash, err := computeInputHash(ctx, taskData)
	if err != nil {
		return swf.JobKey{}, err
	}
	jobPolicy := normalizeRunPolicy(target.RunPolicy)
	initialChapter, err := taskDataToChapter(taskData, 0, target.JobType, r.requestWorkerID(workerID), chapterTypeJobStart, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
		Attempt:       1,
		RunPolicy:     &jobPolicy,
		Metadata:      metadataForStartChapter(target.Metadata),
		Prerequisites: prereqs,
	})
	if err != nil {
		return swf.JobKey{}, err
	}
	if _, err := r.strataClient.CreateStory(strataContext(ctx), storyKeyForJob(jobKey), story.CreateOptions{RequestID: uuid.New().String(), InitialChapter: initialChapter}); err != nil {
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
		cleanupArtifacts(artifacts, r.logger)
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, target.JobType, schedulerMetadata, waitForIDs, jobPayload{RunPolicy: jobPolicy}, workerID, &scheduledAt); err != nil {
		return swf.JobKey{}, err
	}
	return jobKey, nil
}

func (r *Runtime) loadScheduleRow(ctx context.Context, key swf.ScheduleKey) (scheduleRow, bool, error) {
	row, err := scanScheduleRow(r.db.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM swf_schedules WHERE tenant_id = ? AND schedule_id = ?`, key.TenantId, key.ScheduleId))
	if err == sql.ErrNoRows {
		return scheduleRow{}, false, nil
	}
	return row, err == nil, err
}

func (r *Runtime) loadScheduleRowTx(ctx context.Context, tx *sql.Tx, key swf.ScheduleKey) (scheduleRow, bool, error) {
	row, err := scanScheduleRow(tx.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM swf_schedules WHERE tenant_id = ? AND schedule_id = ?`, key.TenantId, key.ScheduleId))
	if err == sql.ErrNoRows {
		return scheduleRow{}, false, nil
	}
	return row, err == nil, err
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
		&row.nextFireAtNS,
		&row.nextJobID,
		&row.createdAtNS,
		&row.updatedAtNS,
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

func storedTargetFromSchedule(target swf.ScheduleTarget) (storedScheduleTarget, error) {
	var data json.RawMessage
	if target.Data != nil {
		raw, err := target.Data.GetData()
		if err != nil {
			return storedScheduleTarget{}, err
		}
		data = append(json.RawMessage(nil), raw...)
	}
	return storedScheduleTarget{
		JobType:   target.JobType,
		Data:      data,
		RunPolicy: target.RunPolicy,
		Metadata:  swf.NormalizeJSON(target.Metadata),
	}, nil
}

func (t storedScheduleTarget) toScheduleTarget() swf.ScheduleTarget {
	return swf.ScheduleTarget{
		JobType:   t.JobType,
		Data:      swf.JobData(&swf.SimpleTaskData{Data: append(json.RawMessage(nil), t.Data...)}),
		RunPolicy: t.RunPolicy,
		Metadata:  append(json.RawMessage(nil), t.Metadata...),
	}
}

func scheduleInfoFromRow(row scheduleRow) swf.ScheduleInfo {
	key := swf.ScheduleKey{TenantId: row.tenantID, ScheduleId: row.scheduleID}
	var nextFireAt *time.Time
	if row.nextFireAtNS.Valid {
		nextFireAt = nullTimeFromNS(row.nextFireAtNS)
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
		CreatedAt:      timeFromNS(row.createdAtNS),
		UpdatedAt:      timeFromNS(row.updatedAtNS),
	}
}

func nullIntArg(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
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

func prereqJobIDs(prereqs []swf.JobPrerequisite) []string {
	out := make([]string, 0, len(prereqs))
	for _, prereq := range prereqs {
		if prereq.JobID != "" {
			out = append(out, prereq.JobID)
		}
	}
	return out
}

func scheduleReasonFromNullableString(detail sql.NullString) string {
	if !detail.Valid {
		return ""
	}
	return scheduleReasonFromCompletionDetail(detail.String)
}
