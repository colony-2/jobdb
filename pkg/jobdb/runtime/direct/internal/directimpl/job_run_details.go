package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/artifact"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/pagination"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

const defaultJobRunChaptersPageSize = 200

type jobRunAccessor interface {
	pgwfDB(ctx context.Context) pgwf.DB
	loadStory(ctx context.Context, key story.Key) (story.Story, error)
	loadChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
}

func (r *Runtime) GetJobRun(ctx context.Context, req jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error) {
	return getJobRun(ctx, r, req)
}

func getJobRun(ctx context.Context, accessor jobRunAccessor, req jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.JobKey.Validate(); err != nil {
		return jobdb.GetJobRunResponse{}, err
	}

	includeInputs, includeOutputs, includeArtifacts, includeAttemptInputs := normalizeGetJobRunOptions(req)

	statusInfo, err := pgwf.GetJobStatus(ctx, accessor.pgwfDB(ctx), pgwf.TenantID(req.JobKey.TenantId), pgwf.JobID(req.JobKey.JobId))
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return jobdb.GetJobRunResponse{}, jobdb.ErrJobNotFound
	}
	if err != nil {
		return jobdb.GetJobRunResponse{}, fmt.Errorf("failed to get job status: %w", err)
	}
	runPolicy := jobdb.RunPolicy{}
	if len(statusInfo.Payload) > 0 {
		if payload, err := decodeJobPayload(statusInfo.Payload); err == nil {
			runPolicy = payload.RunPolicy
		}
	}
	retryPolicy := normalizeRunPolicy(runPolicy).Retry

	jobStatus := convertPgwfStatusToJobDB(statusInfo.Status, statusInfo.CancelRequested, statusInfo.ArchivedAt)
	resp := jobdb.GetJobRunResponse{
		Job: jobdb.JobRunSummary{
			JobKey:     req.JobKey,
			Status:     jobStatus,
			CreatedAt:  statusInfo.CreatedAt,
			ArchivedAt: statusInfo.ArchivedAt,
			Metadata:   jobdb.AppMetadataFromStoredMetadata(statusInfo.Metadata),
		},
	}

	st, err := accessor.loadStory(ctx, storyKeyForJob(req.JobKey))
	if err != nil {
		return jobdb.GetJobRunResponse{}, fmt.Errorf("failed to load story: %w", err)
	}

	iter, err := st.Chapters(ctx, story.ChaptersOptions{
		PageSize:  defaultJobRunChaptersPageSize,
		Direction: story.DirectionForward,
	})
	if err != nil {
		return jobdb.GetJobRunResponse{}, fmt.Errorf("failed to list chapters: %w", err)
	}

	var (
		jobType              string
		startSet             bool
		startInputRef        *jobdb.InputReference
		attempts             []jobdb.JobAttempt
		attemptIndex               = map[int]int{}
		activeAttempt              = 1
		currentAttempt             = 0
		currentRunIdx              = -1
		lastOrdinal          int64 = -1
		lastCompletedAttempt int
		lastCompletedOutcome jobdb.TaskOutcome
	)

	ensureAttempt := func(num int) int {
		if num <= 0 {
			num = 1
		}
		if idx, ok := attemptIndex[num]; ok {
			return idx
		}
		attempt := jobdb.JobAttempt{
			Attempt:  num,
			InputRef: startInputRef,
		}
		if num == 1 && startSet {
			attempt.CreatedAt = resp.Start.CreatedAt
			attempt.WorkerID = resp.Start.WorkerID
		}
		attempts = append(attempts, attempt)
		idx := len(attempts) - 1
		attemptIndex[num] = idx
		return idx
	}

	for iter.HasNext() {
		chap, err := iter.Next(ctx)
		if errors.Is(err, pagination.ErrNoMoreItems) {
			break
		}
		if err != nil {
			return jobdb.GetJobRunResponse{}, fmt.Errorf("failed to iterate chapters: %w", err)
		}

		lastOrdinal = chap.Ordinal()
		env, decErr := decodeChapterEnvelope(chap.Body())
		if decErr != nil {
			return jobdb.GetJobRunResponse{}, fmt.Errorf("%w: decode chapter: %v", jobdb.ErrWorkflowNotDeterministic, decErr)
		}

		if chap.Ordinal() == 0 && !startSet {
			if env.ChapterType != chapterTypeJobStart {
				return jobdb.GetJobRunResponse{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal 0", jobdb.ErrWorkflowNotDeterministic, env.ChapterType)
			}
			jobType = env.Meta.TaskType
			resp.Job.JobType = jobType
			startInputRef = &jobdb.InputReference{Ordinal: 0, Hash: env.Meta.InputHash}
			startInput, err := buildTaskIOFromPayload(ctx, env.Payload, chap.Artifacts(), req.JobKey.JobId, chap.Ordinal(), includeInputs, includeArtifacts)
			if err != nil {
				return jobdb.GetJobRunResponse{}, err
			}
			if !includeInputs {
				startInput = nil
			}
			resp.Start = jobdb.JobStart{
				Ordinal:   chap.Ordinal(),
				WorkerID:  env.Meta.WorkerID,
				CreatedAt: env.Meta.CreatedAt,
				Input:     startInput,
			}
			startSet = true
			_ = ensureAttempt(1)
			continue
		}

		attempt, err := buildTaskAttempt(ctx, accessor, storyKeyForJob(req.JobKey), chap, env, includeInputs, includeOutputs || env.ChapterType == chapterTypeJobAttemptOutcome, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return jobdb.GetJobRunResponse{}, err
		}

		if env.ChapterType == chapterTypeJobAttemptOutcome {
			attemptNum := attempt.Attempt
			if attemptNum <= 0 {
				attemptNum = 1
			}
			idx := ensureAttempt(attemptNum)
			attempts[idx].Ordinal = attempt.Ordinal
			attempts[idx].Attempt = attemptNum
			attempts[idx].WorkerID = attempt.WorkerID
			attempts[idx].CreatedAt = attempt.CreatedAt
			attempts[idx].InputRef = attempt.InputRef
			attempts[idx].Output = attempt.Output
			attempts[idx].Outcome = attempt.Outcome
			activeAttempt = attemptNum + 1
			lastCompletedAttempt = attemptNum
			lastCompletedOutcome = attempt.Outcome
			currentAttempt = 0
			currentRunIdx = -1
			continue
		}

		if env.ChapterType != chapterTypeTaskAttemptOutcome && env.ChapterType != chapterTypeRestartExtra {
			return jobdb.GetJobRunResponse{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal %d", jobdb.ErrWorkflowNotDeterministic, env.ChapterType, chap.Ordinal())
		}
		idx := ensureAttempt(activeAttempt)
		if currentAttempt != activeAttempt {
			currentAttempt = activeAttempt
			currentRunIdx = -1
		}
		if currentRunIdx == -1 || attempt.Attempt <= 1 || attempts[idx].Tasks[currentRunIdx].TaskType != env.Meta.TaskType {
			attempts[idx].Tasks = append(attempts[idx].Tasks, jobdb.TaskRun{
				TaskRunID: fmt.Sprintf("%s:%d", env.Meta.TaskType, chap.Ordinal()),
				TaskType:  env.Meta.TaskType,
				Attempts:  []jobdb.TaskAttempt{},
			})
			currentRunIdx = len(attempts[idx].Tasks) - 1
		}
		attempts[idx].Tasks[currentRunIdx].Attempts = append(attempts[idx].Tasks[currentRunIdx].Attempts, attempt)
	}

	if resp.Job.JobType == "" {
		resp.Job.JobType = jobdb.JobTypeFromNextNeed(statusInfo.NextNeed)
	}

	currentAttemptNum := 0
	if statusInfo.ArchivedAt == nil && resp.Job.Status != jobdb.JobStatusCancelled {
		if lastCompletedAttempt == 0 {
			currentAttemptNum = 1
		} else if shouldSynthesizeNextAttempt(lastCompletedAttempt, lastCompletedOutcome, retryPolicy) {
			currentAttemptNum = lastCompletedAttempt + 1
		}
	}
	if currentAttemptNum > 0 {
		_ = ensureAttempt(currentAttemptNum)
	}

	if statusInfo.ArchivedAt == nil {
		runtimeRun, ok, err := buildRuntimeTaskRun(ctx, accessor, storyKeyForJob(req.JobKey), statusInfo, lastOrdinal, includeInputs, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return jobdb.GetJobRunResponse{}, err
		}
		if ok && currentAttemptNum > 0 {
			idx := ensureAttempt(currentAttemptNum)
			attempts[idx].Tasks = append(attempts[idx].Tasks, runtimeRun)
		}
	}

	if !includeOutputs && len(attempts) > 0 {
		latest := latestJobAttempt(attempts)
		for i := range attempts {
			if attempts[i].Attempt != latest.Attempt || attempts[i].Ordinal != latest.Ordinal {
				attempts[i].Output = nil
			}
		}
	}

	resp.Attempts = attempts
	return resp, nil
}

func normalizeGetJobRunOptions(req jobdb.GetJobRunRequest) (bool, bool, bool, bool) {
	if !req.IncludeInputs && !req.IncludeOutputs && !req.IncludeArtifacts && !req.IncludeAttemptInputs {
		return true, true, true, false
	}
	return req.IncludeInputs, req.IncludeOutputs, req.IncludeArtifacts, req.IncludeAttemptInputs
}

func latestJobAttempt(attempts []jobdb.JobAttempt) jobdb.JobAttempt {
	best := attempts[0]
	for i := 1; i < len(attempts); i++ {
		attempt := attempts[i]
		if attempt.Attempt > best.Attempt || (attempt.Attempt == best.Attempt && attempt.Ordinal > best.Ordinal) {
			best = attempt
		}
	}
	return best
}

func shouldSynthesizeNextAttempt(lastAttempt int, outcome jobdb.TaskOutcome, policy jobdb.RetryPolicy) bool {
	if lastAttempt <= 0 {
		return false
	}
	if outcome.Status != jobdb.TaskOutcomeStatusFailed {
		return false
	}
	if policy.MaximumAttempts <= 0 {
		policy = normalizeRetryPolicy(policy)
	}
	if lastAttempt >= int(policy.MaximumAttempts) {
		return false
	}
	if outcome.Error == nil {
		return true
	}
	switch outcome.Error.Kind {
	case jobdb.TaskErrorKindTimeout:
		if outcome.Error.Retryable == nil {
			return false
		}
		return *outcome.Error.Retryable
	case jobdb.TaskErrorKindSystem:
		if outcome.Error.Retryable == nil {
			return true
		}
		return *outcome.Error.Retryable
	case jobdb.TaskErrorKindApp:
		return true
	default:
		return true
	}
}

func buildTaskAttempt(ctx context.Context, accessor jobRunAccessor, key story.Key, chap story.Chapter, env chapterEnvelope, includeInputs, includeOutputs, includeArtifacts, includeAttemptInputs bool) (jobdb.TaskAttempt, error) {
	attemptNum := env.Meta.Attempt
	if attemptNum <= 0 {
		attemptNum = 1
	}

	output, err := buildTaskIOFromPayload(ctx, env.Payload, chap.Artifacts(), key.StoryID, chap.Ordinal(), includeOutputs, includeArtifacts)
	if err != nil {
		return jobdb.TaskAttempt{}, err
	}
	if !includeOutputs {
		output = nil
	}

	var input *jobdb.TaskIO
	if includeInputs {
		if includeAttemptInputs && env.Meta.InputRef != nil {
			resolved, err := resolveInputRef(ctx, accessor, key, env.Meta.InputRef, includeArtifacts)
			if err != nil {
				return jobdb.TaskAttempt{}, err
			}
			input = resolved
		} else if env.Meta.Input != nil {
			input = &jobdb.TaskIO{Data: append([]byte(nil), env.Meta.Input...)}
		}
	}

	outcome, err := outcomeFromEnvelope(env)
	if err != nil {
		return jobdb.TaskAttempt{}, err
	}

	state := outcome.Status
	if outcome.Status == "" {
		state = jobdb.TaskAttemptStateSucceeded
	}

	return jobdb.TaskAttempt{
		Ordinal:       chap.Ordinal(),
		Attempt:       attemptNum,
		WorkerID:      env.Meta.WorkerID,
		CreatedAt:     env.Meta.CreatedAt,
		InputHash:     env.Meta.InputHash,
		InputRef:      env.Meta.InputRef,
		RunPolicy:     env.Meta.RunPolicy,
		Retryable:     env.Meta.Retryable,
		MaxAttempts:   intPtr(env.Meta.MaxAttempts),
		NextAttemptAt: env.Meta.NextAttemptAt,
		BackoffMillis: int64Ptr(env.Meta.BackoffMillis),
		Input:         input,
		Output:        output,
		State:         state,
		Outcome:       outcome,
	}, nil
}

func buildRuntimeTaskRun(ctx context.Context, accessor jobRunAccessor, key story.Key, status *pgwf.JobStatusInfo, lastOrdinal int64, includeInputs, includeArtifacts, includeAttemptInputs bool) (jobdb.TaskRun, bool, error) {
	if status == nil || status.NextNeed == "" {
		return jobdb.TaskRun{}, false, nil
	}

	state, runtime := runtimeStateFromStatus(status)
	if state == "" {
		return jobdb.TaskRun{}, false, nil
	}

	runtimeOrdinal := lastOrdinal + 1
	if runtimeOrdinal < 0 {
		runtimeOrdinal = 0
	}

	var input *jobdb.TaskIO
	var inputRef *jobdb.InputReference
	if lastOrdinal >= 0 {
		inputRef = &jobdb.InputReference{Ordinal: lastOrdinal}
	}
	if includeInputs {
		if includeAttemptInputs && inputRef != nil {
			resolved, err := resolveInputRef(ctx, accessor, key, inputRef, includeArtifacts)
			if err != nil {
				return jobdb.TaskRun{}, false, err
			}
			input = resolved
		}
	}

	attempt := jobdb.TaskAttempt{
		Ordinal:  runtimeOrdinal,
		Attempt:  1,
		InputRef: inputRef,
		Input:    input,
		State:    state,
		Runtime:  runtime,
	}

	run := jobdb.TaskRun{
		TaskRunID: fmt.Sprintf("%s:%d", status.NextNeed, runtimeOrdinal),
		TaskType:  status.NextNeed,
		Attempts:  []jobdb.TaskAttempt{attempt},
	}
	return run, true, nil
}

func runtimeStateFromStatus(status *pgwf.JobStatusInfo) (string, *jobdb.TaskRuntime) {
	if status == nil {
		return "", nil
	}
	runtime := &jobdb.TaskRuntime{
		NextNeed:       strPtr(status.NextNeed),
		AvailableAt:    timePtr(status.AvailableAt),
		WaitFor:        status.WaitFor,
		LeaseOwner:     status.LeaseID,
		LeaseExpiresAt: status.LeaseExpiresAt,
	}

	now := time.Now().UTC()
	switch status.Status {
	case pgwf.JobStatusReady:
		if !status.AvailableAt.After(now) {
			return jobdb.TaskAttemptStateReady, runtime
		}
		return jobdb.TaskAttemptStateWaiting, runtime
	case pgwf.JobStatusActive:
		return jobdb.TaskAttemptStateLeased, runtime
	case pgwf.JobStatusAwaitingFuture, pgwf.JobStatusPendingJobs:
		return jobdb.TaskAttemptStateWaiting, runtime
	default:
		return jobdb.TaskAttemptStateWaiting, runtime
	}
}

func resolveInputRef(ctx context.Context, accessor jobRunAccessor, key story.Key, ref *jobdb.InputReference, includeArtifacts bool) (*jobdb.TaskIO, error) {
	if ref == nil {
		return nil, nil
	}
	chap, err := accessor.loadChapter(ctx, key, ref.Ordinal)
	if err != nil {
		return nil, err
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		return nil, fmt.Errorf("%w: decode input chapter: %v", jobdb.ErrWorkflowNotDeterministic, err)
	}
	return buildTaskIOFromPayload(ctx, env.Payload, chap.Artifacts(), key.StoryID, ref.Ordinal, true, includeArtifacts)
}

func buildTaskIOFromPayload(ctx context.Context, payload json.RawMessage, artifacts []artifact.Artifact, jobID string, ordinal int64, includeData bool, includeArtifacts bool) (*jobdb.TaskIO, error) {
	if !includeData && !includeArtifacts {
		return nil, nil
	}
	out := &jobdb.TaskIO{}
	if includeData && payload != nil {
		out.Data = append([]byte(nil), payload...)
	}
	if includeArtifacts {
		infos, err := buildArtifactInfos(ctx, artifacts, jobID, ordinal)
		if err != nil {
			return nil, err
		}
		out.Artifacts = infos
	}
	if out.Data == nil && len(out.Artifacts) == 0 {
		return nil, nil
	}
	return out, nil
}

func buildArtifactInfos(ctx context.Context, artifacts []artifact.Artifact, jobID string, ordinal int64) ([]jobdb.ArtifactInfo, error) {
	if len(artifacts) == 0 {
		return nil, nil
	}
	out := make([]jobdb.ArtifactInfo, 0, len(artifacts))
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		sha, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		var key *jobdb.ArtifactKey
		if jobID != "" && ordinal >= 0 && art.Name() != "" {
			k := jobdb.ArtifactKey{
				JobId:       jobID,
				TaskOrdinal: ordinal,
				Name:        art.Name(),
				SizeBytes:   art.SizeBytes(),
			}
			key = &k
		}
		out = append(out, jobdb.ArtifactInfo{
			ID:          art.ID(),
			Name:        art.Name(),
			ContentType: art.ContentType(),
			SizeBytes:   art.SizeBytes(),
			Sha256:      sha,
			Key:         key,
		})
	}
	return out, nil
}

func outcomeFromEnvelope(env chapterEnvelope) (jobdb.TaskOutcome, error) {
	outcome := jobdb.TaskOutcome{
		PayloadKind: env.PayloadKind,
	}

	switch env.PayloadKind {
	case payloadKindApp:
		outcome.Status = jobdb.TaskOutcomeStatusSucceeded
		return outcome, nil
	case payloadKindAppError:
		var p jobdb.AppErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return jobdb.TaskOutcome{}, err
		}
		outcome.Status = jobdb.TaskOutcomeStatusFailed
		outcome.Error = &jobdb.TaskError{
			Kind:       jobdb.TaskErrorKindApp,
			Message:    p.Message,
			Level:      p.Level,
			Attrs:      p.Attrs,
			InputRef:   p.InputRef,
			Stacktrace: p.Stacktrace,
		}
		return outcome, nil
	case payloadKindSystemError:
		var p jobdb.SystemErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return jobdb.TaskOutcome{}, err
		}
		outcome.Status = jobdb.TaskOutcomeStatusFailed
		outcome.Error = &jobdb.TaskError{
			Kind:       jobdb.TaskErrorKindSystem,
			Message:    p.Message,
			Component:  p.Component,
			Code:       p.Code,
			Retryable:  boolPtr(p.Retryable),
			InputRef:   p.InputRef,
			Stacktrace: p.Stacktrace,
		}
		return outcome, nil
	case payloadKindTimeout:
		var p jobdb.TimeoutPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return jobdb.TaskOutcome{}, err
		}
		outcome.Status = jobdb.TaskOutcomeStatusFailed
		outcome.Error = &jobdb.TaskError{
			Kind:      jobdb.TaskErrorKindTimeout,
			Message:   p.Message,
			Component: p.Component,
			Code:      p.Code,
			Retryable: boolPtr(p.Retryable),
			Scope:     p.Scope,
			After:     &p.After,
			InputRef:  p.InputRef,
		}
		return outcome, nil
	default:
		outcome.Status = jobdb.TaskOutcomeStatusFailed
		outcome.Error = &jobdb.TaskError{
			Kind:    jobdb.TaskErrorKindSystem,
			Message: fmt.Sprintf("unsupported payload kind %q", env.PayloadKind),
			Code:    "unknown_payload_kind",
		}
		return outcome, nil
	}
}

func boolPtr(v bool) *bool {
	val := v
	return &val
}

func intPtr(v int) *int {
	if v <= 0 {
		return nil
	}
	val := v
	return &val
}

func int64Ptr(v int64) *int64 {
	if v <= 0 {
		return nil
	}
	val := v
	return &val
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	cp := t
	return &cp
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
