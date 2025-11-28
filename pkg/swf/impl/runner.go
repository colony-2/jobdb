package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata/strata-go/pkg/client/core"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type runner struct {
	jobId        pgwf.JobID
	worker       *swf.WorkSet
	storyCounter int64
	engine       *swfEngineImpl
	lease        *pgwf.Lease
	logger       *slog.Logger
	jobPolicy    swf.RunPolicy
	capability   pgwf.Capability
	ctx          context.Context
}

func (r *runner) GetJobId() swf.JobId {
	return swf.JobId(r.jobId)
}

func notificationJobID(child swf.JobId) pgwf.JobID {
	return pgwf.JobID(fmt.Sprintf("%s-notify", child))
}

type asyncChildSpawn struct {
	ChildJobID        string `json:"child_job_id"`
	JobType           string `json:"job_type"`
	InputHash         string `json:"input_hash,omitempty"`
	NotificationJobID string `json:"notification_job_id,omitempty"`
}

func panicToAppError(rec interface{}) error {
	return swf.AppError{Payload: swf.AppErrorPayload{Message: fmt.Sprintf("panic: %v", rec), Level: "error"}}
}

func (r *runner) awaitUntil(wakeAt time.Time, ordinal int64, attempt int) error {
	if wakeAt.IsZero() || time.Now().After(wakeAt) {
		return nil
	}
	ctx := r.ctx

	ch := r.engine.AwaitUntil(r.jobId, r.capability, r.lease, ordinal, attempt, wakeAt)
	if ch == nil {
		prematureCloseOut()
		return nil
	}

	// Clear any stale signal before waiting.
	select {
	case <-ch:
	default:
	}

	select {
	case sig := <-ch:
		if sig.Kind == awaitSignalKindRecycle {
			prematureCloseOut()
		}
	case <-ctx.Done():
		prematureCloseOut()
		return ctx.Err()
	}
	return nil
}

func (r *runner) awaitChild(ctx context.Context, childJobID swf.JobId, ordinal int64, notificationJobID pgwf.JobID) (swf.TaskData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if td, done, err := r.engine.jobResultIfComplete(ctx, childJobID); err != nil {
			return nil, err
		} else if done {
			return td, nil
		}

		if err := r.engine.ensureNotificationJob(ctx, notificationJobID, pgwf.JobID(childJobID), r.jobId, ordinal); err != nil {
			return nil, err
		}

		ch := r.engine.AwaitChild(r.jobId, r.capability, r.lease, ordinal, pgwf.JobID(childJobID), notificationJobID)
		if ch == nil {
			prematureCloseOut()
			return nil, nil
		}

		select {
		case <-ch:
		default:
		}

		select {
		case sig := <-ch:
			if sig.Kind == awaitSignalKindRecycle {
				prematureCloseOut()
			}
		case <-ctx.Done():
			prematureCloseOut()
			return nil, ctx.Err()
		}
	}
}

func (r *runner) DoTask(policy swf.RunPolicy, taskType string, data swf.TaskData) (swf.TaskData, error) {
	ordinal := r.storyCounter
	r.storyCounter++
	ctx := context.TODO()

	inputHash, err := computeInputHash(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("compute input hash: %w", err)
	}
	inputRef := &swf.InputReference{Ordinal: ordinal - 1}
	if inputRef.Ordinal < 0 {
		inputRef.Ordinal = 0
	}
	inputRef.Hash = inputHash

	basePolicy := r.jobPolicy
	effectivePolicy := mergeRunPolicy(policy, basePolicy)
	retryCfg := normalizeRetryPolicy(effectivePolicy.Retry)
	maxAttempts := int(retryCfg.MaximumAttempts)
	attempt := 1

	key := story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}
	chap, err := r.engine.strata.Chapter(ctx, key, ordinal)
	if err == nil {
		env, decErr := decodeChapterEnvelope(chap.Body())
		if decErr != nil {
			return nil, fmt.Errorf("%w: decode cached chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
		}
		if env.Meta.InputHash == "" {
			return nil, fmt.Errorf("%w: ordinal %d task %s missing input hash", swf.ErrMissingInputHash, ordinal, taskType)
		}
		if env.Meta.InputHash != inputHash {
			return nil, fmt.Errorf("%w: ordinal %d task %s", swf.ErrWorkflowNotDeterministic, ordinal, taskType)
		}
		priorAttempt := env.Meta.Attempt
		if priorAttempt > 0 {
			attempt = priorAttempt + 1
		}

		td, payloadErr := envelopeToTaskData(env, chap.Artifacts())
		if payloadErr != nil {
			retryable := isRetryable(payloadErr, retryCfg)
			if !retryable || priorAttempt >= maxAttempts {
				return nil, payloadErr
			}
			backoff := time.Duration(0)
			if priorAttempt > 0 {
				backoff = computeBackoff(retryCfg, priorAttempt)
			}
			if backoff > 0 {
				wakeAt := env.Meta.CreatedAt.Add(backoff)
				if time.Now().Before(wakeAt) {
					_ = r.awaitUntil(wakeAt, ordinal, priorAttempt)
				}
			}
		} else {
			return td, nil
		}
	} else if !errors.Is(err, core.ErrNotFound) {
		return nil, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
	}

	worker, capabilityExistsLocally := r.worker.TaskWorkers[taskType]
	if !capabilityExistsLocally {
		inputOrdinal := ordinal - 1
		if inputOrdinal < 0 {
			inputOrdinal = 0
		}

		err = r.lease.Reschedule(context.TODO(), r.engine.udb, pgwf.JobDependencies{
			NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
			WaitFor:  nil,
		}, jobPayload{
			RunPolicy: r.jobPolicy,
			TaskWait: &taskWait{
				InputStep:  inputOrdinal,
				OutputStep: ordinal,
				Next:       r.worker.JobWorker.Name(),
			},
		})

		if err != nil {
			return nil, fmt.Errorf("failed to reschedule job: %w", err)
		}

		prematureCloseOut()
		return nil, nil
	}

	for {
		var output swf.TaskData
		var taskErr error
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					taskErr = panicToAppError(rec)
				}
			}()
			output, taskErr = worker.Run(
				swf.NewTaskContext(
					r.GetJobId(),
					ordinal,
					r.logger.With("task", taskType, "step", ordinal, "attempt", attempt),
					func(wakeAt time.Time) error {
						return r.awaitUntil(wakeAt, ordinal, attempt)
					},
					func(jobType string, td swf.TaskData) (*swf.Future, error) {
						return r.SpawnAsync(jobType, td)
					},
				),
				data,
			)
		}()

		payloadKind := payloadKindApp
		originalErr := taskErr
		var payload json.RawMessage
		artifacts := []swf.Artifact{}
		if taskErr != nil {
			var tdErr error
			payload, payloadKind, tdErr = errorPayloadFromError(taskErr, inputRef)
			if tdErr != nil {
				return nil, tdErr
			}
		} else {
			// success
			dataBytes, err := output.GetData()
			if err != nil {
				return nil, err
			}
			raw, err := dataBytes.ToBytes()
			if err != nil {
				return nil, err
			}
			payload = json.RawMessage(raw)
			artifacts, err = output.GetArtifacts()
			if err != nil {
				return nil, err
			}
		}

		retryable := isRetryable(originalErr, retryCfg)
		now := time.Now().UTC()
		backoff := time.Duration(0)
		if originalErr != nil && retryable && attempt < maxAttempts {
			backoff = computeBackoff(retryCfg, attempt)
		}
		meta := chapterMetadata{
			Attempt:  attempt,
			InputRef: inputRef,
		}

		chap, err := payloadToChapter(payload, artifacts, ordinal, taskType, r.engine.workerId, payloadKind, inputHash, now, meta)
		if err != nil {
			return nil, err
		}

		err = r.engine.strata.SaveChapter(context.TODO(), key, chap)
		if err != nil {
			return nil, err
		}

		if originalErr == nil {
			return output, nil
		}
		if retryable && attempt < maxAttempts {
			attempt++
			if backoff > 0 {
				_ = r.awaitUntil(now.Add(backoff), ordinal, attempt-1)
			}
			continue
		}
		return nil, originalErr
	}
}

func prematureCloseOut() {
	// do any finalization
	runtime.Goexit()
}

var _ swf.JobContext = &runner{}

type RunError struct {
	Err error
}

func (r *runner) getChapter(ordinal int64) (story.Chapter, error) {
	return r.engine.strata.Chapter(context.TODO(), story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}, ordinal)
}

func (r *runner) Logger() *slog.Logger {
	return r.logger
}

func (r *runner) AwaitDuration(waitFor swf.Duration) error {
	wait := waitFor.ToDuration()
	if wait <= 0 {
		return nil
	}
	return r.awaitUntil(time.Now().Add(wait), r.storyCounter, 0)
}

func (r *runner) SpawnAsync(jobType string, data swf.TaskData) (*swf.Future, error) {
	if jobType == "" {
		return nil, fmt.Errorf("job type is required")
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ordinal := r.storyCounter
	r.storyCounter++

	childJobID := swf.JobId(fmt.Sprintf("%s-%d", r.jobId, ordinal))
	notifyJobID := notificationJobID(childJobID)

	inputHash, err := computeInputHash(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("compute child input hash: %w", err)
	}

	key := story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}
	if cached, err := r.engine.strata.Chapter(ctx, key, ordinal); err == nil {
		if env, decErr := decodeChapterEnvelope(cached.Body()); decErr == nil && env.PayloadKind == payloadKindAppChildJob {
			var existing asyncChildSpawn
			if unmarshalErr := json.Unmarshal(env.Payload, &existing); unmarshalErr == nil {
				if existing.ChildJobID != "" {
					childJobID = swf.JobId(existing.ChildJobID)
				}
				if existing.NotificationJobID != "" {
					notifyJobID = pgwf.JobID(existing.NotificationJobID)
				}
				if existing.InputHash != "" && existing.InputHash != inputHash {
					return nil, fmt.Errorf("%w: async child input mismatch at ordinal %d", swf.ErrWorkflowNotDeterministic, ordinal)
				}
				if existing.JobType != "" && existing.JobType != jobType {
					return nil, fmt.Errorf("%w: async child job type mismatch at ordinal %d", swf.ErrWorkflowNotDeterministic, ordinal)
				}
			}
		}
	} else if !errors.Is(err, core.ErrNotFound) {
		return nil, err
	} else {
		// record the spawn metadata before submitting the child job
		spawn := asyncChildSpawn{
			ChildJobID:        string(childJobID),
			JobType:           jobType,
			InputHash:         inputHash,
			NotificationJobID: string(notifyJobID),
		}
		raw, err := json.Marshal(spawn)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		chap, err := payloadToChapter(json.RawMessage(raw), nil, ordinal, jobType, r.engine.workerId, payloadKindAppChildJob, inputHash, now, chapterMetadata{})
		if err != nil {
			return nil, err
		}
		if err := r.engine.strata.SaveChapter(context.TODO(), key, chap); err != nil {
			return nil, err
		}
	}

	// ensure the child story exists
	childKey := story.Key{AnthologyID: r.engine.tenantId, StoryID: string(childJobID)}
	if _, err := r.engine.strata.Chapter(ctx, childKey, 0); err != nil {
		if !errors.Is(err, core.ErrNotFound) {
			return nil, err
		}
		now := time.Now().UTC()
		co, err := taskDataToCreatOptions(data, 0, jobType, r.engine.workerId, payloadKindApp, inputHash, now, chapterMetadata{Attempt: 1})
		if err != nil {
			return nil, err
		}
		if _, err := r.engine.strata.CreateStory(ctx, childKey, co); err != nil {
			return nil, err
		}
	}

	runPolicy := r.jobPolicy
	runPolicy.Retry = normalizeRetryPolicy(runPolicy.Retry)
	if err := r.engine.ensureChildAndNotificationJobs(ctx, pgwf.JobID(childJobID), notifyJobID, jobType, runPolicy, swf.JobId(r.jobId), ordinal); err != nil {
		return nil, err
	}

	return swf.NewFuture(childJobID, func(waitCtx context.Context) (swf.TaskData, error) {
		return r.awaitChild(waitCtx, childJobID, ordinal, notifyJobID)
	}), nil
}

func (r *runner) Run(ctx context.Context, lease *pgwf.Lease) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.ctx = ctx
	if r.engine != nil {
		defer r.engine.resetAwaitState(r.jobId)
	}
	_ = lease.WithKeepAlive(r.engine.udb)

	key := story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}
	chap0, err := r.getChapter(0)
	if err != nil {
		r.logger.Error("failed to get initial chapter", "error", err)
		return
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		r.logger.Error("failed to decode initial chapter", "error", err)
		return
	}
	if env0.Meta.RunPolicy != nil {
		r.jobPolicy = mergeRunPolicy(*env0.Meta.RunPolicy, r.jobPolicy)
	}
	inputData, err := envelopeToTaskData(env0, chap0.Artifacts())
	if err != nil {
		r.logger.Error("failed to decode initial chapter payload", "error", err)
		return
	}

	retryCfg := normalizeRetryPolicy(r.jobPolicy.Retry)
	r.jobPolicy.Retry = retryCfg

	inputHash, err := computeInputHash(ctx, inputData)
	if err != nil {
		r.logger.Error("failed to hash job input", "error", err)
		return
	}
	inputRef := &swf.InputReference{Ordinal: 0, Hash: inputHash}

	for {
		var output swf.JobData
		var jobErr error
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					jobErr = panicToAppError(rec)
				}
			}()
			output, jobErr = r.worker.JobWorker.Run(r, inputData)
		}()

		ordinal := r.storyCounter
		r.storyCounter++

		attempt := 1
		maxAttempts := int(retryCfg.MaximumAttempts)
		if cached, err := r.engine.strata.Chapter(ctx, key, ordinal); err == nil {
			env, decErr := decodeChapterEnvelope(cached.Body())
			if decErr != nil {
				r.logger.Error("decode cached job result", "error", decErr)
				return
			}
			if env.Meta.InputHash != "" && env.Meta.InputHash != inputHash {
				r.logger.Error("job run not deterministic", "ordinal", ordinal)
				return
			}
			priorAttempt := env.Meta.Attempt
			if priorAttempt > 0 {
				attempt = priorAttempt + 1
			}
			_, payloadErr := envelopeToTaskData(env, cached.Artifacts())
			if payloadErr == nil {
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
			retryable := isRetryable(payloadErr, retryCfg)
			if !retryable || priorAttempt >= maxAttempts {
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
			backoff := time.Duration(0)
			if priorAttempt > 0 {
				backoff = computeBackoff(retryCfg, priorAttempt)
			}
			if backoff > 0 {
				wakeAt := env.Meta.CreatedAt.Add(backoff)
				if time.Now().Before(wakeAt) {
					_ = r.awaitUntil(wakeAt, ordinal, priorAttempt)
				}
			}
		} else if err != nil && !errors.Is(err, core.ErrNotFound) {
			r.logger.Error("failed to check cached job attempt", "error", err)
			return
		}

		if jobErr != nil {
			r.logger.Error("job worker run failed", "error", jobErr, "attempt", attempt)
		}

		payloadKind := payloadKindApp
		originalErr := jobErr
		var payload json.RawMessage
		artifacts := []swf.Artifact{}
		if originalErr != nil {
			var tdErr error
			payload, payloadKind, tdErr = errorPayloadFromError(originalErr, inputRef)
			if tdErr != nil {
				r.logger.Error("failed to marshal error payload", "error", tdErr)
				return
			}
		} else {
			if output == nil {
				raw, _ := json.Marshal(swf.SystemErrorPayload{Message: "missing job output", InputRef: inputRef})
				payload = raw
				payloadKind = payloadKindSystemError
			} else {
				dataBytes, err := output.GetData()
				if err != nil {
					r.logger.Error("failed to get job output data", "error", err)
					return
				}
				raw, err := dataBytes.ToBytes()
				if err != nil {
					r.logger.Error("failed to marshal job output", "error", err)
					return
				}
				payload = raw
				artifacts, err = output.GetArtifacts()
				if err != nil {
					r.logger.Error("failed to get job output artifacts", "error", err)
					return
				}
			}
		}

		retryable := isRetryable(originalErr, retryCfg)
		now := time.Now().UTC()
		backoff := time.Duration(0)
		if originalErr != nil && retryable && attempt < maxAttempts {
			backoff = computeBackoff(retryCfg, attempt)
		}
		meta := chapterMetadata{
			Attempt:  attempt,
			InputRef: inputRef,
		}

		chap, err := payloadToChapter(payload, artifacts, ordinal, r.worker.JobWorker.Name(), r.engine.workerId, payloadKind, inputHash, now, meta)
		if err != nil {
			r.logger.Error("failed to build chapter", "error", err)
			return
		}

		err = r.engine.strata.SaveChapter(context.TODO(), key, chap)
		if err != nil {
			r.logger.Error("failed to save chapter", "error", err)
			return
		}

		err = lease.Complete(ctx, r.engine.udb)
		if err != nil {
			r.logger.Error("failed to complete lease", "error", err)
		}

		if originalErr == nil {
			return
		}
		if retryable && attempt < maxAttempts {
			attempt++
			if backoff > 0 {
				_ = r.awaitUntil(now.Add(backoff), ordinal, attempt-1)
			}
			continue
		}
		return
	}
}
