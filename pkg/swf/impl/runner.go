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
}

func (r *runner) GetJobId() swf.JobId {
	return swf.JobId(r.jobId)
}

func panicToAppError(rec interface{}) error {
	return swf.AppError{Payload: swf.AppErrorPayload{Message: fmt.Sprintf("panic: %v", rec), Level: "error"}}
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
		// Use stored policy if present to ensure deterministic replays.
		if env.Meta.RunPolicy != nil {
			effectivePolicy = mergeRunPolicy(policy, *env.Meta.RunPolicy)
			retryCfg = normalizeRetryPolicy(effectivePolicy.Retry)
			maxAttempts = int(retryCfg.MaximumAttempts)
		}
		if env.Meta.MaxAttempts > 0 {
			maxAttempts = env.Meta.MaxAttempts
		}
		if env.Meta.Attempt > 0 {
			attempt = env.Meta.Attempt + 1
		}

		td, payloadErr := envelopeToTaskData(env, chap.Artifacts())
		if payloadErr != nil {
			retryable := isRetryable(payloadErr, retryCfg)
			if env.Meta.Retryable != nil {
				retryable = *env.Meta.Retryable
			}
			if !retryable || env.Meta.Attempt >= maxAttempts {
				return nil, payloadErr
			}
			if env.Meta.NextAttemptAt != nil {
				waitFor := time.Until(*env.Meta.NextAttemptAt)
				if waitFor > 0 {
					_ = r.AwaitDuration(swf.Duration(waitFor))
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
		}, taskWait{
			InputStep:  inputOrdinal,
			OutputStep: ordinal,
			Next:       r.worker.JobWorker.Name(),
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
			output, taskErr = worker.Run(swf.TaskContext{
				JobId:  r.GetJobId(),
				Step:   ordinal,
				Logger: r.logger.With("task", taskType, "step", ordinal, "attempt", attempt),
			}, data)
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
		var nextAttemptAt *time.Time
		if originalErr != nil && retryable && attempt < maxAttempts {
			backoff = computeBackoff(retryCfg, attempt)
			na := now.Add(backoff)
			nextAttemptAt = &na
		}
		meta := chapterMetadata{
			Attempt:       attempt,
			MaxAttempts:   maxAttempts,
			BackoffMillis: backoff.Milliseconds(),
			Retryable:     &retryable,
			InputRef:      inputRef,
			RunPolicy:     &effectivePolicy,
		}
		if nextAttemptAt != nil {
			meta.NextAttemptAt = nextAttemptAt
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
				_ = r.AwaitDuration(swf.Duration(backoff))
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
	time.Sleep(waitFor.ToDuration())
	return nil
}

func (r *runner) Run(ctx context.Context, lease *pgwf.Lease) {
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
			if env.Meta.RunPolicy != nil {
				r.jobPolicy = mergeRunPolicy(*env.Meta.RunPolicy, r.jobPolicy)
				retryCfg = normalizeRetryPolicy(r.jobPolicy.Retry)
				maxAttempts = int(retryCfg.MaximumAttempts)
			}
			if env.Meta.MaxAttempts > 0 {
				maxAttempts = env.Meta.MaxAttempts
			}
			if env.Meta.Attempt > 0 {
				attempt = env.Meta.Attempt + 1
			}
			_, payloadErr := envelopeToTaskData(env, cached.Artifacts())
			if payloadErr == nil {
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
			retryable := isRetryable(payloadErr, retryCfg)
			if env.Meta.Retryable != nil {
				retryable = *env.Meta.Retryable
			}
			if !retryable || env.Meta.Attempt >= maxAttempts {
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
			if env.Meta.NextAttemptAt != nil {
				waitFor := time.Until(*env.Meta.NextAttemptAt)
				if waitFor > 0 {
					_ = r.AwaitDuration(swf.Duration(waitFor))
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
		var nextAttemptAt *time.Time
		if originalErr != nil && retryable && attempt < maxAttempts {
			backoff = computeBackoff(retryCfg, attempt)
			na := now.Add(backoff)
			nextAttemptAt = &na
		}
		meta := chapterMetadata{
			Attempt:       attempt,
			MaxAttempts:   maxAttempts,
			BackoffMillis: backoff.Milliseconds(),
			Retryable:     &retryable,
			InputRef:      inputRef,
			RunPolicy:     &r.jobPolicy,
		}
		if nextAttemptAt != nil {
			meta.NextAttemptAt = nextAttemptAt
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
				_ = r.AwaitDuration(swf.Duration(backoff))
			}
			continue
		}
		return
	}
}
