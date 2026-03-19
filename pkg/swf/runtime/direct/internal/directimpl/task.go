package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type taskHandleImpl struct {
	jobID         string
	tenantId      string
	payload       json.RawMessage
	metadata      json.RawMessage
	inputOrdinal  int64
	outputOrdinal int64
	inputChapter  story.Chapter
	runtime       *Runtime
	nextNeed      pgwf.Capability
	taskType      string
	createdAt     time.Time
}

func (h *taskHandleImpl) TaskOrdinalToComplete() int64 {
	return h.outputOrdinal
}

func (h *taskHandleImpl) TaskType() string {
	return h.taskType
}

func (h *taskHandleImpl) CreatedAt() time.Time {
	return h.createdAt
}

func (h *taskHandleImpl) Metadata() json.RawMessage {
	if h.metadata == nil {
		return json.RawMessage(`{}`)
	}
	cpy := make(json.RawMessage, len(h.metadata))
	copy(cpy, h.metadata)
	return cpy
}

func (h *taskHandleImpl) chapter() (story.Chapter, error) {
	if h.inputChapter == nil {
		jobKey := h.JobKey()
		if h.runtime == nil {
			return nil, fmt.Errorf("task handle backend is nil")
		}
		chapter, err := h.runtime.strataClient.Chapter(context.TODO(), storyKeyForJob(jobKey), h.inputOrdinal)
		if err != nil {
			return nil, err
		}

		h.inputChapter = chapter
	}
	return h.inputChapter, nil
}

func (h *taskHandleImpl) Data() (swf.TaskData, error) {
	c, err := h.chapter()
	if err != nil {
		return nil, err
	}
	return chapterToTaskData(c, h.JobKey())
}

func (h *taskHandleImpl) JobKey() swf.JobKey {
	return swf.JobKey{
		TenantId: h.tenantId,
		JobId:    h.jobID,
	}
}

func (h *taskHandleImpl) Finish(ctx context.Context, taskData swf.TaskData) error {
	tw, err := extractTaskWaitFromRaw(h.payload)
	if err != nil {
		return err
	}
	if tw == nil {
		return nil
	}
	return h.runtime.CompleteTaskIfWaiting(ctx, swf.CompleteTaskIfWaitingRequest{
		JobKey:        h.JobKey(),
		Capability:    swf.JobTypeFromNextNeed(string(h.nextNeed)) + ":" + h.taskType,
		ResumeNeed:    string(h.nextNeed),
		InputOrdinal:  tw.InputStep,
		OutputOrdinal: tw.OutputStep,
		InputHash:     tw.InputHash,
		Data:          taskData,
	})
}

func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req swf.CompleteTaskIfWaitingRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	jobKey := req.JobKey
	job, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId), pgwf.GetJobOptions{IncludePayload: true})
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return swf.ErrJobNotFound
	}
	if err != nil {
		return fmt.Errorf("failed to load job: %w", err)
	}
	tw, err := extractTaskWaitFromRaw(job.Payload)
	if err != nil {
		return err
	}
	if tw == nil {
		return nil
	}
	if req.Capability != "" && job.NextNeed != req.Capability {
		return nil
	}
	if req.ResumeNeed != "" && tw.Next != req.ResumeNeed {
		return nil
	}
	if req.InputOrdinal != 0 && tw.InputStep != req.InputOrdinal {
		return nil
	}
	if tw.OutputStep != req.OutputOrdinal {
		return nil
	}
	if req.InputHash != "" && tw.InputHash != req.InputHash {
		return nil
	}

	var inputChapter story.Chapter
	if tw.InputStep > 0 {
		inputChapter, err = r.strataClient.Chapter(ctx, storyKeyForJob(jobKey), tw.InputStep)
		if err != nil {
			return fmt.Errorf("failed to load input chapter: %w", err)
		}
	}

	var payload jobPayload
	_ = json.Unmarshal(job.Payload, &payload)

	meta := chapterMetadata{}
	if inputChapter != nil {
		if env, decErr := decodeChapterEnvelope(inputChapter.Body()); decErr == nil {
			if env.Meta.Attempt > 0 {
				meta.Attempt = env.Meta.Attempt
			}
			if env.Meta.MaxAttempts > 0 {
				meta.MaxAttempts = env.Meta.MaxAttempts
			}
			if env.Meta.NextAttemptAt != nil {
				meta.NextAttemptAt = env.Meta.NextAttemptAt
			}
			if env.Meta.BackoffMillis > 0 {
				meta.BackoffMillis = env.Meta.BackoffMillis
			}
			if env.Meta.Retryable != nil {
				meta.Retryable = env.Meta.Retryable
			}
			if env.Meta.InputRef != nil {
				meta.InputRef = env.Meta.InputRef
			}
		}
	}
	if payload.RunPolicy.Retry.MaximumAttempts > 0 {
		meta.RunPolicy = &payload.RunPolicy
	}

	workerID := r.workerID
	taskType := taskTypeFromCapability(job.NextNeed)
	if req.Capability != "" {
		taskType = taskTypeFromCapability(req.Capability)
	}
	if taskType == "" || taskType == job.NextNeed || (req.Capability != "" && taskType == req.Capability) {
		return fmt.Errorf("task type not found in capability")
	}
	chap, err := taskDataToChapter(req.Data, tw.OutputStep, taskType, workerID, chapterTypeTaskAttemptOutcome, payloadKindApp, tw.InputHash, time.Now().UTC(), meta)
	if err != nil {
		return err
	}
	err = r.strataClient.SaveChapter(ctx, storyKeyForJob(jobKey), chap)
	if err != nil {
		if errors.Is(err, core.ErrConflict) {
			return nil
		}
		return err
	}
	artifacts, _ := req.Data.GetArtifacts()
	assignArtifactKeys(artifacts, jobKey.JobId, tw.OutputStep)
	tenantID := pgwf.TenantID(jobKey.TenantId)
	resumeNeed := tw.Next
	if req.ResumeNeed != "" {
		resumeNeed = req.ResumeNeed
	}

	return pgwf.RescheduleUnheldJob(
		ctx,
		r.pgwfDB(ctx),
		tenantID,
		pgwf.JobID(jobKey.JobId),
		pgwf.WorkerID(workerID), pgwf.JobDependencies{NextNeed: pgwf.Capability(resumeNeed)},
		jobPayload{RunPolicy: payload.RunPolicy})
}

var _ swf.TaskHandle = &taskHandleImpl{}

func chapterToTaskData(chapter story.Chapter, jobKey swf.JobKey) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(chapter.Artifacts()))
	for _, a := range chapter.Artifacts() {
		artifacts = append(artifacts, fromStrataArtifact(a))
	}
	assignArtifactKeys(artifacts, jobKey.JobId, chapter.Ordinal())

	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return nil, err
	}

	return envelopeToTaskData(env, artifacts)
}
