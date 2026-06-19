package directimpl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type artifactFingerprint struct {
	Name   string
	Digest string
	Size   int64
}

type restartExtraExpectation struct {
	Present       bool
	InputHash     string
	InputRef      *swf.InputReference
	Payload       json.RawMessage
	Artifacts     []artifactFingerprint
	Prerequisites []swf.JobPrerequisite
}

func (r *Runtime) reconcileExistingSubmitJob(ctx context.Context, req swf.SubmitJobRequest, jobKey swf.JobKey, inputHash string, prereqs []swf.JobPrerequisite, waitFor []pgwf.JobID, jobPolicy swf.RunPolicy) (swf.JobHandle, bool, error) {
	start, exists, err := r.loadExistingStartChapter(ctx, jobKey)
	if err != nil {
		return swf.JobHandle{}, false, err
	}
	if !exists {
		pgwfExists, err := r.pgwfJobExists(ctx, jobKey)
		if err != nil {
			return swf.JobHandle{}, false, err
		}
		if pgwfExists {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists in pgwf without a matching strata story", jobKey))
		}
		return swf.JobHandle{}, false, nil
	}

	if err := compareSubmitStartChapter(jobKey, start, req.Job.JobType, inputHash, req.Job.Metadata, prereqs, jobPolicy); err != nil {
		return swf.JobHandle{}, true, err
	}
	storedMetadata, err := swf.BuildJobMetadataEnvelope(req.Job.Metadata, swf.RuntimeJobMetadata{})
	if err != nil {
		return swf.JobHandle{}, true, err
	}

	detail, err := r.loadPgwfJob(ctx, jobKey)
	switch {
	case errors.Is(err, pgwf.ErrJobNotFound):
		lastOrdinal, ordinalErr := r.storyLastOrdinal(ctx, jobKey)
		if ordinalErr != nil {
			return swf.JobHandle{}, true, ordinalErr
		}
		if lastOrdinal != 0 {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s has strata history through ordinal %d but no pgwf record to recover", jobKey, lastOrdinal))
		}
		if err := r.ensureSubmittedJobRecord(ctx, jobKey, req.Job.JobType, storedMetadata, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID, req.Job.AvailableAt); err != nil {
			return swf.JobHandle{}, true, err
		}
	case err != nil:
		return swf.JobHandle{}, true, err
	default:
		if !jsonObjectsEqual(detail.Metadata, storedMetadata) {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
		}
	}

	return swf.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) reconcileExistingRestartJob(ctx context.Context, req swf.SubmitRestartJobRequest, jobKey swf.JobKey, prereqs []swf.JobPrerequisite, waitFor []pgwf.JobID, jobType string, jobPolicy swf.RunPolicy, extra restartExtraExpectation) (swf.JobHandle, bool, error) {
	storyExists, err := r.storyExists(ctx, jobKey)
	if err != nil {
		return swf.JobHandle{}, false, err
	}
	if !storyExists {
		pgwfExists, err := r.pgwfJobExists(ctx, jobKey)
		if err != nil {
			return swf.JobHandle{}, false, err
		}
		if pgwfExists {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists in pgwf without a matching strata story", jobKey))
		}
		return swf.JobHandle{}, false, nil
	}

	if err := r.compareRestartStoryPrefix(ctx, req.Job, jobKey, extra); err != nil {
		return swf.JobHandle{}, true, err
	}

	detail, err := r.loadPgwfJob(ctx, jobKey)
	switch {
	case errors.Is(err, pgwf.ErrJobNotFound):
		expectedLast := req.Job.LastStepToKeep
		if extra.Present {
			expectedLast++
		}
		lastOrdinal, ordinalErr := r.storyLastOrdinal(ctx, jobKey)
		if ordinalErr != nil {
			return swf.JobHandle{}, true, ordinalErr
		}
		if lastOrdinal != expectedLast {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s has strata history through ordinal %d but no pgwf record to recover", jobKey, lastOrdinal))
		}
		if err := r.ensureSubmittedJobRecord(ctx, jobKey, jobType, nil, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID, nil); err != nil {
			return swf.JobHandle{}, true, err
		}
	case err != nil:
		return swf.JobHandle{}, true, err
	default:
		if !jsonObjectsEqual(detail.Metadata, nil) {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with unexpected metadata", jobKey))
		}
	}

	return swf.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) ensureSubmittedJobRecord(ctx context.Context, jobKey swf.JobKey, jobType string, metadata json.RawMessage, waitFor []pgwf.JobID, payload jobPayload, workerID string, availableAt *time.Time) error {
	err := r.startJob(ctx, jobKey, jobType, metadata, waitFor, payload, workerID, availableAt)
	if err == nil {
		return nil
	}
	detail, getErr := r.loadPgwfJob(ctx, jobKey)
	if errors.Is(getErr, pgwf.ErrJobNotFound) {
		return err
	}
	if getErr != nil {
		return getErr
	}
	if !jsonObjectsEqual(detail.Metadata, metadata) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	if availableAt != nil && !detail.AvailableAt.Equal(availableAt.UTC()) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different availableAt", jobKey))
	}
	return nil
}

func (r *Runtime) loadExistingStartChapter(ctx context.Context, jobKey swf.JobKey) (story.Chapter, bool, error) {
	key := storyKeyForJob(jobKey)
	chapter, err := r.loadChapter(ctx, key, 0)
	if err == nil {
		return chapter, true, nil
	}
	if !errors.Is(err, core.ErrNotFound) {
		return nil, false, err
	}
	if exists, storyErr := r.storyExists(ctx, jobKey); storyErr != nil {
		return nil, false, storyErr
	} else if exists {
		return nil, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without a start chapter", jobKey))
	}
	return nil, false, nil
}

func (r *Runtime) storyExists(ctx context.Context, jobKey swf.JobKey) (bool, error) {
	_, err := r.loadStory(ctx, storyKeyForJob(jobKey))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, core.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (r *Runtime) storyLastOrdinal(ctx context.Context, jobKey swf.JobKey) (int64, error) {
	st, err := r.loadStory(ctx, storyKeyForJob(jobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return -1, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s is missing its strata story", jobKey))
		}
		return -1, err
	}
	last, err := st.GetLastChapter(ctx)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return -1, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without visible chapters", jobKey))
		}
		return -1, err
	}
	return last.Ordinal(), nil
}

func (r *Runtime) loadPgwfJob(ctx context.Context, jobKey swf.JobKey) (*pgwf.JobDetail, error) {
	return pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId), pgwf.GetJobOptions{})
}

func (r *Runtime) pgwfJobExists(ctx context.Context, jobKey swf.JobKey) (bool, error) {
	_, err := r.loadPgwfJob(ctx, jobKey)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return false, nil
	}
	return false, err
}

func compareSubmitStartChapter(jobKey swf.JobKey, chapter story.Chapter, jobType string, inputHash string, metadata json.RawMessage, prereqs []swf.JobPrerequisite, jobPolicy swf.RunPolicy) error {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s start chapter could not be decoded: %v", jobKey, err))
	}
	if env.ChapterType != chapterTypeJobStart {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with chapter type %q at ordinal 0", jobKey, env.ChapterType))
	}
	if env.Meta.TaskType != jobType {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different job type", jobKey))
	}
	if env.Meta.InputHash != inputHash {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different input", jobKey))
	}
	// Legacy start chapters may not carry metadata. When present, compare it so
	// partial-submit recovery can validate the full submit shape before pgwf exists.
	if len(bytes.TrimSpace(env.Meta.Metadata)) > 0 && !jsonObjectsEqual(env.Meta.Metadata, metadata) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	existingPolicy := swf.RunPolicy{}
	if env.Meta.RunPolicy != nil {
		existingPolicy = normalizeRunPolicy(*env.Meta.RunPolicy)
	} else {
		existingPolicy = normalizeRunPolicy(existingPolicy)
	}
	if !reflect.DeepEqual(existingPolicy, jobPolicy) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different run policy", jobKey))
	}
	if !reflect.DeepEqual(normalizePrereqSlice(env.Meta.Prerequisites), normalizePrereqSlice(prereqs)) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different prerequisites", jobKey))
	}
	return nil
}

func (r *Runtime) compareRestartStoryPrefix(ctx context.Context, job swf.SubmitRestartJob, targetJobKey swf.JobKey, extra restartExtraExpectation) error {
	sourceKey := storyKeyForJob(job.PriorJobKey)
	targetKey := storyKeyForJob(targetJobKey)

	for ordinal := int64(0); ordinal <= job.LastStepToKeep; ordinal++ {
		sourceChapter, err := r.loadChapter(ctx, sourceKey, ordinal)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				return swf.NewExistingJobMismatchError(fmt.Sprintf("source job %s is missing chapter %d required for restart", job.PriorJobKey, ordinal))
			}
			return err
		}
		targetChapter, err := r.loadChapter(ctx, targetKey, ordinal)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s is missing copied restart chapter %d", targetJobKey, ordinal))
			}
			return err
		}
		same, compareErr := sameStoryChapter(ctx, sourceChapter, targetChapter)
		if compareErr != nil {
			return compareErr
		}
		if !same {
			return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart history at ordinal %d", targetJobKey, ordinal))
		}
	}

	nextOrdinal := job.LastStepToKeep + 1
	targetNext, err := r.loadChapter(ctx, targetKey, nextOrdinal)
	switch {
	case errors.Is(err, core.ErrNotFound):
		if extra.Present {
			return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s is missing the restart extra chapter at ordinal %d", targetJobKey, nextOrdinal))
		}
		return nil
	case err != nil:
		return err
	}

	env, err := decodeChapterEnvelope(targetNext.Body())
	if err != nil {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s chapter %d could not be decoded: %v", targetJobKey, nextOrdinal, err))
	}
	if !extra.Present {
		if env.ChapterType == chapterTypeRestartExtra {
			return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with restart extra output that was not requested", targetJobKey))
		}
		return nil
	}
	return compareRestartExtraChapter(ctx, targetJobKey, targetNext, extra)
}

func compareRestartExtraChapter(ctx context.Context, jobKey swf.JobKey, chapter story.Chapter, expected restartExtraExpectation) error {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s restart extra chapter could not be decoded: %v", jobKey, err))
	}
	if env.ChapterType != chapterTypeRestartExtra {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart chapter type", jobKey))
	}
	if env.PayloadKind != payloadKindApp {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart payload kind", jobKey))
	}
	if env.Meta.TaskType != restartExtraTaskType {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart task type", jobKey))
	}
	if env.Meta.InputHash != expected.InputHash {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra input", jobKey))
	}
	if !reflect.DeepEqual(env.Meta.InputRef, expected.InputRef) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra input reference", jobKey))
	}
	if !reflect.DeepEqual(normalizePrereqSlice(env.Meta.Prerequisites), normalizePrereqSlice(expected.Prerequisites)) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart prerequisites", jobKey))
	}
	if !bytes.Equal(env.Payload, expected.Payload) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra output", jobKey))
	}
	gotArtifacts, err := storyChapterArtifacts(ctx, chapter)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(gotArtifacts, expected.Artifacts) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra artifacts", jobKey))
	}
	return nil
}

func buildRestartExtraExpectation(ctx context.Context, job swf.SubmitRestartJob, prereqs []swf.JobPrerequisite) (restartExtraExpectation, error) {
	if job.ExtraTaskOutput == nil {
		return restartExtraExpectation{}, nil
	}
	hashInput := job.ExtraTaskInput
	if hashInput == nil {
		hashInput = swf.NewTaskDataOrPanic(map[string]any{})
	}
	inputHash, err := computeInputHash(ctx, hashInput)
	if err != nil {
		return restartExtraExpectation{}, err
	}
	payload, err := job.ExtraTaskOutput.GetData()
	if err != nil {
		return restartExtraExpectation{}, err
	}
	artifacts, err := taskDataArtifacts(ctx, job.ExtraTaskOutput)
	if err != nil {
		return restartExtraExpectation{}, err
	}
	return restartExtraExpectation{
		Present:       true,
		InputHash:     inputHash,
		InputRef:      &swf.InputReference{Ordinal: job.LastStepToKeep, Hash: inputHash},
		Payload:       append(json.RawMessage(nil), payload...),
		Artifacts:     artifacts,
		Prerequisites: normalizePrereqSlice(prereqs),
	}, nil
}

func sameStoryChapter(ctx context.Context, left story.Chapter, right story.Chapter) (bool, error) {
	if !bytes.Equal(left.Body(), right.Body()) {
		return false, nil
	}
	leftArtifacts, err := storyChapterArtifacts(ctx, left)
	if err != nil {
		return false, err
	}
	rightArtifacts, err := storyChapterArtifacts(ctx, right)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(leftArtifacts, rightArtifacts), nil
}

func storyChapterArtifacts(ctx context.Context, chapter story.Chapter) ([]artifactFingerprint, error) {
	if chapter == nil {
		return nil, nil
	}
	out := make([]artifactFingerprint, 0, len(chapter.Artifacts()))
	for _, art := range chapter.Artifacts() {
		if art == nil {
			continue
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, artifactFingerprint{
			Name:   art.Name(),
			Digest: digest,
			Size:   art.SizeBytes(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func taskDataArtifacts(ctx context.Context, data swf.TaskData) ([]artifactFingerprint, error) {
	if data == nil {
		return nil, nil
	}
	artifacts, err := data.GetArtifacts()
	if err != nil {
		return nil, err
	}
	out := make([]artifactFingerprint, 0, len(artifacts))
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, artifactFingerprint{
			Name:   art.Name(),
			Digest: digest,
			Size:   art.Size(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func normalizePrereqSlice(prereqs []swf.JobPrerequisite) []swf.JobPrerequisite {
	if len(prereqs) == 0 {
		return nil
	}
	return append([]swf.JobPrerequisite(nil), prereqs...)
}

func metadataForStartChapter(raw json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`)
	}
	return append(json.RawMessage(nil), raw...)
}

func jsonObjectsEqual(left json.RawMessage, right json.RawMessage) bool {
	leftNorm, leftErr := normalizeJSONObject(left)
	rightNorm, rightErr := normalizeJSONObject(right)
	if leftErr != nil || rightErr != nil {
		return bytes.Equal(bytes.TrimSpace(left), bytes.TrimSpace(right))
	}
	return bytes.Equal(leftNorm, rightNorm)
}

func normalizeJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value == nil {
		value = map[string]any{}
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(normalized), nil
}
