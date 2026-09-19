package runtimecore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/segmentio/ksuid"
)

// SubmitJob validates and writes the first chapter before creating scheduler
// state. Retrying an explicit job ID reconciles an identical first chapter.
func (r *Runtime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	return r.submitJobWithParent(ctx, req, "")
}

func (r *Runtime) submitJobWithParent(ctx context.Context, req jobdb.SubmitJobRequest, parentJobID string) (jobdb.JobHandle, error) {
	return r.submitJobWithSchedule(ctx, req, parentJobID, nil)
}

func (r *Runtime) submitJobWithSchedule(ctx context.Context, req jobdb.SubmitJobRequest, parentJobID string, occurrence *jobdb.ScheduleOccurrenceMetadata) (jobdb.JobHandle, error) {
	if err := jobdb.ValidateIdentifier(req.Job.JobType); err != nil {
		return jobdb.JobHandle{}, err
	}
	if err := r.validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	jobID := req.Job.JobID
	if jobID == "" {
		jobID = ksuid.New().String()
	}
	key := jobdb.JobKey{TenantId: req.Job.TenantId, JobId: jobID}
	if err := key.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if req.Job.JobType == "" {
		return jobdb.JobHandle{}, fmt.Errorf("job type is required")
	}
	if err := jobdb.ValidateApplicationMetadata(req.Job.Metadata); err != nil {
		return jobdb.JobHandle{}, err
	}
	initialPayload, initialRevision, err := clientpayload.Initial(req.Job.ClientPayloadUpdate)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	initialDigest, err := clientpayload.Digest(initialPayload)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	schemaHash, err := ResolveActiveSchemaForNewJob(ctx, r.schemas, key.TenantId, req.Job.Schema)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	prereqs, waits, err := normalizeJobPrerequisites(key, req.Job.Prerequisites)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	payload, artifacts, uploads, sourceArtifacts, inputHash, err := prepareInitialTaskData(ctx, req.Job.Data)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	policy := normalizeCoreRunPolicy(req.Job.RunPolicy)
	metadata := append(json.RawMessage(nil), req.Job.Metadata...)
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	workerID := req.WorkerID
	if workerID == "" {
		workerID = "jobdb-core-" + ksuid.New().String()
	}
	createdAt := req.RequestTime
	if createdAt.IsZero() {
		createdAt = r.now()
	}
	meta := runtimecodec.ChapterMeta{
		Version: runtimecodec.EnvelopeVersion, Ordinal: 0, InitialPayloadDigest: initialDigest,
		TaskType: req.Job.JobType, WorkerID: workerID,
		CreatedAt: createdAt.UTC(), InputHash: inputHash,
		Attempt: 1, RunPolicy: &policy, Metadata: metadata,
		Prerequisites: prereqs,
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	chapterMetadata, err := runtimecodec.ChapterMetadataFromJSON(metaJSON)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	initial := jobdb.Chapter{
		Ordinal: 0, TaskType: req.Job.JobType,
		Body:      jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: payload}},
		InputHash: inputHash, CreatedAt: createdAt.UTC(),
		Metadata: chapterMetadata, Artifacts: artifacts,
	}
	if err := ValidateFirstChapter(ctx, r.schemas,
		jobdb.JobSchemaKey{TenantId: key.TenantId, SchemaHash: schemaHash}, initial); err != nil {
		return jobdb.JobHandle{}, err
	}
	encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: initial, ArtifactUploads: uploads})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	logKey := ChapterLogKey{JobKey: key}
	existingChapter := false
	if err := r.chapters.Create(ctx, logKey, encoded); err != nil {
		if req.Job.JobID == "" || !errors.Is(err, jobdb.ErrConflict) {
			return jobdb.JobHandle{}, err
		}
		if err := r.validateExistingInitialChapter(ctx, key, initial); err != nil {
			return jobdb.JobHandle{}, err
		}
		existingChapter = true
	}
	for _, artifact := range sourceArtifacts {
		jobdb.AssignArtifactKey(artifact, jobdb.ArtifactKey{
			JobId: key.JobId, TaskOrdinal: 0, Name: artifact.Name(), SizeBytes: artifact.Size(),
		})
		_ = artifact.Cleanup()
	}
	if existingChapter {
		stored, err := r.scheduler.GetJob(ctx, key)
		if err == nil {
			if stored.InitialPayloadDigest != initialDigest {
				return jobdb.JobHandle{}, jobdb.NewExistingJobMismatchError("initial client payload differs")
			}
			if err := validateStoredJobFacts(stored, key, req.Job.JobType,
				schemaHash, parentJobID, metadata, policy, occurrence); err != nil {
				return jobdb.JobHandle{}, err
			}
			return jobdb.JobHandle{JobKey: key}, nil
		}
		if !errors.Is(err, jobdb.ErrJobNotFound) {
			return jobdb.JobHandle{}, err
		}
	}
	created, err := r.scheduler.CreateJob(ctx, CreateJobRequest{
		JobKey: key, JobType: req.Job.JobType, ParentJobID: parentJobID, RunPolicy: policy,
		ClientPayload: initialPayload, ClientPayloadRevision: initialRevision, InitialPayloadDigest: initialDigest,
		AppMetadata: metadata, SchemaHash: schemaHash, Schedule: occurrence,
		WaitForJobIDs: waits, AvailableAt: req.Job.AvailableAt,
		CreatedAt: createdAt.UTC(), WorkerID: workerID,
	})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if err := validateStoredJobFacts(created, key, req.Job.JobType,
		schemaHash, parentJobID, metadata, policy, occurrence); err != nil {
		return jobdb.JobHandle{}, err
	}
	return jobdb.JobHandle{JobKey: key}, nil
}

func validateStoredJobFacts(stored StoredJob, key jobdb.JobKey, jobType, schemaHash, parentJobID string,
	metadata json.RawMessage, policy jobdb.RunPolicy, occurrence *jobdb.ScheduleOccurrenceMetadata) error {
	if stored.JobKey != key || stored.JobType != jobType ||
		stored.SchemaHash != schemaHash || stored.ParentJobID != parentJobID ||
		!sameJSONObject(stored.AppMetadata, metadata) ||
		!reflect.DeepEqual(stored.RunPolicy, policy) {
		return jobdb.NewExistingJobMismatchError(
			fmt.Sprintf("job %s has different scheduler facts", key))
	}
	if !sameScheduleOccurrence(stored.Schedule, occurrence) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s has different schedule occurrence", key))
	}
	return nil
}

func sameScheduleOccurrence(left, right *jobdb.ScheduleOccurrenceMetadata) bool {
	if left == nil || right == nil {
		return left == right
	}
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && sameJSONObject(a, b)
}

func (r *Runtime) validateExistingInitialChapter(ctx context.Context, key jobdb.JobKey, want jobdb.Chapter) error {
	encoded, err := r.chapters.Get(ctx, ChapterLogKey{JobKey: key}, 0)
	if err != nil {
		return err
	}
	got, err := DecodeChapter(encoded)
	if err != nil {
		return err
	}
	if got.TaskType != want.TaskType {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different job type", key))
	}
	if got.InputHash != want.InputHash || !reflect.DeepEqual(got.Body, want.Body) ||
		!slices.Equal(got.Artifacts, want.Artifacts) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different input", key))
	}
	gotMeta, err := initialChapterMetadata(got.Metadata)
	if err != nil {
		return err
	}
	wantMeta, err := initialChapterMetadata(want.Metadata)
	if err != nil {
		return err
	}
	if gotMeta.InitialPayloadDigest != wantMeta.InitialPayloadDigest {
		return jobdb.NewExistingJobMismatchError("initial client payload differs")
	}
	if !sameJSONObject(gotMeta.Metadata, wantMeta.Metadata) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", key))
	}
	if !reflect.DeepEqual(gotMeta.RunPolicy, wantMeta.RunPolicy) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different run policy", key))
	}
	if !slices.Equal(gotMeta.Prerequisites, wantMeta.Prerequisites) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different prerequisites", key))
	}
	return nil
}

func initialChapterMetadata(metadata jobdb.ChapterMetadata) (runtimecodec.ChapterMeta, error) {
	raw, err := runtimecodec.ChapterMetadataToJSON(metadata)
	if err != nil {
		return runtimecodec.ChapterMeta{}, err
	}
	var meta runtimecodec.ChapterMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return runtimecodec.ChapterMeta{}, err
	}
	return meta, nil
}

func normalizeJobPrerequisites(key jobdb.JobKey, input []jobdb.JobPrerequisite) ([]jobdb.JobPrerequisite, []string, error) {
	seen := make(map[string]bool, len(input))
	out := make([]jobdb.JobPrerequisite, 0, len(input))
	waits := make([]string, 0, len(input))
	for _, prereq := range input {
		if strings.TrimSpace(prereq.JobID) == "" || prereq.JobID == key.JobId {
			return nil, nil, fmt.Errorf("prerequisite job id must be nonempty and distinct from job id")
		}
		if seen[prereq.JobID] {
			continue
		}
		seen[prereq.JobID] = true
		if prereq.Condition == "" {
			prereq.Condition = jobdb.JobPrereqComplete
		}
		if prereq.Condition != jobdb.JobPrereqComplete && prereq.Condition != jobdb.JobPrereqSuccess {
			return nil, nil, fmt.Errorf("invalid prerequisite condition %q", prereq.Condition)
		}
		out = append(out, prereq)
		waits = append(waits, prereq.JobID)
	}
	return out, waits, nil
}

func normalizeCoreRunPolicy(policy jobdb.RunPolicy) jobdb.RunPolicy {
	if policy.Retry.MaximumAttempts <= 0 {
		policy.Retry.MaximumAttempts = 1
	}
	if policy.Retry.BackoffCoefficient == 0 {
		policy.Retry.BackoffCoefficient = 1
	}
	if policy.InvocationTimeout != nil && *policy.InvocationTimeout < 0 {
		policy.InvocationTimeout = nil
	}
	if policy.TotalTimeout != nil && *policy.TotalTimeout < 0 {
		policy.TotalTimeout = nil
	}
	return policy
}

func prepareInitialTaskData(ctx context.Context, taskData jobdb.TaskData) (json.RawMessage, []jobdb.StoredArtifact, []jobdb.ArtifactUpload, []jobdb.Artifact, string, error) {
	if taskData == nil {
		return nil, nil, nil, nil, "", fmt.Errorf("job data is required")
	}
	payload, err := taskData.GetData()
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	if !json.Valid(payload) {
		return nil, nil, nil, nil, "", fmt.Errorf("job data must be valid JSON")
	}
	artifacts, err := taskData.GetArtifacts()
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	parts := make([]string, 0, len(artifacts))
	descriptors := make([]jobdb.StoredArtifact, 0, len(artifacts))
	uploads := make([]jobdb.ArtifactUpload, 0, len(artifacts))
	for _, artifact := range artifacts {
		if artifact == nil {
			return nil, nil, nil, nil, "", fmt.Errorf("job artifact is nil")
		}
		hash, err := artifact.Sha256(ctx)
		if err != nil {
			return nil, nil, nil, nil, "", err
		}
		parts = append(parts, artifact.Name()+"|"+hash)
		descriptors = append(descriptors, jobdb.StoredArtifact{
			Name: artifact.Name(), Digest: hash, Size: artifact.Size(),
		})
		uploads = append(uploads, jobdb.ArtifactUpload{
			Name: artifact.Name(), Size: artifact.Size(), Open: artifact.Open,
		})
	}
	sort.Strings(parts)
	hasher := sha256.New()
	_, _ = hasher.Write(payload)
	for _, part := range parts {
		_, _ = hasher.Write([]byte(part))
	}
	return append(json.RawMessage(nil), payload...), descriptors, uploads, artifacts,
		fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func sameJSONObject(left, right json.RawMessage) bool {
	var a, b map[string]any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return bytes.Equal(bytes.TrimSpace(left), bytes.TrimSpace(right))
	}
	return reflect.DeepEqual(a, b)
}
