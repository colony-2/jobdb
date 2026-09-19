package runtimecore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/segmentio/ksuid"
)

const restartExtraTaskType = "__restart_extra__"

// SubmitRestartJob clones a visible chapter prefix and creates a fresh
// scheduler job. It rejects a prefix cut through a retry chain.
func (r *Runtime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	return r.submitRestartJobWithParent(ctx, req, "")
}

func (r *Runtime) submitRestartJobWithParent(ctx context.Context, req jobdb.SubmitRestartJobRequest, parentJobID string) (jobdb.JobHandle, error) {
	if err := r.validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	job := req.Job
	initial, revision, err := clientpayload.Initial(job.ClientPayloadUpdate)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	digest, err := clientpayload.Digest(initial)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if err := job.PriorJobKey.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if job.LastStepToKeep < 0 {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep must be nonnegative")
	}
	jobID := job.JobID
	if jobID == "" {
		jobID = ksuid.New().String()
	}
	key := jobdb.JobKey{TenantId: job.PriorJobKey.TenantId, JobId: jobID}
	if err := key.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if key == job.PriorJobKey {
		return jobdb.JobHandle{}, fmt.Errorf("restart job id must differ from source job id")
	}
	schemaHash, err := ResolveActiveSchemaForNewJob(ctx, r.schemas, key.TenantId, job.Schema)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	prereqs, waits, err := normalizeJobPrerequisites(key, job.Prerequisites)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	source := ChapterLogKey{JobKey: job.PriorJobKey}
	firstEncoded, err := r.chapters.Get(ctx, source, 0)
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("load source first chapter: %w", err)
	}
	first, err := DecodeChapter(firstEncoded)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	firstMeta, err := initialChapterMetadata(first.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	policy := jobdb.RunPolicy{}
	if firstMeta.RunPolicy != nil {
		policy = normalizeCoreRunPolicy(*firstMeta.RunPolicy)
	}
	jobType := first.TaskType
	if jobType == "" {
		return jobdb.JobHandle{}, fmt.Errorf("source first chapter has no job type")
	}
	nextOrdinal := job.LastStepToKeep + 1
	nextEncoded, err := r.chapters.Get(ctx, source, nextOrdinal)
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d has no following chapter: %w", job.LastStepToKeep, err)
	}
	nextChapter, err := DecodeChapter(nextEncoded)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	nextMeta, err := initialChapterMetadata(nextChapter.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if nextMeta.Attempt > 1 {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain", job.LastStepToKeep)
	}
	workerID := req.WorkerID
	if workerID == "" {
		workerID = "jobdb-core-" + ksuid.New().String()
	}
	createdAt := req.RequestTime
	if createdAt.IsZero() {
		createdAt = r.now()
	}
	var extra *EncodedChapter
	var outputArtifacts []jobdb.Artifact
	if job.ExtraTaskOutput != nil {
		hashInput := job.ExtraTaskInput
		if hashInput == nil {
			hashInput = jobdb.NewTaskDataOrPanic(map[string]any{})
		}
		_, _, _, _, inputHash, err := prepareInitialTaskData(ctx, hashInput)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		payload, artifacts, uploads, sourceArtifacts, _, err := prepareInitialTaskData(ctx, job.ExtraTaskOutput)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		outputArtifacts = sourceArtifacts
		meta := runtimecodec.ChapterMeta{
			Version: runtimecodec.EnvelopeVersion,
			Ordinal: nextOrdinal, TaskType: restartExtraTaskType,
			WorkerID: workerID, CreatedAt: createdAt.UTC(),
			InputHash: inputHash, Attempt: 1,
			InputRef:      &jobdb.InputReference{Ordinal: job.LastStepToKeep, Hash: inputHash},
			Prerequisites: prereqs,
		}
		raw, err := json.Marshal(meta)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		metadata, err := runtimecodec.ChapterMetadataFromJSON(raw)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		chapter := jobdb.Chapter{
			Ordinal: nextOrdinal, TaskType: restartExtraTaskType,
			Body:      jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: payload}},
			InputHash: inputHash, CreatedAt: createdAt.UTC(),
			Metadata: metadata, Artifacts: artifacts,
		}
		if err := ValidateOrdinaryChapter(ctx, r.schemas,
			jobdb.JobSchemaKey{TenantId: key.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
			return jobdb.JobHandle{}, err
		}
		encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: chapter, ArtifactUploads: uploads})
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		extra = &encoded
	}
	if schemaHash != "" {
		for ordinal := int64(0); ordinal <= job.LastStepToKeep; ordinal++ {
			encoded, err := r.chapters.Get(ctx, source, ordinal)
			if err != nil {
				return jobdb.JobHandle{}, err
			}
			chapter, err := DecodeChapter(encoded)
			if err != nil {
				return jobdb.JobHandle{}, err
			}
			validate := ValidateOrdinaryChapter
			if ordinal == 0 {
				validate = ValidateFirstChapter
			}
			if err := validate(ctx, r.schemas,
				jobdb.JobSchemaKey{TenantId: key.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
				return jobdb.JobHandle{}, err
			}
		}
	}
	destination := ChapterLogKey{JobKey: key}
	existing := false
	if err := r.chapters.ClonePrefix(ctx, ClonePrefixRequest{
		SourceKey: source, DestinationKey: destination,
		LastOrdinal: job.LastStepToKeep, Append: extra,
	}); err != nil {
		if job.JobID == "" || !errors.Is(err, jobdb.ErrConflict) {
			return jobdb.JobHandle{}, err
		}
		if err := r.validateExistingRestart(ctx, source, destination, job.LastStepToKeep, extra); err != nil {
			return jobdb.JobHandle{}, err
		}
		existing = true
	}
	for _, artifact := range outputArtifacts {
		jobdb.AssignArtifactKey(artifact, jobdb.ArtifactKey{
			JobId: key.JobId, TaskOrdinal: nextOrdinal,
			Name: artifact.Name(), SizeBytes: artifact.Size(),
		})
		_ = artifact.Cleanup()
	}
	metadata := json.RawMessage(`{}`)
	if existing {
		stored, err := r.scheduler.GetJob(ctx, key)
		if err == nil {
			if stored.InitialPayloadDigest != digest {
				return jobdb.JobHandle{}, jobdb.NewExistingJobMismatchError("initial client payload differs")
			}
			if err := validateStoredJobFacts(stored, key, jobType, schemaHash, parentJobID, metadata, policy, nil); err != nil {
				return jobdb.JobHandle{}, err
			}
			return jobdb.JobHandle{JobKey: key}, nil
		}
		if !errors.Is(err, jobdb.ErrJobNotFound) {
			return jobdb.JobHandle{}, err
		}
	}
	stored, err := r.scheduler.CreateJob(ctx, CreateJobRequest{
		ClientPayload: initial, ClientPayloadRevision: revision, InitialPayloadDigest: digest,
		JobKey: key, JobType: jobType, ParentJobID: parentJobID, RunPolicy: policy,
		AppMetadata: metadata, SchemaHash: schemaHash,
		WaitForJobIDs: waits, CreatedAt: createdAt.UTC(), WorkerID: workerID,
	})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if stored.InitialPayloadDigest != digest {
		return jobdb.JobHandle{}, jobdb.NewExistingJobMismatchError("initial client payload differs")
	}
	if err := validateStoredJobFacts(stored, key, jobType, schemaHash, parentJobID, metadata, policy, nil); err != nil {
		return jobdb.JobHandle{}, err
	}
	return jobdb.JobHandle{JobKey: key}, nil
}

func (r *Runtime) validateExistingRestart(ctx context.Context, source, target ChapterLogKey,
	lastOrdinal int64, extra *EncodedChapter) error {
	wantCount := lastOrdinal + 1
	if extra != nil {
		wantCount++
	}
	count, err := r.chapters.Count(ctx, target)
	if err != nil {
		return err
	}
	if count != wantCount {
		return jobdb.NewExistingJobMismatchError("restart chapter count differs")
	}
	for ordinal := int64(0); ordinal <= lastOrdinal; ordinal++ {
		want, err := r.chapters.Get(ctx, source, ordinal)
		if err != nil {
			return err
		}
		got, err := r.chapters.Get(ctx, target, ordinal)
		if err != nil {
			return err
		}
		if !bytes.Equal(got.Payload, want.Payload) || !slices.Equal(got.Artifacts, want.Artifacts) {
			return jobdb.NewExistingJobMismatchError(fmt.Sprintf("restart prefix chapter %d differs", ordinal))
		}
	}
	if extra != nil {
		got, err := r.chapters.Get(ctx, target, lastOrdinal+1)
		if err != nil {
			return err
		}
		gotChapter, err := DecodeChapter(got)
		if err != nil {
			return err
		}
		wantChapter, err := DecodeChapter(*extra)
		if err != nil {
			return err
		}
		gotMeta, err := initialChapterMetadata(gotChapter.Metadata)
		if err != nil {
			return err
		}
		wantMeta, err := initialChapterMetadata(wantChapter.Metadata)
		if err != nil {
			return err
		}
		if gotChapter.TaskType != wantChapter.TaskType ||
			gotChapter.InputHash != wantChapter.InputHash ||
			!reflect.DeepEqual(gotChapter.Body, wantChapter.Body) ||
			!slices.Equal(gotChapter.Artifacts, wantChapter.Artifacts) ||
			!reflect.DeepEqual(gotMeta.InputRef, wantMeta.InputRef) ||
			!reflect.DeepEqual(gotMeta.Prerequisites, wantMeta.Prerequisites) {
			return jobdb.NewExistingJobMismatchError("restart extra chapter differs")
		}
	}
	return nil
}
