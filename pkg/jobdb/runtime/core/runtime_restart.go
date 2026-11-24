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
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/jobid"
	"github.com/segmentio/ksuid"
)

const restartExtraTaskType = "__restart_extra__"

// SubmitRestartJob clones a visible chapter prefix and creates a fresh
// scheduler job. It rejects a prefix cut through a retry chain.
func (r *Runtime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	return r.submitRestartJobWithParent(ctx, req, "")
}

func (r *Runtime) submitRestartJobWithParent(ctx context.Context, req jobdb.SubmitRestartJobRequest, parentJobID string) (handle jobdb.JobHandle, retErr error) {
	if req.Job.ShareGoal {
		caps := r.JobGoalCapabilities()
		if !caps.ImmutableMembership || !caps.AtomicAdmission {
			return jobdb.JobHandle{}, jobdb.ErrJobGoalSemanticsUnsupported
		}
	}
	if err := r.validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ValidateRestartApplicationPayload(ctx, req.Job, r.ChapterApplicationPayloadLimit()); err != nil {
		return jobdb.JobHandle{}, err
	}
	parentJobID, parentErr := r.submissionParent(ctx, req.Job.PriorJobKey.TenantId, parentJobID)
	if parentErr != nil {
		return jobdb.JobHandle{}, parentErr
	}
	if projectErr := jobdb.RequireProjectWrite(ctx, r.projects, req.Job.PriorJobKey.TenantId); projectErr != nil {
		var projectZero0 jobdb.JobHandle
		return projectZero0, projectErr
	}

	preparation, err := goalstate.RestartPreparation(ctx, req, parentJobID)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if _, err := r.submissionParent(ctx, preparation.Key.TenantId, parentJobID); err != nil {
		return jobdb.JobHandle{}, err
	}
	reserved, err := r.scheduler.PrepareJob(ctx, preparation)
	err = jobdb.ClassifyRuntimeIO(err, "prepare_restart", &preparation.Key, nil)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	defer func() {
		// Keep stable retry identity if authority is lost after staging. The
		// reservation may predate this call and cannot be safely discarded here.
		if retErr != nil && !errors.Is(retErr, jobdb.ErrExecutionLeaseLost) {
			if cleanupErr := r.scheduler.DiscardJobPreparation(context.WithoutCancel(ctx), reserved.Key); cleanupErr != nil {
				retErr = errors.Join(retErr, jobdb.ClassifyRuntimeIO(cleanupErr, "discard_job_preparation", &reserved.Key, nil))
			}
		}
	}()
	req.Job.JobID = reserved.Key.JobId
	if reserved.Published {
		if _, err := r.submissionParent(ctx, reserved.Key.TenantId, parentJobID); err != nil {
			return jobdb.JobHandle{}, err
		}
		return reserved.Target.Handle(reserved.Key, reserved.GoalID), nil
	}
	prior, err := r.scheduler.GetJob(ctx, req.Job.PriorJobKey)
	err = jobdb.ClassifyRuntimeIO(err, "get_restart_source", &req.Job.PriorJobKey, nil)
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("load source job metadata: %w", err)
	}
	if err := prior.RecordKind.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if prior.RecordKind.IsTerminalRecord() {
		if _, err := jobListSummaryFromStored(prior); err != nil {
			return jobdb.JobHandle{}, err
		}
		return jobdb.JobHandle{}, jobdb.ErrJobRecordOperationUnsupported
	}
	job, executionRestart, err := ResolveExecutionRestart(ctx, r, req.Job)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
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
		jobID = jobid.New()
	}
	key := jobdb.JobKey{TenantId: job.PriorJobKey.TenantId, JobId: jobID}
	if err := key.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if key == job.PriorJobKey {
		return jobdb.JobHandle{}, fmt.Errorf("restart job id must differ from source job id")
	}
	metadata := cloneRaw(prior.AppMetadata)
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
	zero := int64(0)
	err = jobdb.ClassifyRuntimeIO(err, "get_restart_chapter", &job.PriorJobKey, &zero)
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("load source first chapter: %w", err)
	}
	first, err := DecodeChapter(firstEncoded)
	err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDurableDecode, "decode_restart_chapter", &job.PriorJobKey, &zero)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	firstMeta, err := initialChapterMetadata(first.Metadata)
	err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDurableDecode, "decode_restart_metadata", &job.PriorJobKey, &zero)
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
	err = jobdb.ClassifyRuntimeIO(err, "get_restart_chapter", &job.PriorJobKey, &nextOrdinal)
	if err != nil {
		if executionRestart != nil || !errors.Is(err, jobdb.ErrChapterNotFound) {
			return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d has no following chapter: %w", job.LastStepToKeep, err)
		}
		// Only a stored submission may stand alone. A hole in a longer log
		// must not turn a damaged history into a fresh execution.
		count, err := r.chapters.Count(ctx, source)
		err = jobdb.ClassifyRuntimeIO(err, "count_restart_chapters", &job.PriorJobKey, nil)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		prefixOnly := count == job.LastStepToKeep+1
		if prefixOnly && count > 1 {
			last, e := r.chapters.Get(ctx, source, count-1)
			ordinal := count - 1
			e = jobdb.ClassifyRuntimeIO(e, "get_restart_chapter", &job.PriorJobKey, &ordinal)
			if e != nil {
				return jobdb.JobHandle{}, e
			}
			typed, e := DecodeChapter(last)
			e = jobdb.ClassifyExecutionInfrastructure(e, jobdb.ExecutionInfrastructureDurableDecode, "decode_restart_chapter", &job.PriorJobKey, &last.Ordinal)
			if e != nil {
				return jobdb.JobHandle{}, e
			}
			prefixOnly = runtimecodec.ChapterIs(typed, runtimecodec.ChapterTypeAdditionalInput)
		}
		if !prefixOnly || !runtimecodec.ChapterIs(first, runtimecodec.ChapterTypeJobStart) {
			return jobdb.JobHandle{}, fmt.Errorf("source is not a submission-only history")
		}
	} else {
		nextChapter, err := DecodeChapter(nextEncoded)
		err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDurableDecode, "decode_restart_chapter", &job.PriorJobKey, &nextOrdinal)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		nextMeta, err := initialChapterMetadata(nextChapter.Metadata)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		if executionRestart == nil && nextMeta.Attempt > 1 {
			return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain", job.LastStepToKeep)
		}
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
	if executionRestart != nil {
		raw, err := json.Marshal(executionRestart)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		chapter := jobdb.Chapter{Ordinal: nextOrdinal, TaskType: "__execution__", CreatedAt: createdAt.UTC(), Body: jobdb.ExecutionChapter{Record: raw}}
		if err := ValidateOrdinaryChapter(ctx, r.chapterRegistry(),
			jobdb.JobSchemaKey{TenantId: key.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
			return jobdb.JobHandle{}, err
		}
		encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: chapter})
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		extra = &encoded
	}
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
		if err := ValidateOrdinaryChapter(ctx, r.chapterRegistry(),
			jobdb.JobSchemaKey{TenantId: key.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
			return jobdb.JobHandle{}, err
		}
		encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: chapter, ArtifactUploads: uploads})
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		extra = &encoded
	}
	// Retained history is linked unchanged. The new schema and payload limit
	// apply only to newly appended chapters, not the source prefix.
	destination := ChapterLogKey{JobKey: key}
	existing := false
	if err := r.chapters.ClonePrefix(ctx, ClonePrefixRequest{
		SourceKey: source, DestinationKey: destination,
		LastOrdinal: job.LastStepToKeep, Append: extra,
	}); err != nil {
		if job.JobID == "" || !errors.Is(err, jobdb.ErrConflict) {
			return jobdb.JobHandle{}, jobdb.ClassifyRuntimeIO(err, "clone_restart", &key, nil)
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
	if existing {
		stored, err := r.scheduler.GetJob(ctx, key)
		err = jobdb.ClassifyRuntimeIO(err, "get_restart_destination", &key, nil)
		if err == nil {
			if stored.InitialPayloadDigest != digest {
				return jobdb.JobHandle{}, jobdb.NewExistingJobMismatchError("initial client payload differs")
			}
			if err := validateStoredJobFacts(stored, key, jobType, schemaHash, parentJobID, metadata, policy); err != nil {
				return jobdb.JobHandle{}, err
			}
			if _, err := r.submissionParent(ctx, key.TenantId, parentJobID); err != nil {
				return jobdb.JobHandle{}, err
			}
			return stored.Cell.Handle(key, stored.GoalID), nil
		}
		if !errors.Is(err, jobdb.ErrJobNotFound) {
			return jobdb.JobHandle{}, err
		}
	}
	if _, err := r.submissionParent(ctx, key.TenantId, parentJobID); err != nil {
		return jobdb.JobHandle{}, err
	}
	stored, err := r.scheduler.CreateJob(ctx, CreateJobRequest{Cell: reserved.Target, GoalID: reserved.GoalID, ShareGoal: preparation.ShareGoal, Dependencies: preparation.Dependencies,
		ClientPayload: initial, ClientPayloadRevision: revision, InitialPayloadDigest: digest,
		JobKey: key, JobType: jobType, ParentJobID: parentJobID, RunPolicy: policy,
		AppMetadata: metadata, SchemaHash: schemaHash,
		WaitForJobIDs: waits, CreatedAt: createdAt.UTC(), WorkerID: workerID,
	})
	err = jobdb.ClassifyRuntimeIO(err, "publish_restart", &key, nil)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if stored.InitialPayloadDigest != digest {
		return jobdb.JobHandle{}, jobdb.NewExistingJobMismatchError("initial client payload differs")
	}
	if err := validateStoredJobFacts(stored, key, jobType, schemaHash, parentJobID, metadata, policy); err != nil {
		return jobdb.JobHandle{}, err
	}
	return stored.Cell.Handle(key, stored.GoalID), nil
}

func (r *Runtime) validateExistingRestart(ctx context.Context, source, target ChapterLogKey,
	lastOrdinal int64, extra *EncodedChapter) error {
	wantCount := lastOrdinal + 1
	if extra != nil {
		wantCount++
	}
	count, err := r.chapters.Count(ctx, target)
	err = jobdb.ClassifyRuntimeIO(err, "count_restart_chapters", &target.JobKey, nil)
	if err != nil {
		return err
	}
	if count != wantCount {
		return jobdb.NewExistingJobMismatchError("restart chapter count differs")
	}
	for ordinal := int64(0); ordinal <= lastOrdinal; ordinal++ {
		want, err := r.chapters.Get(ctx, source, ordinal)
		err = jobdb.ClassifyRuntimeIO(err, "get_restart_chapter", &source.JobKey, &ordinal)
		if err != nil {
			return err
		}
		got, err := r.chapters.Get(ctx, target, ordinal)
		err = jobdb.ClassifyRuntimeIO(err, "get_restart_chapter", &target.JobKey, &ordinal)
		if err != nil {
			return err
		}
		if !bytes.Equal(got.Payload, want.Payload) || !slices.Equal(got.Artifacts, want.Artifacts) {
			return jobdb.NewExistingJobMismatchError(fmt.Sprintf("restart prefix chapter %d differs", ordinal))
		}
	}
	if extra != nil {
		got, err := r.chapters.Get(ctx, target, lastOrdinal+1)
		ordinal := lastOrdinal + 1
		err = jobdb.ClassifyRuntimeIO(err, "get_restart_chapter", &target.JobKey, &ordinal)
		if err != nil {
			return err
		}
		gotChapter, err := DecodeChapter(got)
		err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDurableDecode, "decode_restart_chapter", &target.JobKey, &got.Ordinal)
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
