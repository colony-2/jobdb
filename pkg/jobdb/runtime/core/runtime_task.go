package runtimecore

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/segmentio/ksuid"
)

// CompleteTaskIfWaiting writes an external task result only while the native
// scheduler still waits for the exact task coordinates.
func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	if err := req.Route.Validate(); err != nil {
		return err
	}
	if req.Route.TaskType == "" {
		return fmt.Errorf("task route required")
	}
	if err := r.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := clientpayload.ValidateUpdate(req.ClientPayloadUpdate, false); err != nil {
		return err
	}
	if err := req.JobKey.Validate(); err != nil {
		return err
	}
	waiting, err := r.scheduler.GetWaitingTask(ctx, req.JobKey)
	if err != nil {
		return err
	}
	task := waiting.Task
	route := jobdb.Route{JobType: waiting.JobType, TaskType: task.TaskType}
	if req.Route != route {
		return fmt.Errorf("%w: waiting route differs from request", jobdb.ErrConflict)
	}
	if req.ResumeJobType != "" && req.ResumeJobType != task.ResumeJobType {
		return fmt.Errorf("%w: waiting resume route differs from request", jobdb.ErrConflict)
	}
	if req.InputOrdinal != 0 && req.InputOrdinal != task.InputOrdinal {
		return fmt.Errorf("%w: waiting input ordinal differs from request", jobdb.ErrConflict)
	}
	if req.OutputOrdinal != task.OutputOrdinal ||
		(req.InputHash != "" && req.InputHash != task.InputHash) {
		return fmt.Errorf("%w: waiting task coordinates differ from request", jobdb.ErrConflict)
	}
	stored, err := r.scheduler.GetJob(ctx, req.JobKey)
	if err != nil {
		return err
	}
	if stored.CancelRequested || stored.Store == jobdb.JobStoreArchived || stored.WorkKind != WorkKindTask || stored.TaskWork == nil ||
		!reflect.DeepEqual(*stored.TaskWork, task) ||
		(stored.LeaseExpiresAt != nil && stored.LeaseExpiresAt.After(r.now())) {
		return fmt.Errorf("%w: job is not waiting for an unheld external task", jobdb.ErrConflict)
	}
	if _, _, err := clientpayload.Apply(stored.ClientPayload, stored.ClientPayloadRevision, req.ClientPayloadUpdate); err != nil {
		return err
	}
	var inherited runtimecodec.ChapterMeta
	if task.InputOrdinal > 0 {
		input, err := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: req.JobKey,
			Ordinal: task.InputOrdinal})
		if err != nil {
			return err
		}
		inherited, err = initialChapterMetadata(input.Metadata)
		if err != nil {
			return err
		}
	}
	payload, descriptors, uploads, sourceArtifacts, _, err := prepareInitialTaskData(ctx, req.Data)
	if err != nil {
		return err
	}
	workerID := "jobdb-core-" + ksuid.New().String()
	meta := runtimecodec.ChapterMeta{
		Version: runtimecodec.EnvelopeVersion,
		Ordinal: task.OutputOrdinal, TaskType: task.TaskType,
		WorkerID: workerID, CreatedAt: r.now(), InputHash: task.InputHash,
		Attempt: inherited.Attempt, MaxAttempts: inherited.MaxAttempts,
		NextAttemptAt: inherited.NextAttemptAt,
		BackoffMillis: inherited.BackoffMillis, Retryable: inherited.Retryable,
		InputRef: inherited.InputRef,
	}
	if stored.RunPolicy.Retry.MaximumAttempts > 0 {
		meta.RunPolicy = &stored.RunPolicy
	}
	rawMetadata, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	chapterMetadata, err := runtimecodec.ChapterMetadataFromJSON(rawMetadata)
	if err != nil {
		return err
	}
	chapter := jobdb.Chapter{
		Ordinal: task.OutputOrdinal, TaskType: task.TaskType,
		Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: payload},
		}},
		InputHash: task.InputHash, CreatedAt: meta.CreatedAt,
		Metadata: chapterMetadata, Artifacts: descriptors,
	}
	if err := ValidateOrdinaryChapter(ctx, r.schemas,
		jobdb.JobSchemaKey{TenantId: req.JobKey.TenantId,
			SchemaHash: stored.SchemaHash}, chapter); err != nil {
		return err
	}
	encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: chapter, ArtifactUploads: uploads})
	if err != nil {
		return err
	}
	logKey := ChapterLogKey{JobKey: req.JobKey}
	count, err := r.chapters.Count(ctx, logKey)
	if err != nil {
		return err
	}
	if count != task.OutputOrdinal {
		return fmt.Errorf("%w: task output ordinal %d is not appendable; expected %d",
			jobdb.ErrConflict, task.OutputOrdinal, count)
	}
	if err := r.chapters.Append(ctx, logKey, encoded); err != nil {
		return err
	}
	for _, artifact := range sourceArtifacts {
		jobdb.AssignArtifactKey(artifact, jobdb.ArtifactKey{
			JobId: req.JobKey.JobId, TaskOrdinal: task.OutputOrdinal,
			Name: artifact.Name(), SizeBytes: artifact.Size(),
		})
		_ = artifact.Cleanup()
	}
	_, err = r.scheduler.CompleteTaskWork(ctx, CompleteTaskWorkMutation{
		JobKey: req.JobKey, WorkerID: workerID,
		Task: waiting, ClientPayloadUpdate: req.ClientPayloadUpdate, Now: r.now(),
	})
	return err
}
