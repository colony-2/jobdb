package runtimecore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

// GetJob combines the scheduler status with the final chapter for completed
// jobs. The data error remains deferred until callers inspect JobInfo.Data.
func (r *Runtime) GetJob(ctx context.Context, key jobdb.JobKey) (jobdb.JobInfo, error) {
	if err := r.validate(); err != nil {
		return jobdb.JobInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobInfo{}, err
	}
	stored, err := r.scheduler.GetJob(ctx, key)
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	info := jobdb.JobInfo{
		Status: stored.Status, SchemaHash: stored.SchemaHash,
		ClientPayload: append([]byte(nil), stored.ClientPayload...), ClientPayloadRevision: stored.ClientPayloadRevision, ExecutionState: executionState(stored.RunPolicy, stored.TaskWork),
		Data: &runtimeJobData{err: jobdb.ErrJobNotComplete},
	}
	if stored.Store != jobdb.JobStoreArchived {
		return info, nil
	}
	logKey := ChapterLogKey{JobKey: key}
	count, err := r.chapters.Count(ctx, logKey)
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	if count < 1 {
		return jobdb.JobInfo{}, fmt.Errorf("archived job %s has no chapters", key)
	}
	encoded, err := r.chapters.Get(ctx, logKey, count-1)
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	chapter, err := DecodeChapter(encoded)
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	_, kind, payload, err := runtimecodec.ChapterBodyToWire(chapter.Body)
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	artifacts := make([]jobdb.Artifact, 0, len(chapter.Artifacts))
	for _, storedArtifact := range chapter.Artifacts {
		lookup := ArtifactLookup{JobKey: key, Ordinal: chapter.Ordinal,
			Name: storedArtifact.Name, Digest: storedArtifact.Digest}
		artifact := jobdb.NewArtifact(storedArtifact.Name, func() (io.ReadCloser, int64, error) {
			reader, err := r.chapters.OpenArtifact(context.Background(), lookup)
			if err != nil {
				return nil, 0, err
			}
			stream, err := reader.Open()
			return stream, reader.Size(), err
		}, nil)
		jobdb.AssignArtifactKey(artifact, jobdb.ArtifactKey{
			JobId: key.JobId, TaskOrdinal: chapter.Ordinal,
			Name: storedArtifact.Name, SizeBytes: storedArtifact.Size,
		})
		artifacts = append(artifacts, artifact)
	}
	data := &jobdb.EnvelopedTaskData{
		SimpleTaskData: jobdb.SimpleTaskData{Data: append(jobdb.Data(nil), payload...), Artifacts: artifacts},
		Kind:           kind,
	}
	var dataErr error
	switch kind {
	case runtimecodec.PayloadKindApp:
	case runtimecodec.PayloadKindTimeout:
		var value jobdb.TimeoutPayload
		dataErr = decodeOutcomeError(payload, &value, func() error { return &jobdb.TimeoutError{Payload: value} })
	case runtimecodec.PayloadKindAppError:
		var value jobdb.AppErrorPayload
		dataErr = decodeOutcomeError(payload, &value, func() error { return &jobdb.AppError{Payload: value} })
	case runtimecodec.PayloadKindSystemError:
		var value jobdb.SystemErrorPayload
		dataErr = decodeOutcomeError(payload, &value, func() error { return &jobdb.SystemError{Payload: value} })
	default:
		dataErr = fmt.Errorf("unsupported chapter payload kind %q", kind)
	}
	info.Data = &runtimeJobData{taskData: data, err: dataErr}
	return info, nil
}

func decodeOutcomeError(raw json.RawMessage, target any, makeError func() error) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return err
	}
	return makeError()
}

type runtimeJobData struct {
	taskData jobdb.TaskData
	err      error
}

func (d *runtimeJobData) GetData() (jobdb.Data, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	data, err := d.taskData.GetData()
	if err != nil {
		return data, err
	}
	return data, d.err
}

func (d *runtimeJobData) GetDataOrPanic() jobdb.Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *runtimeJobData) GetArtifacts() ([]jobdb.Artifact, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	return d.taskData.GetArtifacts()
}

func (d *runtimeJobData) TaskDataResult() (jobdb.TaskData, error) {
	return d.taskData, d.err
}
