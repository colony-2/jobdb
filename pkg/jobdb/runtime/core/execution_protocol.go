package runtimecore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

type executionHistoryReader interface {
	ListChapters(context.Context, jobdb.ListChaptersRequest) ([]jobdb.Chapter, error)
	GetChapter(context.Context, jobdb.ChapterRef) (jobdb.Chapter, error)
}

// ResolveExecutionRestart validates a logical boundary without changing storage.
func ResolveExecutionRestart(ctx context.Context, r executionHistoryReader, job jobdb.SubmitRestartJob) (jobdb.SubmitRestartJob, *jobdb.ExecutionRecord, error) {
	if job.ExecutionRestart == nil {
		return job, nil, nil
	}
	if job.ExtraTaskInput != nil || job.ExtraTaskOutput != nil {
		return job, nil, fmt.Errorf("execution restarts cannot contain synthetic task data")
	}
	target := job.ExecutionRestart
	if target.ScopeID == "" || (len(target.Payload) > 0 && !json.Valid(target.Payload)) {
		return job, nil, fmt.Errorf("invalid execution restart target or payload")
	}
	chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: job.PriorJobKey})
	if err != nil {
		return job, nil, err
	}
	found, header, cut := false, false, false
	var version uint32
	var retained json.RawMessage
	for _, chapter := range chapters {
		body, ok := chapter.Body.(jobdb.ExecutionChapter)
		if !ok {
			continue
		}
		record, err := jobdb.DecodeExecutionRecord(body.Record)
		if err != nil {
			return job, nil, err
		}
		if record.Kind == "header" {
			header = true
			version = record.Version
		}
		prefixRaw := chapter.Metadata.Fields["metadata"].Map[executionmeta.Key].String
		if record.Kind == "restart" && len(record.Descriptor) > 0 {
			prefixRaw = string(record.Descriptor)
		}
		prefix, err := executionmeta.Decode(prefixRaw)
		if err != nil {
			return job, nil, err
		}
		for i, raw := range prefix {
			item, err := jobdb.DecodeExecutionRecord(raw)
			if err != nil {
				return job, nil, err
			}
			// A continuation and its admission records belong to the execution
			// being replaced, even when compact history placed them in an older
			// restart descriptor. Cut before the first such continuation, not
			// just before the declaration that follows it. Still validate the
			// declaration below: a continuation alone is not a restart target.
			if !cut && item.ID == target.ScopeID && (item.Kind == "continuation" || item.Kind == "scope" || item.Kind == "task") {
				retained, err = json.Marshal(prefix[:i])
				if err != nil {
					return job, nil, err
				}
				job.LastStepToKeep = chapter.Ordinal - 1
				cut = true
			}
			if (item.Kind != "scope" && item.Kind != "task") || item.ID != target.ScopeID {
				continue
			}
			if found {
				return job, nil, fmt.Errorf("ambiguous execution restart boundary")
			}
			var options struct {
				Restart bool `json:"restart"`
			}
			if err := json.Unmarshal(item.Descriptor, &options); err != nil {
				return job, nil, err
			}
			if len(target.Payload) > 0 && !options.Restart {
				return job, nil, fmt.Errorf("target scope has no restart handler")
			}
			found = true
		}
		if !cut && record.Kind == "continuation" && record.ID == target.ScopeID {
			retained, err = json.Marshal(prefix)
			if err != nil {
				return job, nil, err
			}
			job.LastStepToKeep = chapter.Ordinal - 1
			cut = true
		}
	}
	if !header || !found {
		return job, nil, fmt.Errorf("execution restart boundary not found")
	}
	return job, &jobdb.ExecutionRecord{Version: version, Kind: "restart", ID: target.ScopeID, Descriptor: retained, Source: &job.PriorJobKey, Payload: append(json.RawMessage(nil), target.Payload...), Dependencies: append([]jobdb.Dependency(nil), job.Dependencies...), Prerequisites: append([]jobdb.JobPrerequisite(nil), job.Prerequisites...)}, nil
}

// ExecutionCompletion preserves a wait's explicit invocation/input association.
// The scheduler still atomically validates and claims the waiting coordinates.
func ExecutionCompletion(ctx context.Context, r executionHistoryReader, req jobdb.CompleteTaskIfWaitingRequest, inputOrdinal int64) (*jobdb.ExecutionRecord, error) {
	ch, err := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: req.JobKey, Ordinal: inputOrdinal})
	if err != nil {
		return nil, err
	}
	body, ok := ch.Body.(jobdb.ExecutionChapter)
	if !ok {
		return nil, nil
	}
	wait, err := jobdb.DecodeExecutionRecord(body.Record)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(wait)
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	if wait.Kind != "wait" || wait.Input == nil || req.InputHash != digest || req.InputOrdinal != inputOrdinal {
		return nil, fmt.Errorf("%w: invalid execution wait generation", jobdb.ErrConflict)
	}
	// A deadline makes timeout processing eligible; it does not commit an
	// outcome. Until a worker claims the slot, an external answer can win the
	// scheduler's atomic claim. Generation, lease and terminal-state checks
	// still prevent answers from replacing work already claimed or completed.
	data, err := req.Data.GetData()
	if err != nil {
		return nil, err
	}
	snapshot := &jobdb.ExecutionSnapshot{Data: append(json.RawMessage(nil), data...)}
	arts, err := req.Data.GetArtifacts()
	if err != nil {
		return nil, err
	}
	for _, art := range arts {
		data, err := art.Bytes(ctx)
		if err != nil {
			return nil, err
		}
		snapshot.Artifacts = append(snapshot.Artifacts, jobdb.ExecutionArtifact{Name: art.Name(), Data: data, Key: &jobdb.ArtifactKey{JobId: req.JobKey.JobId, TaskOrdinal: req.OutputOrdinal, Name: art.Name(), SizeBytes: int64(len(data))}})
	}
	return &jobdb.ExecutionRecord{Version: wait.Version, Kind: "result", ID: wait.ID, Activation: wait.Activation, At: time.Now().UTC(), Input: wait.Input, Output: snapshot}, nil
}
