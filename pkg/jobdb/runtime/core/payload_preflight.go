package runtimecore

import (
	"context"
	"encoding/json"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ValidateSubmissionApplicationPayload rejects known oversized input before
// preparation reserves an identity or fingerprints/opens any artifacts.
func ValidateSubmissionApplicationPayload(ctx context.Context, job jobdb.SubmitJob, limit int64) error {
	if job.Data == nil {
		return nil
	}
	raw, err := job.Data.GetData()
	if err != nil {
		return err
	}
	return jobdb.ValidateChapterApplicationPayload(ctx, jobdb.Chapter{TaskType: job.JobType, Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: raw}}}, limit)
}

// ValidateRestartApplicationPayload checks newly added application data before
// reserving the restart identity. Full reconstruction is checked when assembled.
func ValidateRestartApplicationPayload(ctx context.Context, job jobdb.SubmitRestartJob, limit int64) error {
	if job.ExecutionRestart != nil {
		record := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "restart", ID: job.ExecutionRestart.ScopeID, Source: &job.PriorJobKey, Payload: job.ExecutionRestart.Payload}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return jobdb.ValidateChapterApplicationPayload(ctx, jobdb.Chapter{Ordinal: job.LastStepToKeep + 1, TaskType: "__execution__", Body: jobdb.ExecutionChapter{Record: raw}}, limit)
	}
	if job.ExtraTaskOutput == nil {
		return nil
	}
	raw, err := job.ExtraTaskOutput.GetData()
	if err != nil {
		return err
	}
	return jobdb.ValidateChapterApplicationPayload(ctx, jobdb.Chapter{Ordinal: job.LastStepToKeep + 1, Body: jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: raw}}}, limit)
}
