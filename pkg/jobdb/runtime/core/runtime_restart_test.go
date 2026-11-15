package runtimecore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"

	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type restartTestScheduler struct{ runtimecore.Scheduler }

func (restartTestScheduler) PrepareJob(_ context.Context, p runtimecore.JobPreparation) (runtimecore.PreparedJob, error) {
	return runtimecore.PreparedJob{Key: p.Key, GoalID: "goal"}, nil
}
func (restartTestScheduler) GetJob(_ context.Context, key jobdb.JobKey) (runtimecore.StoredJob, error) {
	return runtimecore.StoredJob{JobKey: key, AppMetadata: json.RawMessage(`{}`)}, nil
}
func (restartTestScheduler) CreateJob(_ context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	return runtimecore.StoredJob{JobKey: req.JobKey, JobType: req.JobType, SchemaHash: req.SchemaHash, ParentJobID: req.ParentJobID, AppMetadata: req.AppMetadata, RunPolicy: req.RunPolicy, InitialPayloadDigest: req.InitialPayloadDigest}, nil
}

type restartBoundaryHistory struct {
	runtimecore.ChapterLog
	reads  []int64
	clones []runtimecore.ClonePrefixRequest
}

func (h *restartBoundaryHistory) Get(_ context.Context, _ runtimecore.ChapterLogKey, ordinal int64) (runtimecore.EncodedChapter, error) {
	h.reads = append(h.reads, ordinal)
	// Only metadata-bearing boundary chapters are needed for ordinal restarts.
	if ordinal != 0 && ordinal != 43 {
		return runtimecore.EncodedChapter{}, fmt.Errorf("unexpected retained-history read at %d", ordinal)
	}
	ch := jobdb.Chapter{Ordinal: ordinal, TaskType: "legacy", Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: json.RawMessage(`null`)}}}
	return runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: ch})
}
func (h *restartBoundaryHistory) ClonePrefix(_ context.Context, req runtimecore.ClonePrefixRequest) error {
	h.clones = append(h.clones, req)
	return nil
}

func TestRestartValidatesOnlyNewChapters(t *testing.T) {
	for _, mode := range []string{"retained", "valid-extra", "invalid-extra-schema", "oversized-extra"} {
		t.Run(mode, func(t *testing.T) {
			history := &restartBoundaryHistory{}
			rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: restartTestScheduler{}, Chapters: history, Schemas: newMemorySchemaStore()})
			require.NoError(t, err)
			req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "source"}, JobID: "restart", LastStepToKeep: 42, Schema: &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":false}`)}}}
			switch mode {
			case "valid-extra":
				req.Job.Schema.Schema = json.RawMessage(`{"chapterShape":{"properties":{"ordinal":{"minimum":43}}}}`)
				req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic("small")
			case "invalid-extra-schema":
				req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic("small")
			case "oversized-extra":
				req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic(strings.Repeat("x", 300000))
			}
			_, err = rt.SubmitRestartJob(celltest.Context(context.Background()), req)
			switch mode {
			case "invalid-extra-schema":
				require.ErrorIs(t, err, jobdb.ErrJobSchemaValidation)
				require.Empty(t, history.clones)
			case "oversized-extra":
				require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)
				require.Empty(t, history.reads)
				require.Empty(t, history.clones)
			default:
				require.NoError(t, err)
				require.Equal(t, []int64{0, 43}, history.reads)
				require.Len(t, history.clones, 1)
				require.Equal(t, req.Job.PriorJobKey, history.clones[0].SourceKey.JobKey)
				require.EqualValues(t, 42, history.clones[0].LastOrdinal)
				if mode == "valid-extra" {
					require.NotNil(t, history.clones[0].Append)
				} else {
					require.Nil(t, history.clones[0].Append)
				}
			}
		})
	}
}

func (restartTestScheduler) DiscardJobPreparation(context.Context, jobdb.JobKey) error { return nil }

type submissionBoundaryHistory struct {
	runtimecore.ChapterLog
	following error
	count     int64
	countErr  error
	invalid   bool
	present   *runtimecore.EncodedChapter
	cloned    bool
}

func (h *submissionBoundaryHistory) Get(_ context.Context, _ runtimecore.ChapterLogKey, ordinal int64) (runtimecore.EncodedChapter, error) {
	if ordinal != 0 {
		if h.present != nil {
			return *h.present, nil
		}
		return runtimecore.EncodedChapter{}, h.following
	}
	ch := jobdb.Chapter{TaskType: "recipe", Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: json.RawMessage(`{"input":"original"}`)}}}
	if h.invalid {
		ch.Body = jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: json.RawMessage(`null`)}}
	}
	return runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: ch})
}
func (h *submissionBoundaryHistory) Count(context.Context, runtimecore.ChapterLogKey) (int64, error) {
	return h.count, h.countErr
}
func (h *submissionBoundaryHistory) ClonePrefix(_ context.Context, req runtimecore.ClonePrefixRequest) error {
	h.cloned = true
	return nil
}

func TestSubmissionOnlyRestartDiscriminatesErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		count    int64
		countErr error
		invalid  bool
		cut      int64
		succeeds bool
	}{
		{name: "specific", err: jobdb.ErrChapterNotFound, count: 1, succeeds: true},
		{name: "wrapped-specific", err: fmt.Errorf("read: %w", jobdb.ErrChapterNotFound), count: 1, succeeds: true},
		{name: "job-not-found", err: jobdb.ErrJobNotFound},
		{name: "broad-not-found", err: fmt.Errorf("not found")},
		{name: "permission", err: os.ErrPermission},
		{name: "cancelled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "io", err: io.ErrUnexpectedEOF},
		{name: "unexpected", err: errors.New("storage fault")},
		{name: "sparse", err: jobdb.ErrChapterNotFound, count: 2},
		{name: "invalid-first", err: jobdb.ErrChapterNotFound, count: 1, invalid: true},
		{name: "count-failure", err: jobdb.ErrChapterNotFound, countErr: io.ErrUnexpectedEOF},
		{name: "other-cut", err: jobdb.ErrChapterNotFound, count: 1, cut: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			history := &submissionBoundaryHistory{following: tc.err, count: tc.count, countErr: tc.countErr, invalid: tc.invalid}
			rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: restartTestScheduler{}, Chapters: history, Schemas: newMemorySchemaStore()})
			require.NoError(t, err)
			_, err = rt.SubmitRestartJob(celltest.Context(context.Background()), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", JobID: "destination", PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "source"}, LastStepToKeep: tc.cut}})
			if tc.succeeds {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				if tc.count == 0 && tc.countErr == nil {
					require.ErrorIs(t, err, tc.err)
				}
			}
			require.Equal(t, tc.succeeds, history.cloned)
		})
	}
}

func TestSubmissionRestartStillValidatesPresentChapterOne(t *testing.T) {
	for _, mode := range []string{"valid", "retry", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			chapter := jobdb.Chapter{Ordinal: 1, TaskType: "task", Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: json.RawMessage(`null`)}}}}
			if mode == "retry" {
				chapter.Metadata = jobdb.ChapterMetadata{Fields: map[string]jobdb.ChapterMetadataValue{"attempt": {Kind: jobdb.ChapterMetadataInt, Int: 2}}}
			}
			encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: chapter})
			require.NoError(t, err)
			if mode == "corrupt" {
				encoded.Payload = []byte("bad chapter")
			}
			history := &submissionBoundaryHistory{present: &encoded}
			rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: restartTestScheduler{}, Chapters: history, Schemas: newMemorySchemaStore()})
			require.NoError(t, err)
			_, err = rt.SubmitRestartJob(celltest.Context(context.Background()), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", JobID: "destination", PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "source"}}})
			if mode == "valid" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, mode == "valid", history.cloned)
		})
	}
}
