package runtimecore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
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
			req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "source"}, JobID: "restart", LastStepToKeep: 42, Schema: &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":false}`)}}}
			switch mode {
			case "valid-extra":
				req.Job.Schema.Schema = json.RawMessage(`{"chapterShape":{"properties":{"ordinal":{"minimum":43}}}}`)
				req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic("small")
			case "invalid-extra-schema":
				req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic("small")
			case "oversized-extra":
				req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic(strings.Repeat("x", 300000))
			}
			_, err = rt.SubmitRestartJob(context.Background(), req)
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
