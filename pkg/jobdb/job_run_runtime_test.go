package jobdb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type projectionRuntime struct {
	WorkflowRuntime
	job      JobSummary
	chapters []Chapter
}

func (r projectionRuntime) ListJobs(context.Context, ListJobsRequest) (ListJobsResponse, error) {
	return ListJobsResponse{Jobs: []JobSummary{r.job}}, nil
}
func (r projectionRuntime) GetJob(context.Context, JobKey) (JobInfo, error) { return JobInfo{}, nil }
func (r projectionRuntime) ListChapters(context.Context, ListChaptersRequest) ([]Chapter, error) {
	return r.chapters, nil
}

func TestGetJobRunGroupedPrefix(t *testing.T) {
	ctx := context.Background()
	key := JobKey{TenantId: "tenant", JobId: "group"}
	start := JobStartChapter{Input: ApplicationInputBytes{Data: []byte(`"creator"`)}, Append: &ApplicationInputBytes{Data: []byte(`null`)}}
	extra := AdditionalInputChapter{Input: ApplicationInputBytes{Data: []byte(`2`)}}
	raw, err := json.Marshal(ExecutionRecord{Version: ExecutionPatternVersion, Kind: "restart", ID: "restart", Source: &key})
	require.NoError(t, err)
	execution := ExecutionChapter{Record: raw}
	for _, pointers := range []bool{false, true} {
		t.Run(map[bool]string{false: "values", true: "pointers"}[pointers], func(t *testing.T) {
			var startBody ChapterBody = start
			var extraBody ChapterBody = extra
			var execBody ChapterBody = execution
			if pointers {
				startBody = &start
				extraBody = &extra
				execBody = &execution
			}
			r := projectionRuntime{job: JobSummary{JobKey: key, JobType: "batch", Status: JobStatusReady}, chapters: []Chapter{{Ordinal: 0, Body: startBody}, {Ordinal: 4, Body: extraBody}}}
			run, err := GetJobRun(ctx, r, GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeAttemptInputs: true})
			require.NoError(t, err)
			require.Equal(t, `"creator"`, string(run.Start.Input.Data))
			require.Empty(t, run.Execution)
			require.Len(t, run.Attempts, 1)
			require.Empty(t, run.Attempts[0].Outcome.Status)
			require.Len(t, run.Attempts[0].Tasks, 1) // The pending runtime task, never an append outcome.
			pending := run.Attempts[0].Tasks[0]
			require.Equal(t, "batch:5", pending.TaskRunID)
			require.EqualValues(t, 5, pending.Attempts[0].Ordinal)
			require.EqualValues(t, 4, pending.Attempts[0].InputRef.Ordinal)
			require.NotNil(t, pending.Attempts[0].Input)
			require.Equal(t, TaskAttemptStateReady, pending.Attempts[0].State)
			meta, err := chapterMetadataFromJSON(json.RawMessage(`{"input_ref":{"ordinal":0,"hash":"creator"}}`))
			require.NoError(t, err)
			r.chapters = append(r.chapters, Chapter{Ordinal: 7, Body: execBody}, Chapter{Ordinal: 9, Metadata: meta, TaskType: "task", Body: TaskAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{Output: ApplicationOutputBytes{Data: []byte(`"task result"`)}}}})
			run, err = GetJobRun(ctx, r, GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeOutputs: true, IncludeAttemptInputs: true})
			require.NoError(t, err)
			require.Len(t, run.Execution, 1)
			require.EqualValues(t, 7, run.Execution[0].Ordinal)
			require.Len(t, run.Attempts[0].Tasks, 2)
			actual := run.Attempts[0].Tasks[0].Attempts[0]
			require.EqualValues(t, 9, actual.Ordinal)
			require.EqualValues(t, 0, actual.InputRef.Ordinal)
			require.Equal(t, `"creator"`, string(actual.Input.Data))
		})
	}
	for _, prefix := range [][]Chapter{
		{{Ordinal: 0, Body: JobStartChapter{Input: start.Input}}},
		{{Ordinal: 0, Body: start}, {Ordinal: 1, Body: execution}},
		{{Ordinal: 0, Body: start}, {Ordinal: 1, Body: TaskAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{}}}},
		{{Ordinal: 0, Body: start}, {Ordinal: 1, Body: JobAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{}}}},
		{},
	} {
		r := projectionRuntime{job: JobSummary{JobKey: key}, chapters: append(prefix, Chapter{Ordinal: 10, Body: extra})}
		_, err := GetJobRun(ctx, r, GetJobRunRequest{JobKey: key})
		require.ErrorIs(t, err, ErrWorkflowNotDeterministic)
	}
}

func TestGetJobRunGroupedRetryAndLatestOutput(t *testing.T) {
	key := JobKey{TenantId: "tenant", JobId: "job"}
	archived := time.Now()
	for _, completed := range []bool{false, true} {
		var baseline GetJobRunResponse
		for _, grouped := range []bool{false, true} {
			start := JobStartChapter{Input: ApplicationInputBytes{Data: []byte(`"creator"`)}}
			if grouped {
				start.Append = &ApplicationInputBytes{Data: []byte(`1`)}
			}
			policyMeta, err := chapterMetadataFromJSON(json.RawMessage(`{"run_policy":{"Retry":{"MaximumAttempts":3}}}`))
			require.NoError(t, err)
			r := projectionRuntime{job: JobSummary{JobKey: key, JobType: "batch", Status: JobStatusReady}, chapters: []Chapter{{Ordinal: 0, Body: start, Metadata: policyMeta}}}
			if grouped {
				r.chapters = append(r.chapters, Chapter{Ordinal: 1, Body: AdditionalInputChapter{Input: ApplicationInputBytes{Data: []byte(`2`)}}})
			}
			meta, err := chapterMetadataFromJSON(json.RawMessage(`{"attempt":1}`))
			require.NoError(t, err)
			r.chapters = append(r.chapters, Chapter{Ordinal: 3, Metadata: meta, Body: JobAttemptOutcomeChapter{Outcome: SystemErrorOutcome{Error: SystemErrorPayload{Message: "retry", Retryable: true}}}})
			if completed {
				meta, err = chapterMetadataFromJSON(json.RawMessage(`{"attempt":2}`))
				require.NoError(t, err)
				r.chapters = append(r.chapters, Chapter{Ordinal: 5, Metadata: meta, Body: JobAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{Output: ApplicationOutputBytes{Data: []byte(`"done"`)}}}})
				r.job.Status = JobStatusCompleted
				r.job.ArchivedAt = &archived
			}
			run, err := GetJobRun(context.Background(), r, GetJobRunRequest{JobKey: key, IncludeInputs: true})
			require.NoError(t, err)
			require.Len(t, run.Attempts, 2)
			require.Nil(t, run.Attempts[0].Output)
			if completed {
				require.Equal(t, `"done"`, string(run.Attempts[1].Output.Data))
			} else {
				require.Equal(t, 2, run.Attempts[1].Attempt)
				require.Empty(t, run.Attempts[1].Outcome.Status)
			}
			if grouped {
				require.Equal(t, baseline, run)
			} else {
				baseline = run
			}
		}
	}
}
