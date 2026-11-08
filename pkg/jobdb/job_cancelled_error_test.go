package jobdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetOutputPreservesCancellationDetail(t *testing.T) {
	for _, tc := range []struct{ name, detail string }{
		{"empty", ""},
		{"whitespace", " \t\n "},
		{"unicode", "  Use existing artifacts.\n请不要生成 PDF ☃\n"},
		{"long", strings.Repeat("operator feedback ☃\n", 5000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, previousFailure := range []bool{false, true} {
				run := GetJobRunResponse{Job: JobRunSummary{
					Status: JobStatusCancelled, CompletionStatus: "cancelled", CompletionDetail: tc.detail,
				}}
				if previousFailure {
					run.Attempts = []JobAttempt{{Outcome: TaskOutcome{Status: TaskOutcomeStatusFailed,
						Error: &TaskError{Kind: TaskErrorKindApp, Message: "old attempt failure"},
					}}}
				}
				output, err := run.GetOutput(nil, "tenant")
				require.Nil(t, output)
				require.ErrorIs(t, err, ErrJobCancelled)
				require.NotErrorIs(t, err, ErrJobFailed)
				require.NotErrorIs(t, err, context.Canceled)
				want := "job cancelled"
				if tc.detail != "" {
					want += ": " + tc.detail
				}
				require.EqualError(t, err, want)
				wrapped := fmt.Errorf("await result: %w", err)
				require.ErrorIs(t, wrapped, ErrJobCancelled)
				var cancelled *JobCancelledError
				require.ErrorAs(t, wrapped, &cancelled)
				require.Equal(t, tc.detail, cancelled.Detail)
				require.Same(t, ErrJobCancelled, errors.Unwrap(cancelled))
			}
		})
	}
}

func TestGetOutputFailureRetainsCause(t *testing.T) {
	run := GetJobRunResponse{Job: JobRunSummary{Status: JobStatusCompleted}, Attempts: []JobAttempt{{
		Outcome: TaskOutcome{Status: TaskOutcomeStatusFailed, Error: &TaskError{
			Kind: TaskErrorKindApp, Message: "application failure", Attrs: map[string]any{"code": "review_rejected"},
		}},
	}}}
	output, err := run.GetOutput(nil, "tenant")
	require.Nil(t, output)
	require.ErrorIs(t, err, ErrJobFailed)
	require.NotErrorIs(t, err, ErrJobCancelled)
	var failed *JobFailedError
	require.ErrorAs(t, err, &failed)
	var app *AppError
	require.ErrorAs(t, failed.Cause, &app)
	require.Equal(t, "application failure", app.Payload.Message)
	require.Equal(t, "review_rejected", app.Payload.Attrs["code"])
	require.EqualError(t, err, "job failed: application failure")
}
