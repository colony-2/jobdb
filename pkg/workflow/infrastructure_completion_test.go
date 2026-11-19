package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestInfrastructureCompletionSource(t *testing.T) {
	t.Run("snapshot before completion", func(t *testing.T) {
		key := JobKey{TenantId: "tenant", JobId: "job"}
		for _, value := range []int64{0, 7} {
			chapter := &Chapter{Ordinal: value}
			cause := errors.New("lost completion acknowledgement")
			base := &completionMutationLease{fakeExecutionLease: &fakeExecutionLease{job: JobHandle{JobKey: key}}, err: cause}
			lease := &guardedLease{ExecutionLease: base, guard: &infrastructureGuard{}}
			err := lease.Complete(context.Background(), CompleteExecutionRequest{Chapter: chapter})
			var detail *jobdb.ExecutionInfrastructureError
			require.ErrorAs(t, err, &detail)
			require.ErrorIs(t, err, cause)
			require.Equal(t, &value, detail.Ordinal, "capture the attempted position before calling the backend")
			require.Equal(t, &key, detail.JobKey)
		}
	})
	t.Run("runner lost acknowledgement", func(t *testing.T) {
		cause := errors.New("lost final completion acknowledgement")
		runner, lease, rt := infrastructureRunner(t, func(_ ExecutionContext, in JobData) (JobData, error) { return in, nil })
		lease.completeErr = cause
		_, err := runner.DoJob(celltest.Context(context.Background()))
		require.ErrorIs(t, err, cause)
		var detail *jobdb.ExecutionInfrastructureError
		require.ErrorAs(t, err, &detail)
		_, stops, calls, reschedules := lease.snapshot()
		require.Equal(t, 1, stops)
		require.Empty(t, reschedules)
		require.Len(t, calls, 1, "do not retry a possibly committed completion")
		require.NotNil(t, calls[0].Chapter)
		require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, detail.Reason)
		require.Equal(t, "complete", detail.Operation)
		require.Equal(t, &runner.jobKey, detail.JobKey)
		require.Equal(t, &calls[0].Chapter.Ordinal, detail.Ordinal)
		stored, readErr := rt.GetChapter(context.Background(), ChapterRef{JobKey: runner.jobKey, Ordinal: calls[0].Chapter.Ordinal})
		require.NoError(t, readErr, "an error does not undo the committed terminal chapter")
		require.Equal(t, *calls[0].Chapter, stored)
	})

	for _, chapter := range []*Chapter{nil, {Ordinal: 0}, {Ordinal: 7}} {
		name := "no chapter"
		var ordinal *int64
		if chapter != nil {
			ordinal = &chapter.Ordinal
			name = fmt.Sprintf("ordinal %d", chapter.Ordinal)
		}
		t.Run(name, func(t *testing.T) {
			key := JobKey{TenantId: "tenant", JobId: "job"}
			other := JobKey{TenantId: "other", JobId: "prerequisite"}
			originOrdinal := int64(23)
			cause := errors.New("completion unavailable")
			known := jobdb.ClassifyExecutionInfrastructure(cause, jobdb.ExecutionInfrastructureDurableDecode, "read_prerequisite", &other, &originOrdinal)
			unknown := jobdb.ClassifyExecutionInfrastructure(cause, jobdb.ExecutionInfrastructureRuntimeIO, "upstream", nil, nil)
			for _, tc := range []struct {
				name string
				err  error
			}{
				{"untyped", cause},
				{"typed known origin", fmt.Errorf("wrapped: %w", known)},
				{"typed unknown origin", fmt.Errorf("wrapped: %w", unknown)},
				{"lease lost", jobdb.ErrExecutionLeaseLost},
				{"cancelled", context.Canceled},
				{"conflict", jobdb.ErrConflict},
				{"success", nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					base := &fakeExecutionLease{job: JobHandle{JobKey: key}, completeErr: tc.err}
					guard := &infrastructureGuard{}
					lease := &guardedLease{ExecutionLease: base, guard: guard}
					req := CompleteExecutionRequest{Chapter: chapter}
					err := lease.Complete(context.Background(), req)
					if tc.err == cause {
						var detail *jobdb.ExecutionInfrastructureError
						require.ErrorAs(t, err, &detail)
						require.ErrorIs(t, err, cause)
						require.Equal(t, &key, detail.JobKey)
						require.Equal(t, ordinal, detail.Ordinal)
						require.Equal(t, "complete", detail.Operation)
					} else {
						require.Equal(t, tc.err, err, "preserve typed origins and semantic errors")
					}
					if jobdb.IsExecutionInfrastructureError(err) {
						require.Same(t, err, guard.failure())
						require.Same(t, err, lease.Complete(context.Background(), req))
						require.Same(t, err, lease.Reschedule(context.Background(), RescheduleExecutionRequest{}))
					} else {
						require.NoError(t, guard.failure())
					}
					_, _, calls, reschedules := base.snapshot()
					require.Len(t, calls, 1)
					require.Empty(t, reschedules)
				})
			}
		})
	}
}

type completionMutationLease struct {
	*fakeExecutionLease
	err error
}

func (l *completionMutationLease) Complete(_ context.Context, req CompleteExecutionRequest) error {
	req.Chapter.Ordinal = 99
	return l.err
}
