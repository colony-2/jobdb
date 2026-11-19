package jobdb

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInfrastructureClassificationPreservesOrigin(t *testing.T) {
	cause := errors.New("decode")
	for _, err := range []error{ErrWorkflowNotDeterministic, fmt.Errorf("wrapped: %w", ErrWorkflowNotDeterministic), errors.Join(cause, ErrWorkflowNotDeterministic)} {
		require.True(t, IsExecutionInfrastructureError(err))
		got := ClassifyExecutionInfrastructure(err, ExecutionInfrastructureDurableDecode, "decode", nil, nil)
		require.ErrorIs(t, got, ErrWorkflowNotDeterministic)
	}
	zero := int64(0)
	key := JobKey{TenantId: "tenant", JobId: "prerequisite"}
	other := JobKey{TenantId: "tenant", JobId: "target"}
	for _, origin := range []*JobKey{nil, &key} {
		for _, ordinal := range []*int64{nil, &zero} {
			err := ClassifyExecutionInfrastructure(cause, ExecutionInfrastructureDurableDecode, "decode", origin, ordinal)
			got := ClassifyExecutionInfrastructure(err, ExecutionInfrastructureRuntimeIO, "target", &other, &zero)
			var detail *ExecutionInfrastructureError
			require.ErrorAs(t, got, &detail)
			require.Equal(t, origin, detail.JobKey)
			require.Equal(t, ordinal, detail.Ordinal)
			require.ErrorIs(t, got, cause)
		}
	}
	for _, err := range []error{ErrExecutionLeaseLost, ErrLeasePermissionDenied, ErrConflict} {
		require.False(t, IsExecutionInfrastructureError(ClassifyRuntimeIO(err, "append", &key, nil)))
	}
	joined := errors.Join(ClassifyExecutionInfrastructure(cause, ExecutionInfrastructureRuntimeIO, "read", &key, nil), ErrExecutionLeaseLost)
	require.ErrorIs(t, ClassifyRuntimeIO(joined, "read", nil, nil), ErrExecutionLeaseLost)
}
