package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffControl(t *testing.T) {
	t.Run("B01b_precedence", func(t *testing.T) {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded, jobdb.NewTimeoutError("task", time.Second, jobdb.TimeoutScopeTotal, nil, false), resourceInfrastructure(errors.New("storage failed"))} {
			run := &executionRun{r: &workerRunner{}, active: map[*executionContext]bool{}}
			c := &executionContext{run: run, id: "task/attempt:2"}
			signal := &resourceWaitSignal{owner: c}
			joined := errors.Join(signal, cause)
			require.Same(t, joined, c.interceptResource(c.id, nil, joined))
			require.Nil(t, run.resourcePending)
		}
	})
	t.Run("B01c_text_is_ordinary", func(t *testing.T) {
		c := &executionContext{run: &executionRun{}}
		err := errors.New("EAGAIN resource temporarily unavailable")
		require.Same(t, err, c.interceptResource("task", nil, err))
		require.Nil(t, c.run.resourcePending)
	})
	t.Run("B01d_freeze_and_cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(context.Canceled)
		run := &executionRun{r: &workerRunner{}, active: map[*executionContext]bool{}}
		root := &executionContext{run: run, ctx: ctx, cancel: cancel}
		sibling := root.child("sibling", time.Time{}, time.Time{})
		defer sibling.close()
		owner := root.child("task/attempt:2", time.Time{}, time.Time{})
		defer owner.close()
		require.ErrorIs(t, root.interceptResource(owner.id, nil, &resourceWaitSignal{owner: owner}), errResourcePreparing)
		require.ErrorIs(t, context.Cause(sibling.ctx), errResourcePreparing)
		called := false
		_, err := sibling.invokeAsync(func() (TaskData, error) { called = true; return nil, nil })
		require.ErrorIs(t, err, errResourcePreparing)
		require.False(t, called)
		require.Same(t, owner, run.resourcePending.owner)
	})
	t.Run("B01e_failed_quiescence", func(t *testing.T) {
		run := &executionRun{taskCallbacks: 1}
		err := run.commitResource(context.Background())
		require.True(t, jobdb.IsExecutionInfrastructureError(err))
		require.True(t, strings.Contains(err.Error(), "quiesce"))
		require.False(t, run.suspended)
	})
}
