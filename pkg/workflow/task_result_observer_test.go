package workflow_test

import (
	"context"
	"testing"

	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestTaskResultObserverAdapter(t *testing.T) {
	ctx := context.WithValue(context.Background(), resultStateKey{}, "caller")
	var provider wf.TaskResultObserverProvider = wf.ExecutionWorker{}
	got, callback := provider.NewTaskResultObserver(ctx)
	require.Same(t, ctx, got)
	require.Nil(t, callback)
	derived := context.WithValue(ctx, resultStateKey{}, "derived")
	var event wf.TaskEndEvent
	provider = wf.ExecutionWorker{ObserveTaskResults: func(in context.Context) (context.Context, func(wf.TaskEndEvent)) {
		require.Same(t, ctx, in)
		return derived, func(e wf.TaskEndEvent) { event = e }
	}}
	got, callback = provider.NewTaskResultObserver(ctx)
	require.Same(t, derived, got)
	callback(wf.TaskEndEvent{InvocationID: "forwarded"})
	require.Equal(t, "forwarded", event.InvocationID)
}
