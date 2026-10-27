package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

// Only declarations may be reconstructed beyond the committed prefix. A missing
// outcome or control decision must never execute its live construction callback.
func TestReplayFrontierDoesNotInventDurableEvents(t *testing.T) {
	for _, kind := range []string{"scope", "task", "attempt", "result", "catch", "decision", "wait", "sleep", "join", "admission", "dispatch"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			e := &executionRun{r: &workerRunner{replay: true}, ordinal: 7, initialVersions: jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion}, active: map[*executionContext]bool{}}
			c := &executionContext{run: e, ctx: ctx, cancel: cancel, deadline: time.Now().Add(-time.Hour)}
			calls := 0
			rec, err := c.event(kind, "boundary", func() *jobdb.ExecutionRecord { calls++; return &jobdb.ExecutionRecord{Descriptor: []byte(`{}`)} })
			if kind == "scope" || kind == "task" || kind == "attempt" {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				require.Equal(t, "boundary", rec.ID)
				require.True(t, rec.At.IsZero())
			} else {
				var miss ReplayCacheMissError
				require.ErrorAs(t, err, &miss)
				require.Zero(t, calls)
			}
			require.EqualValues(t, 7, e.ordinal)
			require.Empty(t, e.pending)
			require.False(t, e.live)
			require.Nil(t, c.timer)
			require.NoError(t, c.ctx.Err())
		})
	}
}

func TestReplayUnfinishedBackoffAndMissingCatch(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	e := &executionRun{r: &workerRunner{replay: true}, ordinal: 5, active: map[*executionContext]bool{}}
	c := &executionContext{run: e, ctx: ctx, cancel: cancel, deadline: time.Now().Add(-time.Hour)}
	var miss ReplayCacheMissError
	require.ErrorAs(t, c.waitUntil(time.Now().Add(time.Hour), time.Time{}), &miss)
	require.Equal(t, ReplayCacheMissAwaitNotReady, miss.Reason)
	// Elapsed backoff permits reconstruction, even after execution deadlines.
	require.NoError(t, c.waitUntil(time.Now().Add(-time.Minute), time.Time{}))
	predicateCalls := 0
	rules := []CatchRule{{ID: "catch", Match: func(error) bool { predicateCalls++; return true }, Handle: func(ExecutionContext, error) (CatchResult, error) { panic("unrecorded handler") }}}
	p, err := makeExecutionPolicy(ScopeOptions{Catch: rules})
	require.NoError(t, err)
	_, _, _, err = c.handleFailure("task/attempt:1", time.Time{}, p, rules, 1, nil, errors.New("recorded failure"), nil)
	require.ErrorAs(t, err, &miss)
	require.Equal(t, ReplayCacheMissAwaitNotReady, miss.Reason)
	require.Zero(t, predicateCalls)
	require.Empty(t, e.pending)
	require.False(t, e.live)
}
