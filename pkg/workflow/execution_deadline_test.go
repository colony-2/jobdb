package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestExecutionDeadlineWithoutTimerDelivery(t *testing.T) {
	for _, phase := range []string{"admission", "completion"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			// Keep the timer pending to model a delayed timer goroutine deterministically.
			timer := time.NewTimer(time.Hour)
			defer timer.Stop()
			deadline := time.Now().Add(-time.Second)
			if phase == "completion" {
				deadline = time.Now().Add(10 * time.Millisecond)
			}
			c := &executionContext{ctx: ctx, cancel: cancel, timer: timer, deadline: deadline, totalDeadline: deadline, run: &executionRun{live: true, r: &workerRunner{}}}
			called := false
			out, err := c.invoke(func() (TaskData, error) {
				called = true
				time.Sleep(20 * time.Millisecond)
				return NewTaskDataOrPanic("too late"), nil
			})
			require.Nil(t, out)
			var timeout *jobdb.TimeoutError
			require.ErrorAs(t, err, &timeout)
			require.Equal(t, phase == "completion", called)
		})
	}
}
