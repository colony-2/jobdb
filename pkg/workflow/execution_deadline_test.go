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
				deadline = time.Now().Add(time.Hour)
			}
			c := &executionContext{ctx: ctx, cancel: cancel, timer: timer, deadline: deadline, totalDeadline: deadline, run: &executionRun{live: true, r: &workerRunner{}}}
			called := false
			out, err := c.invoke(func() (TaskData, error) {
				called = true
				// Expire only after the callback starts; scheduling delays cannot
				// accidentally turn the completion case into an admission case.
				c.run.mu.Lock()
				c.deadline = time.Now().Add(-time.Second)
				c.totalDeadline = c.deadline
				c.run.mu.Unlock()
				return NewTaskDataOrPanic("too late"), nil
			})
			require.Nil(t, out)
			var timeout *jobdb.TimeoutError
			require.ErrorAs(t, err, &timeout)
			require.Equal(t, phase == "completion", called)
		})
	}
}

func TestExecutionStartedWorkerAndHandlerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	run := &executionRun{live: true, r: &workerRunner{}, active: map[*executionContext]bool{}}
	root := &executionContext{ctx: ctx, cancel: cancel, run: run}
	attemptDeadline := time.Now().Add(time.Hour)
	totalDeadline := attemptDeadline.Add(time.Hour)
	body := root.child("body", attemptDeadline, totalDeadline)
	defer body.close()
	nested := body.child("nested-task", attemptDeadline, totalDeadline)
	defer nested.close()

	start := func(c *executionContext) <-chan error {
		callbackContext := c.Context()
		invoke := c.invoke
		if c == nested {
			callbackContext = c.taskContext().Context()
			invoke = c.invokeAsync
		}
		started := make(chan struct{})
		stopped := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := invoke(func() (TaskData, error) {
				defer close(stopped)
				close(started)
				<-callbackContext.Done()
				return nil, context.Cause(callbackContext)
			})
			<-stopped
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("callback did not start")
		}
		return done
	}
	expire := func(c *executionContext) {
		run.mu.Lock()
		c.deadline = time.Now().Add(-time.Second)
		c.armLocked()
		run.mu.Unlock()
	}
	awaitTimeout := func(done <-chan error) {
		select {
		case err := <-done:
			var timeout *jobdb.TimeoutError
			require.ErrorAs(t, err, &timeout)
		case <-time.After(5 * time.Second):
			t.Fatal("started callback did not observe cancellation")
		}
	}
	workerDone := start(nested)
	expire(body)
	awaitTimeout(workerDone)
	require.NoError(t, root.Context().Err())

	// A handler is a fresh child of the enclosing context. Attempt expiry must
	// not cancel it; expiry of its remaining budget must cancel it.
	handler := root.child("handler", totalDeadline, totalDeadline)
	defer handler.close()
	require.NoError(t, handler.Context().Err())
	handlerDone := start(handler)
	expire(handler)
	awaitTimeout(handlerDone)
}

func TestExecutionRecordedTimeoutUnwindsNestedScopes(t *testing.T) {
	for _, kind := range []string{"timeout", "application-error", "sibling"} {
		t.Run(kind, func(t *testing.T) {
			id := "/parent/attempt:1"
			errorKind := payloadKindTimeout
			if kind == "application-error" {
				errorKind = payloadKindAppError
			}
			if kind == "sibling" {
				id = "/sibling/attempt:1"
			}
			rec := jobdb.ExecutionRecord{Kind: "result", ID: id, ErrorKind: errorKind}
			e := &executionRun{r: &workerRunner{replay: true}, active: map[*executionContext]bool{}, events: []executionEvent{{record: rec, ordinal: 7}}}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			c := &executionContext{run: e, ctx: ctx, cancel: cancel}
			_, err := c.finishBody("result", "/parent/attempt:1/child/attempt:1/grandchild/attempt:1", nil, errRecordedBoundary, nil)
			if kind != "timeout" {
				require.ErrorIs(t, err, ErrWorkflowNotDeterministic)
				require.Zero(t, e.cursor)
				return
			}
			require.ErrorIs(t, err, errRecordedBoundary)
			require.Nil(t, e.fatal)
			require.Zero(t, e.cursor, "inner scopes must not consume an ancestor's outcome")
			_, err = c.finishBody("result", "/parent/attempt:1/child/attempt:1", nil, err, nil)
			require.ErrorIs(t, err, errRecordedBoundary)
			require.Zero(t, e.cursor)
			got, err := c.finishBody("result", "/parent/attempt:1", nil, err, nil)
			require.NoError(t, err)
			require.Equal(t, rec, got)
			require.Equal(t, 1, e.cursor)
		})
	}
}
