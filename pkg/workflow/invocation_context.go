package workflow

import "context"

// Keep caller values and cancellation, while also bounding the call by the
// enclosing worker invocation (even when the caller uses context.Background).
func invocationCallContext(ctx, invocation context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := context.WithCancel(ctx)
	if invocation == nil {
		return callCtx, cancel
	}
	if deadline, ok := invocation.Deadline(); ok {
		var deadlineCancel context.CancelFunc
		callCtx, deadlineCancel = context.WithDeadline(callCtx, deadline)
		oldCancel := cancel
		cancel = func() { deadlineCancel(); oldCancel() }
	}
	stop := context.AfterFunc(invocation, cancel)
	if invocation.Err() != nil {
		cancel()
	}
	return callCtx, func() { stop(); cancel() }
}
