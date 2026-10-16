package workflow

import "time"

// ExpireExecutionScopeForTest lets integration tests expire a scope at a known
// admission point without depending on transport speed or timer scheduling.
// This hook is compiled only into tests; it is not part of the client API.
func ExpireExecutionScopeForTest(scope ExecutionContext) {
	c := scope.(*executionContext)
	c.run.mu.Lock()
	defer c.run.mu.Unlock()
	c.deadline = time.Now().Add(-time.Second)
	c.totalDeadline = c.deadline
	c.armLocked()
}

// ExpireExecutionAttemptForTest expires an admitted attempt without exhausting
// the total budget available to its catch handler or retries.
func ExpireExecutionAttemptForTest(scope ExecutionContext) {
	c := scope.(*executionContext)
	c.run.mu.Lock()
	defer c.run.mu.Unlock()
	c.deadline = time.Now().Add(-time.Second)
	c.armLocked()
}

// ExpireExecutionTaskAttemptForTest targets the already-running task instead of
// its enclosing scope, so only the task's retry budget is exercised.
func ExpireExecutionTaskAttemptForTest(scope ExecutionContext, task TaskContext) bool {
	run := scope.(*executionContext).run
	run.mu.Lock()
	defer run.mu.Unlock()
	for c := range run.active {
		if c.ctx == task.Context() {
			c.deadline = time.Now().Add(-time.Second)
			c.armLocked()
			return true
		}
	}
	return false
}
