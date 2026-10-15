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
