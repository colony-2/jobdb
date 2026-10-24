package workflow

import (
	"context"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// CreateDelegation issues a restricted lease using this task's execution lease.
// NotAfter is automatically capped by the current attempt, enclosing execution,
// execution-context deadline, and parent capability. A shorter requested deadline
// is preserved. With no finite bound, authority lasts while the parent lease lives.
// The ctx argument bounds this call, not the issued credential's lifetime.
// Permissions are never widened. Issued credentials are not individually revoked
// when the task returns; their deadline and parent liveness still govern access.
// Manually constructed contexts and runtimes without LeaseDelegator support return
// jobdb.ErrLeaseDelegationUnsupported. Offline replay cannot issue credentials.
func (tc TaskContext) CreateDelegation(ctx context.Context, req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
	if tc.createDelegation == nil {
		return nil, jobdb.ErrLeaseDelegationUnsupported
	}
	return tc.createDelegation(ctx, req)
}

func (c *executionContext) createTaskDelegation(ctx context.Context, req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
	if err := context.Cause(c.ctx); err != nil {
		return nil, err
	}
	if c.run.r.replay {
		return nil, ErrReplayShouldNeverMutate
	}
	parent := c.run.r.lease
	if parent == nil {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	issuer, ok := c.run.r.runtime.(jobdb.LeaseDelegator)
	if !ok {
		return nil, jobdb.ErrLeaseDelegationUnsupported
	}

	// Reuse the engine's selected absolute budget, including preparation time.
	// Do not substitute the renewable owner lease's current expiry snapshot.
	deadline := c.deadline
	if outer, ok := c.ctx.Deadline(); ok {
		deadline = earlier(deadline, outer)
	}
	if restricted, ok := parent.(interface{ Delegation() *jobdb.DelegationInfo }); ok {
		if info := restricted.Delegation(); info != nil && info.NotAfter != nil {
			deadline = earlier(deadline, *info.NotAfter)
		}
	}
	if !deadline.IsZero() && (req.NotAfter == nil || deadline.Before(*req.NotAfter)) {
		req.NotAfter = &deadline
	}
	if req.NotAfter != nil {
		// Do not retain or modify a caller-owned time pointer.
		deadline = req.NotAfter.UTC()
		req.NotAfter = &deadline
	}

	callCtx, cancel := invocationCallContext(ctx, c.ctx)
	defer cancel()
	check := func() error {
		// Cancellation propagation can lag; check the execution directly too.
		if err := context.Cause(c.ctx); err != nil {
			return err
		}
		if err := callCtx.Err(); err != nil {
			return err
		}
		if req.NotAfter != nil && !req.NotAfter.After(time.Now()) {
			return jobdb.ErrLeasePermissionDenied
		}
		return nil
	}
	if err := check(); err != nil {
		return nil, err
	}
	child, err := issuer.CreateDelegation(callCtx, parent, req)
	if err != nil {
		return nil, err
	}
	if err := check(); err != nil {
		return nil, err
	}
	return child, nil
}
