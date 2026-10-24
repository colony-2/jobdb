package workflow

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type taskDelegationRuntime struct {
	WorkflowRuntime
	create func(context.Context, ExecutionLease, jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error)
}

func (r *taskDelegationRuntime) CreateDelegation(ctx context.Context, parent ExecutionLease, req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
	return r.create(ctx, parent, req)
}

type taskDelegationParent struct {
	*sessionTestLease
	info *jobdb.DelegationInfo
}

func (p *taskDelegationParent) Delegation() *jobdb.DelegationInfo { return p.info }

func TestTaskDelegationBounds(t *testing.T) {
	base := time.Now().UTC().Add(time.Hour)
	for _, test := range []struct {
		name                                 string
		engine, execution, parent, requested time.Time
		want                                 time.Time
	}{
		{name: "unbounded"},
		{name: "engine", engine: base, want: base},
		{name: "execution-context", engine: base.Add(time.Hour), execution: base, want: base},
		{name: "parent", engine: base.Add(time.Hour), parent: base, requested: base.Add(2 * time.Hour), want: base},
		{name: "requested", engine: base.Add(time.Hour), parent: base.Add(2 * time.Hour), requested: base, want: base},
		{name: "later-request-is-capped", engine: base, requested: base.Add(time.Hour), want: base},
		{name: "parent-inherited", parent: base, want: base},
	} {
		t.Run(test.name, func(t *testing.T) {
			execution := context.Background()
			if !test.execution.IsZero() {
				var cancel context.CancelFunc
				execution, cancel = context.WithDeadline(execution, test.execution)
				defer cancel()
			}
			execution, cancel := context.WithCancelCause(execution)
			defer cancel(context.Canceled)
			parent := &taskDelegationParent{sessionTestLease: newSessionTestLease()}
			if !test.parent.IsZero() {
				parent.info = &jobdb.DelegationInfo{NotAfter: &test.parent}
			}
			// The short renewable lease expiry must not cap the child's hard deadline.
			wrapped := &leaseSession{ExecutionLease: parent, current: parent}
			var received []jobdb.CreateDelegationRequest
			rt := &taskDelegationRuntime{create: func(ctx context.Context, p ExecutionLease, req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
				require.Same(t, wrapped, p)
				received = append(received, req)
				return parent, nil
			}}
			c := &executionContext{ctx: execution, deadline: test.engine, run: &executionRun{r: &workerRunner{lease: wrapped, runtime: rt}}}
			tc := c.taskContext()
			permissions := jobdb.DelegationPermissions{Secrets: []string{"model-key"}, Read: &jobdb.DelegationChapters{Ordinals: []int64{0}}}
			req := jobdb.CreateDelegationRequest{Permissions: permissions}
			if !test.requested.IsZero() {
				req.NotAfter = &test.requested
			}
			// This call timeout is deliberately earlier than every authority deadline.
			callCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			for range 2 {
				child, err := tc.CreateDelegation(callCtx, req)
				require.NoError(t, err)
				require.Same(t, parent, child)
			}
			require.Len(t, received, 2)
			for _, got := range received {
				require.Equal(t, permissions, got.Permissions)
				if test.want.IsZero() {
					require.Nil(t, got.NotAfter)
				} else {
					require.Equal(t, test.want, *got.NotAfter)
				}
				if req.NotAfter != nil {
					require.NotSame(t, req.NotAfter, got.NotAfter)
					require.Equal(t, test.requested, *req.NotAfter)
				}
			}
		})
	}
}

func TestTaskDelegationRejectsUnavailableExecution(t *testing.T) {
	_, err := (TaskContext{}).CreateDelegation(context.Background(), jobdb.CreateDelegationRequest{})
	require.ErrorIs(t, err, jobdb.ErrLeaseDelegationUnsupported)
	for _, kind := range []string{"unsupported", "missing-lease", "replay", "canceled-task", "canceled-call", "expired-engine", "expired-request", "zero-request", "expired-parent", "issuer-error", "canceled-during-issue", "expired-during-issue"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				execution, cancel := context.WithCancelCause(context.Background())
				defer cancel(context.Canceled)
				parent := &taskDelegationParent{sessionTestLease: newSessionTestLease()}
				requested := time.Now().Add(time.Hour)
				req := jobdb.CreateDelegationRequest{}
				calls := 0
				rt := &taskDelegationRuntime{create: func(context.Context, ExecutionLease, jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
					calls++
					if kind == "issuer-error" {
						return nil, jobdb.ErrLeasePermissionDenied
					}
					if kind == "canceled-during-issue" {
						cancel(context.Canceled)
					}
					return parent, nil
				}}
				c := &executionContext{ctx: execution, run: &executionRun{r: &workerRunner{lease: parent, runtime: rt}}}
				callCtx := context.Background()
				want := jobdb.ErrLeasePermissionDenied
				switch kind {
				case "unsupported":
					c.run.r.runtime = nil
					want = jobdb.ErrLeaseDelegationUnsupported
				case "missing-lease":
					c.run.r.lease = nil
					want = jobdb.ErrExecutionLeaseLost
				case "replay":
					c.run.r.replay = true
					want = ErrReplayShouldNeverMutate
				case "canceled-task":
					cancel(context.Canceled)
					want = context.Canceled
				case "canceled-call":
					var stop context.CancelFunc
					callCtx, stop = context.WithCancel(callCtx)
					stop()
					want = context.Canceled
				case "expired-engine":
					c.deadline = time.Now().Add(-time.Second)
				case "expired-parent":
					past := time.Now().Add(-time.Second)
					parent.info = &jobdb.DelegationInfo{NotAfter: &past}
				case "expired-request":
					requested = time.Now().Add(-time.Second)
					req.NotAfter = &requested
				case "zero-request":
					req.NotAfter = new(time.Time)
				case "canceled-during-issue":
					want = context.Canceled
				case "expired-during-issue":
					// A trusted issuer returning after the selected bound must not expose a token.
					c.deadline = time.Now().Add(20 * time.Millisecond)
					rt.create = func(_ context.Context, _ ExecutionLease, req jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
						calls++
						time.Sleep(time.Until(*req.NotAfter) + time.Millisecond)
						return parent, nil
					}
				}
				child, err := c.taskContext().CreateDelegation(callCtx, req)
				require.ErrorIs(t, err, want)
				require.Nil(t, child)
				expectedCalls := 0
				if kind == "issuer-error" || kind == "canceled-during-issue" || kind == "expired-during-issue" {
					expectedCalls = 1
				}
				require.Equal(t, expectedCalls, calls)
			})
		})
	}
}

func TestTaskDelegationDoesNotChangeTimeoutCause(t *testing.T) {
	cause := NewTimeoutError("scope", time.Minute, TimeoutScopeTotal, nil, false)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	c := &executionContext{ctx: ctx, run: &executionRun{r: &workerRunner{}}}
	_, err := c.taskContext().CreateDelegation(context.Background(), jobdb.CreateDelegationRequest{})
	require.True(t, errors.Is(err, cause))
	require.Same(t, cause, context.Cause(ctx))
}
