package workflow_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type delegationObserver struct{ starts []wf.TaskStartEvent }

func (*delegationObserver) OnJobStart(wf.JobStartEvent)       {}
func (o *delegationObserver) OnTaskStart(e wf.TaskStartEvent) { o.starts = append(o.starts, e) }
func (*delegationObserver) OnTaskEnd(wf.TaskEndEvent)         {}
func (*delegationObserver) OnJobEnd(wf.JobEndEvent)           {}

func childInfo(t *testing.T, child jobdb.ExecutionLease) *jobdb.DelegationInfo {
	t.Helper()
	info := child.(interface{ Delegation() *jobdb.DelegationInfo }).Delegation()
	require.NotNil(t, info)
	return info
}

func TestTaskDelegationExecutionBudgets(t *testing.T) {
	for _, kind := range []string{"unbounded", "task-attempt", "task-total", "scope-attempt", "scope-total", "job-attempt", "job-total", "caller", "retry", "external-recovery"} {
		t.Run(kind, func(t *testing.T) {
			rt := alternateRuntime(t, "remote")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			short, long := jobdb.Duration(time.Minute), jobdb.Duration(time.Hour)
			call := invocation("delegate")
			var scope wf.ScopeOptions
			var jobPolicy jobdb.RunPolicy
			var callerDeadline time.Time
			if kind != "unbounded" {
				call.Policy.InvocationTimeout = &long
				scope.AttemptTimeout = time.Hour
			}
			switch kind {
			case "task-attempt", "external-recovery":
				call.Policy.InvocationTimeout = &short
			case "task-total", "retry":
				call.Policy.TotalTimeout = &short
				if kind == "retry" {
					call.Policy.Retry.MaximumAttempts = 2
				}
			case "scope-attempt":
				scope.AttemptTimeout = time.Minute
			case "scope-total":
				scope.TotalTimeout = time.Minute
			case "job-attempt":
				jobPolicy.InvocationTimeout = &short
			case "job-total":
				jobPolicy.TotalTimeout = &short
			case "caller":
				callerDeadline = time.Now().Add(time.Minute).UTC()
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(ctx, callerDeadline)
				defer stop()
			}
			var deadlines []*time.Time
			var saved wf.TaskContext
			var preparedAt time.Time
			task := executionTestTask{"echo", func(tc wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				saved = tc
				req := jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Read: &jobdb.DelegationChapters{Ordinals: []int64{0}}}}
				child, err := tc.CreateDelegation(ctx, req)
				require.NoError(t, err)
				first := childInfo(t, child).NotAfter
				time.Sleep(time.Millisecond)
				again, err := tc.CreateDelegation(ctx, req)
				require.NoError(t, err)
				require.Equal(t, first, childInfo(t, again).NotAfter)
				deadlines = append(deadlines, first)
				if kind == "retry" && len(deadlines) == 1 {
					return nil, errors.New("retry")
				}
				return in, nil
			}}
			w := wf.ExecutionWorker{JobType: "task-delegation", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("outer", scope, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					return s.DoScope("inner", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
							time.Sleep(5 * time.Millisecond)
							preparedAt = time.Now()
							return in, nil
						})
					})
				})
			}}
			h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: w.Name(), RunPolicy: jobPolicy, Data: jobdb.NewTaskDataOrPanic("input")}})
			require.NoError(t, err)
			run := func(tasks []wf.TaskWorker, observer wf.ReplayObserver) wf.JobRunOutcome {
				r, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: w, TaskWorkers: tasks, WorkerID: "delegate", LeaseDuration: time.Hour})
				require.NoError(t, err)
				out, err := r.Run(observer)
				require.NoError(t, err)
				return out
			}
			if kind == "external-recovery" {
				require.Equal(t, wf.JobRunSuspended, run(nil, nil).Status)
				time.Sleep(5 * time.Millisecond)
			}
			observer := &delegationObserver{}
			require.Equal(t, wf.JobRunCompleted, run([]wf.TaskWorker{task}, observer).Status)
			require.NotEmpty(t, deadlines)
			var want time.Time
			switch kind {
			case "task-attempt":
				want = observer.starts[0].At.Add(time.Minute)
				require.True(t, want.Before(preparedAt.Add(time.Minute)), "preparation must consume the attempt budget")
			case "caller":
				want = callerDeadline
			case "unbounded":
			default:
				for _, rec := range executionRecords(t, rt, h.JobKey) {
					switch {
					case (kind == "task-total" || kind == "retry") && rec.Kind == "task":
						want = rec.TotalDeadline
					case kind == "external-recovery" && rec.Kind == "wait":
						want = rec.Deadline
					case kind == "scope-total" && rec.Kind == "scope" && strings.Contains(rec.ID, "outer"):
						want = rec.TotalDeadline
					case kind == "scope-attempt" && rec.Kind == "attempt" && strings.Contains(rec.ID, "outer") && !strings.Contains(rec.ID, "inner"):
						want = rec.Deadline
					case kind == "job-total" && rec.Kind == "scope" && strings.Contains(rec.ID, "$job") && !strings.Contains(rec.ID, "outer"):
						want = rec.TotalDeadline
					case kind == "job-attempt" && rec.Kind == "attempt" && strings.Contains(rec.ID, "$job") && !strings.Contains(rec.ID, "outer"):
						want = rec.Deadline
					}
				}
				require.False(t, want.IsZero(), "expected durable budget for %s", kind)
			}
			for _, got := range deadlines {
				if kind == "unbounded" {
					require.Nil(t, got)
				} else {
					require.NotNil(t, got)
					require.True(t, want.Equal(*got), "want %s, got %s", want, *got)
				}
			}
			if kind == "retry" {
				require.Len(t, deadlines, 2)
			}
			_, err = saved.CreateDelegation(context.Background(), jobdb.CreateDelegationRequest{})
			require.Error(t, err, "completed task must not issue credentials")
			count := len(deadlines)
			before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
			require.NoError(t, err)
			_, err = replayExecutionTest(t, rt, h.JobKey, w, task)
			require.NoError(t, err)
			require.Len(t, deadlines, count, "replay must not issue credentials")
			after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestTaskDelegationRemoteHandoff(t *testing.T) {
	backend := alternateRuntime(t, "sqlite")
	handler := remote.NewServer(backend)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	ownerRuntime, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	childRuntime, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	ctx := context.Background()
	var imported jobdb.RenewableExecutionLease
	var owner jobdb.ExecutionLease
	var saved wf.TaskContext
	read := func(lease jobdb.ExecutionLease, ordinal int64) error {
		_, err := childRuntime.GetChapter(ctx, jobdb.ChapterRef{JobKey: lease.Job().JobKey, Ordinal: ordinal, LeaseID: lease.LeaseID(), LeaseToken: lease.(interface{ LeaseToken() string }).LeaseToken()})
		return err
	}
	task := executionTestTask{"echo", func(tc wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
		saved = tc
		req := jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Read: &jobdb.DelegationChapters{Ordinals: []int64{0}}}}
		before := requests.Load()
		child, err := tc.CreateDelegation(ctx, req)
		require.NoError(t, err)
		require.Equal(t, before, requests.Load(), "creation must be offline")
		capability, err := remote.ExportLease(child)
		require.NoError(t, err)
		encoded, err := capability.Encode()
		require.NoError(t, err)
		capability, err = remote.DecodeLeaseCapability(encoded)
		require.NoError(t, err)
		imported, err = childRuntime.ImportLease(ctx, capability)
		require.NoError(t, err)
		require.NoError(t, read(imported, 0))
		require.ErrorIs(t, read(imported, 999), jobdb.ErrLeasePermissionDenied)
		refreshed, err := imported.Renew(ctx)
		require.NoError(t, err)
		require.Equal(t, childInfo(t, imported), childInfo(t, refreshed))

		// A requested shorter deadline is enforced by the server while the owner lives.
		deadline := time.Now().Add(200 * time.Millisecond)
		req.NotAfter = &deadline
		short, err := tc.CreateDelegation(ctx, req)
		require.NoError(t, err)
		require.True(t, deadline.Equal(*childInfo(t, short).NotAfter))
		time.Sleep(time.Until(deadline) + time.Millisecond)
		require.ErrorIs(t, read(short, 0), jobdb.ErrLeasePermissionDenied)
		_, err = short.Renew(ctx)
		require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
		require.NoError(t, read(imported, 0))
		require.NoError(t, read(owner, 0))
		return in, nil
	}}
	w := wf.ExecutionWorker{JobType: "delegation-handoff", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		out, err := c.DoTask(invocation("delegate"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
		if err != nil {
			return nil, err
		}
		// Ending a task closes issuance, but does not individually revoke issued children.
		_, err = saved.CreateDelegation(ctx, jobdb.CreateDelegationRequest{})
		require.Error(t, err)
		require.NoError(t, read(imported, 0))
		return out, nil
	}}
	key := submitExecutionTest(t, ownerRuntime, w)
	r, err := wf.GetJobForRun(ctx, ownerRuntime, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "owner", LeaseDuration: time.Hour, BeforeRun: func(_ context.Context, lease jobdb.ExecutionLease) error { owner = lease; return nil }})
	require.NoError(t, err)
	out, err := r.Run(nil)
	require.NoError(t, err)
	require.Equal(t, wf.JobRunCompleted, out.Status)
	require.ErrorIs(t, read(imported, 0), jobdb.ErrExecutionLeaseLost)
	_, err = imported.Renew(ctx)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}
