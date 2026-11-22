package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// ResourceBackoffHarness is deliberately separate from Harness: old backend
// owners continue to compile. Durable mode requires real storage controls.
type ResourceBackoffHarness struct {
	Name    string
	Durable bool
	New     func(testing.TB) ResourceBackoffFixture
}
type ResourceBackoffFixture struct {
	Runtime jobdb.ConsumerRuntime
	// ClaimAppend exercises the backend append CAS directly and releases a successful claim.
	ClaimAppend func(context.Context, jobdb.JobKey, time.Time) error
	// SetTime controls backend eligibility and transaction time, not client time.
	SetTime   func(time.Time)
	SetRandom func(float64)
	// Reopen must close/reconnect actual storage and return a newly opened runtime.
	Reopen func() jobdb.ConsumerRuntime
	// Fault injects a transaction error after validated/sequence/job/receipt.
	// An empty stage disables injection.
	Fault func(string)
	// Barrier blocks a real transition after validation inside its atomic section.
	Barrier func() (entered <-chan struct{}, release func())
}

func (f ResourceBackoffFixture) Validate(durable bool) error {
	if f.Runtime == nil || f.SetTime == nil || f.SetRandom == nil || f.ClaimAppend == nil {
		return fmt.Errorf("resource conformance requires runtime, append claim, backend clock and RNG")
	}
	if durable && (f.Reopen == nil || f.Fault == nil || f.Barrier == nil) {
		return fmt.Errorf("durable resource conformance requires reopen, transaction faults and barrier")
	}
	return nil
}

// PrepareResourceBackoffCheckpoint writes a genuine versioned checkpoint through
// the public chapter contract. Backend owners can use it in external consumers.
func PrepareResourceBackoffCheckpoint(ctx context.Context, rt jobdb.ConsumerRuntime, lease jobdb.ExecutionLease, id string, attempt int, policy jobdb.ResourceBackoffPolicy) (jobdb.ResourceBackoffRequest, error) {
	return prepareResourceCheckpoint(ctx, rt, lease, id, attempt, policy, time.Time{})
}

func prepareResourceCheckpoint(ctx context.Context, rt jobdb.ConsumerRuntime, lease jobdb.ExecutionLease, id string, attempt int, policy jobdb.ResourceBackoffPolicy, deadline time.Time) (jobdb.ResourceBackoffRequest, error) {
	chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: lease.Job().JobKey})
	if err != nil {
		return jobdb.ResourceBackoffRequest{}, err
	}
	rec := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "resource_checkpoint", ID: id, Deadline: deadline, TotalDeadline: deadline, At: time.Unix(1700000000, 0).UTC(), Resource: &jobdb.ResourceCheckpoint{Version: 1, Invocation: id, Attempt: attempt, Policy: policy, ExecutionTaskType: lease.Route().TaskType, PayloadRevision: lease.ClientPayloadRevision()}}
	rec.Payload, _ = json.Marshal(rec.Resource)
	raw, _ := json.Marshal(rec)
	ordinal := int64(len(chapters))
	err = rt.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), Ref: jobdb.ChapterRef{JobKey: lease.Job().JobKey, Ordinal: ordinal}, Chapter: jobdb.Chapter{Ordinal: ordinal, TaskType: "__execution__", CreatedAt: rec.At, Body: jobdb.ExecutionChapter{Record: raw}}})
	if err != nil {
		return jobdb.ResourceBackoffRequest{}, err
	}
	task := lease.ExecutionState().TaskWait
	if task != nil {
		copy := *task
		copy.OutputOrdinal = ordinal + 1
		task = &copy
	}
	return jobdb.ResourceBackoffRequest{Version: 1, RequestID: fmt.Sprintf("checkpoint-%d", ordinal), Invocation: id, Attempt: attempt, Reason: jobdb.ResourceBackoffReason, Policy: policy, Resume: jobdb.ResourceResume{Version: 1, CheckpointOrdinal: ordinal, CheckpointHash: jobdb.ResourceCheckpointHash(raw), Invocation: id, Attempt: attempt, Route: lease.Route(), TaskWait: task, PayloadRevision: lease.ClientPayloadRevision()}}, nil
}

// RunResourceBackoffConformance exercises public backend semantics. It rejects
// an empty -run selection, missing fixtures, and false durability advertisement.
// Workflow, HTTP authority and offline migrations have additional owner suites.
func RunResourceBackoffConformance(t *testing.T, h ResourceBackoffHarness) {
	t.Helper()
	if h.Name == "" || h.New == nil {
		t.Fatal("resource harness name and constructor required")
	}
	selected := 0
	run := func(name string, check func(*testing.T, *resourceCase)) {
		t.Run(name, func(t *testing.T) {
			f := h.New(t)
			require.NoError(t, f.Validate(h.Durable))
			c := &resourceCase{t: t, f: f, ctx: celltest.Context(context.Background()), at: time.Now().UTC().Truncate(time.Millisecond)}
			c.f.SetTime(c.at)
			c.f.SetRandom(.5)
			c.setup()
			require.NoError(t, c.lease.ExecutionState().ResourceBackoff.Validate(h.Durable))
			if !h.Durable {
				require.Equal(t, 0, c.lease.ExecutionState().ResourceBackoff.DurabilityVersion)
			}
			selected++
			check(t, c)
		})
	}
	run("B03f_policy_binding", func(t *testing.T, c *resourceCase) {
		c.accept()
		c.wake()
		p := c.req.Policy
		p.Jitter = 0
		next := c.prepare(c.req.Invocation, c.req.Attempt, p)
		_, err := c.port().RescheduleResourceBackoff(c.ctx, next)
		require.ErrorIs(t, err, jobdb.ErrConflict)
		next = c.prepare(c.req.Invocation, 2, p)
		receipt, err := c.port().RescheduleResourceBackoff(c.ctx, next)
		require.NoError(t, err)
		require.EqualValues(t, 1, receipt.Count)
	})
	run("B04_B04a_retained_sequences", func(t *testing.T, c *resourceCase) {
		c.accept()
		c.wake()
		other := c.prepare("other/attempt:1", 1, c.req.Policy)
		r, err := c.port().RescheduleResourceBackoff(c.ctx, other)
		require.NoError(t, err)
		require.EqualValues(t, 1, r.Count)
		c.at = r.WakeAt
		c.f.SetTime(c.at)
		c.claim()
		same := c.prepare(c.req.Invocation, 1, c.req.Policy)
		r, err = c.port().RescheduleResourceBackoff(c.ctx, same)
		require.NoError(t, err)
		require.EqualValues(t, 2, r.Count)
	})

	run("B04_tenant_job_and_attempt_isolation", func(t *testing.T, c *resourceCase) {
		first := c.accept()
		for _, key := range []jobdb.JobKey{{TenantId: c.key.TenantId, JobId: "another-job"}, {TenantId: "another-tenant", JobId: c.key.JobId}} {
			h, err := celltest.Submit(c.f.Runtime, c.ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: key.TenantId, JobID: key.JobId, Cell: "test", CellRepo: "example.test/test", JobType: "resource", Data: jobdb.NewTaskDataOrPanic("another")}})
			require.NoError(t, err)
			other := &resourceCase{t: t, f: c.f, ctx: c.ctx, at: c.at, key: h.JobKey}
			other.claim()
			other.req = other.prepare(c.req.Invocation, 1, c.req.Policy)
			receipt := other.accept()
			require.EqualValues(t, 1, receipt.Count)
		}
		again, err := c.port().RescheduleResourceBackoff(c.ctx, c.req)
		require.NoError(t, err)
		require.Equal(t, first, again)
	})
	run("B05_B10_atomic_validation", func(t *testing.T, c *resourceCase) {
		before := c.info()
		bad := c.req
		bad.Resume.CheckpointHash = string(make([]byte, 64))
		_, err := c.port().RescheduleResourceBackoff(c.ctx, bad)
		require.Error(t, err)
		require.Equal(t, before, c.info())
		bad = c.req
		rev := int64(77)
		bad.ClientPayloadUpdate = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"n":1}`), ExpectedRevision: &rev}
		_, err = c.port().RescheduleResourceBackoff(c.ctx, bad)
		require.Error(t, err)
		require.Equal(t, before, c.info())
		c.accept()
	})
	run("B06_exact_receipt", func(t *testing.T, c *resourceCase) {
		old := c.lease
		first := c.accept()
		c.f.SetRandom(0)
		again, err := old.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(c.ctx, c.req)
		require.NoError(t, err)
		require.Equal(t, first, again)
		bad := c.req
		bad.Policy.Jitter = 0
		_, err = old.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(c.ctx, bad)
		require.ErrorIs(t, err, jobdb.ErrConflict)
	})
	run("B06a_late_duplicate", func(t *testing.T, c *resourceCase) {
		old := c.lease
		first := c.accept()
		c.wake()
		c.at = c.at.Add(2 * time.Minute)
		c.f.SetTime(c.at)
		c.claim()
		before := c.info()
		again, err := old.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(c.ctx, c.req)
		require.NoError(t, err)
		require.Equal(t, first, again)
		require.Equal(t, before, c.info())
		require.EqualValues(t, 4, *before.ExecutionState.CrashBudgetRemaining)
	})
	run("B07_competing_transitions", func(t *testing.T, c *resourceCase) {
		old := c.lease
		var wg sync.WaitGroup
		results := make(chan error, 2)
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				r := c.req
				r.RequestID = fmt.Sprint(i)
				_, err := old.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(c.ctx, r)
				results <- err
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		wins := 0
		for err := range results {
			if err == nil {
				wins++
			}
		}
		require.Equal(t, 1, wins)
		require.EqualValues(t, 5, *c.info().ExecutionState.CrashBudgetRemaining)
	})
	run("B07a_single_reclaim", func(t *testing.T, c *resourceCase) {
		c.at = c.at.Add(2 * time.Minute)
		c.f.SetTime(c.at)
		var wg sync.WaitGroup
		leases := make(chan jobdb.ExecutionLease, 2)
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				l, err := c.f.Runtime.GetJobLease(c.ctx, c.claimRequest())
				require.NoError(t, err)
				leases <- l
			}()
		}
		wg.Wait()
		close(leases)
		wins := 0
		for l := range leases {
			if l != nil {
				wins++
			}
		}
		require.Equal(t, 1, wins)
		require.EqualValues(t, 4, *c.info().ExecutionState.CrashBudgetRemaining)
	})
	run("B07a_task_poll_target_completion_append_reclaim", func(t *testing.T, c *resourceCase) {
		c.taskRoute()
		state := c.lease.ExecutionState().TaskWait
		route := c.lease.Route()
		c.at = c.at.Add(2 * time.Minute)
		c.f.SetTime(c.at)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var completed atomic.Bool
		for _, claim := range []func(){
			func() {
				_, err := c.f.Runtime.GetJobLease(c.ctx, jobdb.GetJobLeaseRequest{JobKey: c.key, WorkerID: "target", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
				require.NoError(t, err)
			},
			func() {
				_, err := c.f.Runtime.PollWork(c.ctx, jobdb.PollWorkRequest{TenantId: c.key.TenantId, WorkerID: "poll", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
				require.NoError(t, err)
			},
			func() { _ = c.f.ClaimAppend(c.ctx, c.key, c.at) },
			func() {
				err := c.f.Runtime.CompleteTaskIfWaiting(c.ctx, jobdb.CompleteTaskIfWaitingRequest{JobKey: c.key, Route: route, InputOrdinal: state.InputOrdinal, OutputOrdinal: state.OutputOrdinal, InputHash: state.InputHash, ResumeJobType: state.ResumeJobType, Data: jobdb.NewTaskDataOrPanic("answer")})
				completed.Store(err == nil)
			},
		} {
			wg.Add(1)
			go func(claim func()) { defer wg.Done(); <-start; claim() }(claim)
		}
		close(start)
		wg.Wait()
		want := int64(4)
		if completed.Load() {
			want = 5 // Successful completion performs an ordinary reschedule.
		}
		require.EqualValues(t, want, *c.info().ExecutionState.CrashBudgetRemaining, "one expired execution lease was reclaimed; successful ordinary reschedule resets the default")
	})
	run("B09_B09b_independent_gate", func(t *testing.T, c *resourceCase) {
		c.accept()
		l, err := c.f.Runtime.GetJobLease(c.ctx, c.claimRequest())
		require.NoError(t, err)
		require.Nil(t, l)
		ls, err := c.f.Runtime.PollWork(c.ctx, jobdb.PollWorkRequest{TenantId: c.key.TenantId, WorkerID: "poll", Routes: []jobdb.Route{{JobType: "resource"}}})
		require.NoError(t, err)
		require.Empty(t, ls)
		listed, err := c.f.Runtime.ListJobs(c.ctx, jobdb.ListJobsRequest{TenantIds: []string{c.key.TenantId}, AvailableRoutes: []jobdb.Route{{JobType: "resource"}}})
		require.NoError(t, err)
		require.Empty(t, listed.Jobs)
		require.Equal(t, jobdb.JobStatusAwaitingFuture, c.info().Status)
		c.wake()
	})

	run("B09_task_completion_gate", func(t *testing.T, c *resourceCase) {
		c.taskRoute()
		c.req = c.prepare("external/attempt:1", 1, c.req.Policy)
		c.accept()
		before := c.info()
		completion := jobdb.CompleteTaskIfWaitingRequest{JobKey: c.key, Route: c.req.Resume.Route, InputOrdinal: c.req.Resume.TaskWait.InputOrdinal, OutputOrdinal: c.req.Resume.TaskWait.OutputOrdinal, InputHash: c.req.Resume.TaskWait.InputHash, ResumeJobType: "resource", Data: jobdb.NewTaskDataOrPanic("external output")}
		require.Error(t, c.f.Runtime.CompleteTaskIfWaiting(c.ctx, completion))
		require.Equal(t, before, c.info())
		l, err := c.f.Runtime.GetJobLease(c.ctx, jobdb.GetJobLeaseRequest{JobKey: c.key, WorkerID: "task", Routes: []jobdb.Route{c.req.Resume.Route}})
		require.NoError(t, err)
		require.Nil(t, l)
		c.at = c.receipt.WakeAt
		c.f.SetTime(c.at)
		require.NoError(t, c.f.Runtime.CompleteTaskIfWaiting(c.ctx, completion))
		require.EqualValues(t, 5, *c.info().ExecutionState.CrashBudgetRemaining)
	})

	run("B09b_deadline_keeps_resource_gate", func(t *testing.T, c *resourceCase) {
		c.taskRoute()
		p := jobdb.DefaultResourceBackoffPolicy()
		p.Initial, p.Maximum, p.Jitter = 30*time.Second, 30*time.Second, 0
		deadline := c.at.Add(time.Second)
		var err error
		c.req, err = prepareResourceCheckpoint(c.ctx, c.f.Runtime, c.lease, "external/attempt:1", 1, p, deadline)
		require.NoError(t, err)
		first := c.accept()
		require.True(t, deadline.Before(first.WakeAt))
		if h.Durable {
			c.f.Runtime = c.f.Reopen()
		}
		c.f.SetTime(deadline.Add(time.Nanosecond))
		before := c.info()
		chapters, err := c.f.Runtime.ListChapters(c.ctx, jobdb.ListChaptersRequest{JobKey: c.key})
		require.NoError(t, err)
		wait := c.req.Resume.TaskWait
		require.ErrorIs(t, c.f.Runtime.CompleteTaskIfWaiting(c.ctx, jobdb.CompleteTaskIfWaitingRequest{JobKey: c.key, Route: c.req.Resume.Route, InputOrdinal: wait.InputOrdinal, OutputOrdinal: wait.OutputOrdinal, InputHash: wait.InputHash, ResumeJobType: wait.ResumeJobType, Data: jobdb.NewTaskDataOrPanic("too early")}), jobdb.ErrConflict)
		require.Error(t, c.f.ClaimAppend(c.ctx, c.key, deadline.Add(time.Nanosecond)))
		l, err := c.f.Runtime.GetJobLease(c.ctx, jobdb.GetJobLeaseRequest{JobKey: c.key, Routes: []jobdb.Route{c.req.Resume.Route}})
		require.NoError(t, err)
		require.Nil(t, l)
		ls, err := c.f.Runtime.PollWork(c.ctx, jobdb.PollWorkRequest{TenantId: c.key.TenantId, Routes: []jobdb.Route{c.req.Resume.Route}})
		require.NoError(t, err)
		require.Empty(t, ls)
		listed, err := c.f.Runtime.ListJobs(c.ctx, jobdb.ListJobsRequest{TenantIds: []string{c.key.TenantId}, AvailableRoutes: []jobdb.Route{c.req.Resume.Route}})
		require.NoError(t, err)
		require.Empty(t, listed.Jobs)
		require.Equal(t, before, c.info(), "early output/append/claims must not mutate state")
		after, err := c.f.Runtime.ListChapters(c.ctx, jobdb.ListChaptersRequest{JobKey: c.key})
		require.NoError(t, err)
		require.Equal(t, chapters, after)
		// Deadline handling is independently eligible on the job route.
		require.Equal(t, jobdb.JobStatusReady, before.Status)
		c.claim()
		require.Equal(t, jobdb.Route{JobType: "resource"}, c.lease.Route())
	})
	for _, mode := range []string{"job_target", "job_poll", "task_target", "task_poll"} {
		run("B11a_zero_credit_deadline_"+mode, func(t *testing.T, c *resourceCase) {
			if strings.HasPrefix(mode, "task") {
				c.taskRoute()
			}
			p := jobdb.DefaultResourceBackoffPolicy()
			one := int64(1)
			p.CrashAllowance, p.Initial, p.Maximum, p.Jitter = &one, time.Second, time.Second, 0
			deadline := c.at.Add(10 * time.Second)
			var err error
			c.req, err = prepareResourceCheckpoint(c.ctx, c.f.Runtime, c.lease, "work/attempt:1", 1, p, deadline)
			require.NoError(t, err)
			first := c.accept()
			claim := func() jobdb.ExecutionLease {
				l, err := c.f.Runtime.GetJobLease(c.ctx, jobdb.GetJobLeaseRequest{JobKey: c.key, WorkerID: "crash", Routes: []jobdb.Route{c.req.Resume.Route}, LeaseDuration: time.Second})
				require.NoError(t, err)
				return l
			}
			c.f.SetTime(first.WakeAt)
			require.NotNil(t, claim())
			c.f.SetTime(first.WakeAt.Add(time.Second))
			final := claim()
			require.NotNil(t, final)
			require.EqualValues(t, 0, *final.ExecutionState().CrashBudgetRemaining)
			require.Equal(t, jobdb.JobStatusActive, c.info().Status)
			c.f.SetTime(first.WakeAt.Add(2 * time.Second))
			require.Equal(t, jobdb.JobStatusCrashConcern, c.info().Status)
			require.Nil(t, claim(), "exhausted credit cannot admit another body before the deadline")
			if h.Durable {
				c.f.Runtime = c.f.Reopen()
			}
			c.f.SetTime(deadline)
			require.Equal(t, jobdb.JobStatusReady, c.info().Status)
			listed, err := c.f.Runtime.ListJobs(c.ctx, jobdb.ListJobsRequest{TenantIds: []string{c.key.TenantId}, AvailableRoutes: []jobdb.Route{{JobType: "resource"}}})
			require.NoError(t, err)
			require.Len(t, listed.Jobs, 1)
			require.EqualValues(t, 0, *listed.Jobs[0].ExecutionState.CrashBudgetRemaining)
			require.Error(t, c.f.ClaimAppend(c.ctx, c.key, deadline), "terminal admission grants no append credit")
			if strings.HasPrefix(mode, "task") {
				require.Nil(t, claim(), "deadline recovery belongs to the job route")
			}
			var terminal jobdb.ExecutionLease
			if strings.HasSuffix(mode, "poll") {
				ls, err := c.f.Runtime.PollWork(c.ctx, jobdb.PollWorkRequest{TenantId: c.key.TenantId, WorkerID: "timeout", Routes: []jobdb.Route{{JobType: "resource"}}, LeaseDuration: time.Second})
				require.NoError(t, err)
				require.Len(t, ls, 1)
				terminal = ls[0]
			} else {
				terminal, err = c.f.Runtime.GetJobLease(c.ctx, c.claimRequest())
				require.NoError(t, err)
				require.NotNil(t, terminal)
			}
			require.EqualValues(t, 0, *terminal.ExecutionState().CrashBudgetRemaining)
			require.Equal(t, first.Resume, *terminal.ExecutionState().ResourceResume)
			// A crashed terminal handler remains recoverable without refilling.
			c.f.SetTime(deadline.Add(2 * time.Minute))
			c.claim()
			require.EqualValues(t, 0, *c.lease.ExecutionState().CrashBudgetRemaining)
			again, err := c.f.Runtime.(jobdb.ResourceBackoffTransport).RescheduleResourceBackoffByID(c.ctx, c.key, first.LeaseID, first.WorkerID, c.req)
			require.NoError(t, err)
			require.Equal(t, first, again)
			require.EqualValues(t, 0, *c.info().ExecutionState.CrashBudgetRemaining)
		})
	}

	for _, external := range []bool{false, true} {
		run(fmt.Sprintf("B11_zero_credit_engine_timeout_external_%t", external), func(t *testing.T, c *resourceCase) {
			var calls, builds atomic.Int32
			task := resourceDeadlineTask{run: func(ctx workflow.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				calls.Add(1)
				return nil, workflow.EAGAINWait(ctx, workflow.WithInitialWait(30*time.Second), workflow.WithBackoffJitter(0), workflow.WithCrashBudget(1))
			}}
			worker := workflow.ExecutionWorker{JobType: "resource-engine", Execute: func(ctx workflow.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return ctx.DoTask(workflow.TaskCall{Site: "work", Command: json.RawMessage(`{}`), TaskType: task.Name(), ContractVersion: 1, Policy: jobdb.RunPolicy{TotalTimeout: jobdb.AsDuration(5 * time.Second)}}, func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil })
			}}
			handle, err := celltest.Submit(c.f.Runtime, c.ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: c.key.TenantId, Cell: "test", CellRepo: "example.test/test", JobID: "engine-timeout", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("durable")}})
			require.NoError(t, err)
			run := func(tasks ...workflow.TaskWorker) workflow.JobRunOutcome {
				r, err := workflow.GetJobForRun(c.ctx, c.f.Runtime, workflow.GetJobForRunRequest{JobKey: handle.JobKey, JobWorker: worker, TaskWorkers: tasks, WorkerID: "engine", LeaseDuration: time.Minute})
				require.NoError(t, err)
				out, err := r.Run(nil)
				require.NoError(t, err)
				return out
			}
			if external {
				out := run()
				require.Equal(t, workflow.JobRunSuspended, out.Status, "%+v", out.JobError)
			}
			out := run(task)
			require.Equal(t, workflow.JobRunSuspended, out.Status, "%+v", out.JobError)
			chapters, err := c.f.Runtime.ListChapters(c.ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			var deadline time.Time
			for _, ch := range chapters {
				if body, ok := ch.Body.(jobdb.ExecutionChapter); ok {
					rec, err := jobdb.DecodeExecutionRecord(body.Record)
					require.NoError(t, err)
					if rec.Kind == "resource_checkpoint" {
						deadline = rec.TotalDeadline
					}
				}
			}
			require.False(t, deadline.IsZero())
			// Scheduler time is injected: reclaim twice without running the body.
			at := c.at.Add(time.Minute)
			claim := func() jobdb.ExecutionLease {
				l, err := c.f.Runtime.GetJobLease(c.ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "crash", Routes: []jobdb.Route{{JobType: worker.Name()}}, LeaseDuration: time.Second})
				require.NoError(t, err)
				require.NotNil(t, l)
				return l
			}
			c.f.SetTime(at)
			require.EqualValues(t, 1, *claim().ExecutionState().CrashBudgetRemaining)
			c.f.SetTime(at.Add(time.Second))
			require.EqualValues(t, 0, *claim().ExecutionState().CrashBudgetRemaining)
			c.f.SetTime(at.Add(2 * time.Second))
			if h.Durable {
				c.f.Runtime = c.f.Reopen()
			}
			// Execution timers are real original timers, separate from backend time.
			time.Sleep(max(time.Until(deadline), 0))
			out = run(task)
			require.Equal(t, workflow.JobRunFailed, out.Status)
			require.EqualValues(t, 1, calls.Load(), "timeout recovery must not reenter the task body")
			require.EqualValues(t, 1, builds.Load(), "durable input must not be rebuilt")
			info, err := c.f.Runtime.GetJob(c.ctx, handle.JobKey)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusCompleted, info.Status)
			require.EqualValues(t, 0, *info.ExecutionState.CrashBudgetRemaining)
			var timeout jobdb.TimeoutError
			require.ErrorAs(t, out.JobError, &timeout)
			require.Equal(t, jobdb.TimeoutScopeTotal, timeout.Payload.Scope)
		})
	}

	run("B09a_ordinary_join_alternative", func(t *testing.T, c *resourceCase) {
		dep, err := celltest.Submit(c.f.Runtime, c.ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: c.key.TenantId, Cell: "test", JobType: "dependency", Data: jobdb.NewTaskDataOrPanic("pending")}})
		require.NoError(t, err)
		at := c.at.Add(time.Second)
		require.NoError(t, c.lease.Reschedule(c.ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "resource"}, WaitForJobIDs: []string{dep.JobKey.JobId}, WaitUntil: &at}))
		c.f.SetTime(at)
		c.claim()
		require.Equal(t, jobdb.JobStatusActive, c.info().Status)
	})
	run("B11a_ordinary_completion_after_due", func(t *testing.T, c *resourceCase) {
		c.taskRoute()
		wait := c.lease.ExecutionState().TaskWait
		require.NotNil(t, wait)
		route := c.lease.Route()
		after := time.Second
		alternate := jobdb.Route{JobType: "resource"}
		require.NoError(t, c.lease.Reschedule(c.ctx, jobdb.RescheduleExecutionRequest{NextRoute: route, TaskWait: wait, AlternateRoute: &alternate, AlternateAfter: &after}))
		c.f.SetTime(c.at.Add(2 * time.Second))
		require.NoError(t, c.f.Runtime.CompleteTaskIfWaiting(c.ctx, jobdb.CompleteTaskIfWaitingRequest{JobKey: c.key, Route: route, InputOrdinal: wait.InputOrdinal, OutputOrdinal: wait.OutputOrdinal, InputHash: wait.InputHash, ResumeJobType: "resource", Data: jobdb.NewTaskDataOrPanic("after deadline")}))
	})
	run("B09_append_gate_B13a_parent_not_refilled", func(t *testing.T, c *resourceCase) {
		q := jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "resource-chain", Job: jobdb.SubmitJob{TenantId: c.key.TenantId, Cell: "test", JobType: "resource", Data: jobdb.NewTaskDataOrPanic("start")}, Append: json.RawMessage(`1`)}}
		h, err := c.f.Runtime.StartOrAppendJob(c.ctx, q)
		require.NoError(t, err)
		c.key = h.JobKey
		c.claim()
		c.req = c.prepare("append/attempt:1", 1, c.req.Policy)
		c.accept()
		before := c.info()
		require.Error(t, c.f.ClaimAppend(c.ctx, c.key, c.at))
		require.Equal(t, before, c.info())
		c.at = c.receipt.WakeAt.Add(2 * time.Minute)
		c.f.SetTime(c.at)
		c.claim()
		_, err = c.lease.SubmitJob(c.ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: c.key.TenantId, Cell: "test", JobType: "child", Data: jobdb.NewTaskDataOrPanic("child")}})
		require.NoError(t, err)
		require.EqualValues(t, 5, *c.info().ExecutionState.CrashBudgetRemaining)
		c.at = c.at.Add(2 * time.Minute)
		c.f.SetTime(c.at)
		require.NoError(t, c.f.ClaimAppend(c.ctx, c.key, c.at))
		require.EqualValues(t, 4, *c.info().ExecutionState.CrashBudgetRemaining)
	})
	run("B13_countdown_boundary", func(t *testing.T, c *resourceCase) {
		for n := 4; n >= 0; n-- {
			c.at = c.at.Add(2 * time.Minute)
			c.f.SetTime(c.at)
			c.claim()
			info := c.info()
			require.EqualValues(t, n, *info.ExecutionState.CrashBudgetRemaining)
			require.Equal(t, jobdb.JobStatusActive, info.Status)
		}
		c.at = c.at.Add(2 * time.Minute)
		c.f.SetTime(c.at)
		l, err := c.f.Runtime.GetJobLease(c.ctx, c.claimRequest())
		require.NoError(t, err)
		require.Nil(t, l)
		require.Equal(t, jobdb.JobStatusCrashConcern, c.info().Status)
	})
	run("B13a_B13c_ordinary_reset_and_renew", func(t *testing.T, c *resourceCase) {
		p := c.req.Policy
		n := int64(100)
		p.CrashAllowance = &n
		c.req = c.prepare(c.req.Invocation, 1, p)
		c.accept()
		c.wake()
		before := c.info()
		renew := c.lease.(jobdb.RenewableExecutionLease)
		_, err := renew.Renew(c.ctx)
		require.NoError(t, err)
		require.EqualValues(t, 100, *c.info().ExecutionState.CrashBudgetRemaining)
		require.Equal(t, before.ExecutionState.ResourceResume, c.info().ExecutionState.ResourceResume)
		require.NoError(t, c.lease.Reschedule(c.ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "resource"}}))
		require.EqualValues(t, 5, *c.info().ExecutionState.CrashBudgetRemaining)
	})
	run("B13b_guarded_clear", func(t *testing.T, c *resourceCase) {
		require.ErrorIs(t, c.f.Runtime.ClearCrashConcern(c.ctx, jobdb.ClearCrashConcernRequest{JobKey: c.key}), jobdb.ErrConflict)
		c.at = c.at.Add(2 * time.Minute)
		c.f.SetTime(c.at)
		require.NoError(t, c.f.Runtime.ClearCrashConcern(c.ctx, jobdb.ClearCrashConcernRequest{JobKey: c.key}))
		c.claim()
		require.EqualValues(t, 4, *c.info().ExecutionState.CrashBudgetRemaining)
	})
	run("B15a_S02_snapshot", func(t *testing.T, c *resourceCase) {
		snapshot := c.lease.ExecutionState()
		c.accept()
		require.Equal(t, snapshot, c.lease.ExecutionState())
		list, err := c.f.Runtime.ListJobs(c.ctx, jobdb.ListJobsRequest{TenantIds: []string{c.key.TenantId}})
		require.NoError(t, err)
		require.Len(t, list.Jobs, 1)
		require.Equal(t, c.info().ExecutionState, list.Jobs[0].ExecutionState)
	})
	run("B17_retained_after_cancel", func(t *testing.T, c *resourceCase) {
		old := c.lease
		first := c.accept()
		require.NoError(t, c.f.Runtime.CancelJob(c.ctx, jobdb.CancelJobRequest{JobKey: c.key}))
		again, err := old.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(c.ctx, c.req)
		require.NoError(t, err)
		require.Equal(t, first, again)
		require.Equal(t, jobdb.JobStatusCancelled, c.info().Status)
	})
	if h.Durable {
		run("B08_reopen", func(t *testing.T, c *resourceCase) {
			first := c.accept()
			c.f.Runtime = c.f.Reopen()
			c.f.SetTime(c.at)
			before := c.info()
			again, err := c.f.Runtime.(jobdb.ResourceBackoffTransport).RescheduleResourceBackoffByID(c.ctx, c.key, first.LeaseID, first.WorkerID, c.req)
			require.NoError(t, err)
			require.Equal(t, first, again)
			require.Equal(t, before, c.info())
			c.wake()
		})
		run("B05a_transaction_rollback", func(t *testing.T, c *resourceCase) {
			before := c.info()
			for _, stage := range []string{"validated", "sequence", "job", "receipt"} {
				c.f.Fault(stage)
				_, err := c.port().RescheduleResourceBackoff(c.ctx, c.req)
				require.Error(t, err, stage)
				require.Equal(t, before, c.info(), stage)
			}
			c.f.Fault("")
			receipt := c.accept()
			require.EqualValues(t, 1, receipt.Count)
		})
		run("B07_barrier", func(t *testing.T, c *resourceCase) {
			entered, releaseBackend := c.f.Barrier()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(releaseBackend) }
			defer release()
			done := make(chan error, 1)
			go func() { _, err := c.port().RescheduleResourceBackoff(c.ctx, c.req); done <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("backend barrier was not reached")
			}
			// While the first real transaction is held after validation, an exact
			// duplicate cannot report an acknowledgment for its uncommitted state.
			blocked, cancel := context.WithTimeout(c.ctx, 25*time.Millisecond)
			defer cancel()
			_, err := c.port().RescheduleResourceBackoff(blocked, c.req)
			require.Error(t, err, "receipt escaped the held transaction")
			release()
			require.NoError(t, <-done)
			require.Equal(t, jobdb.JobStatusAwaitingFuture, c.info().Status)
			first, err := c.port().RescheduleResourceBackoff(c.ctx, c.req)
			require.NoError(t, err)
			require.EqualValues(t, 1, first.Count)
		})
	}
	if selected == 0 {
		t.Fatal("resource conformance selected zero checks")
	}
}

type resourceCase struct {
	t       *testing.T
	f       ResourceBackoffFixture
	ctx     context.Context
	at      time.Time
	key     jobdb.JobKey
	lease   jobdb.ExecutionLease
	req     jobdb.ResourceBackoffRequest
	receipt jobdb.ResourceBackoffReceipt
}

func (c *resourceCase) setup() {
	h, err := celltest.Submit(c.f.Runtime, c.ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "resource-tenant", Cell: "test", CellRepo: "example.test/test", JobID: "resource-job", JobType: "resource", Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(c.t, err)
	c.key = h.JobKey
	c.claim()
	c.req = c.prepare("task/attempt:1", 1, jobdb.DefaultResourceBackoffPolicy())
}
func (c *resourceCase) claimRequest() jobdb.GetJobLeaseRequest {
	return jobdb.GetJobLeaseRequest{JobKey: c.key, WorkerID: "resource-worker", Routes: []jobdb.Route{{JobType: "resource"}}, LeaseDuration: time.Minute}
}
func (c *resourceCase) claim() {
	l, err := c.f.Runtime.GetJobLease(c.ctx, c.claimRequest())
	require.NoError(c.t, err)
	require.NotNil(c.t, l)
	c.lease = l
}
func (c *resourceCase) prepare(id string, attempt int, p jobdb.ResourceBackoffPolicy) jobdb.ResourceBackoffRequest {
	r, err := PrepareResourceBackoffCheckpoint(c.ctx, c.f.Runtime, c.lease, id, attempt, p)
	require.NoError(c.t, err)
	return r
}
func (c *resourceCase) port() jobdb.ResourceBackoffLease {
	p, ok := c.lease.(jobdb.ResourceBackoffLease)
	require.True(c.t, ok, "missing optional lease port")
	return p
}
func (c *resourceCase) accept() jobdb.ResourceBackoffReceipt {
	r, err := c.port().RescheduleResourceBackoff(c.ctx, c.req)
	require.NoError(c.t, err)
	c.receipt = r
	return r
}
func (c *resourceCase) wake() { c.at = c.receipt.WakeAt; c.f.SetTime(c.at); c.claim() }
func (c *resourceCase) info() jobdb.JobInfo {
	info, err := c.f.Runtime.GetJob(c.ctx, c.key)
	require.NoError(c.t, err)
	info.Data = nil
	return info
}

func (c *resourceCase) taskRoute() {
	chapters, err := c.f.Runtime.ListChapters(c.ctx, jobdb.ListChaptersRequest{JobKey: c.key})
	require.NoError(c.t, err)
	route := jobdb.Route{JobType: "resource", TaskType: "external"}
	wait := &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: int64(len(chapters)), InputHash: "durable-input", ResumeJobType: "resource"}
	require.NoError(c.t, c.lease.Reschedule(c.ctx, jobdb.RescheduleExecutionRequest{NextRoute: route, TaskWait: wait}))
	lease, err := c.f.Runtime.GetJobLease(c.ctx, jobdb.GetJobLeaseRequest{JobKey: c.key, WorkerID: "external", Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
	require.NoError(c.t, err)
	require.NotNil(c.t, lease)
	c.lease = lease
}

// Real task adapter used by the zero-credit deadline engine conformance check.
type resourceDeadlineTask struct {
	run func(workflow.TaskContext, jobdb.TaskData) (jobdb.TaskData, error)
}

func (resourceDeadlineTask) Name() string { return "resource-deadline-task" }
func (t resourceDeadlineTask) Run(ctx workflow.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	return t.run(ctx, in)
}
