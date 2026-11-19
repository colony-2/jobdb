package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type resultStateKey struct{}
type resultCallerKey struct{}
type resultState struct {
	mu     sync.Mutex
	total  int
	events []wf.TaskEndEvent
}

func (s *resultState) sum() int { s.mu.Lock(); defer s.mu.Unlock(); return s.total }
func (s *resultState) received() []wf.TaskEndEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]wf.TaskEndEvent(nil), s.events...)
}
func (s *resultState) accept(e wf.TaskEndEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	if e.Output != nil {
		if err := json.Unmarshal(e.Output.GetDataOrPanic(), &n); err != nil {
			panic(err)
		}
	}
	s.total += n
	s.events = append(s.events, e)
}

type resultFactory struct {
	mu     sync.Mutex
	states []*resultState
}

func (f *resultFactory) new(ctx context.Context) (context.Context, func(wf.TaskEndEvent)) {
	s := &resultState{}
	f.mu.Lock()
	f.states = append(f.states, s)
	f.mu.Unlock()
	return context.WithValue(ctx, resultStateKey{}, s), s.accept
}
func (f *resultFactory) all() []*resultState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*resultState(nil), f.states...)
}
func resultStateFrom(ctx context.Context) *resultState {
	return ctx.Value(resultStateKey{}).(*resultState)
}
func resultBackends(t *testing.T, test func(*testing.T, jobdb.WorkflowRuntime)) {
	t.Helper()
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) { test(t, alternateRuntime(t, backend)) })
	}
}
func resultEngine(t *testing.T, rt jobdb.WorkflowRuntime, w wf.JobWorker, tasks ...wf.TaskWorker) wf.Engine {
	t.Helper()
	e, err := wf.NewEngineBuilder().WithRuntime(rt).WithWorkerTenantId("tenant").PlusWorkers(w, tasks...).BuildEngine()
	require.NoError(t, err)
	return e
}
func resultRun(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, w wf.JobWorker, tasks ...wf.TaskWorker) *wf.JobRunnable {
	t.Helper()
	run, err := wf.GetJobForRun(celltest.Context(context.Background()), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: tasks, WorkerID: "observer-test", LeaseDuration: time.Minute})
	require.NoError(t, err)
	return run
}

// Independent storage inspection checks more than agreement between observer streams.
func assertResultHistory(t *testing.T, rt jobdb.WorkflowRuntime, key jobdb.JobKey, events []wf.TaskEndEvent) {
	t.Helper()
	chapters, err := rt.ListChapters(celltest.Context(context.Background()), jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	types := map[string]string{}
	for _, record := range executionRecords(t, rt, key) {
		if record.Kind == "task" {
			var descriptor struct {
				Call wf.TaskCall `json:"call"`
			}
			require.NoError(t, json.Unmarshal(record.Descriptor, &descriptor))
			types[record.ID] = descriptor.Call.TaskType
		}
	}
	found := 0
	for _, ch := range chapters {
		body, ok := ch.Body.(jobdb.ExecutionChapter)
		if !ok {
			continue
		}
		rec, err := jobdb.DecodeExecutionRecord(body.Record)
		require.NoError(t, err)
		if rec.Kind != "result" || !strings.Contains(rec.ID, `/task:`) {
			continue
		}
		require.Less(t, found, len(events))
		e := events[found]
		found++
		require.Equal(t, key, e.JobKey)
		require.Equal(t, rec.ID, e.InvocationID)
		require.Equal(t, ch.Ordinal, e.Ordinal)
		require.True(t, rec.At.Equal(e.At))
		require.Equal(t, types[rec.ID[:strings.LastIndex(rec.ID, "/attempt:")]], e.TaskType)
		require.True(t, strings.HasSuffix(rec.ID, fmt.Sprintf("/attempt:%d", e.AttemptNumber)))
		if rec.Output != nil {
			require.JSONEq(t, string(rec.Output.Data), string(e.Output.GetDataOrPanic()))
		} else {
			require.Nil(t, e.Output)
		}
		if rec.ErrorKind == "" {
			require.NoError(t, e.Err)
		} else {
			require.Error(t, e.Err)
			require.Contains(t, string(rec.Error), e.Err.Error())
		}
	}
	require.Equal(t, len(events), found)
}

type resultObserver struct {
	unfinishedObserver
	onEnd func(wf.TaskEndEvent)
	order []string
}

func (o *resultObserver) OnTaskStart(e wf.TaskStartEvent) {
	o.order = append(o.order, "start")
	o.unfinishedObserver.OnTaskStart(e)
}
func (o *resultObserver) OnTaskEnd(e wf.TaskEndEvent) {
	o.order = append(o.order, "end")
	if o.onEnd != nil {
		o.onEnd(e)
	}
	o.unfinishedObserver.OnTaskEnd(e)
}

type resultBlockedListener struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
	events  []wf.TaskEndEvent
}

func (l *resultBlockedListener) OnJobStart(wf.JobStartEvent) { close(l.entered); <-l.release }
func (*resultBlockedListener) OnTaskStart(wf.TaskStartEvent) {}
func (l *resultBlockedListener) OnTaskEnd(e wf.TaskEndEvent) { l.events = append(l.events, e) }
func (l *resultBlockedListener) OnJobEnd(wf.JobEndEvent)     { close(l.done) }

func TestTaskResultObserverOrdering(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		f := &resultFactory{}
		var runs, builds, predicates atomic.Int32
		listener := &resultBlockedListener{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
		release := sync.OnceFunc(func() { close(listener.release) })
		defer release()
		task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
			i := int(runs.Add(1)) - 1
			require.Equal(t, []int{0, 3, 15, 26}[i], resultStateFrom(c.Context()).sum())
			n := []int{3, 12, 11, 13}[i]
			if i == 1 || i == 2 {
				return jobdb.NewTaskDataOrPanic(n), errors.New("failed report")
			}
			return jobdb.NewTaskDataOrPanic(n), nil
		}}
		w := wf.ExecutionWorker{JobType: "observer-order", ObserveTaskResults: f.new, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			s := resultStateFrom(c.Context())
			require.Zero(t, s.sum())
			build := func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return in, nil }
			_, err := c.DoTask(invocation("first"), build)
			if err != nil {
				return nil, err
			}
			require.Equal(t, 3, s.sum())
			call := invocation("retry")
			call.Policy.Retry.MaximumAttempts = 2
			call.Catch = []wf.CatchRule{{ID: "recover", Match: func(error) bool {
				predicates.Add(1)
				require.Contains(t, []int{15, 26}, s.sum())
				return s.sum() == 26
			}, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				require.Equal(t, 26, s.sum())
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic(1000)}, nil
			}}}
			out, err := c.DoTask(call, build)
			if err != nil {
				return nil, err
			}
			require.JSONEq(t, "1000", string(out.GetDataOrPanic()))
			require.Equal(t, 26, s.sum())
			out, err = c.DoTask(invocation("last"), build)
			require.Equal(t, 39, s.sum())
			return out, err
		}}
		key := submitExecutionTest(t, rt, w)
		run := resultRun(t, rt, key, w, task)
		finished := make(chan error, 1)
		go func() {
			out, err := run.Run(listener)
			if err == nil && out.Status != wf.JobRunCompleted {
				err = fmt.Errorf("outcome: %+v", out)
			}
			finished <- err
		}()
		select {
		case <-listener.entered:
		case <-time.After(10 * time.Second):
			t.Fatal("listener never started")
		}
		select {
		case err := <-finished:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("blocked listener delayed orchestration")
		}
		states := f.all()
		require.Len(t, states, 1)
		require.Equal(t, 39, states[0].sum())
		live := states[0].received()
		require.Len(t, live, 4)
		assertResultHistory(t, rt, key, live)
		release()
		select {
		case <-listener.done:
		case <-time.After(10 * time.Second):
			t.Fatal("listener did not drain")
		}
		require.Len(t, listener.events, 4)
		_, err := run.Run(nil)
		require.NoError(t, err)
		require.Len(t, f.all(), 1)
		_, err = resultRun(t, rt, key, w, task).Run(nil)
		require.NoError(t, err)
		require.Len(t, f.all(), 1)
		before, err := rt.ListChapters(celltest.Context(context.Background()), jobdb.ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		e := resultEngine(t, rt, w, task)
		for i := 0; i < 2; i++ {
			observer := &resultObserver{onEnd: func(event wf.TaskEndEvent) {
				s := f.all()[len(f.all())-1]
				require.Equal(t, []int{3, 15, 26, 39}[len(s.received())-1], s.sum())
			}}
			_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key, Observer: observer})
			require.NoError(t, err)
			s := f.all()[i+1]
			require.Equal(t, 39, s.sum())
			require.Len(t, s.received(), 4)
			require.Equal(t, []string{"start", "end", "start", "end", "start", "end", "start", "end"}, observer.order)
			assertResultHistory(t, rt, key, s.received())
		}
		require.EqualValues(t, 4, runs.Load())
		require.EqualValues(t, 3, builds.Load())
		require.EqualValues(t, 2, predicates.Load())
		after, err := rt.ListChapters(celltest.Context(context.Background()), jobdb.ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		require.Equal(t, before, after)
		override := w
		other := &resultFactory{}
		override.ObserveTaskResults = other.new
		_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key, JobWorker: override})
		require.NoError(t, err)
		require.Len(t, other.all(), 1)
		require.Len(t, f.all(), 3)
		// A descriptor mismatch stops before the mismatched result is consumed.
		override.Execute = func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			call := invocation("first")
			call.Command = json.RawMessage(`{"changed":true}`)
			return c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
		}
		_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key, JobWorker: override})
		require.ErrorIs(t, err, wf.ErrWorkflowNotDeterministic)
		require.Empty(t, other.all()[1].received())
	})
}

// A custom worker demonstrates that discovery does not depend on ExecutionWorker.
type resultCustomWorker struct {
	name    string
	execute func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error)
	factory *resultFactory
}

func (w resultCustomWorker) Name() string { return w.name }
func (w resultCustomWorker) Run(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
	return w.execute(c, in)
}
func (w resultCustomWorker) NewTaskResultObserver(c context.Context) (context.Context, func(wf.TaskEndEvent)) {
	return w.factory.new(c)
}

type resultPlainWorker struct {
	name    string
	execute func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error)
}

func (w resultPlainWorker) Name() string { return w.name }
func (w resultPlainWorker) Run(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
	return w.execute(c, in)
}

func TestTaskResultObserverEntryPaths(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		body := func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			return c.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(7), nil })
		}
		task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { return in, nil }}
		for _, mode := range []string{"lease", "registered", "plain", "nil"} {
			t.Run(mode, func(t *testing.T) {
				f := &resultFactory{}
				var w wf.JobWorker = resultCustomWorker{mode, body, f}
				if mode == "plain" {
					w = resultPlainWorker{mode, body}
				}
				if mode == "nil" {
					w = wf.ExecutionWorker{JobType: mode, Execute: body}
				}
				h, err := celltest.Submit(rt, celltest.Context(context.Background()), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: mode, Data: jobdb.NewTaskDataOrPanic(0)}})
				require.NoError(t, err)
				e := resultEngine(t, rt, w, task)
				if mode == "registered" {
					ctx, cancel := context.WithCancel(celltest.Context(context.Background()))
					defer cancel()
					done := make(chan struct{})
					go func() { e.Run(ctx); close(done) }()
					require.Eventually(t, func() bool {
						info, err := rt.GetJob(celltest.Context(context.Background()), h.JobKey)
						return err == nil && info.Status == jobdb.JobStatusCompleted
					}, 10*time.Second, 10*time.Millisecond)
					cancel()
					<-done
				} else {
					var run *wf.JobRunnable
					if mode == "lease" {
						ctx := celltest.Context(context.Background())
						lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "supplied", Routes: []jobdb.Route{{JobType: mode}}, LeaseDuration: time.Minute})
						require.NoError(t, err)
						require.NotNil(t, lease)
						run, err = wf.GetJobForRunWithLease(ctx, rt, lease, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}})
						require.NoError(t, err)
					} else {
						run = resultRun(t, rt, h.JobKey, w, task)
					}
					out, err := run.Run(nil)
					require.NoError(t, err)
					require.Equal(t, wf.JobRunCompleted, out.Status)
				}
				observer := &unfinishedObserver{}
				_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: h.JobKey, Observer: observer})
				require.NoError(t, err)
				require.Len(t, observer.ends, 1)
				if mode == "plain" || mode == "nil" {
					require.Empty(t, f.all())
				} else {
					require.Len(t, f.all(), 2)
					for _, s := range f.all() {
						require.Equal(t, 7, s.sum())
						assertResultHistory(t, rt, h.JobKey, s.received())
					}
				}
			})
		}
	})
}

func TestTaskResultObserverRetryContextLifetime(t *testing.T) {
	resultSecretBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		for _, nilCallback := range []bool{false, true} {
			t.Run(fmt.Sprintf("nil-callback=%t", nilCallback), func(t *testing.T) {
				f := &resultFactory{}
				var factories atomic.Int32
				var derived []context.Context
				var entries []int
				var replaying bool
				check := func(ctx context.Context) {
					require.Equal(t, "caller", ctx.Value(resultCallerKey{}))
					require.NotNil(t, resultStateFrom(ctx))
					require.NoError(t, ctx.Err())
					_, ok := jobdb.CellAuthorityFromContext(ctx)
					require.True(t, ok)
					resolver, ok := wf.SecretResolverFromContext(ctx)
					if replaying {
						require.False(t, ok)
					} else {
						require.True(t, ok)
						secrets, err := resolver.ResolveSecrets(ctx, []string{"optional"})
						require.NoError(t, err)
						require.Equal(t, "observer-secret", secrets.Secrets["optional"].Value)
					}
				}
				factory := func(ctx context.Context) (context.Context, func(wf.TaskEndEvent)) {
					factories.Add(1)
					out, cb := f.new(ctx)
					derived = append(derived, out)
					if nilCallback {
						return out, nil
					}
					return out, cb
				}
				var taskRuns int
				task := executionTestTask{"echo", func(c wf.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
					check(c.Context())
					taskRuns++
					if taskRuns == 1 {
						return jobdb.NewTaskDataOrPanic(5), errors.New("retry whole job")
					}
					return jobdb.NewTaskDataOrPanic(7), nil
				}}
				w := wf.ExecutionWorker{JobType: fmt.Sprintf("root-lifetime-%t", nilCallback), ObserveTaskResults: factory, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					check(c.Context())
					entries = append(entries, resultStateFrom(c.Context()).sum())
					return c.DoScope("nested", wf.ScopeOptions{}, func(s wf.ExecutionContext) (jobdb.JobData, error) {
						check(s.Context())
						return s.DoTask(invocation("work"), func(ctx context.Context) (jobdb.TaskData, error) { check(ctx); return in, nil })
					})
				}}
				ctx := context.WithValue(celltest.Context(context.Background()), resultCallerKey{}, "caller")
				h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic(0), RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 2}}}})
				require.NoError(t, err)
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "root-retry", LeaseDuration: time.Minute})
				require.NoError(t, err)
				out, err := run.Run(nil)
				require.NoError(t, err)
				require.Equal(t, wf.JobRunCompleted, out.Status)
				require.EqualValues(t, 1, factories.Load())
				wantEntries := []int{0, 5}
				wantTotal := 12
				if nilCallback {
					wantEntries = []int{0, 0}
					wantTotal = 0
				}
				require.Equal(t, wantEntries, entries)
				require.Equal(t, wantTotal, f.all()[0].sum())
				require.ErrorIs(t, derived[0].Err(), context.Canceled)
				replaying = true
				entries = nil
				_, err = resultEngine(t, rt, w, task).ReplayJobRun(ctx, wf.ReplayRunRequest{JobKey: h.JobKey})
				require.NoError(t, err)
				require.Equal(t, wantEntries, entries)
				require.Equal(t, wantTotal, f.all()[1].sum())
				require.EqualValues(t, 2, factories.Load())
				require.Equal(t, 2, taskRuns)
			})
		}
	})
}

func TestTaskResultObserverOverlappingInvocations(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		f := &resultFactory{}
		var gate atomic.Bool
		entered := make(chan *resultState, 4)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		w := wf.ExecutionWorker{JobType: "overlap", ObserveTaskResults: f.new, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			s := resultStateFrom(c.Context())
			require.Zero(t, s.sum())
			if gate.Load() {
				entered <- s
				select {
				case <-release:
				case <-c.Context().Done():
					return nil, c.Context().Err()
				}
			}
			return c.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(7), nil })
		}}
		task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
			require.Zero(t, resultStateFrom(c.Context()).sum())
			return in, nil
		}}
		source := submitExecutionTest(t, rt, w)
		out, err := runExecutionTest(t, rt, source, w, task)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunCompleted, out.Status)
		e := resultEngine(t, rt, w, task)
		gate.Store(true)
		keys := []jobdb.JobKey{submitExecutionTest(t, rt, w), submitExecutionTest(t, rt, w)}
		runs := []*wf.JobRunnable{resultRun(t, rt, keys[0], w, task), resultRun(t, rt, keys[1], w, task)}
		done := make(chan error, 4)
		for _, run := range runs {
			go func() {
				out, err := run.Run(nil)
				if err == nil && out.Status != wf.JobRunCompleted {
					err = fmt.Errorf("outcome: %+v", out)
				}
				done <- err
			}()
		}
		for i := 0; i < 2; i++ {
			go func() {
				_, err := e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: source})
				done <- err
			}()
		}
		unique := map[*resultState]bool{}
		for i := 0; i < 4; i++ {
			select {
			case s := <-entered:
				require.False(t, unique[s])
				unique[s] = true
				require.Zero(t, s.sum())
			case <-time.After(10 * time.Second):
				t.Fatal("invocations failed to overlap")
			}
		}
		unblock()
		for i := 0; i < 4; i++ {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("invocation did not finish")
			}
		}
		require.Len(t, f.all(), 5)
		for _, s := range f.all() {
			require.Equal(t, 7, s.sum())
			require.Len(t, s.received(), 1)
		}
	})
}

func TestTaskResultObserverPreRunHandoff(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		f := &resultFactory{}
		w := wf.ExecutionWorker{JobType: "pre-handoff", ObserveTaskResults: f.new, Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) {
			t.Error("admission handoff executed body")
			return nil, nil
		}}
		key := submitExecutionTest(t, rt, w)
		run, err := wf.GetJobForRun(celltest.Context(context.Background()), rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, WorkerID: "admission", LeaseDuration: time.Minute, BeforeRun: func(ctx context.Context, l jobdb.ExecutionLease) error {
			return l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "elsewhere"}})
		}})
		require.NoError(t, err)
		out, err := run.Run(nil)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunSuspended, out.Status)
		require.Empty(t, f.all())
	})
}

type resultFaultRuntime struct {
	jobdb.WorkflowRuntime
	mode   string
	fired  atomic.Bool
	cancel context.CancelFunc
}

func (r *resultFaultRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	body, ok := req.Chapter.Body.(jobdb.ExecutionChapter)
	if !ok {
		return r.WorkflowRuntime.PutChapter(ctx, req)
	}
	rec, err := jobdb.DecodeExecutionRecord(body.Record)
	if err != nil {
		return err
	}
	if rec.Kind != "result" || !strings.Contains(rec.ID, `/task:`) || !r.fired.CompareAndSwap(false, true) {
		return r.WorkflowRuntime.PutChapter(ctx, req)
	}
	if r.mode == "reject" {
		return errors.New("injected rejected result")
	}
	if r.mode == "winner" || r.mode == "conflict" {
		rec.Output = &jobdb.ExecutionSnapshot{Data: json.RawMessage(`17`)}
		rec.ErrorKind = ""
		rec.Error = nil
		rec.At = rec.At.Add(time.Millisecond)
		if r.mode == "conflict" {
			rec.ID += "-other"
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		req.Chapter.Body = jobdb.ExecutionChapter{Record: raw}
		req.Chapter.CreatedAt = rec.At
	}
	if err := r.WorkflowRuntime.PutChapter(ctx, req); err != nil {
		return err
	}
	if r.cancel != nil {
		r.cancel()
		return nil
	}
	if r.mode == "winner" || r.mode == "conflict" {
		return jobdb.ErrConflict
	}
	return errors.New("injected lost result acknowledgement")
}
func TestTaskResultObserverReconciliation(t *testing.T) {
	resultBackends(t, func(t *testing.T, base jobdb.WorkflowRuntime) {
		for _, mode := range []string{"ack-lost", "winner", "reject", "conflict", "cancel-after-accept"} {
			t.Run(mode, func(t *testing.T) {
				f := &resultFactory{}
				rt := &resultFaultRuntime{WorkflowRuntime: base, mode: mode}
				ctx, cancel := context.WithCancel(celltest.Context(context.Background()))
				defer cancel()
				if mode == "cancel-after-accept" {
					rt.cancel = cancel
				}
				w := wf.ExecutionWorker{JobType: "fault-" + mode, ObserveTaskResults: f.new, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					return c.DoTask(invocation("work"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
				}}
				task := executionTestTask{"echo", func(wf.TaskContext, jobdb.TaskData) (jobdb.TaskData, error) {
					return jobdb.NewTaskDataOrPanic(2), errors.New("local failure")
				}}
				key := submitExecutionTest(t, base, w)
				run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "fault", LeaseDuration: time.Minute})
				require.NoError(t, err)
				out, err := run.Run(nil)
				require.True(t, rt.fired.Load())
				require.Len(t, f.all(), 1)
				switch mode {
				case "ack-lost", "reject", "conflict":
					require.Error(t, err)
					require.Empty(t, f.all()[0].received())
				case "cancel-after-accept":
					require.Error(t, err)
					require.Equal(t, 2, f.all()[0].sum())
					assertResultHistory(t, base, key, f.all()[0].received())
				default:
					require.NoError(t, err)
					require.Len(t, f.all()[0].received(), 1)
					assertResultHistory(t, base, key, f.all()[0].received())
					if mode == "winner" {
						require.Equal(t, wf.JobRunCompleted, out.Status)
						require.Equal(t, 17, f.all()[0].sum())
						require.NoError(t, f.all()[0].received()[0].Err)
					} else {
						require.Equal(t, wf.JobRunFailed, out.Status)
						require.Equal(t, 2, f.all()[0].sum())
						require.ErrorContains(t, f.all()[0].received()[0].Err, "local failure")
					}
				}
				_, replayErr := resultEngine(t, base, w).ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key})
				s := f.all()[1]
				switch mode {
				case "reject":
					require.Error(t, replayErr)
					require.Empty(t, s.received())
				case "conflict":
					require.ErrorIs(t, replayErr, wf.ErrWorkflowNotDeterministic)
					require.Empty(t, s.received())
				case "winner":
					require.NoError(t, replayErr)
					require.Equal(t, 17, s.sum())
					assertResultHistory(t, base, key, s.received())
				default:
					require.Error(t, replayErr)
					require.Equal(t, 2, s.sum())
					assertResultHistory(t, base, key, s.received())
				}
			})
		}
	})
}

func TestTaskResultObserverResumeAndFrontier(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		f := &resultFactory{}
		var builds, runs atomic.Int32
		w := wf.ExecutionWorker{JobType: "observer-resume", ObserveTaskResults: f.new, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			s := resultStateFrom(c.Context())
			require.Zero(t, s.sum())
			build := func(n int) func(context.Context) (jobdb.TaskData, error) {
				return func(context.Context) (jobdb.TaskData, error) { builds.Add(1); return jobdb.NewTaskDataOrPanic(n), nil }
			}
			_, err := c.DoTask(invocation("prefix"), build(3))
			if err != nil {
				return nil, err
			}
			require.Equal(t, 3, s.sum())
			call := invocation("external")
			call.TaskType = "human"
			_, err = c.DoTask(call, build(11))
			if err != nil {
				return nil, err
			}
			require.Equal(t, 14, s.sum())
			out, err := c.DoTask(invocation("suffix"), build(13))
			require.Equal(t, 27, s.sum())
			return out, err
		}}
		task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
		key := submitExecutionTest(t, rt, w)
		out, err := runExecutionTest(t, rt, key, w, task)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunSuspended, out.Status)
		require.Equal(t, 3, f.all()[0].sum())
		require.Len(t, f.all()[0].received(), 1)
		observer := &unfinishedObserver{}
		e := resultEngine(t, rt, w)
		_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key, Observer: observer})
		var miss wf.ReplayCacheMissError
		require.ErrorAs(t, err, &miss)
		require.Equal(t, wf.ReplayCacheMissTaskResultMissing, miss.Reason)
		require.Len(t, observer.starts, 2)
		require.True(t, observer.starts[1].Unfinished)
		require.Len(t, observer.ends, 1)
		require.Equal(t, 3, f.all()[1].sum())
		require.Len(t, f.all()[1].received(), 1)
		pending, err := e.GetWaitingTask(celltest.Context(context.Background()), key)
		require.NoError(t, err)
		require.NoError(t, pending.Finish(celltest.Context(context.Background()), jobdb.NewTaskDataOrPanic(11)))
		require.Equal(t, 3, f.all()[0].sum(), "storage completion alone must not notify an old invocation")
		out, err = runExecutionTest(t, rt, key, w, task)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunCompleted, out.Status)
		require.Equal(t, 27, f.all()[2].sum())
		require.Len(t, f.all()[2].received(), 3)
		assertResultHistory(t, rt, key, f.all()[2].received())
		_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key})
		require.NoError(t, err)
		require.Equal(t, 27, f.all()[3].sum())
		assertResultHistory(t, rt, key, f.all()[3].received())
		require.EqualValues(t, 3, builds.Load())
		require.EqualValues(t, 2, runs.Load())
	})
}

func TestTaskResultObserverRestart(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		f := &resultFactory{}
		var runs atomic.Int32
		var restartHooks atomic.Int32
		w := wf.ExecutionWorker{JobType: "observer-restart", ObserveTaskResults: f.new, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			s := resultStateFrom(c.Context())
			require.Zero(t, s.sum())
			build := func(n int) func(context.Context) (jobdb.TaskData, error) {
				return func(context.Context) (jobdb.TaskData, error) { return jobdb.NewTaskDataOrPanic(n), nil }
			}
			_, err := c.DoTask(invocation("prefix"), build(3))
			if err != nil {
				return nil, err
			}
			require.Equal(t, 3, s.sum())
			patch := 11
			_, err = c.DoScope("patched", wf.ScopeOptions{OnRestart: func(raw json.RawMessage) error {
				restartHooks.Add(1)
				require.Equal(t, 3, s.sum())
				return json.Unmarshal(raw, &patch)
			}}, func(p wf.ExecutionContext) (jobdb.JobData, error) {
				require.Equal(t, 3, s.sum())
				out, err := p.DoTask(invocation("middle"), build(patch))
				require.Equal(t, 3+patch, s.sum())
				return out, err
			})
			if err != nil {
				return nil, err
			}
			out, err := c.DoTask(invocation("suffix"), build(patch+2))
			if err != nil {
				return nil, err
			}
			require.Equal(t, 5+2*patch, s.sum())
			_, err = c.DoScope("synthetic", wf.ScopeOptions{Catch: []wf.CatchRule{{ID: "replace", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic(1000)}, nil
			}}}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("scope only") })
			require.Equal(t, 5+2*patch, s.sum())
			return out, err
		}}
		task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) { runs.Add(1); return in, nil }}
		source := submitExecutionTest(t, rt, w)
		out, err := runExecutionTest(t, rt, source, w, task)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunCompleted, out.Status)
		require.Equal(t, 27, f.all()[0].sum())
		original, err := rt.ListChapters(celltest.Context(context.Background()), jobdb.ListChaptersRequest{JobKey: source})
		require.NoError(t, err)
		h, err := rt.SubmitRestartJob(celltest.Context(context.Background()), jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: executionRestartTarget(t, rt, source, "patched"), Payload: json.RawMessage(`17`)}}})
		require.NoError(t, err)
		require.Len(t, f.all(), 1, "creating a restart must not notify")
		out, err = runExecutionTest(t, rt, h.JobKey, w, task)
		require.NoError(t, err)
		require.Equal(t, wf.JobRunCompleted, out.Status)
		require.Equal(t, 39, f.all()[1].sum())
		events := f.all()[1].received()
		require.Len(t, events, 3)
		assertResultHistory(t, rt, h.JobKey, events)
		require.Equal(t, f.all()[0].received()[0].InvocationID, events[0].InvocationID)
		require.Equal(t, f.all()[0].received()[0].Ordinal, events[0].Ordinal)
		require.JSONEq(t, "17", string(events[1].Output.GetDataOrPanic()))
		require.JSONEq(t, "19", string(events[2].Output.GetDataOrPanic()))
		_, err = resultEngine(t, rt, w).ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: h.JobKey})
		require.NoError(t, err)
		require.Equal(t, 39, f.all()[2].sum())
		assertResultHistory(t, rt, h.JobKey, f.all()[2].received())
		require.EqualValues(t, 5, runs.Load())
		require.EqualValues(t, 2, restartHooks.Load())
		after, err := rt.ListChapters(celltest.Context(context.Background()), jobdb.ListChaptersRequest{JobKey: source})
		require.NoError(t, err)
		require.Equal(t, original, after)
	})
}

func TestTaskResultObserverCancelledLateWork(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		f := &resultFactory{}
		entered := make(chan struct{})
		release := make(chan struct{})
		returned := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		ctx, cancel := context.WithCancel(celltest.Context(context.Background()))
		defer cancel()
		w := wf.ExecutionWorker{JobType: "observer-late", ObserveTaskResults: f.new, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			return c.DoTask(invocation("late"), func(context.Context) (jobdb.TaskData, error) { return in, nil })
		}}
		task := executionTestTask{"echo", func(c wf.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
			close(entered)
			<-c.Context().Done()
			<-release
			defer close(returned)
			return jobdb.NewTaskDataOrPanic(999), nil
		}}
		key := submitExecutionTest(t, rt, w)
		run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key, JobWorker: w, TaskWorkers: []wf.TaskWorker{task}, WorkerID: "late", LeaseDuration: time.Minute})
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { _, err := run.Run(nil); done <- err }()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("worker never entered")
		}
		// Inspect the active frontier through a separate invocation before cancelling.
		e := resultEngine(t, rt, w)
		observer := &unfinishedObserver{}
		_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key, Observer: observer})
		var miss wf.ReplayCacheMissError
		require.ErrorAs(t, err, &miss)
		require.Len(t, observer.starts, 1)
		require.True(t, observer.starts[0].Unfinished)
		require.Empty(t, observer.ends)
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("Run waited for abandoned worker")
		}
		before, err := rt.ListChapters(celltest.Context(context.Background()), jobdb.ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		unblock()
		select {
		case <-returned:
		case <-time.After(10 * time.Second):
			t.Fatal("late worker did not finish")
		}
		_, err = e.ReplayJobRun(celltest.Context(context.Background()), wf.ReplayRunRequest{JobKey: key})
		require.ErrorAs(t, err, &miss)
		require.Len(t, f.all(), 3)
		for _, s := range f.all() {
			require.Empty(t, s.received())
			require.Zero(t, s.sum())
		}
		after, err := rt.ListChapters(celltest.Context(context.Background()), jobdb.ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}

// Supply the same real provider contract through each backend and HTTP transport.
type resultSecretProvider struct{}

func (resultSecretProvider) ResolveSecrets(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	if req.Lease.LeaseID == "" {
		return jobdb.SecretResolveResult{}, errors.New("missing lease authority")
	}
	return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"optional": {Value: "observer-secret"}}}, nil
}
func resultSecretBackends(t *testing.T, test func(*testing.T, jobdb.WorkflowRuntime)) {
	t.Helper()
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			var rt jobdb.WorkflowRuntime = toy.New(toy.WithSecretProvider(resultSecretProvider{}))
			if backend != "toy" {
				store, err := sqlite.NewFromConfig(celltest.Context(context.Background()), sqlite.Config{DBPath: filepath.Join(t.TempDir(), "observer.db"), SecretProvider: resultSecretProvider{}})
				require.NoError(t, err)
				t.Cleanup(func() { _ = store.Close(context.Background()) })
				rt = store
			}
			if backend == "remote" {
				server := httptest.NewServer(celltest.Handler(remote.NewServer(rt)))
				t.Cleanup(server.Close)
				client, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				rt = client
			}
			test(t, rt)
		})
	}
}

func TestTaskResultObserverWaitsForCallbackReturn(t *testing.T) {
	resultBackends(t, func(t *testing.T, rt jobdb.WorkflowRuntime) {
		entered := make(chan struct{})
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var active atomic.Bool
		var attempts atomic.Int32
		f := &resultFactory{}
		factory := func(ctx context.Context) (context.Context, func(wf.TaskEndEvent)) {
			derived, accept := f.new(ctx)
			return derived, func(e wf.TaskEndEvent) {
				active.Store(true)
				defer active.Store(false)
				accept(e)
				if e.AttemptNumber == 1 {
					close(entered)
					<-release
				}
			}
		}
		task := executionTestTask{"echo", func(c wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
			require.False(t, active.Load(), "retry started before observer returned")
			if attempts.Add(1) == 1 {
				return jobdb.NewTaskDataOrPanic(5), errors.New("retry")
			}
			require.Equal(t, 5, resultStateFrom(c.Context()).sum())
			return jobdb.NewTaskDataOrPanic(7), nil
		}}
		w := wf.ExecutionWorker{JobType: "observer-return", ObserveTaskResults: factory, Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
			call := invocation("work")
			call.Policy.Retry.MaximumAttempts = 2
			out, err := c.DoTask(call, func(context.Context) (jobdb.TaskData, error) { return in, nil })
			require.False(t, active.Load(), "orchestration continued before observer returned")
			return out, err
		}}
		key := submitExecutionTest(t, rt, w)
		run := resultRun(t, rt, key, w, task)
		done := make(chan error, 1)
		go func() {
			out, err := run.Run(nil)
			if err == nil && out.Status != wf.JobRunCompleted {
				err = fmt.Errorf("outcome: %+v", out)
			}
			done <- err
		}()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("observer did not enter")
		}
		require.True(t, active.Load())
		require.EqualValues(t, 1, attempts.Load())
		require.Equal(t, 5, f.all()[0].sum())
		select {
		case <-done:
			t.Fatal("Run overtook blocked observer")
		default:
		}
		unblock()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not finish")
		}
		require.EqualValues(t, 2, attempts.Load())
		require.Equal(t, 12, f.all()[0].sum())
	})
}
