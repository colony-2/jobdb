package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type configurationCountingStore struct {
	configuration.Store
	calls atomic.Int32
}

func (s *configurationCountingStore) Read(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
	s.calls.Add(1)
	return s.Store.Read(ctx, tenant)
}

type configurationReadingTask struct {
	captured TaskContext
	admin    jobdb.ConfigurationAdmin
}

func (*configurationReadingTask) Name() string { return "configuration-task" }
func (w *configurationReadingTask) Run(ctx TaskContext, input TaskData) (TaskData, error) {
	w.captured = ctx
	// Public fields cannot override the tenant used by the injected capability.
	ctx.JobKey.TenantId = "another-tenant"
	first, err := ctx.GetConfiguration(context.Background())
	if err != nil {
		return nil, err
	}
	_, err = w.admin.SetTenantConfigurationValue(context.Background(), jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", Value: json.RawMessage(`"new-task-value"`)})
	if err != nil {
		return nil, err
	}
	second, err := ctx.GetConfiguration(context.Background())
	if err != nil {
		return nil, err
	}
	if first.Revision != second.Revision || string(second.Values["setting"]) != `"configuration-initial-value"` {
		return nil, fmt.Errorf("task configuration changed or wrong tenant")
	}
	return input, nil
}

type configurationReadingJob struct {
	captured ExecutionContext
	admin    jobdb.ConfigurationAdmin
	read     bool
}

func (*configurationReadingJob) Name() string { return "configuration-job" }
func (w *configurationReadingJob) Run(ctx ExecutionContext, input JobData) (JobData, error) {
	w.captured = ctx
	if w.read {
		first, err := ctx.GetConfiguration(context.Background())
		if err != nil {
			return nil, err
		}
		_, err = w.admin.SetTenantConfigurationValue(context.Background(), jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", Value: json.RawMessage(`"new-job-value"`)})
		if err != nil {
			return nil, err
		}
		second, err := ctx.GetConfiguration(context.Background())
		if err != nil {
			return nil, err
		}
		if first.Revision != second.Revision {
			return nil, fmt.Errorf("job configuration changed")
		}
	}
	return ctx.DoTask(TaskCall{Site: "task", Command: []byte("{}"), TaskType: "configuration-task", ContractVersion: 1, Policy: RunPolicy{}}, func(context.Context) (TaskData, error) { return input, nil })
}
func TestConfigurationContextsAreCachedAndInvocationBound(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, transport := range []string{"embedded", "http"} {
			for _, readJob := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/job=%v", backend, transport, readJob), func(t *testing.T) {
					ctx := context.Background()
					var rt jobdb.WorkflowRuntime
					var service *configuration.Service
					if backend == "toy" {
						r := toy.New()
						rt = r
						service = r.Service
					} else {
						r, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
						require.NoError(t, err)
						defer r.Close(ctx)
						rt = r
						service = r.Service
					}
					counter := &configurationCountingStore{Store: service.Store}
					service.Store = counter
					if transport == "http" {
						server := httptest.NewServer(remote.NewServer(rt))
						defer server.Close()
						r, err := remote.New(server.URL, nil)
						require.NoError(t, err)
						rt = r
					}
					admin := rt.(jobdb.ConfigurationAdmin)
					_, err := admin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "setting", Value: json.RawMessage(`"configuration-initial-value"`)})
					require.NoError(t, err)
					task := &configurationReadingTask{admin: admin}
					job := &configurationReadingJob{admin: admin, read: readJob}
					ws := mustWorkSetForRunnerTest(t, job, task)
					handle, err := rt.SubmitJob(ctx, SubmitJobRequest{Job: SubmitJob{TenantId: "tenant", JobType: job.Name(), Data: NewTaskDataOrPanic("input")}})
					require.NoError(t, err)
					lease, err := rt.GetJobLease(ctx, GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "worker", Routes: []Route{{JobType: job.Name()}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					_, err = rt.ListChapters(ctx, ListChaptersRequest{JobKey: handle.JobKey})
					require.NoError(t, err)
					require.Zero(t, counter.calls.Load(), "startup or history prefetched configuration")
					runner := newWorkerRunner(rt, ws, lease, workerRunnerOptions{})
					_, err = runner.DoJob(ctx)
					require.NoError(t, err)
					want := int32(1)
					require.Equal(t, want, counter.calls.Load())
					_, err = task.captured.GetConfiguration(ctx)
					require.Error(t, err)
					_, err = job.captured.GetConfiguration(ctx)
					require.Error(t, err)
					require.Equal(t, want, counter.calls.Load())
					chapters, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: handle.JobKey})
					require.NoError(t, err)
					raw, err := json.Marshal(chapters)
					require.NoError(t, err)
					require.NotContains(t, string(raw), "new-task-value")
					require.NotContains(t, string(raw), "new-job-value")
					replay := newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: handle.JobKey, Replay: true})
					if !readJob {
						_, err = replay.DoJob(ctx)
						require.NoError(t, err)
					} else {
						_, err = replay.GetConfiguration(ctx)
						require.ErrorIs(t, err, ErrReplayShouldNeverMutate)
					}
					require.Equal(t, want, counter.calls.Load())
				})
			}
		}
	}
}

type configurationSessionLease struct {
	*sessionTestLease
	read func(context.Context) (jobdb.ConfigurationSnapshot, error)
}

func (l *configurationSessionLease) GetConfiguration(ctx context.Context) (jobdb.ConfigurationSnapshot, error) {
	return l.read(ctx)
}
func TestConfigurationContextErrorsAndLateResults(t *testing.T) {
	_, err := (TaskContext{}).GetConfiguration(context.Background())
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	runner := &workerRunner{replay: true}
	_, err = runner.GetConfiguration(context.Background())
	require.ErrorIs(t, err, ErrReplayShouldNeverMutate)
	runner.replay = false
	_, err = runner.GetConfiguration(context.Background())
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	ctx, cancel := context.WithCancel(context.Background())
	lease := &configurationSessionLease{sessionTestLease: newSessionTestLease()}
	lease.read = func(callCtx context.Context) (jobdb.ConfigurationSnapshot, error) {
		cancel()
		<-callCtx.Done()
		return configuration.Empty(), nil
	}
	runner = &workerRunner{ctx: ctx, lease: lease}
	result, err := runner.GetConfiguration(context.Background())
	require.ErrorIs(t, err, jobdb.ErrConfigurationUnavailable)
	require.Empty(t, result.Revision)
}
func TestConfigurationLookupAllowsRenewalAndDoesNotCoalesce(t *testing.T) {
	base := newSessionTestLease()
	started := make(chan struct{}, 2)
	renewed := make(chan struct{})
	release := make(chan struct{})
	lease := &configurationSessionLease{sessionTestLease: base}
	var renewals atomic.Int32
	lease.read = func(ctx context.Context) (jobdb.ConfigurationSnapshot, error) {
		started <- struct{}{}
		select {
		case <-release:
			return configuration.Empty(), nil
		case <-ctx.Done():
			return jobdb.ConfigurationSnapshot{}, ctx.Err()
		}
	}
	base.renew = func(context.Context) (jobdb.RenewableExecutionLease, error) {
		n := renewals.Add(1)
		next := *base
		next.expiry = time.Now().Add(600 * time.Millisecond)
		if n == 2 {
			close(renewed)
		}
		return &configurationSessionLease{sessionTestLease: &next, read: lease.read}, nil
	}
	session, err := startLeaseSession(context.Background(), lease)
	require.NoError(t, err)
	defer session.close()
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, e := session.GetConfiguration(context.Background()); done <- e }()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent read was coalesced")
		}
	}
	select {
	case <-renewed:
	case <-time.After(2 * time.Second):
		t.Fatal("configuration read blocked renewal")
	}
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
}

type expiredConfigurationTask struct {
	captured chan TaskContext
	release  chan struct{}
}

func (*expiredConfigurationTask) Name() string { return "expired-task" }
func (w *expiredConfigurationTask) Run(ctx TaskContext, input TaskData) (TaskData, error) {
	if _, err := ctx.GetConfiguration(context.Background()); err != nil {
		return nil, err
	}
	w.captured <- ctx
	<-w.release
	return input, nil
}

type catchesConfigurationTaskTimeout struct{ task *expiredConfigurationTask }

func (*catchesConfigurationTaskTimeout) Name() string { return "catches-timeout" }
func (w *catchesConfigurationTaskTimeout) Run(ctx ExecutionContext, input JobData) (JobData, error) {
	_, err := ctx.DoTask(TaskCall{Site: "task", Command: []byte("{}"), TaskType: w.task.Name(), ContractVersion: 1, Policy: RunPolicy{InvocationTimeout: AsDuration(20 * time.Millisecond)}}, func(context.Context) (TaskData, error) { return input, nil })
	if err == nil {
		return nil, fmt.Errorf("task did not time out")
	}
	taskCtx := <-w.task.captured
	_, secretErr := taskCtx.GetConfiguration(context.Background())
	close(w.task.release)
	if secretErr == nil {
		return nil, fmt.Errorf("expired task context still resolved secrets")
	}
	// The job still has a live lease; its own context remains usable.
	_, err = ctx.GetConfiguration(context.Background())
	return input, err
}

func TestTaskConfigurationContextExpiresWhileJobLeaseRemainsLive(t *testing.T) {
	ctx := context.Background()
	provider := &configurationCountingStore{Store: &configuration.MemoryStore{}}
	runtime := toy.New()
	runtime.Service.Store = provider
	task := &expiredConfigurationTask{captured: make(chan TaskContext, 1), release: make(chan struct{})}
	job := &catchesConfigurationTaskTimeout{task: task}
	ws := mustWorkSetForRunnerTest(t, job, task)
	handle, err := runtime.SubmitJob(ctx, SubmitJobRequest{Job: SubmitJob{TenantId: "tenant", JobType: job.Name(), Data: NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	lease, err := runtime.GetJobLease(ctx, GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "worker", Routes: []Route{{JobType: job.Name()}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{})
	_, err = runner.DoJob(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(1), provider.calls.Load())
}
