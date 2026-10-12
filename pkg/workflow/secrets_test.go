package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type workflowSecretProvider struct{ calls atomic.Int32 }

func (p *workflowSecretProvider) ResolveSecrets(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	p.calls.Add(1)
	return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "private-provider-value"}}}, nil
}

type secretReadingTask struct{ captured TaskContext }

func (*secretReadingTask) Name() string { return "secret-task" }
func (w *secretReadingTask) Run(ctx TaskContext, input TaskData) (TaskData, error) {
	w.captured = ctx
	// Repeated reads must reach the provider each time.
	for i := 0; i < 2; i++ {
		result, err := ctx.ResolveSecrets(context.Background(), []string{"token", "denied"})
		if err != nil {
			return nil, err
		}
		if len(result.Secrets) != 1 || result.Secrets["token"].Value != "private-provider-value" {
			return nil, fmt.Errorf("unexpected secret result")
		}
	}
	return input, nil
}

type secretReadingJob struct {
	readJob  bool
	captured JobContext
}

func (*secretReadingJob) Name() string { return "secret-job" }
func (w *secretReadingJob) Run(ctx JobContext, input JobData) (JobData, error) {
	w.captured = ctx
	if w.readJob {
		result, err := ctx.ResolveSecrets(context.Background(), []string{"token"})
		if err != nil {
			return nil, err
		}
		if result.Secrets["token"].Value != "private-provider-value" {
			return nil, fmt.Errorf("unexpected secret result")
		}
	}
	return ctx.DoTask(RunPolicy{}, "secret-task", input)
}

func TestWorkflowContextsResolveSecretsWithoutRecordingOrReplayingValues(t *testing.T) {
	for _, readJob := range []bool{false, true} {
		t.Run(fmt.Sprint("job-read=", readJob), func(t *testing.T) {
			ctx := context.Background()
			provider := &workflowSecretProvider{}
			runtime := toy.New(toy.WithSecretProvider(provider))
			job, task := &secretReadingJob{readJob: readJob}, &secretReadingTask{}
			ws := mustWorkSetForRunnerTest(t, job, task)
			handle, err := runtime.SubmitJob(ctx, SubmitJobRequest{Job: SubmitJob{TenantId: "tenant", JobType: job.Name(), Data: NewTaskDataOrPanic("input")}})
			require.NoError(t, err)
			lease, err := runtime.GetJobLease(ctx, GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "worker", Routes: []Route{{JobType: job.Name()}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{})
			_, err = runner.DoJob(ctx)
			require.NoError(t, err)
			wantCalls := int32(2)
			if readJob {
				wantCalls++
			}
			require.Equal(t, wantCalls, provider.calls.Load())
			chapters, err := runtime.ListChapters(ctx, ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			raw, err := json.Marshal(chapters)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "private-provider-value")
			// Saved contexts cannot read once their invocation has ended.
			result, err := task.captured.ResolveSecrets(ctx, []string{"token"})
			require.Error(t, err)
			require.Empty(t, result.Secrets)
			result, err = job.captured.ResolveSecrets(ctx, []string{"token"})
			require.Error(t, err)
			require.Empty(t, result.Secrets)
			require.Equal(t, wantCalls, provider.calls.Load())
			if !readJob {
				replay := newWorkerRunner(runtime, ws, nil, workerRunnerOptions{JobKey: handle.JobKey, Replay: true})
				_, err = replay.DoJob(ctx)
				require.NoError(t, err)
				require.Equal(t, wantCalls, provider.calls.Load())
			}
		})
	}
}

func TestSecretContextRejectsReplayAndMissingLease(t *testing.T) {
	runner := &workerRunner{replay: true}
	_, err := runner.ResolveSecrets(context.Background(), []string{"token"})
	require.ErrorIs(t, err, ErrReplayShouldNeverMutate)
	runner.replay = false
	_, err = runner.ResolveSecrets(context.Background(), []string{"token"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	_, err = (TaskContext{}).ResolveSecrets(context.Background(), []string{"token"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}

func TestSecretContextInvocationCancellationDiscardsResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	lease := &secretSessionLease{sessionTestLease: newSessionTestLease()}
	lease.resolve = func(callCtx context.Context, _ []string) (SecretResolveResult, error) {
		cancel()
		<-callCtx.Done()
		return SecretResolveResult{Secrets: map[string]SecretValue{"token": {Value: "late-value"}}}, nil
	}
	runner := &workerRunner{ctx: ctx, lease: lease}
	result, err := runner.ResolveSecrets(context.Background(), []string{"token"})
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
	require.Empty(t, result.Secrets)
}

type expiredSecretTask struct {
	captured chan TaskContext
	release  chan struct{}
}

func (*expiredSecretTask) Name() string { return "expired-task" }
func (w *expiredSecretTask) Run(ctx TaskContext, input TaskData) (TaskData, error) {
	w.captured <- ctx
	<-w.release
	return input, nil
}

type catchesSecretTaskTimeout struct{ task *expiredSecretTask }

func (*catchesSecretTaskTimeout) Name() string { return "catches-timeout" }
func (w *catchesSecretTaskTimeout) Run(ctx JobContext, input JobData) (JobData, error) {
	_, err := ctx.DoTask(RunPolicy{InvocationTimeout: AsDuration(20 * time.Millisecond)}, w.task.Name(), input)
	if err == nil {
		return nil, fmt.Errorf("task did not time out")
	}
	taskCtx := <-w.task.captured
	_, secretErr := taskCtx.ResolveSecrets(context.Background(), []string{"token"})
	close(w.task.release)
	if secretErr == nil {
		return nil, fmt.Errorf("expired task context still resolved secrets")
	}
	// The job still has a live lease; its own context remains usable.
	_, err = ctx.ResolveSecrets(context.Background(), []string{"token"})
	return input, err
}

func TestTaskSecretContextExpiresWhileJobLeaseRemainsLive(t *testing.T) {
	ctx := context.Background()
	provider := &workflowSecretProvider{}
	runtime := toy.New(toy.WithSecretProvider(provider))
	task := &expiredSecretTask{captured: make(chan TaskContext, 1), release: make(chan struct{})}
	job := &catchesSecretTaskTimeout{task: task}
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
