package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type freshIntegrationProvider struct {
	calls    atomic.Int32
	requests chan jobdb.SecretResolveRequest
}

func (p *freshIntegrationProvider) ResolveSecrets(_ context.Context, req jobdb.SecretResolveRequest) (SecretResolveResult, error) {
	p.requests <- req
	expiry := req.Lease.AuthorizedNotAfter
	return SecretResolveResult{Secrets: map[string]SecretValue{"token": {Value: fmt.Sprintf("private-token-%d", p.calls.Add(1)), ExpiresAt: &expiry, LeaseBound: true}}}, nil
}

func TestFreshSecretsBootstrapTasksAndRenewal(t *testing.T) {
	for _, transport := range []string{"embedded", "http"} {
		for _, versioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/versioned=%t", transport, versioned), func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				provider := &freshIntegrationProvider{requests: make(chan jobdb.SecretResolveRequest, 16)}
				var runtime jobdb.WorkflowRuntime = toy.New(toy.WithSecretProvider(provider))
				if transport == "http" {
					server := httptest.NewServer(remote.NewServer(runtime))
					t.Cleanup(server.Close)
					var err error
					runtime, err = remote.New(server.URL, server.Client())
					require.NoError(t, err)
				}
				var bootstrap SecretResolver
				var capturedTask TaskContext
				var capturedJob SecretResolver
				var initialExpiry time.Time
				var currentExpiry func() time.Time
				check := func(resolver SecretResolver, fresh bool, want int) error {
					var result SecretResolveResult
					var err error
					if fresh {
						result, err = resolver.ResolveSecretsFresh(ctx, []string{"token"})
					} else {
						result, err = resolver.ResolveSecrets(ctx, []string{"token"})
					}
					if err != nil {
						return err
					}
					if result.Secrets["token"].Value != fmt.Sprintf("private-token-%d", want) {
						return fmt.Errorf("unexpected cached secret generation")
					}
					return nil
				}
				task := cacheTestTask{run: func(tc TaskContext, data TaskData) (TaskData, error) {
					capturedTask = tc
					return data, check(tc, true, 2)
				}}
				body := func(resolver SecretResolver, taskCall func() (TaskData, error)) (JobData, error) {
					capturedJob = resolver
					if err := check(resolver, false, 1); err != nil {
						return nil, err
					}
					// Wait for the runner's normal heartbeat, without renewing from a read.
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for !currentExpiry().After(initialExpiry) {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-ticker.C:
						}
					}
					if _, err := taskCall(); err != nil {
						return nil, err
					}
					if err := check(resolver, false, 2); err != nil {
						return nil, err
					}
					if err := check(bootstrap, true, 3); err != nil {
						return nil, err
					}
					if err := check(resolver, false, 3); err != nil {
						return nil, err
					}
					if err := check(resolver, true, 4); err != nil {
						return nil, err
					}
					return NewTaskDataOrPanic("done"), nil
				}
				var worker JobWorker = cacheTestJob{run: func(jc JobContext, data JobData) (JobData, error) {
					return body(jc, func() (TaskData, error) { return jc.DoTask(RunPolicy{}, task.Name(), data) })
				}}
				if versioned {
					worker = ExecutionWorker{JobType: "cache-job", Execute: func(ec ExecutionContext, data JobData) (JobData, error) {
						return body(ec, func() (TaskData, error) {
							return ec.DoTask(TaskCall{Site: "read", TaskType: task.Name(), ContractVersion: 1, Command: json.RawMessage(`1`)}, func(context.Context) (TaskData, error) { return data, nil })
						})
					}}
				}
				job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: NewTaskDataOrPanic(1)}})
				require.NoError(t, err)
				runnable, err := GetJobForRun(ctx, runtime, GetJobForRunRequest{JobKey: job.JobKey, JobWorker: worker, TaskWorkers: []TaskWorker{task}, LeaseDuration: 900 * time.Millisecond,
					BeforeRun: func(runCtx context.Context, lease ExecutionLease) error {
						var ok bool
						bootstrap, ok = SecretResolverFromContext(runCtx)
						if !ok {
							return fmt.Errorf("missing bootstrap resolver")
						}
						currentExpiry = lease.(interface{ LeaseExpiry() time.Time }).LeaseExpiry
						initialExpiry = currentExpiry()
						return check(bootstrap, true, 1)
					}})
				require.NoError(t, err)
				outcome, err := runnable.Run(nil)
				require.NoError(t, err)
				require.Equal(t, JobRunCompleted, outcome.Status, "%v", outcome.JobError)
				require.Equal(t, int32(4), provider.calls.Load())
				first, second := <-provider.requests, <-provider.requests
				require.True(t, second.Lease.AuthorizedNotAfter.After(first.Lease.AuthorizedNotAfter))
				require.Equal(t, first.Lease.LeaseID, second.Lease.LeaseID)
				for _, resolver := range []SecretResolver{bootstrap, capturedJob, capturedTask} {
					result, err := resolver.ResolveSecretsFresh(context.Background(), []string{"token"})
					require.Error(t, err)
					require.Empty(t, result.Secrets)
				}
				require.Equal(t, int32(4), provider.calls.Load())
				chapters, err := runtime.ListChapters(ctx, ListChaptersRequest{JobKey: job.JobKey})
				require.NoError(t, err)
				raw, err := json.Marshal(chapters)
				require.NoError(t, err)
				require.NotContains(t, string(raw), "private-token-")
			})
		}
	}
}

func TestBootstrapSecretResolverFollowsAttemptCacheReset(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprint(versioned), func(t *testing.T) {
			ctx := context.Background()
			provider := &freshIntegrationProvider{requests: make(chan jobdb.SecretResolveRequest, 8)}
			runtime := toy.New(toy.WithSecretProvider(provider))
			var bootstrap SecretResolver
			var runs int
			body := func(resolver SecretResolver) (JobData, error) {
				runs++
				result, err := resolver.ResolveSecrets(ctx, []string{"token"})
				if err != nil {
					return nil, err
				}
				if result.Secrets["token"].Value != fmt.Sprintf("private-token-%d", runs) {
					return nil, fmt.Errorf("cache crossed attempt boundary")
				}
				fromHook, err := bootstrap.ResolveSecrets(ctx, []string{"token"})
				if err != nil {
					return nil, err
				}
				if fromHook.Secrets["token"].Value != result.Secrets["token"].Value {
					return nil, fmt.Errorf("bootstrap hook did not follow active attempt")
				}
				if runs == 1 {
					return nil, AppError{Payload: AppErrorPayload{Message: "retry", Level: "error"}}
				}
				return NewTaskDataOrPanic("done"), nil
			}
			var worker JobWorker = cacheTestJob{run: func(jc JobContext, _ JobData) (JobData, error) { return body(jc) }}
			if versioned {
				worker = ExecutionWorker{JobType: "cache-job", Execute: func(ec ExecutionContext, _ JobData) (JobData, error) { return body(ec) }}
			}
			job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: NewTaskDataOrPanic(1), RunPolicy: RunPolicy{Retry: RetryPolicy{MaximumAttempts: 2, BackoffCoefficient: 1}}}})
			require.NoError(t, err)
			runnable, err := GetJobForRun(ctx, runtime, GetJobForRunRequest{JobKey: job.JobKey, JobWorker: worker, BeforeRun: func(ctx context.Context, _ ExecutionLease) error {
				bootstrap, _ = SecretResolverFromContext(ctx)
				_, err := bootstrap.ResolveSecretsFresh(ctx, []string{"token"})
				return err
			}})
			require.NoError(t, err)
			outcome, err := runnable.Run(nil)
			require.NoError(t, err)
			require.Equal(t, JobRunCompleted, outcome.Status, "%v", outcome.JobError)
			require.Equal(t, 2, runs)
			require.Equal(t, int32(2), provider.calls.Load())
		})
	}
}
