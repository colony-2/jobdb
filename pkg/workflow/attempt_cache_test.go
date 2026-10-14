package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type cacheTestLease struct {
	*fakeExecutionLease
	configuration func(context.Context) (ConfigurationSnapshot, error)
	secrets       func(context.Context, []string) (SecretResolveResult, error)
}

func (l *cacheTestLease) GetConfiguration(ctx context.Context) (ConfigurationSnapshot, error) {
	return l.configuration(ctx)
}
func (l *cacheTestLease) ResolveSecrets(ctx context.Context, names []string) (SecretResolveResult, error) {
	return l.secrets(ctx, names)
}

// Run the concurrency and cancellation contract against both cache APIs.
func TestAttemptCachesConcurrentReadsAndFailures(t *testing.T) {
	for _, kind := range []string{"configuration", "secrets"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			entered, release := make(chan struct{}, 1), make(chan struct{})
			fetch := func(ctx context.Context) error {
				n := calls.Add(1)
				if n == 1 {
					return jobdb.ErrConfigurationUnavailable
				}
				entered <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			lease := &cacheTestLease{}
			lease.configuration = func(ctx context.Context) (ConfigurationSnapshot, error) {
				err := fetch(ctx)
				return ConfigurationSnapshot{Revision: "1", Values: map[string]json.RawMessage{"x": json.RawMessage(`1`)}}, err
			}
			lease.secrets = func(ctx context.Context, names []string) (SecretResolveResult, error) {
				err := fetch(ctx)
				return SecretResolveResult{Secrets: map[string]SecretValue{"x": {Value: "one"}}}, err
			}
			runner := &workerRunner{ctx: ctx, lease: lease}
			read := func(ctx context.Context) error {
				if kind == "configuration" {
					_, err := runner.GetConfiguration(ctx)
					return err
				}
				_, err := runner.ResolveSecrets(ctx, []string{"x"})
				return err
			}
			require.Error(t, read(ctx)) // Neither errors nor their accompanying data are cached.
			done := make(chan error, 20)
			go func() { done <- read(ctx) }()
			<-entered
			waiting, stopWaiting := context.WithTimeout(ctx, 20*time.Millisecond)
			require.Error(t, read(waiting))
			stopWaiting()
			canceled, stop := context.WithCancel(ctx)
			stop()
			require.Error(t, read(canceled)) // Waiting for another reader honors cancellation.
			for i := 0; i < 19; i++ {
				go func() { done <- read(ctx) }()
			}
			close(release)
			for i := 0; i < 20; i++ {
				require.NoError(t, <-done)
			}
			require.Equal(t, int32(2), calls.Load())
			canceled, stop = context.WithCancel(ctx)
			stop()
			require.Error(t, read(canceled)) // Cache hits honor caller cancellation too.
			runner.rescheduled.Store(true)
			require.ErrorIs(t, read(ctx), jobdb.ErrExecutionLeaseLost)
			runner.rescheduled.Store(false)
			cancel()
			require.Error(t, read(context.Background()))
			require.Equal(t, int32(2), calls.Load())
		})
	}
}

func TestAttemptConfigurationSnapshotCopies(t *testing.T) {
	source := ConfigurationSnapshot{Revision: "one", Values: map[string]json.RawMessage{"a": json.RawMessage(`{"x":1}`)}}
	calls := 0
	runner := &workerRunner{lease: &cacheTestLease{configuration: func(context.Context) (ConfigurationSnapshot, error) { calls++; return source, nil }}}
	first, err := runner.GetConfiguration(context.Background())
	require.NoError(t, err)
	first.Values["a"][5] = '9'
	first.Values["added"] = json.RawMessage(`true`)
	source.Values["a"][5] = '8'
	source.Values["new"] = json.RawMessage(`true`)
	next, err := runner.GetConfiguration(context.Background())
	require.NoError(t, err)
	require.Equal(t, `{"x":1}`, string(next.Values["a"]))
	require.Len(t, next.Values, 1)
	require.Equal(t, 1, calls)
}

func TestAttemptCachesEmptyResults(t *testing.T) {
	var configurations, secrets int
	runner := &workerRunner{lease: &cacheTestLease{
		configuration: func(context.Context) (ConfigurationSnapshot, error) {
			configurations++
			return ConfigurationSnapshot{}, nil
		},
		secrets: func(context.Context, []string) (SecretResolveResult, error) {
			secrets++
			return SecretResolveResult{}, nil
		},
	}}
	for i := 0; i < 2; i++ {
		configuration, err := runner.GetConfiguration(context.Background())
		require.NoError(t, err)
		require.Empty(t, configuration.Values)
		result, err := runner.ResolveSecrets(context.Background(), []string{"missing"})
		require.NoError(t, err)
		require.Empty(t, result.Secrets)
	}
	require.Equal(t, 1, configurations)
	require.Equal(t, 1, secrets)
}

func TestAttemptSecretsCacheNamesOmissionsCopiesAndExpiry(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	var requests [][]string
	lease := &cacheTestLease{secrets: func(_ context.Context, names []string) (SecretResolveResult, error) {
		requests = append(requests, append([]string(nil), names...))
		result := SecretResolveResult{Secrets: map[string]SecretValue{}}
		for _, name := range names {
			if name != "denied" {
				result.Secrets[name] = SecretValue{Value: fmt.Sprint(len(requests)), ExpiresAt: &expiry}
			}
		}
		return result, nil
	}}
	runner := &workerRunner{lease: lease}
	first, err := runner.ResolveSecrets(context.Background(), []string{"a", "denied"})
	require.NoError(t, err)
	*first.Secrets["a"].ExpiresAt = time.Now().Add(-time.Hour)
	first.Secrets["a"] = SecretValue{Value: "modified"}
	first.Secrets["denied"] = SecretValue{Value: "forged"}
	cached, err := runner.ResolveSecrets(context.Background(), []string{"a", "denied"})
	require.NoError(t, err)
	require.Equal(t, "1", cached.Secrets["a"].Value)
	require.NotContains(t, cached.Secrets, "denied")
	second, err := runner.ResolveSecrets(context.Background(), []string{"b", "denied", "a"})
	require.NoError(t, err)
	require.Equal(t, "2", second.Secrets["a"].Value)
	require.Equal(t, "2", second.Secrets["b"].Value)
	require.NotContains(t, second.Secrets, "denied")
	require.Equal(t, [][]string{{"a", "denied"}, {"a", "b"}}, requests)
	subset, err := runner.ResolveSecrets(context.Background(), []string{"a"})
	require.NoError(t, err)
	require.Len(t, subset.Secrets, 1)
	for _, names := range [][]string{nil, {""}, {"a", "a"}, {string([]byte{0xff})}} {
		_, err = runner.ResolveSecrets(context.Background(), names)
		require.ErrorIs(t, err, jobdb.ErrInvalidSecretRequest)
	}
	require.Len(t, requests, 2)
	// Advance the cached deadline without a wall-clock sleep.
	expired := time.Now().Add(-time.Second)
	runner.secretCache.value["a"].ExpiresAt = &expired
	result, err := runner.ResolveSecrets(context.Background(), []string{"a", "b"})
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
	require.Empty(t, result.Secrets)
	require.Len(t, requests, 2, "expiration must not silently rotate an attempt's value")
	_, err = runner.ResolveSecrets(context.Background(), []string{"c"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, requests[2])
	refreshed, err := runner.ResolveSecrets(context.Background(), []string{"a", "b", "c"})
	require.NoError(t, err)
	for _, value := range refreshed.Secrets {
		require.Equal(t, "3", value.Value)
	}
	require.Len(t, requests, 3)
}

func TestSecretCacheExpansionRefreshesAvailableNames(t *testing.T) {
	var requests [][]string
	fail, omitFoo := false, false
	runner := &workerRunner{lease: &cacheTestLease{secrets: func(_ context.Context, names []string) (SecretResolveResult, error) {
		requests = append(requests, append([]string(nil), names...))
		result := SecretResolveResult{Secrets: map[string]SecretValue{}}
		for _, name := range names {
			if name != "denied" && !(name == "foo" && omitFoo) {
				result.Secrets[name] = SecretValue{Value: fmt.Sprint(len(requests))}
			}
		}
		if fail {
			return result, jobdb.ErrSecretsUnavailable
		}
		return result, nil
	}}}
	ctx := context.Background()
	first, err := runner.ResolveSecrets(ctx, []string{"foo", "denied"})
	require.NoError(t, err)
	require.Equal(t, "1", first.Secrets["foo"].Value)

	fail = true
	result, err := runner.ResolveSecrets(ctx, []string{"bar"})
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
	require.Empty(t, result.Secrets)
	cached, err := runner.ResolveSecrets(ctx, []string{"foo", "denied"})
	require.NoError(t, err)
	require.Equal(t, "1", cached.Secrets["foo"].Value)
	require.NotContains(t, cached.Secrets, "denied")
	require.Len(t, requests, 2)

	fail = false
	result, err = runner.ResolveSecrets(ctx, []string{"bar"})
	require.NoError(t, err)
	require.Len(t, result.Secrets, 1, "return only the caller's requested names")
	require.Equal(t, "3", result.Secrets["bar"].Value)
	cached, err = runner.ResolveSecrets(ctx, []string{"foo", "bar", "denied"})
	require.NoError(t, err)
	require.Equal(t, "3", cached.Secrets["foo"].Value)
	require.Equal(t, "3", cached.Secrets["bar"].Value)
	require.NotContains(t, cached.Secrets, "denied")
	require.Equal(t, "1", first.Secrets["foo"].Value, "earlier caller-owned results stay unchanged")
	require.Len(t, requests, 3)

	omitFoo = true
	_, err = runner.ResolveSecrets(ctx, []string{"baz"})
	require.NoError(t, err)
	omitFoo = false // A previously omitted name stays omitted, even if now allowed.
	_, err = runner.ResolveSecrets(ctx, []string{"qux"})
	require.NoError(t, err)
	cached, err = runner.ResolveSecrets(ctx, []string{"foo", "denied"})
	require.NoError(t, err)
	require.Empty(t, cached.Secrets)
	require.Equal(t, [][]string{
		{"denied", "foo"}, {"bar", "foo"}, {"bar", "foo"},
		{"bar", "baz", "foo"}, {"bar", "baz", "qux"},
	}, requests)
}

func TestSecretCacheExpansionRejectsIncompleteRefresh(t *testing.T) {
	for _, failure := range []string{"canceled", "expired"} {
		t.Run(failure, func(t *testing.T) {
			callCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			runner := &workerRunner{lease: &cacheTestLease{secrets: func(_ context.Context, names []string) (SecretResolveResult, error) {
				calls++
				result := SecretResolveResult{Secrets: map[string]SecretValue{}}
				for _, name := range names {
					result.Secrets[name] = SecretValue{Value: fmt.Sprint(calls)}
				}
				if calls == 2 {
					if failure == "canceled" {
						cancel()
					} else {
						expired := time.Now().Add(-time.Hour)
						value := result.Secrets["foo"]
						value.ExpiresAt = &expired
						result.Secrets["foo"] = value
					}
				}
				return result, nil
			}}}
			_, err := runner.ResolveSecrets(context.Background(), []string{"foo"})
			require.NoError(t, err)
			result, err := runner.ResolveSecrets(callCtx, []string{"bar"})
			require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
			require.Empty(t, result.Secrets)
			cached, err := runner.ResolveSecrets(context.Background(), []string{"foo"})
			require.NoError(t, err)
			require.Equal(t, "1", cached.Secrets["foo"].Value)
			require.Equal(t, 2, calls)
			_, err = runner.ResolveSecrets(context.Background(), []string{"bar"})
			require.NoError(t, err)
			require.Equal(t, 3, calls, "failed fetch must not mark new names as unavailable")
		})
	}
}

type cacheTestJob struct {
	run func(JobContext, JobData) (JobData, error)
}

func (cacheTestJob) Name() string                                        { return "cache-job" }
func (w cacheTestJob) Run(ctx JobContext, data JobData) (JobData, error) { return w.run(ctx, data) }

type cacheTestTask struct {
	run func(TaskContext, TaskData) (TaskData, error)
}

func (cacheTestTask) Name() string                                           { return "cache-task" }
func (w cacheTestTask) Run(ctx TaskContext, data TaskData) (TaskData, error) { return w.run(ctx, data) }

type cacheReader interface {
	GetConfiguration(context.Context) (ConfigurationSnapshot, error)
	ResolveSecrets(context.Context, []string) (SecretResolveResult, error)
}

func readCacheVersion(ctx cacheReader) (string, error) {
	config, err := ctx.GetConfiguration(context.Background())
	if err != nil {
		return "", err
	}
	secrets, err := ctx.ResolveSecrets(context.Background(), []string{"token"})
	if err != nil {
		return "", err
	}
	if config.Revision != secrets.Secrets["token"].Value {
		return "", fmt.Errorf("different cache scopes")
	}
	return config.Revision, nil
}

func TestAttemptCacheLifecycle(t *testing.T) {
	for _, mode := range []string{"task-retry", "job-retry", "resume"} {
		t.Run(mode, func(t *testing.T) {
			runtime := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: mode}
			var configCalls, secretCalls atomic.Int32
			newLease := func() *cacheTestLease {
				return &cacheTestLease{
					fakeExecutionLease: &fakeExecutionLease{runtime: runtime, job: JobHandle{JobKey: key}, route: Route{JobType: "cache-job"}, payload: json.RawMessage(`{}`)},
					configuration: func(context.Context) (ConfigurationSnapshot, error) {
						return ConfigurationSnapshot{Revision: fmt.Sprint(configCalls.Add(1))}, nil
					},
					secrets: func(context.Context, []string) (SecretResolveResult, error) {
						return SecretResolveResult{Secrets: map[string]SecretValue{"token": {Value: fmt.Sprint(secretCalls.Add(1))}}}, nil
					},
				}
			}
			var mu sync.Mutex
			var observed []string
			record := func(ctx cacheReader) error {
				value, err := readCacheVersion(ctx)
				if err != nil {
					return err
				}
				mu.Lock()
				observed = append(observed, value)
				mu.Unlock()
				return nil
			}
			retry := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 2, BackoffCoefficient: 1}}
			var taskRuns, jobRuns int
			task := cacheTestTask{run: func(ctx TaskContext, data TaskData) (TaskData, error) {
				taskRuns++
				if err := record(ctx); err != nil {
					return nil, err
				}
				if mode == "task-retry" && taskRuns == 1 {
					return nil, AppError{Payload: AppErrorPayload{Message: "retry", Level: "error"}}
				}
				return data, nil
			}}
			var old JobContext
			job := cacheTestJob{run: func(ctx JobContext, data JobData) (JobData, error) {
				jobRuns++
				if err := record(ctx); err != nil {
					return nil, err
				}
				if jobRuns == 1 {
					old = ctx
					if mode == "job-retry" {
						return nil, AppError{Payload: AppErrorPayload{Message: "retry", Level: "error"}}
					}
					if mode == "resume" {
						return nil, ctx.Yield(context.Background(), YieldRequest{Reschedule: RescheduleExecutionRequest{NextRoute: Route{JobType: "cache-job"}}})
					}
				} else {
					if _, err := readCacheVersion(old); err == nil {
						return nil, fmt.Errorf("old context survived restart")
					}
				}
				return ctx.DoTask(retry, "cache-task", data)
			}}
			ws := mustWorkSetForRunnerTest(t, job, task)
			seedJobStartForTest(t, runtime, key, job.Name(), NewTaskDataOrPanic("input"), retry)
			runner := newWorkerRunner(runtime, ws, newLease(), workerRunnerOptions{JobPolicy: retry})
			_, err := runner.DoJob(context.Background())
			require.NoError(t, err)
			if mode == "resume" {
				require.True(t, runner.rescheduled.Load())
				runner = newWorkerRunner(runtime, ws, newLease(), workerRunnerOptions{JobPolicy: retry})
				_, err = runner.DoJob(context.Background())
				require.NoError(t, err)
			}
			if mode == "task-retry" {
				require.Equal(t, []string{"1", "1", "1"}, observed)
				require.Equal(t, int32(1), configCalls.Load())
			} else {
				require.Equal(t, []string{"1", "2", "2"}, observed)
				require.Equal(t, int32(2), configCalls.Load())
			}
			require.Equal(t, configCalls.Load(), secretCalls.Load())
		})
	}
}
