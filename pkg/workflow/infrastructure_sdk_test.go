package workflow

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

// Provider errors are handleable unless the provider has classified them as
// execution infrastructure. Ignoring a classified error must still stop the run.
func TestInfrastructureSDKFailures(t *testing.T) {
	source := JobKey{TenantId: "tenant", JobId: "source"}
	ordinal := int64(7)
	cause := errors.New("provider storage unavailable")
	for _, operation := range []string{"delegation", "configuration", "secrets", "fresh-secrets"} {
		for _, failure := range []struct {
			name           string
			err            error
			infrastructure bool
		}{
			{"classified", jobdb.ClassifyExecutionInfrastructure(cause, jobdb.ExecutionInfrastructureRuntimeIO, "provider_read", &source, &ordinal), true},
			{"determinism", jobdb.ErrWorkflowNotDeterministic, true},
			{"provider", cause, false},
			{"semantic", jobdb.ErrInvalidSecretRequest, false},
			{"authority", jobdb.ErrLeasePermissionDenied, false},
			{"unavailable", jobdb.ErrConfigurationUnavailable, false},
			{"cancelled-call", context.Canceled, false},
		} {
			t.Run(operation+"/"+failure.name, func(t *testing.T) {
				var runner *workerRunner
				var rt *runnerTestRuntime
				var observed map[int64]Chapter
				var dispatched int
				var providerCalls int
				injected := func() error { providerCalls++; return failure.err }
				runner, lease, runtime := infrastructureRunner(t, func(c ExecutionContext, in JobData) (JobData, error) {
					tc := c.(*executionContext).taskContext()
					invoke := func() error {
						ctx := celltest.Context(context.Background())
						switch operation {
						case "delegation":
							_, err := tc.CreateDelegation(ctx, jobdb.CreateDelegationRequest{})
							return err
						case "configuration":
							_, err := tc.GetConfiguration(ctx)
							return err
						case "secrets":
							_, err := tc.ResolveSecrets(ctx, []string{"x"})
							return err
						default:
							_, err := tc.ResolveSecretsFresh(ctx, []string{"x"})
							return err
						}
					}
					require.ErrorIs(t, invoke(), failure.err)
					rt.mu.Lock()
					observed = maps.Clone(rt.chapters[runner.jobKey])
					rt.mu.Unlock()
					if failure.infrastructure {
						require.ErrorIs(t, invoke(), failure.err, "later SDK call returns latched error")
						require.Equal(t, 1, providerCalls, "no later provider call")
					}
					_, err := c.DoScope("late", ScopeOptions{}, func(ExecutionContext) (JobData, error) { dispatched++; return in, nil })
					if failure.infrastructure {
						require.ErrorIs(t, err, failure.err)
					} else {
						require.NoError(t, err)
					}
					return in, nil // Deliberately ignore the original error and return success.
				})
				rt = runtime
				guarded := runner.runtime.(*guardedRuntime)
				guarded.WorkflowRuntime = &taskDelegationRuntime{WorkflowRuntime: guarded.WorkflowRuntime, create: func(context.Context, ExecutionLease, jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
					return nil, injected()
				}}
				runner.lease = &cacheTestLease{fakeExecutionLease: lease,
					configuration: func(context.Context) (ConfigurationSnapshot, error) { return ConfigurationSnapshot{}, injected() },
					secrets:       func(context.Context, []string) (SecretResolveResult, error) { return SecretResolveResult{}, injected() },
				}
				_, err := runner.DoJob(celltest.Context(context.Background()))
				_, stops, complete, reschedule := lease.snapshot()
				require.Equal(t, 1, stops)
				require.Empty(t, reschedule)
				if failure.infrastructure {
					require.ErrorIs(t, err, failure.err)
					require.Zero(t, dispatched)
					require.Empty(t, complete)
					require.Equal(t, observed, rt.chapters[runner.jobKey], "history unchanged after observation")
					if failure.name == "classified" {
						var detail *jobdb.ExecutionInfrastructureError
						require.ErrorAs(t, err, &detail)
						require.Equal(t, &source, detail.JobKey)
						require.Equal(t, &ordinal, detail.Ordinal)
						require.Equal(t, "provider_read", detail.Operation)
					}
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, dispatched)
					require.Len(t, complete, 1)
				}
			})
		}
	}
}

func TestInfrastructureSDKFailuresBeforeRun(t *testing.T) {
	for _, operation := range []string{"configuration", "secrets", "fresh-secrets"} {
		t.Run(operation, func(t *testing.T) {
			failure := jobdb.ClassifyExecutionInfrastructure(errors.New("bootstrap provider unavailable"), jobdb.ExecutionInfrastructureRuntimeIO, "bootstrap", nil, nil)
			runner, lease, rt := infrastructureRunner(t, func(ExecutionContext, JobData) (JobData, error) {
				t.Fatal("worker dispatched after bootstrap failure")
				return nil, nil
			})
			before := maps.Clone(rt.chapters[runner.jobKey])
			runner.lease = &cacheTestLease{fakeExecutionLease: lease,
				configuration: func(context.Context) (ConfigurationSnapshot, error) { return ConfigurationSnapshot{}, failure },
				secrets:       func(context.Context, []string) (SecretResolveResult, error) { return SecretResolveResult{}, failure },
			}
			runner.beforeRun = func(ctx context.Context, _ ExecutionLease) error {
				var err error
				if operation == "configuration" {
					_, err = runner.GetConfiguration(ctx)
				} else {
					resolver, ok := SecretResolverFromContext(ctx)
					require.True(t, ok)
					if operation == "fresh-secrets" {
						_, err = resolver.ResolveSecretsFresh(ctx, []string{"x"})
					} else {
						_, err = resolver.ResolveSecrets(ctx, []string{"x"})
					}
				}
				require.ErrorIs(t, err, failure)
				return nil // Bootstrap code can ignore an error too.
			}
			_, err := runner.DoJob(celltest.Context(context.Background()))
			require.ErrorIs(t, err, failure)
			_, stops, complete, reschedule := lease.snapshot()
			require.Equal(t, 1, stops)
			require.Empty(t, complete)
			require.Empty(t, reschedule)
			require.Equal(t, before, rt.chapters[runner.jobKey])
		})
	}
}
