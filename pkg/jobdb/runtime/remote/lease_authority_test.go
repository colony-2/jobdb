package remote

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

// Renewing the backend must not extend authority held by an older handle,
// regardless of whether the handle is embedded or transported over HTTP.
func TestLeaseHandleExpiryAfterIndependentRenewal(t *testing.T) {
	for _, backend := range []string{"sqlite", "toy"} {
		for _, transport := range []string{"embedded", "http"} {
			for _, duringLookup := range []bool{false, true} {
				name := backend + "/" + transport + "/before_lookup"
				if duringLookup {
					name = backend + "/" + transport + "/during_lookup"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					var calls atomic.Int32
					runtime := providerRuntime(t, backend, testSecretProvider(func(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
						calls.Add(1)
						if req.Names[0] == "slow" {
							<-ctx.Done()
						}
						return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{req.Names[0]: {Value: "value"}}}, nil
					}))
					job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
					require.NoError(t, err)
					issued, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: 500 * time.Millisecond})
					require.NoError(t, err)
					old := issued.(jobdb.RenewableExecutionLease)
					renewer := runtime.(interface {
						RenewExecutionLeaseByID(context.Context, jobdb.JobKey, string, string, time.Duration) (jobdb.RenewableExecutionLease, error)
					})
					current, err := renewer.RenewExecutionLeaseByID(ctx, job.JobKey, old.LeaseID(), old.LeaseWorkerID(), time.Minute)
					require.NoError(t, err)
					if transport == "http" {
						signer := newLeaseTokenSigner()
						handler, err := newServer(runtime, signer)
						require.NoError(t, err)
						server := httptest.NewServer(handler)
						t.Cleanup(server.Close)
						client, err := New(server.URL, server.Client())
						require.NoError(t, err)
						convert := func(lease jobdb.RenewableExecutionLease) jobdb.RenewableExecutionLease {
							wire, err := (&proxyServer{tokens: signer}).toAPIExecutionLease(lease, time.Minute)
							require.NoError(t, err)
							remote, err := client.executionLeaseFromAPI(wire)
							require.NoError(t, err)
							return remote.(jobdb.RenewableExecutionLease)
						}
						old, current = convert(old), convert(current)
					}
					if duringLookup {
						result, err := old.ResolveSecrets(ctx, []string{"slow"})
						require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
						require.Empty(t, result.Secrets)
						require.Equal(t, int32(1), calls.Load())
					} else {
						time.Sleep(time.Until(old.LeaseExpiry()) + time.Millisecond)
					}
					before := calls.Load()
					result, err := old.ResolveSecrets(ctx, []string{"token"})
					require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
					require.Empty(t, result.Secrets)
					require.Equal(t, before, calls.Load(), "expired handles must not invoke the provider")
					_, err = old.GetConfiguration(ctx)
					require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
					_, err = old.Renew(ctx)
					require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
					require.ErrorIs(t, old.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
					require.Error(t, old.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}))
					_, err = old.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
					require.Error(t, err)
					_, err = old.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: job.JobKey}})
					require.Error(t, err)
					require.Error(t, old.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{
						Ordinal: 1, TaskType: "job", CreatedAt: time.Now().UTC(),
						Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}},
					}}))
					// The renewed handle still works against the same live backend lease.
					result, err = current.ResolveSecrets(ctx, []string{"token"})
					require.NoError(t, err)
					require.Equal(t, "value", result.Secrets["token"].Value)
					_, err = current.Renew(ctx)
					require.NoError(t, err)
				})
			}
		}
	}
}
