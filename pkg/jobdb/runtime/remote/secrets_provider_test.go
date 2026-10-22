package remote

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
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	sqlitesecrets "github.com/colony-2/jobdb/pkg/jobdb/secrets/sqlite"
	"github.com/stretchr/testify/require"
)

type testSecretProvider func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error)

func (f testSecretProvider) ResolveSecrets(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	return f(ctx, req)
}

func providerRuntime(t *testing.T, backend string, provider jobdb.SecretProvider) jobdb.WorkflowRuntime {
	t.Helper()
	if backend == "toy" {
		return toy.New(toy.WithSecretProvider(provider))
	}
	r, err := sqliteruntime.NewFromConfig(context.Background(), sqliteruntime.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db"), SecretProvider: provider})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close(context.Background())) })
	return r
}

// The same providers work over both kinds of job storage and both transports.
func TestSecretProvidersIndependentOfJobStorage(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, transport := range []string{"embedded", "http"} {
			for _, providerType := range []string{"custom", "sqlite"} {
				t.Run(backend+"/"+transport+"/"+providerType, func(t *testing.T) {
					ctx := context.Background()
					requests := make(chan jobdb.SecretResolveRequest, 4)
					var version atomic.Int32
					var provider jobdb.SecretProvider = testSecretProvider(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
						requests <- req
						return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: fmt.Sprint(version.Load())}}}, nil
					})
					var store *sqlitesecrets.Store
					if providerType == "sqlite" {
						dir := t.TempDir()
						var err error
						store, err = sqlitesecrets.Open(ctx, filepath.Join(dir, "secrets.db"), jobdb.SecretLimits{})
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, store.Close()) })
						provider = store
					}
					backendRuntime := providerRuntime(t, backend, provider)
					runtime := backendRuntime
					if transport == "http" {
						server := httptest.NewServer(NewServer(runtime))
						t.Cleanup(server.Close)
						client, err := New(server.URL, server.Client())
						require.NoError(t, err)
						runtime = client
					}
					job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "job", Metadata: json.RawMessage(`{"team":"build"}`), Data: jobdb.NewTaskDataOrPanic(1)}})
					require.NoError(t, err)
					lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					require.NotNil(t, lease)
					for i := 1; i <= 2; i++ {
						version.Store(int32(i))
						if store != nil {
							require.NoError(t, store.Set(ctx, "tenant", "token", fmt.Sprint(i)))
						}
						result, err := lease.ResolveSecrets(ctx, []string{"token", "missing"})
						require.NoError(t, err)
						require.Len(t, result.Secrets, 1)
						require.Equal(t, fmt.Sprint(i), result.Secrets["token"].Value)
						if store == nil {
							req := <-requests
							require.Equal(t, job.JobKey.TenantId, req.TenantID)
							require.Equal(t, job.JobKey.JobId, req.JobID)
							require.JSONEq(t, `{"team":"build"}`, string(req.AppMetadata))
							require.Nil(t, req.Principal)
							require.Equal(t, []string{"token", "missing"}, req.Names)
						}
					}
					require.NoError(t, runtime.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: job.JobKey}))
					result, err := lease.ResolveSecrets(ctx, []string{"token"})
					require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
					require.Empty(t, result.Secrets)
					require.Empty(t, requests)
					if r, ok := backendRuntime.(*sqliteruntime.Runtime); ok {
						require.NoError(t, r.Close(ctx))
						// Injected providers remain caller-owned and usable after runtime close.
						if store != nil {
							require.NoError(t, store.Set(ctx, "tenant", "token", "still-open"))
						}
					}
				})
			}
		}
	}
}

func TestToyProviderReceivesOnlySignedPrincipal(t *testing.T) {
	received := make(chan jobdb.SecretResolveRequest, 1)
	backend := toy.New(toy.WithSecretProvider(testSecretProvider(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		received <- req
		return jobdb.SecretResolveResult{}, nil
	})))
	signer := newLeaseTokenSigner()
	handler, err := newServer(backend, signer)
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	lease := secretsRemoteLease(t, client, "tenant").(*remoteExecutionLease)
	principal := "signed-subject"
	lease.leaseToken, err = signer.mintWithPrincipal(lease.jobKey, lease.leaseID, "worker", "", time.Now().Add(time.Minute), time.Minute, &principal)
	require.NoError(t, err)
	_, err = lease.ResolveSecrets(context.Background(), []string{"token"})
	require.NoError(t, err)
	req := <-received
	require.Equal(t, &principal, req.Principal)
	status, _ := secretHTTP(t, server.URL, secretsPath(lease), lease.LeaseToken(), `{"names":["token"],"principal":"forged"}`)
	require.Equal(t, 400, status)
	require.Empty(t, received)
}

func TestLeaseBoundProviderContextAcrossTransports(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, transport := range []string{"embedded", "http"} {
			t.Run(backend+"/"+transport, func(t *testing.T) {
				ctx := context.Background()
				requests := make(chan jobdb.SecretResolveRequest, 2)
				provider := testSecretProvider(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
					requests <- req
					expiry := req.Lease.AuthorizedNotAfter
					return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{
						"token":    {Value: "dynamic", LeaseBound: true, ExpiresAt: &expiry},
						"password": {Value: "static"},
					}}, nil
				})
				runtime := providerRuntime(t, backend, provider)
				if transport == "http" {
					server := httptest.NewServer(NewServer(runtime))
					t.Cleanup(server.Close)
					var err error
					runtime, err = New(server.URL, server.Client())
					require.NoError(t, err)
				}
				job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "job", Metadata: json.RawMessage(`{"lease":{"LeaseID":"forged","AuthorizedNotAfter":"2099-01-01T00:00:00Z"}}`), Data: jobdb.NewTaskDataOrPanic(1)}})
				require.NoError(t, err)
				lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				result, err := lease.ResolveSecrets(ctx, []string{"token", "password"})
				require.NoError(t, err)
				req := <-requests
				require.Equal(t, lease.LeaseID(), req.Lease.LeaseID)
				require.Equal(t, "worker", req.Lease.WorkerID)
				require.WithinDuration(t, lease.(jobdb.RenewableExecutionLease).LeaseExpiry(), req.Lease.AuthorizedNotAfter, 0)
				require.False(t, req.Lease.LeaseExpiresAt.Before(req.Lease.AuthorizedNotAfter))
				require.True(t, result.Secrets["token"].LeaseBound)
				require.WithinDuration(t, req.Lease.AuthorizedNotAfter, *result.Secrets["token"].ExpiresAt, 0)
				require.False(t, result.Secrets["password"].LeaseBound)
				require.Nil(t, result.Secrets["password"].ExpiresAt)
			})
		}
	}
}
