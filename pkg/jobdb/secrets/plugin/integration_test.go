package secretplugin

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	secretproviders "github.com/colony-2/jobdb/pkg/jobdb/secrets/providers"
	sqlitesecrets "github.com/colony-2/jobdb/pkg/jobdb/secrets/sqlite"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestPluginCollectionThroughRuntimeAndWorkflow(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, transport := range []string{"embedded", "http"} {
			t.Run(backend+"/"+transport, func(t *testing.T) {
				ctx := context.Background()
				external := fixture(t)
				local, err := sqlitesecrets.Open(ctx, filepath.Join(t.TempDir(), "secrets.db"), jobdb.SecretLimits{})
				require.NoError(t, err)
				t.Cleanup(func() { local.Close() })
				require.NoError(t, local.Set(ctx, "tenant", "password", "local-password"))
				require.NoError(t, local.Set(ctx, "tenant", "token", "local-override"))
				collection, err := secretproviders.New([]secretproviders.Member{{ID: "local", Provider: local}, {ID: "external", Provider: external}}, secretproviders.Options{})
				require.NoError(t, err)
				var runtime jobdb.WorkflowRuntime
				if backend == "toy" {
					runtime = toy.New(toy.WithSecretProvider(collection))
				} else {
					r, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db"), SecretProvider: collection})
					require.NoError(t, err)
					t.Cleanup(func() { r.Close(ctx) })
					runtime = r
				}
				if transport == "http" {
					s := httptest.NewServer(remote.NewServer(runtime))
					t.Cleanup(s.Close)
					runtime, err = remote.New(s.URL, s.Client())
					require.NoError(t, err)
				}
				job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "test", Metadata: json.RawMessage(`{"lease":{"AuthorizedNotAfter":"2099-01-01T00:00:00Z"}}`), Data: jobdb.NewTaskDataOrPanic(1)}})
				require.NoError(t, err)
				lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "test"}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.NotNil(t, lease)
				r, err := lease.ResolveSecrets(ctx, []string{"password", "token", "echo"})
				require.NoError(t, err)
				require.Equal(t, "local", r.Secrets["token"].ProviderID)
				require.Equal(t, "local-override", r.Secrets["token"].Value)
				require.False(t, r.Secrets["password"].LeaseBound)
				var echoed jobdb.SecretResolveRequest
				require.NoError(t, json.Unmarshal([]byte(r.Secrets["echo"].Value), &echoed))
				require.Equal(t, lease.LeaseID(), echoed.Lease.LeaseID)
				require.Equal(t, "worker", echoed.Lease.WorkerID)
				require.Nil(t, echoed.Principal)
				old := lease.(jobdb.RenewableExecutionLease)
				require.Equal(t, old.LeaseExpiry(), echoed.Lease.AuthorizedNotAfter)
				require.NoError(t, local.Remove(ctx, "tenant", "token"))
				r, err = lease.ResolveSecrets(ctx, []string{"password", "token"})
				require.NoError(t, err)
				require.Equal(t, "external", r.Secrets["token"].ProviderID)
				require.True(t, r.Secrets["token"].LeaseBound)
				require.Equal(t, old.LeaseExpiry(), *r.Secrets["token"].ExpiresAt)
				time.Sleep(10 * time.Millisecond)
				renewed, err := old.Renew(ctx)
				require.NoError(t, err)
				require.True(t, renewed.LeaseExpiry().After(old.LeaseExpiry()))
				r, err = old.ResolveSecrets(ctx, []string{"token", "echo"})
				require.NoError(t, err)
				require.Equal(t, old.LeaseExpiry(), *r.Secrets["token"].ExpiresAt)
				require.NoError(t, json.Unmarshal([]byte(r.Secrets["echo"].Value), &echoed))
				require.True(t, echoed.Lease.LeaseExpiresAt.After(echoed.Lease.AuthorizedNotAfter))
				r, err = renewed.ResolveSecrets(ctx, []string{"token"})
				require.NoError(t, err)
				require.Equal(t, renewed.LeaseExpiry(), *r.Secrets["token"].ExpiresAt)
				// Even an invalid shadowed plugin credential must fail the entire request.
				require.NoError(t, local.Set(ctx, "tenant", "overlong", "local"))
				r, err = renewed.ResolveSecrets(ctx, []string{"password", "overlong"})
				require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
				require.Empty(t, r.Secrets)
				require.NoError(t, runtime.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: job.JobKey}))
				r, err = renewed.ResolveSecrets(ctx, []string{"token"})
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				require.Empty(t, r.Secrets)

				// Live workflow caching preserves source until an explicit refresh.
				require.NoError(t, local.Set(ctx, "tenant", "token", "local-override"))
				worker := workflow.ExecutionWorker{JobType: "cache", Execute: func(ec workflow.ExecutionContext, _ jobdb.JobData) (jobdb.JobData, error) {
					first, err := ec.ResolveSecrets(ctx, []string{"token"})
					if err != nil {
						return nil, err
					}
					require.Equal(t, "local", first.Secrets["token"].ProviderID)
					if err = local.Remove(ctx, "tenant", "token"); err != nil {
						return nil, err
					}
					cached, err := ec.ResolveSecrets(ctx, []string{"token"})
					if err != nil {
						return nil, err
					}
					require.Equal(t, first, cached)
					fresh, err := ec.ResolveSecretsFresh(ctx, []string{"token"})
					if err != nil {
						return nil, err
					}
					require.Equal(t, "external", fresh.Secrets["token"].ProviderID)
					require.True(t, fresh.Secrets["token"].LeaseBound)
					return jobdb.NewTaskDataOrPanic("done"), nil
				}}
				job, err = runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic(1)}})
				require.NoError(t, err)
				runnable, err := workflow.GetJobForRun(ctx, runtime, workflow.GetJobForRunRequest{JobKey: job.JobKey, JobWorker: worker, LeaseDuration: time.Minute})
				require.NoError(t, err)
				outcome, err := runnable.Run(nil)
				require.NoError(t, err)
				require.Equal(t, workflow.JobRunCompleted, outcome.Status, "%v", outcome.JobError)
				chapters, err := runtime.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: job.JobKey})
				require.NoError(t, err)
				raw, err := json.Marshal(chapters)
				require.NoError(t, err)
				require.NotContains(t, string(raw), "local-override")
				require.NotContains(t, string(raw), "fixture-credential")
			})
		}
	}
}

type gatedProvider struct {
	provider jobdb.SecretProvider
	entered  chan struct{}
	proceed  chan struct{}
}

func (p gatedProvider) ResolveSecrets(ctx context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	close(p.entered)
	<-p.proceed
	return p.provider.ResolveSecrets(ctx, r)
}

func TestPluginResultRejectedAfterLeaseRevocation(t *testing.T) {
	p := fixture(t)
	gate := gatedProvider{p, make(chan struct{}), make(chan struct{})}
	runtime := toy.New(toy.WithSecretProvider(gate))
	ctx := context.Background()
	job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "test", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "test"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		r, err := lease.ResolveSecrets(ctx, []string{"token"})
		if len(r.Secrets) > 0 {
			t.Error("revoked lease received values")
		}
		done <- err
	}()
	<-gate.entered
	require.NoError(t, runtime.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: job.JobKey}))
	close(gate.proceed)
	require.ErrorIs(t, <-done, jobdb.ErrExecutionLeaseLost)
}
