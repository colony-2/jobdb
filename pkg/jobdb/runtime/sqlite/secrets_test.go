package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/stretchr/testify/require"
)

type secretProviderFunc func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error)

func (f secretProviderFunc) ResolveSecrets(ctx context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	return f(ctx, r)
}

func secretLeaseFixture(t *testing.T) (*Runtime, jobdb.ExecutionLease) {
	t.Helper()
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	require.NoError(t, err)
	t.Cleanup(embedded.Shutdown)
	h, err := embedded.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", JobType: "job", Metadata: json.RawMessage(`{"team":"build"}`), Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	l, err := embedded.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, l)
	return embedded.Runtime, l
}

func TestSecretResolutionContextFreshnessAndNoPersistence(t *testing.T) {
	r, l := secretLeaseFixture(t)
	ctx := context.Background()
	calls := 0
	r.secretProvider = secretProviderFunc(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		calls++
		require.Equal(t, "tenant", req.TenantID)
		require.Equal(t, "job", req.JobID)
		require.Nil(t, req.Principal)
		require.JSONEq(t, `{"team":"build"}`, string(req.AppMetadata))
		req.AppMetadata[0] = 'x'
		return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: fmt.Sprint(calls)}}}, nil
	})
	before, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: l.Job().JobKey})
	require.NoError(t, err)
	for i := 1; i <= 2; i++ {
		result, err := l.ResolveSecrets(ctx, []string{"token", "unknown"})
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(i), result.Secrets["token"].Value)
		require.Len(t, result.Secrets, 1)
	}
	after, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: l.Job().JobKey})
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = l.ResolveSecrets(ctx, []string{"token", "token"})
	require.ErrorIs(t, err, jobdb.ErrInvalidSecretRequest)
	require.Equal(t, 2, calls)
	_, err = r.ResolveSecrets(ctx, jobdb.ResolveSecretsRequest{JobKey: l.Job().JobKey, LeaseID: l.LeaseID(), Names: []string{"token"}})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}

func TestSecretResolutionVerifiedPrincipal(t *testing.T) {
	r, l := secretLeaseFixture(t)
	for _, principal := range []*string{nil, new(string), ptrSecret("signed")} {
		r.secretProvider = secretProviderFunc(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
			require.Equal(t, principal, req.Principal)
			return jobdb.SecretResolveResult{}, nil
		})
		ctx := leaseauth.WithClaims(context.Background(), leaseauth.Claims{TenantID: "tenant", JobID: "job", LeaseID: l.LeaseID(), WorkerID: "worker", ExpiresAt: time.Now().Add(time.Minute), Principal: principal})
		result, err := r.ResolveSecrets(ctx, jobdb.ResolveSecretsRequest{JobKey: l.Job().JobKey, LeaseID: l.LeaseID(), Names: []string{"token"}})
		require.NoError(t, err)
		require.NotNil(t, result.Secrets)
	}
}

func TestSecretResolutionRechecksExpiredTokenAndDoesNotCacheDenials(t *testing.T) {
	r, l := secretLeaseFixture(t)
	calls := 0
	r.secretProvider = secretProviderFunc(func(ctx context.Context, _ jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		calls++
		if calls == 1 {
			return jobdb.SecretResolveResult{}, nil
		}
		if calls == 3 {
			<-ctx.Done()
		}
		return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "new-value"}}}, nil
	})
	result, err := l.ResolveSecrets(context.Background(), []string{"token"})
	require.NoError(t, err)
	require.Empty(t, result.Secrets)
	result, err = l.ResolveSecrets(context.Background(), []string{"token"})
	require.NoError(t, err)
	require.Equal(t, "new-value", result.Secrets["token"].Value)
	ctx := leaseauth.WithClaims(context.Background(), leaseauth.Claims{TenantID: "tenant", JobID: "job", LeaseID: l.LeaseID(), WorkerID: "worker", ExpiresAt: time.Now().Add(200 * time.Millisecond)})
	result, err = r.ResolveSecrets(ctx, jobdb.ResolveSecretsRequest{JobKey: l.Job().JobKey, LeaseID: l.LeaseID(), Names: []string{"token"}})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Empty(t, result.Secrets)
	require.Equal(t, 3, calls)
}
func ptrSecret(s string) *string { return &s }

func TestSecretResolutionRejectsProviderFailures(t *testing.T) {
	for _, kind := range []string{"error", "transient", "extra", "expired", "oversized", "invalid_utf8", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			r, l := secretLeaseFixture(t)
			r.secretLimits.MaxResponseBytes = 128
			r.secretLimits.LookupTimeout = 10 * time.Millisecond
			r.secretProvider = secretProviderFunc(func(ctx context.Context, _ jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
				result := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "do-not-leak"}}}
				switch kind {
				case "error":
					return result, errors.New("do-not-leak")
				case "transient":
					return result, fmt.Errorf("do-not-leak: %w", jobdb.ErrSecretsUnavailable)
				case "extra":
					result.Secrets["unexpected"] = jobdb.SecretValue{Value: "do-not-leak"}
				case "expired":
					expired := time.Now().Add(-time.Second)
					result.Secrets["token"] = jobdb.SecretValue{Value: "do-not-leak", ExpiresAt: &expired}
				case "oversized":
					result.Secrets["token"] = jobdb.SecretValue{Value: string(make([]byte, 128))}
				case "invalid_utf8":
					result.Secrets["token"] = jobdb.SecretValue{Value: string([]byte{255})}
				case "timeout":
					<-ctx.Done()
				}
				return result, nil
			})
			result, err := l.ResolveSecrets(context.Background(), []string{"token"})
			require.Error(t, err)
			require.Empty(t, result.Secrets)
			require.NotContains(t, fmt.Sprintf("%+v %#v", err, err), "do-not-leak")
		})
	}
}

func TestSecretResolutionLeaseLossDuringLookup(t *testing.T) {
	for _, op := range []string{"cancel", "reschedule", "expire", "complete", "supersede"} {
		t.Run(op, func(t *testing.T) {
			r, l := secretLeaseFixture(t)
			ctx := context.Background()
			started, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			r.secretProvider = secretProviderFunc(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
				close(started)
				<-release
				return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "do-not-return"}}}, nil
			})
			go func() {
				result, err := l.ResolveSecrets(ctx, []string{"token"})
				if len(result.Secrets) > 0 {
					done <- errors.New("returned secrets after loss")
				} else {
					done <- err
				}
			}()
			<-started
			switch op {
			case "cancel":
				require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: l.Job().JobKey}))
			case "reschedule":
				require.NoError(t, l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "job"}}))
			case "expire":
				_, err := r.db.Exec("UPDATE jobdb_jobs SET lease_expires_at_ns=1")
				require.NoError(t, err)
			case "complete":
				_, err := r.db.Exec("UPDATE jobdb_jobs SET archived_at_ns=1")
				require.NoError(t, err)
			case "supersede":
				_, err := r.db.Exec("UPDATE jobdb_jobs SET lease_id='replacement'")
				require.NoError(t, err)
			}
			close(release)
			require.ErrorIs(t, <-done, jobdb.ErrExecutionLeaseLost)
		})
	}
}

func TestDefaultSecretStoreLocationAndPersistence(t *testing.T) {
	for _, constructor := range []string{"path", "dsn", "handle"} {
		t.Run(constructor, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			path := filepath.Join(dir, "jobs.db")
			if constructor != "path" {
				path = filepath.Join(dir, "jobs ?#.db")
			}
			dsn := (&url.URL{Scheme: "file", Path: path}).String() + "?mode=rwc"
			var db *sql.DB
			open := func() *Runtime {
				t.Helper()
				var r *Runtime
				var err error
				switch constructor {
				case "path":
					r, err = NewFromConfig(ctx, Config{DBPath: path})
				case "dsn":
					r, err = NewFromConfig(ctx, Config{DSN: dsn, BlobDir: filepath.Join(dir, "blobs")})
				case "handle":
					if db == nil {
						db, err = sql.Open("sqlite", dsn)
						require.NoError(t, err)
						db.SetMaxOpenConns(1)
						t.Cleanup(func() { require.NoError(t, db.Close()) })
					}
					r, err = New(db, WithBlobStoreURI("blobfs://"+filepath.Join(dir, "blobs")))
				}
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, r.Close(ctx)) })
				return r
			}
			r := open()
			require.NoError(t, r.SetDevSecret(ctx, "tenant", "token", "stored-value"))
			require.FileExists(t, path+".secrets.db")
			var count int
			require.NoError(t, r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='secrets'").Scan(&count))
			require.Zero(t, count)
			provider := r.secretProvider
			require.NoError(t, r.Close(ctx))
			_, err := provider.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "tenant", Names: []string{"token"}})
			require.Error(t, err)
			if db != nil {
				require.NoError(t, db.PingContext(ctx))
			}
			r = open()
			result, err := r.secretProvider.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "tenant", Names: []string{"token"}})
			require.NoError(t, err)
			require.Equal(t, "stored-value", result.Secrets["token"].Value)
		})
	}
}

func TestDefaultMemorySecretStoreIsolation(t *testing.T) {
	ctx := context.Background()
	for _, constructor := range []string{"config", "handle"} {
		t.Run(constructor, func(t *testing.T) {
			for i := 0; i < 2; i++ {
				var r *Runtime
				var err error
				blobURI := "blobfs://" + t.TempDir()
				if constructor == "config" {
					r, err = NewFromConfig(ctx, Config{DSN: ":memory:", BlobStoreURI: blobURI})
				} else {
					db, openErr := sql.Open("sqlite", ":memory:")
					require.NoError(t, openErr)
					db.SetMaxOpenConns(1)
					t.Cleanup(func() { require.NoError(t, db.Close()) })
					r, err = New(db, WithBlobStoreURI(blobURI))
				}
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, r.Close(ctx)) })
				result, err := r.secretProvider.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "tenant", Names: []string{"token"}})
				require.NoError(t, err)
				require.Empty(t, result.Secrets)
				require.NoError(t, r.SetDevSecret(ctx, "tenant", "token", "value"))
			}
		})
	}
}

func TestInjectedProviderOverridesDefaultStore(t *testing.T) {
	ctx := context.Background()
	for _, constructor := range []string{"config", "handle"} {
		t.Run(constructor, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			// An unusable sidecar must not affect an explicitly supplied provider.
			require.NoError(t, os.Mkdir(path+".secrets.db", 0700))
			provider := secretProviderFunc(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
				return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: "custom"}}}, nil
			})
			var r *Runtime
			var err error
			if constructor == "config" {
				r, err = NewFromConfig(ctx, Config{DBPath: path, SecretProvider: provider})
			} else {
				db, openErr := sql.Open("sqlite", path)
				require.NoError(t, openErr)
				db.SetMaxOpenConns(1)
				defer db.Close()
				r, err = New(db, WithSecretProvider(provider), WithBlobStoreURI("blobfs://"+t.TempDir()))
			}
			require.NoError(t, err)
			defer r.Close(ctx)
			require.Nil(t, r.devSecrets)
			result, err := r.secretProvider.ResolveSecrets(ctx, jobdb.SecretResolveRequest{})
			require.NoError(t, err)
			require.Equal(t, "custom", result.Secrets["token"].Value)
		})
	}
}
