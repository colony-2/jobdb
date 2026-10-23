package remote

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func secretsSQLite(t *testing.T) *sqliteruntime.Runtime {
	t.Helper()
	dir := t.TempDir()
	r, err := sqliteruntime.NewFromConfig(context.Background(), sqliteruntime.Config{DBPath: filepath.Join(dir, "jobs.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close(context.Background())) })
	return r
}
func secretsRemoteLease(t *testing.T, c *Runtime, tenant string) jobdb.ExecutionLease {
	t.Helper()
	ctx := context.Background()
	h, err := c.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	l, err := c.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, l)
	return l
}
func secretHTTP(t *testing.T, server, path, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, server+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-JobDB-Lease-Token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}
func secretsPath(l jobdb.ExecutionLease) string {
	return fmt.Sprintf("/v1/tenants/%s/jobs/%s/leases/%s/secrets/resolve", l.Job().JobKey.TenantId, l.Job().JobKey.JobId, l.LeaseID())
}

func TestSecretsHTTPAdministrationAndResolution(t *testing.T) {
	backend := secretsSQLite(t)
	server := httptest.NewServer(NewServer(backend))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	ctx := context.Background()
	for _, tenant := range []string{"a", "b"} {
		status, raw := secretHTTP(t, server.URL, "/dev/v1/tenants/"+tenant+"/secrets/set", "", `{"name":"token","value":"`+tenant+`-secret\n"}`)
		require.Equal(t, 204, status, raw)
		require.Empty(t, raw)
	}
	l := secretsRemoteLease(t, client, "a")
	result, err := l.ResolveSecrets(ctx, []string{"token", "missing"})
	require.NoError(t, err)
	require.Equal(t, "a-secret\n", result.Secrets["token"].Value)
	require.Len(t, result.Secrets, 1)
	// Empty values, removal, and rotation are immediately observable through HTTP.
	status, _ := secretHTTP(t, server.URL, "/dev/v1/tenants/a/secrets/set", "", `{"name":"token","value":""}`)
	require.Equal(t, 204, status)
	result, err = l.ResolveSecrets(ctx, []string{"token"})
	require.NoError(t, err)
	value, ok := result.Secrets["token"]
	require.True(t, ok)
	require.Empty(t, value.Value)
	for i := 0; i < 2; i++ {
		status, _ = secretHTTP(t, server.URL, "/dev/v1/tenants/a/secrets/remove", "", `{"name":"token"}`)
		require.Equal(t, 204, status)
	}
	status, raw := secretHTTP(t, server.URL, secretsPath(l), l.(*remoteExecutionLease).LeaseToken(), `{"names":["token"]}`)
	require.Equal(t, 200, status)
	require.JSONEq(t, `{"secrets":{}}`, raw)
	other := secretsRemoteLease(t, client, "b")
	result, err = other.ResolveSecrets(ctx, []string{"token"})
	require.NoError(t, err)
	require.Equal(t, "b-secret\n", result.Secrets["token"].Value)
	status, _ = secretHTTP(t, server.URL, secretsPath(other), l.(*remoteExecutionLease).LeaseToken(), `{"names":["token"]}`)
	require.Equal(t, 409, status)
	for _, body := range []string{`{"names":["token"],"principal":"FORGED-SECRET"}`, `{"names":["token","token"]}`, `{"names":[]}`, `{"names":[null]}`, `{"names":["token"]} {}`, `null`, `{"names":"FORGED-SECRET"}`} {
		status, raw = secretHTTP(t, server.URL, secretsPath(l), l.(*remoteExecutionLease).LeaseToken(), body)
		require.Equal(t, 400, status, body)
		require.NotContains(t, raw, "FORGED-SECRET")
	}
	status, _ = secretHTTP(t, server.URL, secretsPath(l), "", `{"names":["token"]}`)
	require.Equal(t, 409, status)
	status, _ = secretHTTP(t, server.URL, secretsPath(l), l.(*remoteExecutionLease).LeaseToken(), `{"names":["`+strings.Repeat("x", 65536)+`"]}`)
	require.Equal(t, 413, status)
	require.NoError(t, l.KeepAlive(ctx))
	_, err = l.ResolveSecrets(ctx, []string{"token"})
	require.NoError(t, err)
	require.NoError(t, backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: l.Job().JobKey}))
	_, err = l.ResolveSecrets(ctx, []string{"token"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}

type principalRuntime struct {
	*sqliteruntime.Runtime
	principal *string
	calls     int
}

func (r *principalRuntime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	claims, _ := leaseauth.ClaimsFromContext(ctx)
	r.principal = claims.Principal
	r.calls++
	return r.Runtime.ResolveSecrets(ctx, req)
}

func TestSignedSecretPrincipalAndRenewal(t *testing.T) {
	backend := &principalRuntime{Runtime: secretsSQLite(t)}
	signer := newLeaseTokenSigner()
	handler, err := newServer(backend, signer)
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	l := secretsRemoteLease(t, client, "a").(*remoteExecutionLease)
	for _, principal := range []*string{nil, new(string), secretString("trusted")} {
		token, err := signer.mintWithPrincipal(l.jobKey, l.leaseID, "worker", "", time.Now().Add(time.Minute), time.Minute, principal)
		require.NoError(t, err)
		l.leaseToken = token
		_, err = l.ResolveSecrets(context.Background(), []string{"missing"})
		require.NoError(t, err)
		require.Equal(t, principal, backend.principal)
		require.NoError(t, l.KeepAlive(context.Background()))
		claims, err := signer.parse(l.LeaseToken())
		require.NoError(t, err)
		require.Equal(t, principal, claims.Principal)
	}
	count := backend.calls
	parts := strings.Split(l.LeaseToken(), ".")
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	parts[1] = base64.RawURLEncoding.EncodeToString(bytes.ReplaceAll(raw, []byte("trusted"), []byte("forged")))
	l.leaseToken = strings.Join(parts, ".")
	_, err = l.ResolveSecrets(context.Background(), []string{"missing"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Equal(t, count, backend.calls)
}
func secretString(s string) *string { return &s }

func TestSecretsUnavailableBackendsAndSafeClientErrors(t *testing.T) {
	server := httptest.NewServer(NewServer(toy.New()))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	l := secretsRemoteLease(t, client, "a")
	_, err = l.ResolveSecrets(context.Background(), []string{"token"})
	require.ErrorIs(t, err, jobdb.ErrNoSecretProvider)
	status, _ := secretHTTP(t, server.URL, "/dev/v1/tenants/a/secrets/set", "", `{"name":"token","value":"value"}`)
	require.Equal(t, 404, status)
	embedded, err := sqliteruntime.StartEmbeddedRuntime(context.Background())
	require.NoError(t, err)
	defer embedded.Shutdown()
	local := httptest.NewServer(NewServer(embedded.Runtime))
	defer local.Close()
	status, _ = secretHTTP(t, local.URL, "/dev/v1/tenants/a/secrets/set", "", `{"name":"token","value":"value"}`)
	require.Equal(t, 204, status)
	for _, body := range []string{`secret-provider-password`, `{"secrets":{"token":{}}}`, `{"secrets":null}`, `{"secrets":{"extra":{"value":"secret-provider-password"}}}`} {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
		c, err := New(bad.URL, bad.Client())
		require.NoError(t, err)
		result, err := c.ResolveSecrets(context.Background(), jobdb.ResolveSecretsRequest{JobKey: jobdb.JobKey{TenantId: "a", JobId: "job"}, LeaseID: "lease", LeaseToken: "bearer-secret", Names: []string{"token"}})
		require.Error(t, err)
		require.Empty(t, result.Secrets)
		require.NotContains(t, fmt.Sprintf("%+v", err), "secret-provider-password")
		bad.Close()
	}
}

func TestSecretDiagnosticsRedacted(t *testing.T) {
	value := jobdb.SecretValue{Value: "sensitive-value"}
	result := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": value}}
	for _, v := range []any{value, &value, result, &result, jobdb.ResolveSecretsRequest{LeaseToken: "sensitive-value"}} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			require.NotContains(t, fmt.Sprintf(format, v), "sensitive-value")
		}
	}
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(raw), "sensitive-value", "explicit response serialization must expose the requested value")
}

func TestSecretClientDoesNotForwardCapabilityOnRedirect(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("secret resolution followed a redirect")
		w.WriteHeader(500)
	}))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err := New(redirect.URL, redirect.Client())
	require.NoError(t, err)
	_, err = client.ResolveSecrets(context.Background(), jobdb.ResolveSecretsRequest{JobKey: jobdb.JobKey{TenantId: "a", JobId: "j"}, LeaseID: "l", LeaseToken: "private-token", Names: []string{"token"}})
	require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
	require.NotContains(t, err.Error(), "private-token")
}
