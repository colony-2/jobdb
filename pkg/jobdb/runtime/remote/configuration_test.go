package remote_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func configurationBackend(t *testing.T, kind string) jobdb.WorkflowRuntime {
	t.Helper()
	if kind == "toy" {
		return toy.New()
	}
	r, err := sqlite.NewFromConfig(context.Background(), sqlite.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close(context.Background())) })
	return r
}
func TestConfigurationConformance(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, transport := range []string{"embedded", "http"} {
			t.Run(backend+"/"+transport, func(t *testing.T) {
				rt := configurationBackend(t, backend)
				if transport == "http" {
					server := httptest.NewServer(remote.NewServer(rt))
					t.Cleanup(server.Close)
					client, err := remote.New(server.URL, nil)
					require.NoError(t, err)
					rt = client
				}
				runtimetest.RunConfigurationConformance(t, rt)
			})
		}
	}
}
func TestConfigurationHTTPValidation(t *testing.T) {
	rt := toy.New(toy.WithConfigurationLimits(jobdb.ConfigurationLimits{MaxRequestBytes: 256, MaxValueBytes: 64}))
	server := httptest.NewServer(remote.NewServer(rt))
	defer server.Close()
	post := func(path, body, tag string) (int, http.Header, string) {
		req, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if tag != "" {
			req.Header.Set("If-Match", tag)
		}
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Equal(t, "no-store", res.Header.Get("Cache-Control"))
		return res.StatusCode, res.Header, string(b)
	}
	path := "/v1/tenants/a/configuration/set"
	for _, body := range []string{`{}`, `{"name":"x"}`, `{"name":"x","value":1,"extra":2}`, `{"name":"x","value":1,"value":2}`, `{"name":"x","value":{"a":1,"a":2}}`, `{"name":null,"value":1}`, `null`, `[]`, `{"name":"x","value":"\ud800"}`} {
		status, _, response := post(path, body, "")
		require.Equal(t, 400, status, response)
	}
	status, _, _ := post(path, `{"name":"x","value":"`+strings.Repeat("v", 260)+`"}`, "")
	require.Equal(t, 413, status)
	for _, tag := range []string{"*", `W/"cfg-0"`, `"cfg-0", "cfg-1"`, `""`} {
		status, _, _ := post(path, `{"name":"x","value":1}`, tag)
		require.Equal(t, 400, status)
	}
	status, headers, _ := post(path, `{"name":"x","value":null}`, `"cfg-0"`)
	require.Equal(t, 204, status)
	require.NotEmpty(t, headers.Get("ETag"))
	status, _, _ = post(path, `{"name":"x","value":2}`, `"cfg-0"`)
	require.Equal(t, 412, status)
	snap, err := rt.GetTenantConfiguration(context.Background(), "a")
	require.NoError(t, err)
	require.Equal(t, "null", string(snap.Values["x"]))
	// An opaque tenant named "leases" must not be mistaken for a lease route.
	public, e := http.Get(server.URL + "/v1/tenants/leases/configuration")
	require.NoError(t, e)
	require.Equal(t, http.StatusOK, public.StatusCode)
	public.Body.Close()
	// Missing token, forged token, another job, and another tenant all share lease-lost semantics.
	client, err := remote.New(server.URL, nil)
	require.NoError(t, err)
	job, err := client.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "a", JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	lease, err := client.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "w", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	token := lease.(interface{ LeaseToken() string }).LeaseToken()
	for _, test := range []struct{ tenant, job, token string }{{"a", job.JobKey.JobId, ""}, {"a", job.JobKey.JobId, "forged"}, {"b", job.JobKey.JobId, token}, {"a", "missing", token}} {
		url := fmt.Sprintf("%s/v1/tenants/%s/jobs/%s/leases/%s/configuration", server.URL, test.tenant, test.job, lease.LeaseID())
		req, err := http.NewRequest(http.MethodGet, url, nil)
		require.NoError(t, err)
		req.Header.Set("X-JobDB-Lease-Token", test.token)
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		require.Equal(t, 409, res.StatusCode, string(body))
		require.Equal(t, "no-store", res.Header.Get("Cache-Control"))
	}
}
func TestConfigurationRemoteNeverCachesAndSanitizesFailures(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 2 {
			w.WriteHeader(503)
			fmt.Fprint(w, "private database details")
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"cfg-%d"`, n))
		fmt.Fprintf(w, `{"revision":"cfg-%d","values":{"n":%d}}`, n, n)
	}))
	defer server.Close()
	client, err := remote.New(server.URL, nil)
	require.NoError(t, err)
	first, err := client.GetTenantConfiguration(context.Background(), "a")
	require.NoError(t, err)
	failed, err := client.GetTenantConfiguration(context.Background(), "a")
	require.ErrorIs(t, err, jobdb.ErrConfigurationUnavailable)
	require.Empty(t, failed.Values)
	require.NotContains(t, err.Error(), "private")
	third, err := client.GetTenantConfiguration(context.Background(), "a")
	require.NoError(t, err)
	require.NotEqual(t, first.Revision, third.Revision)
	require.EqualValues(t, 3, calls.Load())
	for _, body := range []string{`{"revision":"cfg-1"}`, `{"revision":"cfg-1","values":null}`, `{"revision":"cfg-1","values":{},"extra":1}`, `{"revision":"cfg-1","values":{"n":1,"n":2}}`} {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Header().Set("ETag", `"cfg-1"`); fmt.Fprint(w, body) }))
		c, e := remote.New(bad.URL, nil)
		require.NoError(t, e)
		_, e = c.GetTenantConfiguration(context.Background(), "a")
		bad.Close()
		require.ErrorIs(t, e, jobdb.ErrConfigurationFailed)
	}
}
func TestConfigurationSQLitePersistsAndLimitsRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	ctx := context.Background()
	r, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: path})
	require.NoError(t, err)
	rev, err := r.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "a", Name: "x", Value: json.RawMessage(`"stored-value"`)})
	require.NoError(t, err)
	require.NoError(t, r.Close(ctx))
	r, err = sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: path, ConfigurationLimits: jobdb.ConfigurationLimits{MaxValueBytes: 2}})
	require.NoError(t, err)
	defer r.Close(ctx)
	_, err = r.GetTenantConfiguration(ctx, "a")
	require.ErrorIs(t, err, jobdb.ErrConfigurationReadTooLarge)
	deleted, err := r.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "a", Name: "x", ExpectedRevision: &rev})
	require.NoError(t, err)
	got, err := r.GetTenantConfiguration(ctx, "a")
	require.NoError(t, err)
	require.Empty(t, got.Values)
	require.Equal(t, deleted, got.Revision)
}
