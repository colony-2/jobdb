package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestJobsDeleteCLI(t *testing.T) {
	ctx := context.Background()
	rt := toy.New()
	h, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: "selected", JobType: "recipe", Data: jobdb.NewTaskDataOrPanic(nil), Metadata: json.RawMessage(`{"recipe":"build","env":"dev"}`)}})
	require.NoError(t, err)
	t.Setenv("JOBDB_BEARER_TOKEN", "admin-token")
	handler := remote.NewServer(rt)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "Bearer admin-token", r.Header.Get("Authorization"))
		require.Equal(t, "/v1/tenants/c2/jobs/delete", r.URL.Path)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	run := func(args ...string) (string, error) {
		cmd := NewRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(append([]string{"jobs", "delete", "-u", server.URL}, args...))
		err := cmd.ExecuteContext(ctx)
		return out.String(), err
	}
	for _, args := range [][]string{
		{"--all"}, {"-t", "c2"}, {"-t", "c2", "-a", "-o", "30d"},
		{"-t", "c2", "-o", ""}, {"-t", "c2", "-o", "0d"}, {"-t", "c2", "-o", "-1d"},
		{"-t", "c2", "-o", "NaNd"}, {"-t", "c2", "-o", "1e100d"},
		{"-t", "c2", "-o", "30d", "-b", "2026-01-01T00:00:00Z"},
		{"-t", "c2", "-b", "yesterday"}, {"-t", "c2", "-m", "recipe"},
		{"-t", "c2", "-m", "=build"}, {"-t", "c2", "-m", "app.recipe=build"},
	} {
		_, err := run(args...)
		require.Error(t, err, "%v", args)
	}
	require.Zero(t, requests.Load(), "invalid selectors must not make requests")
	out, err := run("-t", "c2", "-o", "30d", "-n")
	require.NoError(t, err)
	require.Contains(t, out, "Matched 0 jobs")
	cutoff := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	out, err = run("--tenant", "c2", "--created-before", cutoff, "--metadata", "recipe=build", "-m", "env=dev", "--dry-run", "-j")
	require.NoError(t, err)
	require.JSONEq(t, `{"matchedCount":1,"deletedCount":0}`, out)
	out, err = run("-t", "c2", "-b", cutoff, "-m", "recipe=build", "-m", "env=dev", "selected")
	require.NoError(t, err)
	require.Contains(t, out, "Deleted 1 jobs")
	listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
	require.NoError(t, err)
	require.Empty(t, listed.Jobs)
	_, err = rt.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	out, err = run("-t", "c2", "-a", "-j")
	require.NoError(t, err)
	require.JSONEq(t, `{"matchedCount":0,"deletedCount":0}`, out)
}

func TestJobAgeUnits(t *testing.T) {
	for text, want := range map[string]time.Duration{"30d": 30 * 24 * time.Hour, "1.5d": 36 * time.Hour, "12h": 12 * time.Hour, "90m": 90 * time.Minute} {
		got, err := parseJobAge(text)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}
