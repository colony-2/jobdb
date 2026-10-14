package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/stretchr/testify/require"
)

func TestSecretsCLIEndToEnd(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	r, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: filepath.Join(dir, "jobs.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	server := httptest.NewServer(remote.NewServer(r))
	defer server.Close()
	run := func(stdin string, args ...string) string {
		t.Helper()
		cmd := NewRootCmd()
		cmd.SetArgs(append([]string{"secrets"}, args...))
		var output bytes.Buffer
		cmd.SetOut(&output)
		cmd.SetErr(&output)
		cmd.SetIn(strings.NewReader(stdin))
		require.NoError(t, cmd.ExecuteContext(ctx))
		require.NotContains(t, output.String(), "unique-secret-content")
		return output.String()
	}
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "a", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "w", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	run("unique-secret-content\n", "add", "token", "-t", "a", "-u", server.URL, "-f", "-")
	result, err := l.ResolveSecrets(ctx, []string{"token"})
	require.NoError(t, err)
	require.Equal(t, "unique-secret-content\n", result.Secrets["token"].Value)
	file := filepath.Join(dir, "codex.key")
	require.NoError(t, os.WriteFile(file, []byte("unique-secret-content-from-file\n"), 0600))
	run("", "set", "codex_api_key", "-t", "a", "-u", server.URL, "-f", file)
	result, err = l.ResolveSecrets(ctx, []string{"codex_api_key"})
	require.NoError(t, err)
	require.Equal(t, "unique-secret-content-from-file\n", result.Secrets["codex_api_key"].Value)
	require.NoError(t, os.WriteFile(file, []byte(""), 0600))
	run("", "set", "token", "--tenant", "a", "--url", server.URL, "--value-file", file)
	result, err = l.ResolveSecrets(ctx, []string{"token"})
	require.NoError(t, err)
	require.Contains(t, result.Secrets, "token")
	require.Empty(t, result.Secrets["token"].Value)
	run("", "remove", "token", "-t", "a", "-u", server.URL)
	run("", "remove", "token", "--tenant", "a", "--url", server.URL)
	result, err = l.ResolveSecrets(ctx, []string{"token"})
	require.NoError(t, err)
	require.Empty(t, result.Secrets)
}

func TestSecretsCLIConfigurationAndSafeFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, "do-not-leak-provider-response")
	}))
	defer server.Close()
	cmd := NewRootCmd()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"secrets", "remove", "token", "-t", "a", "-u", server.URL})
	err := cmd.Execute()
	require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
	require.ErrorContains(t, err, "server returned HTTP 500")
	require.NotContains(t, output.String(), "do-not-leak")
	require.NotContains(t, err.Error(), "do-not-leak")
}

// Invalid local input must explain what to fix without reading secret values
// into diagnostics or making an HTTP request.
func TestSecretsCLILocalValidationDiagnostics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid local input reached the server")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	invalidUTF8 := "do-not-leak-secret" + string([]byte{255})
	for _, tc := range []struct {
		name           string
		args           []string
		stdin          string
		message        string
		invalidRequest bool
	}{
		{"set missing tenant", []string{"set", "codex_api_key", "-f", "codex.key"}, "", "--tenant (-t) is required", true},
		{"add missing tenant", []string{"add", "token", "-f", "-"}, "", "--tenant (-t) is required", true},
		{"remove missing tenant", []string{"remove", "token"}, "", "--tenant (-t) is required", true},
		{"empty tenant", []string{"set", "token", "-t", "", "-f", "-"}, "", "--tenant (-t) is required", true},
		{"invalid tenant", []string{"remove", "token", "-t", invalidUTF8}, "", "--tenant (-t) must be valid UTF-8", true},
		{"empty name", []string{"remove", "", "-t", "a"}, "", "secret name must be nonempty UTF-8", true},
		{"invalid name", []string{"remove", invalidUTF8, "-t", "a"}, "", "secret name must be nonempty UTF-8", true},
		{"missing file option", []string{"set", "token", "-t", "a"}, "", "--value-file (-f) is required", true},
		{"missing file", []string{"set", "token", "-t", "a", "-f", filepath.Join(t.TempDir(), "missing")}, "", "secret value file could not be opened", false},
		{"invalid value", []string{"set", "token", "-t", "a", "-f", "-"}, invalidUTF8, "secret value must be valid UTF-8", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewRootCmd()
			cmd.SetArgs(append([]string{"secrets", "-u", server.URL}, tc.args...))
			cmd.SetIn(strings.NewReader(tc.stdin))
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			err := cmd.Execute()
			require.ErrorContains(t, err, tc.message)
			if tc.invalidRequest {
				require.ErrorIs(t, err, jobdb.ErrInvalidSecretRequest)
			}
			require.Contains(t, output.String(), tc.message)
			require.NotContains(t, output.String(), "do-not-leak-secret")
			require.NotContains(t, err.Error(), "do-not-leak-secret")
		})
	}
}
