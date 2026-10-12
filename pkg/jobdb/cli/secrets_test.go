package cli

import (
	"bytes"
	"context"
	"encoding/base64"
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
	key := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString(make([]byte, 32))), 0600))
	ctx := context.Background()
	r, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: filepath.Join(dir, "jobs.db"), DevSecretsDB: filepath.Join(dir, "secrets.db"), DevSecretsKeyFile: key})
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
	file := filepath.Join(dir, "value")
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
	cmd := NewRootCmd()
	cmd.SetArgs([]string{"toy", "-s", "store", "-k", "key"})
	require.ErrorContains(t, cmd.Execute(), "SQLite")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, "do-not-leak-provider-response")
	}))
	defer server.Close()
	cmd = NewRootCmd()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"secrets", "remove", "token", "-t", "a", "-u", server.URL})
	err := cmd.Execute()
	require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
	require.NotContains(t, output.String(), "do-not-leak")
	require.NotContains(t, err.Error(), "do-not-leak")
	for _, args := range [][]string{{"secrets", "set", "token", "-t", "a"}, {"secrets", "remove", "token"}} {
		cmd = NewRootCmd()
		cmd.SetArgs(args)
		require.Error(t, cmd.Execute())
	}
}
