package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestConfigurationCLI(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var rt jobdb.WorkflowRuntime = toy.New()
			if backend == "sqlite" {
				r, e := sqlite.NewFromConfig(context.Background(), sqlite.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
				require.NoError(t, e)
				defer r.Close(context.Background())
				rt = r
			}
			var requests atomic.Int32
			handler := remote.NewServer(rt)
			t.Setenv("JOBDB_BEARER_TOKEN", "test-admin-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, "Bearer test-admin-token", r.Header.Get("Authorization"))
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			run := func(input string, args ...string) (string, error) {
				cmd := NewRootCmd()
				var out bytes.Buffer
				cmd.SetOut(&out)
				cmd.SetErr(&out)
				cmd.SetIn(strings.NewReader(input))
				cmd.SetArgs(append([]string{"config"}, args...))
				err := cmd.ExecuteContext(context.Background())
				return out.String(), err
			}
			out, err := run(`{"n":9007199254740993123456789}`, "add", "name", "-t", "a", "-u", server.URL, "-f", "-")
			require.NoError(t, err)
			require.NotContains(t, out, "9007199254740993123456789")
			admin := rt.(jobdb.ConfigurationAdmin)
			got, err := admin.GetTenantConfiguration(context.Background(), "a")
			require.NoError(t, err)
			require.Equal(t, `{"n":9007199254740993123456789}`, string(got.Values["name"]))
			file := filepath.Join(t.TempDir(), "value.json")
			require.NoError(t, os.WriteFile(file, []byte(`false`), 0600))
			_, err = run("", "set", "name", "--tenant", "a", "--url", server.URL, "--value-file", file, "--if-match", configuration.ETag(got.Revision))
			require.NoError(t, err)
			_, err = run(`null`, "set", "name", "-t", "a", "-u", server.URL, "-f", "-", "--if-match", configuration.ETag(got.Revision))
			require.ErrorIs(t, err, jobdb.ErrConfigurationRevisionMismatch)
			for _, raw := range []string{`null`, `0`, `""`} {
				_, err = run(raw, "set", "name", "-t", "a", "-u", server.URL, "-f", "-")
				require.NoError(t, err)
				got, err = admin.GetTenantConfiguration(context.Background(), "a")
				require.NoError(t, err)
				require.Equal(t, raw, string(got.Values["name"]))
			}
			for i := 0; i < 2; i++ {
				_, err = run("", "remove", "name", "-t", "a", "-u", server.URL)
				require.NoError(t, err)
			}
			before := requests.Load()
			for _, raw := range []string{"", `{"x":1,"x":2}`, `1 2`, `"\ud800"`} {
				_, err = run(raw, "set", "name", "-t", "a", "-u", server.URL, "-f", "-")
				require.Error(t, err)
			}
			_, err = run("", "remove", "name", "-u", server.URL)
			require.Error(t, err)
			_, err = run("", "set", "name", "-t", "a", "-u", server.URL)
			require.Error(t, err)
			require.Equal(t, before, requests.Load(), "invalid input reached the server")
		})
	}
}
func TestConfigurationCLISanitizesFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, "private-value-and-token")
	}))
	defer server.Close()
	cmd := NewRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"config", "remove", "x", "-t", "a", "-u", server.URL})
	err := cmd.ExecuteContext(context.Background())
	require.ErrorIs(t, err, jobdb.ErrConfigurationFailed)
	require.NotContains(t, out.String(), "private-value-and-token")
}
