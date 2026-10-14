package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
			out, err := run(`{"n":9007199254740993123456789}`, "add", "name", "-j", "-t", "a", "-u", server.URL, "-f", "-")
			require.NoError(t, err)
			require.NotContains(t, out, "9007199254740993123456789")
			admin := rt.(jobdb.ConfigurationAdmin)
			got, err := admin.GetTenantConfiguration(context.Background(), "a")
			require.NoError(t, err)
			require.Equal(t, `{"n":9007199254740993123456789}`, string(got.Values["name"]))
			file := filepath.Join(t.TempDir(), "value.json")
			require.NoError(t, os.WriteFile(file, []byte(`false`), 0600))
			_, err = run("", "set", "name", "--json", "--tenant", "a", "--url", server.URL, "--value-file", file, "--if-match", configuration.ETag(got.Revision))
			require.NoError(t, err)
			_, err = run(`null`, "set", "name", "--json", "-t", "a", "-u", server.URL, "-f", "-", "--if-match", configuration.ETag(got.Revision))
			require.ErrorIs(t, err, jobdb.ErrConfigurationRevisionMismatch)
			require.ErrorContains(t, err, "--if-match does not match the current tenant revision")
			for _, raw := range []string{`null`, `0`, `""`} {
				_, err = run(raw, "set", "name", "--json", "-t", "a", "-u", server.URL, "-f", "-")
				require.NoError(t, err)
				got, err = admin.GetTenantConfiguration(context.Background(), "a")
				require.NoError(t, err)
				require.Equal(t, raw, string(got.Values["name"]))
			}

			// Both file and stdin input preserve literal strings, even when they
			// look like JSON. JSON mode explicitly chooses the stored type.
			for _, source := range []string{"file", "stdin"} {
				for _, tc := range []struct {
					name, input, flag, want string
				}{
					{"url", "https://api.example.com/v1", "", `"https://api.example.com/v1"`},
					{"whitespace", "  hello\r\n\t", "", `"  hello\r\n\t"`},
					{"empty", "", "", `""`},
					{"unicode and escapes", "héllo \"world\"\\path", "", `"héllo \"world\"\\path"`},
					{"boolean string", "false", "", `"false"`},
					{"null string", "null", "", `"null"`},
					{"number string", "123", "", `"123"`},
					{"object string", `{"n":1}`, "", `"{\"n\":1}"`},
					{"quoted string", `"hello"`, "", `"\"hello\""`},
					{"JSON boolean", "false", "-j", "false"},
					{"JSON null", "null", "--json", "null"},
					{"JSON number", "9007199254740993123456789", "--json", "9007199254740993123456789"},
					{"JSON object", `{"region":"eu"}`, "-j", `{"region":"eu"}`},
					{"JSON array", `[1,false,null]`, "--json", `[1,false,null]`},
					{"JSON string", `"hello"`, "--json", `"hello"`},
				} {
					t.Run(source+"/"+tc.name, func(t *testing.T) {
						path := "-"
						if source == "file" {
							path = filepath.Join(t.TempDir(), "value")
							require.NoError(t, os.WriteFile(path, []byte(tc.input), 0600))
						}
						args := []string{"set", "typed-value", "-t", "a", "-u", server.URL, "-f", path}
						if tc.flag != "" {
							args = append(args, tc.flag)
						}
						_, err := run(tc.input, args...)
						require.NoError(t, err)
						snapshot, err := admin.GetTenantConfiguration(context.Background(), "a")
						require.NoError(t, err)
						var compact bytes.Buffer
						require.NoError(t, json.Compact(&compact, snapshot.Values["typed-value"]))
						require.Equal(t, tc.want, compact.String())
					})
				}
			}
			_, err = run("literal string", "add", "name", "-t", "a", "-u", server.URL, "-f", "-")
			require.NoError(t, err)
			got, err = admin.GetTenantConfiguration(context.Background(), "a")
			require.NoError(t, err)
			require.Equal(t, `"literal string"`, string(got.Values["name"]))
			for i := 0; i < 2; i++ {
				_, err = run("", "remove", "name", "-t", "a", "-u", server.URL)
				require.NoError(t, err)
			}
			before := requests.Load()
			for _, raw := range []string{"", `{"x":1,"x":2}`, `1 2`, `"\ud800"`} {
				_, err = run(raw, "set", "name", "--json", "-t", "a", "-u", server.URL, "-f", "-")
				require.Error(t, err)
			}
			_, err = run("", "remove", "name", "-u", server.URL)
			require.Error(t, err)
			_, err = run("", "set", "name", "--json", "-t", "a", "-u", server.URL)
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

func TestConfigurationCLILocalValidationDiagnostics(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	limits := (jobdb.ConfigurationLimits{}).WithDefaults()
	private := "private-value-and-token"
	invalidUTF8 := private + string([]byte{255})
	for _, tc := range []struct {
		name     string
		args     []string
		input    string
		message  string
		sentinel error
	}{
		{"missing tenant", []string{"set", "region", "-f", "config.json"}, "", "--tenant (-t) is required", jobdb.ErrInvalidConfigurationRequest},
		{"add missing tenant", []string{"add", "region", "-f", "-"}, "", "--tenant (-t) is required", jobdb.ErrInvalidConfigurationRequest},
		{"remove missing tenant", []string{"remove", "region"}, "", "--tenant (-t) is required", jobdb.ErrInvalidConfigurationRequest},
		{"empty tenant", []string{"remove", "region", "-t", ""}, "", "--tenant (-t) is required", jobdb.ErrInvalidConfigurationRequest},
		{"invalid tenant", []string{"remove", "region", "-t", invalidUTF8}, "", "--tenant (-t) must be valid UTF-8", jobdb.ErrInvalidConfigurationRequest},
		{"empty name", []string{"remove", "", "-t", "a"}, "", "configuration name must be nonempty UTF-8", jobdb.ErrInvalidConfigurationRequest},
		{"invalid name", []string{"remove", invalidUTF8, "-t", "a"}, "", "configuration name must be nonempty UTF-8", jobdb.ErrInvalidConfigurationRequest},
		{"oversized name", []string{"set", strings.Repeat("x", limits.MaxNameBytes+1), "-t", "a", "-f", "-"}, "null", "configuration name exceeds", jobdb.ErrConfigurationTooLarge},
		{"missing file option", []string{"set", "region", "-t", "a"}, "", "--value-file (-f) is required", jobdb.ErrInvalidConfigurationRequest},
		{"missing file", []string{"set", "region", "-t", "a", "-f", filepath.Join(t.TempDir(), "missing")}, "", "configuration value file could not be opened", nil},
		{"unquoted string", []string{"set", "region", "--json", "-t", "a", "-f", "-"}, private, "strings need double quotes", jobdb.ErrInvalidConfigurationRequest},
		{"empty value", []string{"set", "region", "--json", "-t", "a", "-f", "-"}, "", "single valid UTF-8 JSON value", jobdb.ErrInvalidConfigurationRequest},
		{"invalid UTF8", []string{"set", "region", "-t", "a", "-f", "-"}, `"` + invalidUTF8 + `"`, "configuration string must be valid UTF-8", jobdb.ErrInvalidConfigurationRequest},
		{"duplicate keys", []string{"set", "region", "--json", "-t", "a", "-f", "-"}, `{"x":1,"x":2}`, "unique object keys", jobdb.ErrInvalidConfigurationRequest},
		{"multiple values", []string{"set", "region", "--json", "-t", "a", "-f", "-"}, `1 2`, "single valid UTF-8 JSON value", jobdb.ErrInvalidConfigurationRequest},
		{"oversized value", []string{"set", "region", "-t", "a", "-f", "-"}, `"` + strings.Repeat("x", limits.MaxValueBytes) + `"`, "configuration value exceeds", jobdb.ErrConfigurationTooLarge},
		{"escaped string too large", []string{"set", "region", "-t", "a", "-f", "-"}, strings.Repeat("\n", limits.MaxValueBytes/2), "configuration value exceeds", jobdb.ErrConfigurationTooLarge},
		{"JSON invalid UTF8", []string{"set", "region", "-t", "a", "-f", "-", "-j"}, `"` + invalidUTF8 + `"`, "single valid UTF-8 JSON value", jobdb.ErrInvalidConfigurationRequest},
		{"invalid revision", []string{"remove", "region", "-t", "a", "--if-match", private}, "", "--if-match must be one quoted revision ETag", jobdb.ErrInvalidConfigurationRequest},
		{"invalid URL", []string{"remove", "region", "-t", "a", "-u", "not-a-url"}, "", "--url (-u) must be an HTTP(S) server URL", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewRootCmd()
			cmd.SetArgs(append([]string{"config", "-u", server.URL}, tc.args...))
			cmd.SetIn(strings.NewReader(tc.input))
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			err := cmd.Execute()
			require.ErrorContains(t, err, tc.message)
			if tc.sentinel != nil {
				require.ErrorIs(t, err, tc.sentinel)
			}
			require.Contains(t, output.String(), tc.message)
			require.NotContains(t, err.Error(), private)
			require.NotContains(t, output.String(), private)
		})
	}
	require.Zero(t, requests.Load(), "invalid local input reached the server")
}
