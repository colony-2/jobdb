package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	secretplugin "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin"
	"github.com/stretchr/testify/require"
)

type cliTestProvider struct{}

func (cliTestProvider) ResolveSecrets(_ context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	if os.Getenv("JOBDB_TEST_PROVIDER_OUTPUT") != "" {
		fmt.Fprintln(os.Stdout, "provider stdout during resolution")
		fmt.Fprintln(os.Stderr, "provider stderr during resolution")
	}
	out := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{}}
	for _, name := range r.Names {
		if name == "token" {
			expiry := r.Lease.AuthorizedNotAfter
			out.Secrets[name] = jobdb.SecretValue{Value: "external", ExpiresAt: &expiry, LeaseBound: true}
		}
	}
	return out, nil
}
func TestCLIPluginSubprocess(t *testing.T) {
	if os.Getenv("JOBDB_SECRET_PLUGIN") == "" {
		return
	}
	if mode := os.Getenv("JOBDB_TEST_PROVIDER_OUTPUT"); mode != "" {
		fmt.Fprintln(os.Stderr, "provider startup diagnostic")
		if mode == "fail" {
			fmt.Println("provider invalid startup stdout")
			os.Exit(2)
		}
	}
	if marker := os.Getenv("JOBDB_TEST_PID_FILE"); marker != "" {
		if err := os.WriteFile(marker, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			os.Exit(2)
		}
	}
	secretplugin.Serve(cliTestProvider{})
}
func cliPlugin(t *testing.T) providerEntry {
	t.Helper()
	exe, err := os.Executable()
	require.NoError(t, err)
	return providerEntry{ID: "external", Type: "plugin", Command: exe, Args: []string{"-test.run=^TestCLIPluginSubprocess$"}, PassEnv: []string{"JOBDB_TEST_PID_FILE", "JOBDB_TEST_PROVIDER_OUTPUT"}}
}

type providerLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *providerLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *providerLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func captureProviderLogs(t *testing.T) *providerLogBuffer {
	t.Helper()
	b := &providerLogBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(b, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return b
}
func writeProviderFile(t *testing.T, dir string, cfg providerFile) string {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "providers.json")
	require.NoError(t, os.WriteFile(path, raw, 0600))
	return path
}

func TestCLIComposesSQLiteAndPluginAndKeepsLocalAdministration(t *testing.T) {
	for _, mode := range []string{"default", "sqlite", "toy"} {
		t.Run(mode, func(t *testing.T) {
			logs := captureProviderLogs(t)
			t.Setenv("JOBDB_TEST_PROVIDER_OUTPUT", "normal")
			dir := t.TempDir()
			cfg := providerFile{Version: 1, DevelopmentAdminProvider: "local", Providers: []providerEntry{{ID: "local", Type: "sqlite", Path: "local.db"}, cliPlugin(t)}}
			path := writeProviderFile(t, dir, cfg)
			original := serveHTTPFunc
			defer func() { serveHTTPFunc = original }()
			serveHTTPFunc = func(ctx context.Context, _ string, handler http.Handler, cleanup func(context.Context) error) error {
				s := httptest.NewServer(handler)
				defer s.Close()
				admin := func(op string, payload string) {
					resp, err := s.Client().Post(s.URL+"/dev/v1/tenants/tenant/secrets/"+op, "application/json", bytes.NewBufferString(payload))
					require.NoError(t, err)
					defer resp.Body.Close()
					require.Equal(t, 204, resp.StatusCode)
				}
				admin("set", `{"name":"token","value":"local"}`)
				runtime, err := remote.New(s.URL, s.Client())
				require.NoError(t, err)
				job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "test", Data: jobdb.NewTaskDataOrPanic(1)}})
				require.NoError(t, err)
				lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "test"}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				r, err := lease.ResolveSecrets(ctx, []string{"token"})
				require.NoError(t, err)
				require.Equal(t, "local", r.Secrets["token"].ProviderID)
				require.Equal(t, "local", r.Secrets["token"].Value)
				renewed, err := lease.(jobdb.RenewableExecutionLease).Renew(ctx)
				require.NoError(t, err)
				lease = renewed
				admin("remove", `{"name":"token"}`)
				r, err = lease.ResolveSecrets(ctx, []string{"token"})
				require.NoError(t, err)
				require.Equal(t, "external", r.Secrets["token"].ProviderID)
				require.True(t, r.Secrets["token"].LeaseBound)
				require.Eventually(t, func() bool {
					output := logs.String()
					return bytes.Contains([]byte(output), []byte("provider stdout during resolution")) && bytes.Contains([]byte(output), []byte("provider stderr during resolution"))
				}, 3*time.Second, 10*time.Millisecond)
				if cleanup != nil {
					return cleanup(ctx)
				}
				return nil
			}
			args := []string{"--db", filepath.Join(dir, "jobs.db"), "--secret-providers-file", path}
			if mode != "default" {
				args = append([]string{mode}, args...)
			}
			cmd := newRootCmd()
			cmd.SetArgs(args)
			require.NoError(t, cmd.ExecuteContext(context.Background()))
			output := logs.String()
			require.Contains(t, output, "provider=local type=sqlite priority=1 development_admin=true")
			require.Contains(t, output, "provider=external type=plugin priority=2 development_admin=false")
			require.Contains(t, output, "count=2 development_admin_provider=local")
			require.Contains(t, output, "provider startup diagnostic")
			require.Contains(t, output, "stream=stdout")
			require.Contains(t, output, "stream=stderr")
			_, err := os.Stat(filepath.Join(dir, "local.db"))
			require.NoError(t, err)
			_, err = os.Stat(filepath.Join(dir, "jobs.db.secrets.db"))
			require.True(t, os.IsNotExist(err), "configured membership should not open the automatic store")
		})
	}
}

func TestProviderFailedStartupOutputIsLogged(t *testing.T) {
	logs := captureProviderLogs(t)
	t.Setenv("JOBDB_TEST_PROVIDER_OUTPUT", "fail")
	path := writeProviderFile(t, t.TempDir(), providerFile{Version: 1, Providers: []providerEntry{cliPlugin(t)}})
	_, err := openSecretProviders(context.Background(), path)
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
	require.NotContains(t, err.Error(), "provider startup")
	output := logs.String()
	require.Contains(t, output, "provider=external")
	require.Contains(t, output, "provider startup diagnostic")
	require.Contains(t, output, "secret provider startup failed")
	require.Contains(t, output, "provider invalid startup stdout")
}

func TestDefaultProviderStartupIsLogged(t *testing.T) {
	for _, mode := range []string{"sqlite", "toy"} {
		t.Run(mode, func(t *testing.T) {
			logs := captureProviderLogs(t)
			previous := serveHTTPFunc
			defer func() { serveHTTPFunc = previous }()
			serveHTTPFunc = func(context.Context, string, http.Handler, func(context.Context) error) error { return nil }
			cmd := newRootCmd()
			cmd.SetArgs([]string{mode, "--db", filepath.Join(t.TempDir(), "jobs.db")})
			require.NoError(t, cmd.ExecuteContext(context.Background()))
			if mode == "sqlite" {
				require.Contains(t, logs.String(), "provider=sqlite type=sqlite automatic=true development_admin=true")
			} else {
				require.Contains(t, logs.String(), "no secret providers configured")
			}
		})
	}
}

func TestConfiguredCollectionDoesNotExposeImplicitAdmin(t *testing.T) {
	logs := captureProviderLogs(t)
	path := writeProviderFile(t, t.TempDir(), providerFile{Version: 1, Providers: []providerEntry{{ID: "local", Type: "sqlite", Path: "local.db"}}})
	c, err := openSecretProviders(context.Background(), path)
	require.NoError(t, err)
	defer c.close()
	require.Contains(t, logs.String(), "development_admin=false")
	require.Contains(t, logs.String(), "development_admin_provider=none")

	for _, backend := range []string{"toy", "sqlite"} {
		var runtime jobdb.WorkflowRuntime
		want := 404
		if backend == "toy" {
			runtime = toy.New(toy.WithSecretProvider(c.provider))
		} else {
			r, err := sqliteruntime.NewFromConfig(context.Background(), sqliteruntime.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db"), SecretProvider: c.provider})
			require.NoError(t, err)
			defer r.Close(context.Background())
			runtime = r
			want = 503
		}
		s := httptest.NewServer(remote.NewServer(c.wrap(runtime)))
		resp, err := s.Client().Post(s.URL+"/dev/v1/tenants/tenant/secrets/set", "application/json", bytes.NewBufferString(`{"name":"token","value":"value"}`))
		require.NoError(t, err)
		require.Equal(t, want, resp.StatusCode)
		resp.Body.Close()
		s.Close()
	}

}

func TestProviderConfigValidatedBeforeOpeningStores(t *testing.T) {
	for _, kind := range []string{"duplicate", "bad-admin", "unknown-type", "bad-plugin"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			cfg := providerFile{Version: 1, Providers: []providerEntry{{ID: "local", Type: "sqlite", Path: "must-not-open.db"}}}
			switch kind {
			case "duplicate":
				cfg.Providers = append(cfg.Providers, cfg.Providers[0])
			case "bad-admin":
				cfg.DevelopmentAdminProvider = "missing"
			case "unknown-type":
				cfg.Providers = append(cfg.Providers, providerEntry{ID: "unknown", Type: "unknown"})
			case "bad-plugin":
				cfg.Providers = append(cfg.Providers, providerEntry{ID: "plugin", Type: "plugin", Command: "relative"})
			}
			_, err := openSecretProviders(context.Background(), writeProviderFile(t, dir, cfg))
			require.Error(t, err)
			_, err = os.Stat(filepath.Join(dir, "must-not-open.db"))
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestProviderStartupAndListenerFailuresReapChildren(t *testing.T) {
	for _, kind := range []string{"startup", "listener"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, "pid")
			t.Setenv("JOBDB_TEST_PID_FILE", marker)
			cfg := providerFile{Version: 1, Providers: []providerEntry{cliPlugin(t)}}
			if kind == "startup" {
				bad := cliPlugin(t)
				bad.ID = "bad"
				bad.Args = []string{"-test.run=^$"}
				cfg.Providers = append(cfg.Providers, bad)
			}
			path := writeProviderFile(t, dir, cfg)
			if kind == "startup" {
				_, err := openSecretProviders(context.Background(), path)
				require.Error(t, err)
			} else {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				defer listener.Close()
				cmd := newRootCmd()
				cmd.SetArgs([]string{"--db", filepath.Join(dir, "jobs.db"), "--listen", listener.Addr().String(), "--secret-providers-file", path})
				require.Error(t, cmd.ExecuteContext(context.Background()))
			}
			raw, err := os.ReadFile(marker)
			require.NoError(t, err)
			pid, err := strconv.Atoi(string(raw))
			require.NoError(t, err)
			process, err := os.FindProcess(pid)
			require.NoError(t, err)
			// The child was already waited by go-plugin; Wait cannot find an active child.
			_, err = process.Wait()
			require.Error(t, err)
		})
	}
}

func TestServeHTTPDrainsRequestsBeforeProviderCleanup(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := reserved.Addr().String()
	require.NoError(t, reserved.Close())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- ServeHTTP(ctx, address, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
			w.WriteHeader(http.StatusNoContent)
		}), func(context.Context) error { close(cleaned); return nil })
	}()
	client := &http.Client{Timeout: 3 * time.Second}
	require.Eventually(t, func() bool {
		resp, err := client.Get("http://" + address + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	}, 3*time.Second, 10*time.Millisecond)
	response := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + address + "/hold")
		if err == nil {
			resp.Body.Close()
		}
		response <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case <-cleaned:
		t.Fatal("providers closed while a request was active")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-response)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not drain")
	}
	select {
	case <-cleaned:
	default:
		t.Fatal("providers were not closed")
	}
}
