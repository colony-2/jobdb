package secretplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/stretchr/testify/require"
)

type fixtureProvider struct{ canceled atomic.Int32 }

func (p *fixtureProvider) ResolveSecrets(ctx context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	out := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{}}
	for _, name := range r.Names {
		v := jobdb.SecretValue{}
		switch name {
		case "echo":
			raw, _ := json.Marshal(r)
			v.Value = string(raw)
		case "token", "overlong":
			expiry := r.Lease.AuthorizedNotAfter
			if name == "overlong" {
				expiry = expiry.Add(time.Second)
			}
			v = jobdb.SecretValue{Value: "fixture-credential", Version: r.Lease.WorkerID, ExpiresAt: &expiry, LeaseBound: true}
		case "error":
			return out, fmt.Errorf("backend credential was sensitive-fixture")
		case "panic":
			panic("sensitive-fixture")
		case "extra":
			out.Secrets["unrequested"] = jobdb.SecretValue{Value: "sensitive-fixture"}
			continue
		case "wait":
			<-ctx.Done()
			p.canceled.Add(1)
			return out, ctx.Err()
		case "canceled":
			v.Value = strconv.Itoa(int(p.canceled.Load()))
		case "crash":
			os.Exit(2)
		case "pid":
			v.Value = strconv.Itoa(os.Getpid())
		case "env":
			v.Value = os.Getenv("JOBDB_TEST_ALLOWED") + ":" + os.Getenv("JOBDB_TEST_PRIVATE")
		case "oversized":
			v.Value = strings.Repeat("sensitive-fixture", 1<<20)
		default:
			continue
		}
		out.Secrets[name] = v
	}
	return out, nil
}

// The test executable is a real subprocess speaking the production protocol.
func TestPluginSubprocess(t *testing.T) {
	if os.Getenv("JOBDB_SECRET_PLUGIN") == "" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	if mode == "hang" {
		time.Sleep(time.Hour)
		return
	}
	if mode == "bad-handshake" {
		fmt.Println("sensitive-fixture")
		return
	}
	if mode == "wrong-version" {
		plugin.Serve(&plugin.ServeConfig{HandshakeConfig: handshake(), Logger: hclog.NewNullLogger(), VersionedPlugins: map[int]plugin.PluginSet{2: {pluginName: &adapter{provider: &fixtureProvider{}}}}, GRPCServer: plugin.DefaultGRPCServer})
		return
	}
	Serve(&fixtureProvider{})
}
func fixtureConfig(t *testing.T, mode string) ProcessConfig {
	t.Helper()
	exe, err := os.Executable()
	require.NoError(t, err)
	return ProcessConfig{Command: exe, Args: []string{"-test.run=^TestPluginSubprocess$", "--", mode}, StartTimeout: 3 * time.Second}
}
func fixture(t *testing.T) *Process {
	t.Helper()
	p, err := Start(context.Background(), fixtureConfig(t, "normal"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	return p
}

func TestPluginLeaseRoundTripAndSafeFailures(t *testing.T) {
	p := fixture(t)
	principal := "subject"
	now := time.Now().UTC()
	capExpiry := now.Add(time.Minute)
	req := jobdb.SecretResolveRequest{TenantID: "tenant", JobID: "job", Principal: &principal, AppMetadata: []byte(`{ "lease": "untrusted", "team": 7 }`), Names: []string{"echo", "token"}, Lease: jobdb.SecretLeaseContext{LeaseID: "lease", WorkerID: "worker", LeaseExpiresAt: now.Add(time.Hour), AuthorizedNotAfter: capExpiry}}
	r, err := p.ResolveSecrets(context.Background(), req)
	require.NoError(t, err)
	var echoed jobdb.SecretResolveRequest
	require.NoError(t, json.Unmarshal([]byte(r.Secrets["echo"].Value), &echoed))
	// JSON decoding normalizes metadata whitespace; test raw protobuf preservation below.
	require.Equal(t, req.Lease, echoed.Lease)
	require.Equal(t, req.Principal, echoed.Principal)
	require.Equal(t, req.TenantID, echoed.TenantID)
	require.Equal(t, req.JobID, echoed.JobID)
	require.True(t, r.Secrets["token"].LeaseBound)
	require.Equal(t, capExpiry, *r.Secrets["token"].ExpiresAt)
	for _, name := range []string{"overlong", "error", "panic", "extra", "oversized"} {
		req.Names = []string{name}
		r, err = p.ResolveSecrets(context.Background(), req)
		require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed, name)
		require.Empty(t, r.Secrets)
		require.NotContains(t, err.Error(), "sensitive-fixture")
	}
	req.Names = []string{"token"}
	req.Lease = jobdb.SecretLeaseContext{}
	_, err = p.ResolveSecrets(context.Background(), req)
	require.Error(t, err)
	for _, principal := range []*string{nil, new(string)} {
		req.Principal = principal
		copy, err := decodeRequest(encodeRequest(req))
		require.NoError(t, err)
		require.Equal(t, req, copy)
	}
}

func TestPluginCancellationConcurrencyAndRestart(t *testing.T) {
	p := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := p.ResolveSecrets(ctx, jobdb.SecretResolveRequest{Names: []string{"wait"}})
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
	require.Eventually(t, func() bool {
		r, err := p.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"canceled"}})
		return err == nil && r.Secrets["canceled"].Value == "1"
	}, time.Second, 10*time.Millisecond)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"pid"}})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	before, err := p.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"pid"}})
	require.NoError(t, err)
	_, err = p.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"crash"}})
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
	require.Eventually(t, func() bool {
		r, err := p.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"pid"}})
		return err == nil && r.Secrets["pid"].Value != before.Secrets["pid"].Value
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, p.Close())
	require.NoError(t, p.Close())
	_, err = p.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"pid"}})
	require.ErrorIs(t, err, jobdb.ErrSecretsUnavailable)
}

func TestPluginStartupFailureAndEnvironment(t *testing.T) {
	for _, mode := range []string{"wrong-version", "bad-handshake", "hang"} {
		t.Run(mode, func(t *testing.T) {
			cfg := fixtureConfig(t, mode)
			cfg.StartTimeout = 200 * time.Millisecond
			start := time.Now()
			p, err := Start(context.Background(), cfg)
			require.Error(t, err)
			require.Nil(t, p)
			require.NotContains(t, err.Error(), "sensitive-fixture")
			require.Less(t, time.Since(start), 3*time.Second)
		})
	}
	t.Setenv("JOBDB_TEST_ALLOWED", "allowed")
	t.Setenv("JOBDB_TEST_PRIVATE", "private")
	cfg := fixtureConfig(t, "normal")
	cfg.PassEnv = []string{"JOBDB_TEST_ALLOWED"}
	ctx, cancel := context.WithCancel(context.Background())
	p, err := Start(ctx, cfg)
	require.NoError(t, err)
	defer p.Close()
	cancel()
	r, err := p.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{Names: []string{"env"}})
	require.NoError(t, err)
	require.Equal(t, "allowed:", r.Secrets["env"].Value)
	cfg.SHA256 = strings.Repeat("0", 64)
	_, err = Start(context.Background(), cfg)
	require.Error(t, err)
	cfg.SHA256 = "bad"
	require.Error(t, ValidateConfig(cfg))
}
