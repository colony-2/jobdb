package secretplugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

// ProcessConfig is operator-controlled. PassEnv names are snapshotted at Start;
// the host environment is otherwise not inherited. SHA256 is an optional hex digest.
type ProcessConfig struct {
	Command         string
	Args            []string
	PassEnv         []string
	SHA256          string
	StartTimeout    time.Duration
	Limits          jobdb.SecretLimits
	MaxMessageBytes int
	MaxConcurrent   int
	// Logger receives subprocess stdout/stderr and startup diagnostics. Nil
	// discards them. Diagnostic output is not sanitized like resolution errors.
	Logger *slog.Logger
}

// ValidateConfig checks configuration without launching an executable.
func ValidateConfig(c ProcessConfig) error {
	bad := errors.New("secret plugin: invalid process configuration")
	if !filepath.IsAbs(c.Command) || strings.ContainsRune(c.Command, 0) || c.StartTimeout < 0 || c.MaxMessageBytes < 0 || c.MaxConcurrent < 0 {
		return bad
	}
	info, err := os.Stat(c.Command)
	if err != nil || !info.Mode().IsRegular() {
		return bad
	}
	for _, a := range c.Args {
		if strings.ContainsRune(a, 0) {
			return bad
		}
	}
	for _, name := range c.PassEnv {
		if name == "" || strings.ContainsAny(name, "=\x00") || strings.HasPrefix(name, "PLUGIN_") || strings.HasPrefix(name, "JOBDB_SECRET_PLUGIN") {
			return bad
		}
	}
	if c.SHA256 != "" {
		b, err := hex.DecodeString(c.SHA256)
		if err != nil || len(b) != sha256.Size {
			return bad
		}
	}
	return nil
}

// Process owns one child at a time. It restarts exited children on later calls,
// never retries a resolution, and must be closed by its caller.
type Process struct {
	config ProcessConfig
	env    []string
	life   context.Context
	cancel context.CancelFunc
	gate   chan struct{}
	slots  chan struct{}
	// Protected by gate, including while starting/stopping a generation.
	client         *plugin.Client
	provider       jobdb.SecretProvider
	stopGeneration context.CancelFunc
	nextStart      time.Time
	backoff        time.Duration
}

func Start(ctx context.Context, c ProcessConfig) (*Process, error) {
	if err := ValidateConfig(c); err != nil {
		return nil, err
	}
	c.Args = append([]string(nil), c.Args...)
	c.PassEnv = append([]string(nil), c.PassEnv...)
	if c.StartTimeout == 0 {
		c.StartTimeout = 10 * time.Second
	}
	o := (ServeOptions{Limits: c.Limits, MaxMessageBytes: c.MaxMessageBytes, MaxConcurrent: c.MaxConcurrent}).defaults()
	c.Limits = o.Limits
	c.MaxMessageBytes = o.MaxMessageBytes
	c.MaxConcurrent = o.MaxConcurrent
	life, cancel := context.WithCancel(context.Background())
	p := &Process{config: c, life: life, cancel: cancel, gate: make(chan struct{}, 1), slots: make(chan struct{}, c.MaxConcurrent)}
	for _, name := range c.PassEnv {
		if value, ok := os.LookupEnv(name); ok {
			p.env = append(p.env, name+"="+value)
		}
	}
	p.gate <- struct{}{}
	if _, err := p.get(ctx); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Process) get(ctx context.Context) (jobdb.SecretProvider, error) {
	select {
	case <-ctx.Done():
		return nil, jobdb.ErrSecretsUnavailable
	case <-p.life.Done():
		return nil, jobdb.ErrSecretsUnavailable
	case <-p.gate:
	}
	defer func() { p.gate <- struct{}{} }()
	if ctx.Err() != nil || p.life.Err() != nil {
		return nil, jobdb.ErrSecretsUnavailable
	}
	if p.client != nil && !p.client.Exited() {
		return p.provider, nil
	}
	if p.client != nil {
		p.stopGeneration()
		p.client.Kill()
		p.client = nil
		p.provider = nil
		p.delayRestart()
	}
	if wait := time.Until(p.nextStart); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, jobdb.ErrSecretsUnavailable
		case <-p.life.Done():
			return nil, jobdb.ErrSecretsUnavailable
		}
	}
	client, provider, stop, err := p.launch(ctx)
	if err != nil {
		p.delayRestart()
		return nil, err
	}
	p.client = client
	p.provider = provider
	p.stopGeneration = stop
	p.backoff = 0
	return provider, nil
}
func (p *Process) delayRestart() {
	if p.backoff == 0 {
		p.backoff = 250 * time.Millisecond
	} else {
		p.backoff = min(30*time.Second, p.backoff*2)
	}
	p.nextStart = time.Now().Add(p.backoff)
}

func (p *Process) launch(ctx context.Context) (*plugin.Client, jobdb.SecretProvider, context.CancelFunc, error) {
	startCtx, cancel := context.WithTimeout(ctx, p.config.StartTimeout)
	defer cancel()
	genCtx, stop := context.WithCancel(p.life)
	cmd := exec.CommandContext(genCtx, p.config.Command, p.config.Args...)
	cmd.Env = append([]string(nil), p.env...)
	cfg := &plugin.ClientConfig{HandshakeConfig: handshake(), Cmd: cmd, SkipHostEnv: true, AutoMTLS: true,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC}, StartTimeout: p.config.StartTimeout,
		Logger: hclog.NewNullLogger(),
		Stderr: processOutput{p.config.Logger, "stderr"}, SyncStdout: processOutput{p.config.Logger, "stdout"}, SyncStderr: processOutput{p.config.Logger, "stderr"},
		VersionedPlugins: map[int]plugin.PluginSet{ProtocolVersion: {pluginName: &adapter{options: ServeOptions{Limits: p.config.Limits, MaxMessageBytes: p.config.MaxMessageBytes}}}},
		GRPCDialOptions:  []grpc.DialOption{grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(p.config.MaxMessageBytes), grpc.MaxCallSendMsgSize(p.config.MaxMessageBytes))},
	}
	if p.config.SHA256 != "" {
		sum, _ := hex.DecodeString(p.config.SHA256)
		cfg.SecureConfig = &plugin.SecureConfig{Checksum: sum, Hash: sha256.New()}
	}
	c := plugin.NewClient(cfg)
	type started struct {
		provider jobdb.SecretProvider
		err      error
	}
	done := make(chan started, 1)
	go func() {
		rpc, err := c.Client()
		if err != nil {
			done <- started{err: err}
			return
		}
		value, err := rpc.Dispense(pluginName)
		provider, ok := value.(jobdb.SecretProvider)
		if err == nil && !ok {
			err = jobdb.ErrSecretResolutionFailed
		}
		done <- started{provider, err}
	}()
	var launchErr error
	select {
	case r := <-done:
		if r.err == nil && startCtx.Err() == nil && p.life.Err() == nil {
			return c, r.provider, stop, nil
		}
		launchErr = errors.Join(r.err, startCtx.Err(), p.life.Err())
	case <-startCtx.Done():
		launchErr = startCtx.Err()
		stop()
		<-done
	case <-p.life.Done():
		launchErr = p.life.Err()
		stop()
		<-done
	}
	stop()
	c.Kill()
	if p.config.Logger != nil {
		p.config.Logger.Error("secret provider startup failed", "error", launchErr)
	}
	return nil, nil, nil, jobdb.ErrSecretsUnavailable
}

// Log chunks immediately, including partial lines. go-plugin writes startup
// stderr and synchronized stderr concurrently; slog handles that concurrency.
// Keeping output in an attribute also escapes embedded control characters.
type processOutput struct {
	logger *slog.Logger
	stream string
}

func (w processOutput) Write(p []byte) (int, error) {
	if w.logger != nil {
		for _, line := range strings.Split(string(p), "\n") {
			if line != "" {
				w.logger.Info("secret provider output", "stream", w.stream, "output", line)
			}
		}
	}
	return len(p), nil
}

func (p *Process) ResolveSecrets(ctx context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	ctx, cancel := context.WithTimeout(ctx, p.config.Limits.LookupTimeout)
	defer cancel()
	stop := context.AfterFunc(p.life, cancel)
	defer stop()
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	defer func() { <-p.slots }()
	provider, err := p.get(ctx)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return provider.ResolveSecrets(ctx, r)
}

// Close cancels active calls, terminates the child, and prevents future restarts.
func (p *Process) Close() error {
	p.cancel()
	<-p.gate
	defer func() { p.gate <- struct{}{} }()
	if p.client != nil {
		p.stopGeneration()
		p.client.Kill()
		p.client = nil
		p.provider = nil
	}
	return nil
}

var _ jobdb.SecretProvider = (*Process)(nil)
