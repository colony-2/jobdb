package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	secretplugin "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin"
	secretproviders "github.com/colony-2/jobdb/pkg/jobdb/secrets/providers"
	sqlitesecrets "github.com/colony-2/jobdb/pkg/jobdb/secrets/sqlite"
)

type providerFile struct {
	Version                  int             `json:"version"`
	MaxParallel              int             `json:"max_parallel"`
	DevelopmentAdminProvider string          `json:"development_admin_provider"`
	LogSelections            bool            `json:"log_selections"`
	Providers                []providerEntry `json:"providers"`
}
type providerEntry struct {
	ID      string   `json:"id"`
	Type    string   `json:"type"`
	Path    string   `json:"path,omitempty"`
	Command string   `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`
	PassEnv []string `json:"pass_env,omitempty"`
	SHA256  string   `json:"sha256,omitempty"`
}

func (e providerEntry) processConfig() secretplugin.ProcessConfig {
	return secretplugin.ProcessConfig{Command: e.Command, Args: e.Args, PassEnv: e.PassEnv, SHA256: e.SHA256}
}

type configuredSecrets struct {
	provider jobdb.SecretProvider
	admin    *sqlitesecrets.Store
	closers  []func() error
}

func (c *configuredSecrets) close() error {
	var errs []error
	for i := len(c.closers) - 1; i >= 0; i-- {
		errs = append(errs, c.closers[i]())
	}
	c.closers = nil
	return errors.Join(errs...)
}

func openSecretProviders(ctx context.Context, path string) (*configuredSecrets, error) {
	out := &configuredSecrets{}
	if path == "" {
		return out, nil
	}
	bad := errors.New("invalid secret providers configuration")
	f, err := os.Open(path)
	if err != nil {
		return nil, bad
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil || len(raw) > 1<<20 {
		return nil, bad
	}
	var cfg providerFile
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil || d.Decode(new(any)) != io.EOF || cfg.Version != 1 || len(cfg.Providers) == 0 || cfg.MaxParallel < 0 {
		return nil, bad
	}
	seen := map[string]string{}
	for i, e := range cfg.Providers {
		if !secretproviders.ValidID(e.ID) || seen[e.ID] != "" {
			return nil, bad
		}
		seen[e.ID] = e.Type
		switch e.Type {
		case "plugin":
			if e.Path != "" || secretplugin.ValidateConfig(e.processConfig()) != nil {
				return nil, bad
			}
		case "sqlite":
			if e.Path == "" || strings.ContainsRune(e.Path, 0) || e.Command != "" || len(e.Args) > 0 || len(e.PassEnv) > 0 || e.SHA256 != "" {
				return nil, bad
			}
			if e.Path != ":memory:" && !filepath.IsAbs(e.Path) {
				cfg.Providers[i].Path = filepath.Join(filepath.Dir(path), e.Path)
			}
		default:
			return nil, bad
		}
	}
	if cfg.DevelopmentAdminProvider != "" && seen[cfg.DevelopmentAdminProvider] != "sqlite" {
		return nil, bad
	}
	ok := false
	defer func() {
		if !ok {
			_ = out.close()
		}
	}()
	members := make([]secretproviders.Member, 0, len(cfg.Providers))
	for _, e := range cfg.Providers {
		var provider jobdb.SecretProvider
		if e.Type == "plugin" {
			p, err := secretplugin.Start(ctx, e.processConfig())
			if err != nil {
				return nil, err
			}
			provider = p
			out.closers = append(out.closers, p.Close)
		} else {
			p, err := sqlitesecrets.Open(ctx, e.Path, jobdb.SecretLimits{})
			if err != nil {
				return nil, err
			}
			provider = p
			out.closers = append(out.closers, p.Close)
			if e.ID == cfg.DevelopmentAdminProvider {
				out.admin = p
			}
		}
		members = append(members, secretproviders.Member{ID: e.ID, Provider: provider})
	}
	options := secretproviders.Options{MaxParallel: cfg.MaxParallel}
	if cfg.LogSelections {
		options.Observe = func(s secretproviders.Selection) {
			slog.Info("secret provider selected", "tenant", s.TenantID, "job", s.JobID, "name", s.Name, "provider", s.ProviderID)
		}
	}
	out.provider, err = secretproviders.New(members, options)
	if err != nil {
		return nil, err
	}
	ok = true
	return out, nil
}

// Embed concrete runtimes so all transport capabilities (including lease
// renewal, schemas, and configuration administration) remain available.
type sqliteAdminRuntime struct {
	*sqliteruntime.Runtime
	admin *sqlitesecrets.Store
}
type toyAdminRuntime struct {
	*toy.Runtime
	admin *sqlitesecrets.Store
}

func setLocalSecret(ctx context.Context, store *sqlitesecrets.Store, limits jobdb.SecretLimits, tenant, name, value string) error {
	raw, _ := json.Marshal(struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}{name, value})
	if int64(len(raw)) > limits.WithDefaults().MaxResponseBytes {
		return jobdb.ErrSecretRequestTooLarge
	}
	return secretsutil.SafeError(store.Set(ctx, tenant, name, value))
}
func (r sqliteAdminRuntime) SetDevSecret(ctx context.Context, tenant, name, value string) error {
	return setLocalSecret(ctx, r.admin, r.SecretResolutionLimits(), tenant, name, value)
}
func (r sqliteAdminRuntime) RemoveDevSecret(ctx context.Context, tenant, name string) error {
	return secretsutil.SafeError(r.admin.Remove(ctx, tenant, name))
}
func (r toyAdminRuntime) SetDevSecret(ctx context.Context, tenant, name, value string) error {
	return setLocalSecret(ctx, r.admin, r.SecretResolutionLimits(), tenant, name, value)
}
func (r toyAdminRuntime) RemoveDevSecret(ctx context.Context, tenant, name string) error {
	return secretsutil.SafeError(r.admin.Remove(ctx, tenant, name))
}
func (c *configuredSecrets) wrap(runtime jobdb.WorkflowRuntime) jobdb.WorkflowRuntime {
	if c.admin == nil {
		return runtime
	}
	switch r := runtime.(type) {
	case *sqliteruntime.Runtime:
		return sqliteAdminRuntime{r, c.admin}
	case *toy.Runtime:
		return toyAdminRuntime{r, c.admin}
	default:
		panic("unsupported CLI runtime")
	}
}
