// A development provider issuing a JWT bounded by the requesting lease.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	secretplugin "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin"
	"github.com/golang-jwt/jwt/v5"
)

type config struct {
	Tenant         string `json:"tenant"`
	Audience       string `json:"audience"`
	SigningKeyFile string `json:"signing_key_file"`
}
type tokenProvider struct {
	tenant, audience string
	key              []byte
}

func (p *tokenProvider) ResolveSecrets(ctx context.Context, r jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	out := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{}}
	// This example grants one configured tenant access to dev_token. Production
	// providers should implement their own job/principal/resource policy here.
	if r.TenantID != p.tenant {
		return out, nil
	}
	for _, name := range r.Names {
		if name != "dev_token" {
			continue
		}
		if ctx.Err() != nil {
			return jobdb.SecretResolveResult{}, ctx.Err()
		}
		if r.Lease.LeaseID == "" || r.Lease.WorkerID == "" || r.Lease.AuthorizedNotAfter.IsZero() || r.Lease.LeaseExpiresAt.IsZero() {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
		now := time.Now()
		expiry := minTime(r.Lease.AuthorizedNotAfter, r.Lease.LeaseExpiresAt)
		expiry = minTime(expiry, now.Add(5*time.Minute)).Add(-time.Second).Truncate(time.Second)
		if !expiry.After(now) {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
		}
		claims := jwt.MapClaims{"iss": "jobdb-development", "aud": p.audience, "sub": r.JobID, "tenant": r.TenantID,
			"lease_id": r.Lease.LeaseID, "worker_id": r.Lease.WorkerID, "iat": now.Unix(), "exp": expiry.Unix()}
		if r.Principal != nil {
			claims["principal"] = *r.Principal
		}
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(p.key)
		if err != nil {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
		out.Secrets[name] = jobdb.SecretValue{Value: token, ExpiresAt: &expiry, LeaseBound: true}
	}
	return out, nil
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func main() {
	path := flag.String("config", "", "provider JSON configuration file")
	flag.Parse()
	p, err := load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "secret provider configuration failed")
		os.Exit(1)
	}
	secretplugin.Serve(p)
}
func load(path string) (*tokenProvider, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var c config
	d := json.NewDecoder(io.LimitReader(f, 64<<10))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return nil, err
	}
	if d.Decode(new(any)) != io.EOF || c.Tenant == "" || c.Audience == "" || c.SigningKeyFile == "" {
		return nil, jobdb.ErrInvalidSecretRequest
	}
	keyPath := c.SigningKeyFile
	if !filepath.IsAbs(keyPath) {
		keyPath = filepath.Join(filepath.Dir(path), keyPath)
	}
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	if len(key) < 32 {
		return nil, jobdb.ErrInvalidSecretRequest
	}
	return &tokenProvider{tenant: c.Tenant, audience: c.Audience, key: key}, nil
}
