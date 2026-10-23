package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestCredentialItselfCannotOutlivePresentingCapability(t *testing.T) {
	p := &tokenProvider{tenant: "tenant", audience: "service", key: []byte(strings.Repeat("x", 32))}
	now := time.Now()
	authority := now.Add(30 * time.Second)
	req := jobdb.SecretResolveRequest{TenantID: "tenant", JobID: "job", Names: []string{"dev_token", "unknown"}, Lease: jobdb.SecretLeaseContext{LeaseID: "attempt", WorkerID: "worker", LeaseExpiresAt: now.Add(time.Hour), AuthorizedNotAfter: authority}}
	r, err := p.ResolveSecrets(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, r.Secrets, 1)
	v := r.Secrets["dev_token"]
	require.True(t, v.LeaseBound)
	require.True(t, v.ExpiresAt.Before(authority))
	parsed, err := jwt.Parse(v.Value, func(*jwt.Token) (any, error) { return p.key, nil }, jwt.WithValidMethods([]string{"HS256"}), jwt.WithAudience("service"), jwt.WithIssuer("jobdb-development"), jwt.WithExpirationRequired())
	require.NoError(t, err)
	expires, err := parsed.Claims.GetExpirationTime()
	require.NoError(t, err)
	require.Equal(t, v.ExpiresAt.Unix(), expires.Unix())
	_, err = jwt.Parse(v.Value, func(*jwt.Token) (any, error) { return p.key, nil }, jwt.WithValidMethods([]string{"HS256"}), jwt.WithTimeFunc(func() time.Time { return authority }))
	require.Error(t, err)
	claims := parsed.Claims.(jwt.MapClaims)
	require.Equal(t, "attempt", claims["lease_id"])
	require.Equal(t, "worker", claims["worker_id"])
	req.Lease.AuthorizedNotAfter = time.Time{}
	_, err = p.ResolveSecrets(context.Background(), req)
	require.Error(t, err)
	req.TenantID = "other"
	r, err = p.ResolveSecrets(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, r.Secrets)
}
